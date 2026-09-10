/**
 * MIT License
 *
 * Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * */
#include "nds_queue.h"

#if UCM_ENABLE_NDS

#include <algorithm>
#include <numeric>
#include <utility>
#include "logger/logger.h"
#include "metrics_api.h"
#include "nds_handle_pool.h"
#include "trans/device.h"

namespace UC::PosixStore {

Status NdsQueue::CheckConfig(const Config& config)
{
    if (config.deviceId < 0) {
        return Status::InvalidParam("nds io engine requires a device({})", config.deviceId);
    }
    // Every shard starts at shardSize * index in the file, so this is the file
    // offset rule of direct I/O.
    if (config.shardSize % kNdsIoAlignment != 0) {
        return Status::InvalidParam("nds shard size({}) is not {}-aligned", config.shardSize,
                                    kNdsIoAlignment);
    }
    // Individual tensor sizes are deliberately not checked. Transfer() coalesces
    // tensors that are contiguous in both device memory and the file, so the
    // alignment rule falls on the merged span rather than on each tensor -- and
    // per-tensor sizes follow the model (head dim, dtype), so they generally are
    // not 4K multiples. An unaligned merged span is still rejected at transfer
    // time, where the real address and length are known.
    const auto total =
        std::accumulate(config.tensorSizes.begin(), config.tensorSizes.end(), size_t{0});
    // Match the CacheStore rule (cache_store.cc CheckConfig): the tensors must
    // fit the shard, not fill it exactly. A shard may carry trailing padding, so
    // requiring equality here would reject layouts the cache path accepts.
    if (total > config.shardSize) {
        return Status::InvalidParam("nds tensor sizes({}) overflow shard({})", total,
                                    config.shardSize);
    }
    return Status::OK();
}

Status NdsQueue::Setup(const Config& config, TaskIdSet* failureSet, const SpaceLayout* layout)
{
    auto s = CheckConfig(config);
    if (s.Failure()) [[unlikely]] { return s; }
    s = NdsDriver::Setup();
    if (s.Failure()) [[unlikely]] { return s; }

    failureSet_ = failureSet;
    layout_ = layout;
    tensorSizes_ = config.tensorSizes;
    deviceId_ = config.deviceId;
    shardSize_ = config.shardSize;
    blockSize_ = config.blockSize;
    nShardPerBlock_ = config.blockSize / config.shardSize;
    timeoutMs_ = config.timeoutMs;
    // The pool has to hold at least as many entries as there are files being
    // transferred at once, otherwise entries are evicted while still in use and
    // no reuse ever happens. Concurrency is that bound: one worker touches one
    // file at a time.
    NdsHandlePool::Instance().Reserve(
        std::max(config.ndsHandlePoolSize, config.dataTransConcurrency));

    auto success =
        loadPool_.SetNWorker(config.dataTransConcurrency)
            .SetWorkerInitFn([this](auto&) { return SetupWorkerDevice(); })
            .SetWorkerFn([this](auto& ios, auto&) { LoadWorker(ios); })
            .SetWorkerTimeoutFn([this](IoUnit& ios, ssize_t tid) { OnIoUnitTimeout(ios); },
                                config.timeoutMs)
            .SetCpuAffinity(config.cpuAffinityCores)
            .Run();
    if (!success) [[unlikely]] {
        return Status::Error(fmt::format("nds load workers({}) start failed",
                                         config.dataTransConcurrency));
    }
    success = dumpPool_.SetNWorker(config.dataTransConcurrency)
                  .SetWorkerInitFn([this](auto&) { return SetupWorkerDevice(); })
                  .SetWorkerFn([this](auto& ios, auto&) { DumpWorker(ios); })
                  .SetWorkerTimeoutFn([this](IoUnit& ios, ssize_t tid) { OnIoUnitTimeout(ios); },
                                      config.timeoutMs)
                  .SetCpuAffinity(config.cpuAffinityCores)
                  .Run();
    if (!success) [[unlikely]] {
        return Status::Error(fmt::format("nds dump workers({}) start failed",
                                         config.dataTransConcurrency));
    }
    return Status::OK();
}

bool NdsQueue::SetupWorkerDevice() const
{
    // NDS transfers touch device memory, so every worker thread needs the
    // device context bound before it can register handles or move data.
    Trans::Device device;
    auto s = device.Setup(deviceId_);
    if (s.Failure()) [[unlikely]] {
        UC_ERROR("Failed({}) to setup device({}) for nds worker.", s, deviceId_);
        return false;
    }
    return true;
}

void NdsQueue::OnIoUnitTimeout(IoUnit& ios)
{
    UC::Metrics::UpdateStats(NAME_TO_METRIC_ID("posix_io_timeout_total"), 1.0);
    ios.task->Fail(Status::Timeout());
    if (!failureSet_->Contains(ios.task->id)) { failureSet_->Insert(ios.task->id); }
    ios.waiter->Done();
}

void NdsQueue::Push(TaskPtr task, WaiterPtr waiter)
{
    waiter->Set(task->desc.size());
    std::list<IoUnit> ios;
    for (auto&& shard : task->desc) { ios.emplace_back<IoUnit>({task, std::move(shard), waiter}); }
    // An empty desc leaves the waiter already satisfied by Set(0); dereferencing
    // front() here would be undefined.
    if (ios.empty()) [[unlikely]] { return; }
    ios.front().firstIo = true;
    if (task->type == TransTask::Type::DUMP) {
        dumpPool_.Push(ios);
    } else {
        loadPool_.Push(ios);
    }
}

void NdsQueue::Cancel(TaskPtr task)
{
    auto& pool = task->type == TransTask::Type::DUMP ? dumpPool_ : loadPool_;
    const auto tid = task->id;
    pool.TraverseWaitQueue(
        [this, tid](IoUnit& ios) {
            return ios.task->id == tid || ios.waiter->IsTimeout(timeoutMs_);
        },
        [this](IoUnit& ios) { OnIoUnitTimeout(ios); },
        [this, tid](IoUnit& ios) {
            return ios.task->id > tid && !ios.waiter->IsTimeout(timeoutMs_);
        });
}

void NdsQueue::LoadWorker(IoUnit& ios)
{
    if (ios.firstIo) {
        auto wait = NowTime::Now() - ios.waiter->startTp;
        UC_DEBUG("Nds load task({}) start running, wait {:.3f}ms.", ios.task->id, wait * 1e3);
        UC::Metrics::UpdateStats(NAME_TO_METRIC_ID("posix_load_queue_wait_duration_ms"),
                                 wait * 1e3);
    }
    if (failureSet_->Contains(ios.task->id)) {
        ios.waiter->Done();
        return;
    }
    auto s = S2H(ios);
    if (s.Failure()) [[unlikely]] {
        ios.task->Fail(s);
        failureSet_->Insert(ios.task->id);
    }
    ios.waiter->Done();
}

void NdsQueue::DumpWorker(IoUnit& ios)
{
    if (ios.firstIo) {
        auto wait = NowTime::Now() - ios.waiter->startTp;
        UC_DEBUG("Nds dump task({}) start running, wait {:.3f}ms.", ios.task->id, wait * 1e3);
        UC::Metrics::UpdateStats(NAME_TO_METRIC_ID("posix_dump_queue_wait_duration_ms"),
                                 wait * 1e3);
    }
    if (failureSet_->Contains(ios.task->id)) {
        ios.waiter->Done();
        return;
    }
    auto s = H2S(ios);
    if (ios.shard.index + 1 == nShardPerBlock_) {
        // Drop the pooled handle before committing: CommitFile renames the .tmp
        // away (or removes it on failure), so the key would otherwise keep
        // naming a handle onto an inode that no longer answers to that path.
        // H2S's lease is already released here -- this is the last shard.
        NdsHandlePool::Instance().Invalidate(layout_->DataFilePath(ios.shard.owner, true));
        layout_->CommitFile(ios.shard.owner, s.Success());
    }
    if (s.Failure()) [[unlikely]] {
        ios.task->Fail(s);
        failureSet_->Insert(ios.task->id);
    }
    ios.waiter->Done();
}

Status NdsQueue::Transfer(NdsFile& file, const Detail::Shard& shard, bool dump)
{
    if (shard.addrs.size() != tensorSizes_.size()) [[unlikely]] {
        return Status::InvalidParam("nds shard has {} addrs but {} tensor sizes",
                                    shard.addrs.size(), tensorSizes_.size());
    }
    auto offset = static_cast<off64_t>(shardSize_ * shard.index);
    // Tensors that are adjacent in device memory and adjacent in the file are
    // coalesced into one transfer. This matters twice over:
    //
    //   * Cost. A shard of a non-layerwise connector holds 2*num_layers tensors,
    //     so per-tensor transfers turn one shard into a hundred-odd small calls
    //     into the driver. The NDS bandwidth tests run at 1MB I/O sizes.
    //   * Alignment. NDS requires the address, length and offset of every
    //     transfer to be 4K aligned, and per-tensor sizes follow the model
    //     (head dim, dtype) so they generally are not. Coalescing moves the
    //     requirement onto the merged span, which is what makes an unaligned
    //     tensor_size_list transferable at all.
    //
    // A Delegator stage above this one gathers each shard into one contiguous
    // device buffer, so the whole shard collapses into a single transfer. When
    // this engine is driven directly the addresses are unrelated and the loop
    // degenerates to one transfer per tensor, as before.
    std::byte* runAddr = nullptr;
    size_t runSize = 0;
    off64_t runOffset = 0;

    const auto flushRun = [&]() -> Status {
        if (runSize == 0) { return Status::OK(); }
        // Only the merged span has to satisfy the driver's alignment rule. A
        // failure here is a property of the layout, not of one block, so name
        // the address instead of letting the driver return a bare pointer error.
        if (!IsNdsAligned(runAddr) || !IsNdsAligned(runSize)) [[unlikely]] {
            UC_ERROR("Nds transfer addr({}) size({}) is not {}-aligned.", static_cast<void*>(runAddr),
                     runSize, kNdsIoAlignment);
            UC::Metrics::UpdateStats(NAME_TO_METRIC_ID("posix_io_errors_total"), 1.0);
            return Status::InvalidParam("nds transfer is not {}-aligned", kNdsIoAlignment);
        }
        auto tp = NowTime::Now();
        auto s = dump ? file.Write(runAddr, runSize, runOffset)
                      : file.Read(runAddr, runSize, runOffset);
        auto transferMs = (NowTime::Now() - tp) * 1e3;
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed({}) to {} file({}:{}) via nds.", s, dump ? "write" : "read",
                     file.Path(), runOffset);
            UC::Metrics::UpdateStats(NAME_TO_METRIC_ID("posix_io_errors_total"), 1.0);
            return s;
        }
        // Paired with the open/register timings in nds_file.cc: together they
        // separate the driver call itself from the per-transfer setup around it.
        UC_DEBUG("Nds {} {}B@{} took {:.3f}ms ({:.3f}GB/s).", dump ? "write" : "read", runSize,
                 runOffset, transferMs, runSize / (transferMs * 1e-3) / 1e9);
        runAddr = nullptr;
        runSize = 0;
        return Status::OK();
    };

    for (size_t i = 0; i < shard.addrs.size(); ++i) {
        const auto size = tensorSizes_[i];
        auto* addr = static_cast<std::byte*>(shard.addrs[i]);
        // Layerwise padding leaves ghost slots with a null address or zero size.
        // They hold no data, but the file offset still advances so the on-disk
        // layout keeps matching tensorSizes_. Such a gap also breaks the run:
        // the next tensor is no longer contiguous in the file.
        if (size == 0 || addr == nullptr) {
            auto s = flushRun();
            if (s.Failure()) [[unlikely]] { return s; }
            offset += static_cast<off64_t>(size);
            continue;
        }
        if (runSize != 0 && runAddr + runSize == addr) {
            runSize += size;
        } else {
            auto s = flushRun();
            if (s.Failure()) [[unlikely]] { return s; }
            runAddr = addr;
            runSize = size;
            runOffset = offset;
        }
        offset += static_cast<off64_t>(size);
    }
    auto s = flushRun();
    if (s.Failure()) [[unlikely]] { return s; }
    return Status::OK();
}

Status NdsQueue::H2S(IoUnit& ios)
{
    const auto& path = layout_->DataFilePath(ios.shard.owner, true);
    // NDS writes never extend the file, so the block is sized on first open.
    auto lease = NdsHandlePool::Instance().Acquire(
        path, NdsFile::OpenFlag::CREATE | NdsFile::OpenFlag::WRITE_ONLY, blockSize_);
    if (!lease) [[unlikely]] {
        UC_ERROR("Failed({}) to open file({}) for nds write.", lease.Error(), path);
        UC::Metrics::UpdateStats(NAME_TO_METRIC_ID("posix_open_errors_total"), 1.0);
        return lease.Error();
    }
    auto held = std::move(lease).Value();
    return Transfer(held.File(), ios.shard, true);
}

Status NdsQueue::S2H(IoUnit& ios)
{
    const auto& path = layout_->DataFilePath(ios.shard.owner, false);
    auto lease = NdsHandlePool::Instance().Acquire(path, NdsFile::OpenFlag::READ_ONLY, 0);
    if (!lease) [[unlikely]] {
        UC_ERROR("Failed({}) to open file({}) for nds read.", lease.Error(), path);
        UC::Metrics::UpdateStats(NAME_TO_METRIC_ID("posix_open_errors_total"), 1.0);
        return lease.Error();
    }
    auto held = std::move(lease).Value();
    return Transfer(held.File(), ios.shard, false);
}

}  // namespace UC::PosixStore

#endif  // UCM_ENABLE_NDS
