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
#include "trans_queue.h"
#include <cstring>
#include <hf3fs_usrbio.h>
#include "ds3fs_file.h"
#include "logger/logger.h"

namespace UC::Ds3fsStore {

Status TransQueue::Setup(const Config& config, TaskIdSet* failureSet, const SpaceLayout* layout)
{
    failureSet_ = failureSet;
    layout_ = layout;
    ioSize_ = config.tensorSize;
    shardSize_ = config.shardSize;
    nShardPerBlock_ = config.blockSize / config.shardSize;
    ioDirect_ = config.ioDirect;
    mountPoint_ = config.hf3fsMountPoint;
    iorEntries_ = config.iorEntries;
    iorDepth_ = config.iorDepth;

    auto success = pool_.SetNWorker(config.streamNumber)
                       .SetWorkerInitFn([this](auto& ctx) { return InitWorkerContext(ctx); })
                       .SetWorkerFn([this](auto& ios, auto& ctx) { Worker(ios, ctx); })
                       .SetWorkerExitFn([this](auto& ctx) { CleanupWorkerContext(ctx); })
                       .Run();
    if (!success) [[unlikely]] {
        return Status::Error(fmt::format("workers({}) start failed", config.streamNumber));
    }
    return Status::OK();
}

bool TransQueue::InitWorkerContext(WorkerContext*& ctx)
{
    try {
        ctx = new WorkerContext();

        auto s = ctx->iov.Create(mountPoint_, ioSize_);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed to create IOV for worker: {}", s);
            delete ctx;
            ctx = nullptr;
            return false;
        }

        s = ctx->iorRead.Create(mountPoint_, iorEntries_, true, iorDepth_);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed to create read IOR for worker: {}", s);
            delete ctx;
            ctx = nullptr;
            return false;
        }

        s = ctx->iorWrite.Create(mountPoint_, iorEntries_, false, iorDepth_);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Failed to create write IOR for worker: {}", s);
            delete ctx;
            ctx = nullptr;
            return false;
        }

        ctx->initialized = true;
        return true;

    } catch (const std::exception& e) {
        UC_ERROR("Exception during worker context init: {}", e.what());
        if (ctx) {
            delete ctx;
            ctx = nullptr;
        }
        return false;
    }
}

void TransQueue::CleanupWorkerContext(WorkerContext*& ctx)
{
    if (ctx) {
        delete ctx;
        ctx = nullptr;
    }
}

void TransQueue::Push(TaskPtr task, WaiterPtr waiter)
{
    waiter->Set(task->desc.size());
    std::list<IoUnit> ios;
    for (auto&& shard : task->desc) {
        ios.emplace_back<IoUnit>({task->id, task->type, std::move(shard), waiter});
    }
    ios.front().firstIo = true;
    pool_.Push(ios);
}

void TransQueue::Worker(IoUnit& ios, WorkerContext* ctx)
{
    if (ios.firstIo) {
        auto wait = NowTime::Now() - ios.waiter->startTp;
        UC_DEBUG("Ds3fs task({}) start running, wait {:.3f}ms.", ios.owner, wait * 1e3);
    }
    if (failureSet_->Contains(ios.owner)) {
        ios.waiter->Done();
        return;
    }
    auto s = Status::OK();
    if (ios.type == TransTask::Type::DUMP) {
        s = H2S(ios, ctx);
        if (ios.shard.index + 1 == nShardPerBlock_) {
            layout_->CommitFile(ios.shard.owner, s.Success());
        }
    } else {
        s = S2H(ios, ctx);
    }
    if (s.Failure()) [[unlikely]] { failureSet_->Insert(ios.owner); }
    ios.waiter->Done();
}

Status TransQueue::H2S(IoUnit& ios, WorkerContext* ctx)
{
    if (!ctx || !ctx->initialized) [[unlikely]] {
        return Status::Error("Worker context not initialized");
    }

    const auto& path = layout_->DataFilePath(ios.shard.owner, true);
    auto flags = Ds3fsFile::OpenFlag::CREATE | Ds3fsFile::OpenFlag::WRITE_ONLY;
    if (ioDirect_) { flags |= Ds3fsFile::OpenFlag::DIRECT; }

    Ds3fsFile file{path};
    auto s = file.Open(flags);
    if (s.Failure()) [[unlikely]] {
        UC_ERROR("Failed to open file({}): {}", path, s);
        return s;
    }

    int fd = file.ReleaseHandle();

    FdGuard fdGuard;
    s = fdGuard.Register(fd);
    if (s.Failure()) [[unlikely]] {
        UC_ERROR("Failed to register fd({}) for file({}): {}", fd, path, s);
        close(fd);
        return s;
    }

    auto offset = shardSize_ * ios.shard.index;
    const size_t numTensors = ios.shard.addrs.size();

    if (numTensors != 1) [[unlikely]] {
        UC_ERROR("Unexpected numTensors={}, expected 1 for H2S", numTensors);
    }

    std::memcpy(ctx->iov.Base(), reinterpret_cast<const void*>(ios.shard.addrs[0]), ioSize_);

    int prepRes = hf3fs_prep_io(ctx->iorWrite.Get(), ctx->iov.Get(), false, ctx->iov.Base(), fd,
                                offset, ioSize_, nullptr);
    if (prepRes < 0) [[unlikely]] {
        UC_ERROR("Failed to prep write io: result={}, path={}", prepRes, path);
        return Status::OsApiError(fmt::format("Failed to prep write io: {}", prepRes));
    }

    int submitRes = hf3fs_submit_ios(ctx->iorWrite.Get());
    if (submitRes < 0) [[unlikely]] {
        UC_ERROR("Failed to submit write io: result={}, path={}", submitRes, path);
        return Status::OsApiError(fmt::format("Failed to submit write ios: {}", submitRes));
    }

    struct hf3fs_cqe cqe;
    int waitRes = hf3fs_wait_for_ios(ctx->iorWrite.Get(), &cqe, 1, 1, nullptr);
    if (waitRes <= 0) [[unlikely]] {
        UC_ERROR("Failed to wait for write io: result={}, path={}", waitRes, path);
        return Status::OsApiError(fmt::format("Failed to wait for write ios: {}", waitRes));
    }

    if (cqe.result < 0) [[unlikely]] {
        UC_ERROR("Write operation failed: result={}, offset={}, size={}, path={}", cqe.result,
                 offset, ioSize_, path);
        return Status::OsApiError(fmt::format("Write operation failed: {}", cqe.result));
    }

    ctx->ioCount++;
    return Status::OK();
}

Status TransQueue::S2H(IoUnit& ios, WorkerContext* ctx)
{
    if (!ctx || !ctx->initialized) [[unlikely]] {
        return Status::Error("Worker context not initialized");
    }

    const auto& path = layout_->DataFilePath(ios.shard.owner, false);
    auto flags = Ds3fsFile::OpenFlag::READ_ONLY;
    if (ioDirect_) { flags |= Ds3fsFile::OpenFlag::DIRECT; }

    Ds3fsFile file{path};
    auto s = file.Open(flags);
    if (s.Failure()) [[unlikely]] {
        UC_ERROR("Failed to open file({}): {}", path, s);
        return s;
    }

    int fd = file.ReleaseHandle();

    FdGuard fdGuard;
    s = fdGuard.Register(fd);
    if (s.Failure()) [[unlikely]] {
        UC_ERROR("Failed to register fd({}) for file({}): {}", fd, path, s);
        close(fd);
        return s;
    }

    auto offset = shardSize_ * ios.shard.index;
    const size_t numTensors = ios.shard.addrs.size();

    if (numTensors != 1) [[unlikely]] {
        UC_ERROR("Unexpected numTensors={}, expected 1 for S2H", numTensors);
    }

    int prepRes = hf3fs_prep_io(ctx->iorRead.Get(), ctx->iov.Get(), true, ctx->iov.Base(), fd,
                                offset, ioSize_, nullptr);
    if (prepRes < 0) [[unlikely]] {
        UC_ERROR("Failed to prep read io: result={}, path={}", prepRes, path);
        return Status::OsApiError(fmt::format("Failed to prep read io: {}", prepRes));
    }

    int submitRes = hf3fs_submit_ios(ctx->iorRead.Get());
    if (submitRes < 0) [[unlikely]] {
        UC_ERROR("Failed to submit read io: result={}, path={}", submitRes, path);
        return Status::OsApiError(fmt::format("Failed to submit read ios: {}", submitRes));
    }

    struct hf3fs_cqe cqe;
    int waitRes = hf3fs_wait_for_ios(ctx->iorRead.Get(), &cqe, 1, 1, nullptr);
    if (waitRes <= 0) [[unlikely]] {
        UC_ERROR("Failed to wait for read io: result={}, path={}", waitRes, path);
        return Status::OsApiError(fmt::format("Failed to wait for read ios: {}", waitRes));
    }

    if (cqe.result < 0) [[unlikely]] {
        UC_ERROR("Read operation failed: result={}, offset={}, size={}, path={}", cqe.result,
                 offset, ioSize_, path);
        return Status::OsApiError(fmt::format("Read operation failed: {}", cqe.result));
    }

    std::memcpy(reinterpret_cast<void*>(ios.shard.addrs[0]), ctx->iov.Base(), ioSize_);

    ctx->ioCount++;
    return Status::OK();
}

}  // namespace UC::Ds3fsStore
