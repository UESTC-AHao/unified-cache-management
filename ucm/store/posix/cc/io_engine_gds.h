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
#ifndef UNIFIEDCACHE_POSIX_STORE_CC_IO_ENGINE_GDS_H
#define UNIFIEDCACHE_POSIX_STORE_CC_IO_ENGINE_GDS_H

#ifdef UCM_ENABLE_GDS

#include <cufile.h>
#include <mutex>
#include <unordered_map>
#include "block_operator.h"
#include "logger/logger.h"
#include "template/task_wrapper.h"
#include "trans_task.h"

namespace UC::PosixStore {

class IoEngineGds : public Detail::TaskWrapper<TransTask, Detail::TaskHandle> {
    struct GdsFileCtx {
        CUfileHandle_t handle{nullptr};
        int32_t refCount{0};
    };

    size_t shardSize_;
    size_t nShardPerBlock_;
    const SpaceLayout* layout_;
    BlockOperator blockOperator_;
    std::unordered_map<int32_t, GdsFileCtx> fileCtxs_;
    std::mutex fileCtxMutex_;

    Status RegisterFile(int32_t fd)
    {
        std::lock_guard<std::mutex> lock{fileCtxMutex_};
        auto it = fileCtxs_.find(fd);
        if (it != fileCtxs_.end()) {
            it->second.refCount++;
            return Status::OK();
        }
        CUfileDescr_t desc{};
        desc.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
        desc.handle.fd = fd;
        CUfileHandle_t fh;
        CUfileError_t err = cuFileHandleRegister(&fh, &desc);
        if (err.err != CU_FILE_SUCCESS) {
            return Status::Error("cuFileHandleRegister failed: {}", cuFileGetErrorString(err));
        }
        fileCtxs_[fd] = {fh, 1};
        return Status::OK();
    }

    void DeregisterFile(int32_t fd)
    {
        std::lock_guard<std::mutex> lock{fileCtxMutex_};
        auto it = fileCtxs_.find(fd);
        if (it == fileCtxs_.end()) { return; }
        if (--it->second.refCount > 0) { return; }
        cuFileHandleDeregister(it->second.handle);
        fileCtxs_.erase(it);
    }

    CUfileHandle_t GetFileHandle(int32_t fd)
    {
        std::lock_guard<std::mutex> lock{fileCtxMutex_};
        auto it = fileCtxs_.find(fd);
        return it != fileCtxs_.end() ? it->second.handle : nullptr;
    }

    Status GdsBufRegister(void* devPtr, size_t size)
    {
        CUfileError_t err = cuFileBufRegister(devPtr, size, 0);
        if (err.err != CU_FILE_SUCCESS && err.err != CU_FILE_CUDA_MEMORY_NOT_REGISTERED) {
            return Status::Error("cuFileBufRegister failed: {}", cuFileGetErrorString(err));
        }
        return Status::OK();
    }

    void GdsBufDeregister(void* devPtr) { cuFileBufDeregister(devPtr); }

public:
    ~IoEngineGds()
    {
        std::lock_guard<std::mutex> lock{fileCtxMutex_};
        for (auto& [fd, ctx] : fileCtxs_) {
            if (ctx.handle) { cuFileHandleDeregister(ctx.handle); }
        }
        fileCtxs_.clear();
        cuFileDriverClose();
    }

    Status Setup(const Config& config, const SpaceLayout* layout)
    {
        CUfileError_t err = cuFileDriverOpen();
        if (err.err != CU_FILE_SUCCESS) {
            return Status::Error("cuFileDriverOpen failed: {}", cuFileGetErrorString(err));
        }
        shardSize_ = config.shardSize;
        nShardPerBlock_ = config.blockSize / config.shardSize;
        layout_ = layout;
        blockOperator_.Setup(layout, config.openConcurrency, config.commitConcurrency);
        return Status::OK();
    }

    Status ReadAsync(int32_t fd, uint64_t offset, void* devPtr, size_t size)
    {
        auto s = GdsBufRegister(devPtr, size);
        if (s.Failure()) { return s; }
        auto fh = GetFileHandle(fd);
        if (!fh) {
            GdsBufDeregister(devPtr);
            return Status::Error("file handle not registered for fd={}", fd);
        }
        ssize_t ret = cuFileRead(fh, devPtr, size, offset, 0);
        GdsBufDeregister(devPtr);
        if (ret < 0) {
            return Status::Error("cuFileRead failed: {}", cuFileGetErrorString(ret));
        }
        return Status::OK();
    }

    Status WriteAsync(int32_t fd, uint64_t offset, void* devPtr, size_t size)
    {
        auto s = GdsBufRegister(devPtr, size);
        if (s.Failure()) { return s; }
        auto fh = GetFileHandle(fd);
        if (!fh) {
            GdsBufDeregister(devPtr);
            return Status::Error("file handle not registered for fd={}", fd);
        }
        ssize_t ret = cuFileWrite(fh, devPtr, size, offset, 0);
        GdsBufDeregister(devPtr);
        if (ret < 0) {
            return Status::Error("cuFileWrite failed: {}", cuFileGetErrorString(ret));
        }
        return Status::OK();
    }

private:
    void CommitBlock(Detail::BlockId id, bool success)
    {
        blockOperator_.Submit(BlockOperator::CommitTask{std::move(id), success});
    }

    template <bool dump>
    void OnIoCallback(const Detail::TaskHandle& tid, WaiterPtr w, int32_t fd, bool last,
                      const Detail::BlockId& id, Status result)
    {
        if (result.Failure()) [[unlikely]] {
            UC_ERROR("GDS IO failed({}) on block({}).", result, id);
            failureSet_.Insert(tid);
        }
        DeregisterFile(fd);
        ::close(fd);
        if constexpr (dump) {
            if (last) { CommitBlock(id, !failureSet_.Contains(tid)); }
        }
        w->Done();
    }

    template <bool dump>
    void OnOpenCallback(const Detail::TaskHandle& tid, WaiterPtr w, const Detail::Shard& shard,
                        const BlockOperator::OpenResult& result)
    {
        const auto last = shard.index + 1 == nShardPerBlock_;
        const auto& id = shard.owner;
        auto handleFailure = [&](int32_t error, int32_t fd) {
            if (error != 0) { failureSet_.Insert(tid); }
            if (fd >= 0) {
                DeregisterFile(fd);
                ::close(fd);
            }
            if constexpr (dump) {
                if (last) { CommitBlock(id, false); }
            }
            w->Done();
        };
        if (result.error != 0) {
            UC_ERROR("Failed({}) to open block({}) for GDS.", result.error, shard.owner);
            handleFailure(result.error, result.fd);
            return;
        }
        auto s = RegisterFile(result.fd);
        if (s.Failure()) {
            handleFailure(-1, result.fd);
            return;
        }
        void* devPtr = shard.addrs.front();
        s = dump ? WriteAsync(result.fd, shard.index * shardSize_, devPtr, shardSize_)
                 : ReadAsync(result.fd, shard.index * shardSize_, devPtr, shardSize_);
        if (s.Failure()) {
            handleFailure(-1, result.fd);
            return;
        }
        OnIoCallback<dump>(tid, w, result.fd, last, id, Status::OK());
    }

    template <bool dump>
    void Dispatch(TaskPtr t, WaiterPtr w)
    {
        const auto flags = O_DIRECT | (dump ? (O_CREAT | O_WRONLY) : O_RDONLY);
        const auto number = t->desc.size();
        w->Set(number);
        std::list<BlockOperator::OpenTask> tasks;
        for (size_t i = 0; i < number; ++i) {
            BlockOperator::OpenTask task;
            const auto& shard = t->desc[i];
            task.id = shard.owner;
            task.activated = dump;
            task.flags = flags;
            task.callback = [this, tid = t->id, w,
                             shard = std::ref(t->desc[i])](BlockOperator::OpenResult result) {
                OnOpenCallback<dump>(tid, w, shard, result);
            };
            tasks.push_back(std::move(task));
        }
        blockOperator_.Submit(std::move(tasks));
    }

    void Dispatch(TaskPtr t, WaiterPtr w) override
    {
        const auto id = t->id;
        const auto& brief = t->desc.brief;
        const auto num = t->desc.size();
        const auto size = shardSize_ * num;
        const auto tp = w->startTp;
        UC_DEBUG("GDS task({},{},{},{}) dispatching.", id, brief, num, size);
        w->SetEpilog([id, brief = std::move(brief), num, size, tp] {
            auto cost = NowTime::Now() - tp;
            UC_DEBUG("GDS task({},{},{},{}) finished, cost {:.3f}ms.", id, brief, num, size,
                     cost * 1e3);
        });
        if (t->type == TransTask::Type::DUMP) {
            Dispatch<true>(t, w);
        } else {
            Dispatch<false>(t, w);
        }
    }
};

}  // namespace UC::PosixStore

#endif  // UCM_ENABLE_GDS

#endif  // UNIFIEDCACHE_POSIX_STORE_CC_IO_ENGINE_GDS_H
