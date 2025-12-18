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
    ioSize_ = config.ioSize;
    shardSize_ = config.shardSize;
    nShardPerBlock_ = config.blockSize / config.shardSize;
    ioDirect_ = config.transferIoDirect;
    mountPoint_ = config.hf3fsMountPoint;
    iorEntries_ = config.iorEntries;
    iorDepth_ = config.iorDepth;

    auto success = pool_.SetNWorker(config.transferStreamNumber)
                       .SetWorkerFn([this](auto& ios, auto&) { Worker(ios); })
                       .Run();
    if (!success) [[unlikely]] {
        return Status::Error(fmt::format("workers({}) start failed", config.transferStreamNumber));
    }
    return Status::OK();
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

void TransQueue::Worker(IoUnit& ios)
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
        s = H2S(ios);
        if (ios.shard.index + 1 == nShardPerBlock_) {
            layout_->CommitFile(ios.shard.owner, s.Success());
        }
    } else {
        s = S2H(ios);
    }
    if (s.Failure()) [[unlikely]] { failureSet_->Insert(ios.owner); }
    ios.waiter->Done();
}

Status TransQueue::DoIo(IorGuard& ior, IovGuard& iov, bool isRead, int fd, size_t offset,
                        size_t size)
{
    int prepRes =
        hf3fs_prep_io(ior.Get(), iov.Get(), isRead, iov.Base(), fd, offset, size, nullptr);
    if (prepRes < 0) [[unlikely]] {
        return Status::OsApiError(
            fmt::format("Failed to prep {} io: {}", isRead ? "read" : "write", prepRes));
    }

    int submitRes = hf3fs_submit_ios(ior.Get());
    if (submitRes < 0) [[unlikely]] {
        return Status::OsApiError(
            fmt::format("Failed to submit {} ios: {}", isRead ? "read" : "write", submitRes));
    }

    struct hf3fs_cqe cqe;
    int waitRes = hf3fs_wait_for_ios(ior.Get(), &cqe, 1, 1, nullptr);
    if (waitRes <= 0) [[unlikely]] {
        return Status::OsApiError(
            fmt::format("Failed to wait for {} ios: {}", isRead ? "read" : "write", waitRes));
    }

    if (cqe.result < 0) [[unlikely]] {
        return Status::OsApiError(
            fmt::format("{} operation failed: {}", isRead ? "Read" : "Write", cqe.result));
    }

    return Status::OK();
}

Status TransQueue::H2S(IoUnit& ios)
{
    const auto& path = layout_->DataFilePath(ios.shard.owner, true);
    Ds3fsFile file{path};
    auto flags = Ds3fsFile::OpenFlag::CREATE | Ds3fsFile::OpenFlag::WRITE_ONLY;
    if (ioDirect_) { flags |= Ds3fsFile::OpenFlag::DIRECT; }

    auto s = file.Open(flags);
    if (s.Failure()) [[unlikely]] {
        UC_ERROR("Failed({}) to open file({}) with flags({}).", s, path, flags);
        return s;
    }

    auto fd = file.Handle();

    FdGuard fdGuard;
    if (auto s = fdGuard.Register(fd); s.Failure()) [[unlikely]] {
        UC_ERROR("{}", s);
        return s;
    }

    IovGuard iov;
    if (auto s = iov.Create(mountPoint_, ioSize_); s.Failure()) [[unlikely]] {
        UC_ERROR("{}", s);
        return s;
    }

    IorGuard ior;
    if (auto s = ior.Create(mountPoint_, iorEntries_, false, iorDepth_); s.Failure()) [[unlikely]] {
        UC_ERROR("{}", s);
        return s;
    }

    auto offset = shardSize_ * ios.shard.index;
    for (const auto& addr : ios.shard.addrs) {
        std::memcpy(iov.Base(), reinterpret_cast<const void*>(addr), ioSize_);

        if (auto s = DoIo(ior, iov, false, fd, offset, ioSize_); s.Failure()) [[unlikely]] {
            UC_ERROR("{}", s);
            return s;
        }

        offset += ioSize_;
    }

    return Status::OK();
}

Status TransQueue::S2H(IoUnit& ios)
{
    const auto& path = layout_->DataFilePath(ios.shard.owner, false);
    Ds3fsFile file{path};
    auto flags = Ds3fsFile::OpenFlag::READ_ONLY;
    if (ioDirect_) { flags |= Ds3fsFile::OpenFlag::DIRECT; }

    auto s = file.Open(flags);
    if (s.Failure()) [[unlikely]] {
        UC_ERROR("Failed({}) to open file({}) with flags({}).", s, path, flags);
        return s;
    }

    auto fd = file.Handle();

    FdGuard fdGuard;
    if (auto s = fdGuard.Register(fd); s.Failure()) [[unlikely]] {
        UC_ERROR("{}", s);
        return s;
    }

    IovGuard iov;
    if (auto s = iov.Create(mountPoint_, ioSize_); s.Failure()) [[unlikely]] {
        UC_ERROR("{}", s);
        return s;
    }

    IorGuard ior;
    if (auto s = ior.Create(mountPoint_, iorEntries_, true, iorDepth_); s.Failure()) [[unlikely]] {
        UC_ERROR("{}", s);
        return s;
    }

    auto offset = shardSize_ * ios.shard.index;
    for (const auto& addr : ios.shard.addrs) {
        if (auto s = DoIo(ior, iov, true, fd, offset, ioSize_); s.Failure()) [[unlikely]] {
            UC_ERROR("{}", s);
            return s;
        }

        std::memcpy(reinterpret_cast<void*>(addr), iov.Base(), ioSize_);
        offset += ioSize_;
    }

    return Status::OK();
}

}  // namespace UC::Ds3fsStore
