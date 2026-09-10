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
#include "nds_handle_pool.h"

#if UCM_ENABLE_NDS

#include <algorithm>
#include <utility>
#include "logger/logger.h"
#include "time/now_time.h"

namespace UC::PosixStore {

/**
 * @brief One pooled file plus the bookkeeping the pool needs for it.
 *
 * Entries are held by shared_ptr so a Lease keeps its file alive even after
 * Invalidate has detached the entry from the map.
 */
struct Entry {
    explicit Entry(std::string path) : file{std::move(path)} {}

    NdsFile file;
    size_t refCount{0};
    /** Detached from the map: retire once the last user leaves, never reuse. */
    bool doomed{false};
    /** Position in NdsHandlePool::idle_, valid only while refCount is zero. */
    std::list<std::shared_ptr<Entry>>::iterator idlePos{};
    bool inIdle{false};
};

NdsHandlePool::Lease::~Lease()
{
    if (pool_ != nullptr && entry_ != nullptr) { pool_->Release(entry_); }
}

NdsHandlePool::Lease& NdsHandlePool::Lease::operator=(Lease&& other) noexcept
{
    if (this == &other) { return *this; }
    if (pool_ != nullptr && entry_ != nullptr) { pool_->Release(entry_); }
    pool_ = other.pool_;
    entry_ = std::move(other.entry_);
    other.pool_ = nullptr;
    return *this;
}

NdsFile& NdsHandlePool::Lease::File() const noexcept { return entry_->file; }

NdsHandlePool& NdsHandlePool::Instance()
{
    static NdsHandlePool instance;
    return instance;
}

NdsHandlePool::NdsHandlePool() { retireThread_ = std::thread(&NdsHandlePool::RetireLoop, this); }

NdsHandlePool::~NdsHandlePool()
{
    stop_.store(true);
    retireCv_.notify_all();
    if (retireThread_.joinable()) { retireThread_.join(); }
    // Handles still held here are deliberately not deregistered: each batch
    // costs a driver-wide quiesce of seconds, so draining the pool would stall
    // process exit. The driver is left open for the same reason (see
    // NdsDriver::Setup), and the kernel reclaims both once the process is gone.
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [path, entry] : entries_) { entry->file.Abandon(); }
    for (auto& entry : retirable_) { entry->file.Abandon(); }
    entries_.clear();
    idle_.clear();
    retirable_.clear();
}

void NdsHandlePool::Reserve(size_t capacity)
{
    if (capacity == 0) { return; }
    std::lock_guard<std::mutex> lock(mutex_);
    capacity_ = std::max(capacity_, capacity);
}

Expected<NdsHandlePool::Lease> NdsHandlePool::Acquire(const std::string& path, uint32_t flags,
                                                      size_t reserveSize)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto iter = entries_.find(path);
        if (iter != entries_.end()) {
            auto& entry = iter->second;
            if (entry->inIdle) {
                idle_.erase(entry->idlePos);
                entry->inIdle = false;
            }
            ++entry->refCount;
            return Lease{this, entry};
        }
    }

    // Opened outside the lock: open() and NdsFileHandleRegister both talk to the
    // driver, and holding the pool mutex across them would serialise every
    // worker behind one file's setup.
    auto entry = std::make_shared<Entry>(path);
    auto status = entry->file.Open(flags, reserveSize);
    if (status.Failure()) { return status; }

    std::lock_guard<std::mutex> lock(mutex_);
    // Another worker may have inserted the same path meanwhile. Keep theirs and
    // retire ours, so one path never has two registrations.
    auto iter = entries_.find(path);
    if (iter != entries_.end()) {
        auto& winner = iter->second;
        if (winner->inIdle) {
            idle_.erase(winner->idlePos);
            winner->inIdle = false;
        }
        ++winner->refCount;
        entry->doomed = true;
        MakeRetirableLocked(std::move(entry));
        return Lease{this, winner};
    }

    entry->refCount = 1;
    entries_.emplace(path, entry);
    EvictLocked();
    return Lease{this, entry};
}

void NdsHandlePool::Release(const std::shared_ptr<Entry>& entry)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (entry->refCount > 0) { --entry->refCount; }
    if (entry->refCount != 0) { return; }
    if (entry->doomed) {
        MakeRetirableLocked(entry);
        return;
    }
    // Idle, not closed: keeping the registration is what avoids the teardown
    // cost on the next transfer of this same file.
    idle_.push_back(entry);
    entry->idlePos = std::prev(idle_.end());
    entry->inIdle = true;
    EvictLocked();
}

void NdsHandlePool::Invalidate(const std::string& path)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = entries_.find(path);
    if (iter == entries_.end()) { return; }
    auto entry = iter->second;
    entries_.erase(iter);
    if (entry->inIdle) {
        idle_.erase(entry->idlePos);
        entry->inIdle = false;
    }
    entry->doomed = true;
    // Dropping the key is all this does. Without it a later dump of the same
    // block would reuse this handle, which by then names the committed file
    // rather than the fresh .tmp -- the write would land in the wrong inode.
    //
    // Deregistering is deliberately left to capacity eviction: it takes a
    // driver-wide quiesce of ~2s that also blocks concurrent registers, so
    // doing it per committed block would cap dump at one block per 2s.
    //
    // A handle still in use is left to its last Release, which lands it in the
    // same place. Either way the caller may rename or unlink the file now: the
    // fd already names the inode, so transfers in flight see the right data.
    if (entry->refCount == 0) { MakeRetirableLocked(std::move(entry)); }
}

size_t NdsHandlePool::Size() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.size();
}

void NdsHandlePool::MakeRetirableLocked(std::shared_ptr<Entry> entry)
{
    // Unreachable by path but still registered, so it holds an fd and counts
    // against capacity. Nothing is deregistered here: capacity is the only
    // thing that triggers that.
    retirable_.push_back(std::move(entry));
}

void NdsHandlePool::EvictLocked()
{
    // The only place a handle is deregistered. Retirable entries go first --
    // they can never be reused -- and idle ones only once those run out.
    while (entries_.size() + retirable_.size() > capacity_) {
        std::shared_ptr<Entry> victim;
        if (!retirable_.empty()) {
            victim = std::move(retirable_.back());
            retirable_.pop_back();
        } else if (!idle_.empty()) {
            victim = idle_.front();
            idle_.pop_front();
            victim->inIdle = false;
            victim->doomed = true;
            // Copy the key: erase() destroys the stored string, and passing a
            // reference that lives inside the entry being removed is asking for
            // trouble.
            const auto key = victim->file.Path();
            entries_.erase(key);
        } else {
            // Everything left is in use. Capacity below the transfer
            // concurrency just means no reuse, never a failure.
            UC_DEBUG("Nds handle pool over capacity({}/{}), all entries in use.", entries_.size(),
                     capacity_);
            return;
        }
        Retire(std::move(victim));
    }
}

void NdsHandlePool::Retire(std::shared_ptr<Entry> entry)
{
    {
        std::lock_guard<std::mutex> lock(retireMutex_);
        retireQueue_.push_back(std::move(entry));
    }
    retireCv_.notify_one();
}

void NdsHandlePool::RetireLoop()
{
    std::vector<std::shared_ptr<Entry>> batch;
    while (true) {
        auto stopping = false;
        {
            std::unique_lock<std::mutex> lock(retireMutex_);
            retireCv_.wait(lock, [this] { return stop_.load() || !retireQueue_.empty(); });
            stopping = stop_.load();
            if (stopping && retireQueue_.empty()) { return; }
            batch.swap(retireQueue_);
        }
        if (stopping) {
            // Shutting down: drop these the same way the pool drops the entries
            // it still holds. Deregistering a full queue at seconds apiece would
            // stall process exit for minutes, and the kernel reclaims the fds
            // and the driver state anyway once the process is gone.
            for (auto& entry : batch) { entry->file.Abandon(); }
            return;
        }
        // Deregister blocks for seconds and takes a driver-wide lock that also
        // stalls concurrent registers, which is why it runs here rather than on
        // a transfer worker.
        auto tp = NowTime::Now();
        for (auto& entry : batch) { entry->file.Close(); }
        UC_DEBUG("Nds retired {} handle(s) in {:.3f}ms.", batch.size(),
                 (NowTime::Now() - tp) * 1e3);
        batch.clear();
    }
}

}  // namespace UC::PosixStore

#endif  // UCM_ENABLE_NDS
