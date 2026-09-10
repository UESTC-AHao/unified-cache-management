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
#ifndef UNIFIEDCACHE_POSIX_STORE_CC_NDS_HANDLE_POOL_H
#define UNIFIEDCACHE_POSIX_STORE_CC_NDS_HANDLE_POOL_H

#include "nds_file.h"

#if UCM_ENABLE_NDS

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "status/status.h"

namespace UC::PosixStore {

/**
 * @brief Keeps NDS-registered files open so transfers stop paying for teardown.
 *
 * NdsFileHandleDeregister measures ~2034ms per call on the validation host,
 * against ~2.7ms for an 8MB transfer -- it is over 99% of a shard's wall clock.
 * Registration itself is cheap (~0.1ms first time, ~0.005ms once the driver has
 * seen the file), so the fix is to stop tearing handles down between transfers.
 *
 * This pays off in proportion to how often one file is reopened. Layerwise dump
 * reopens each block's file once per layer, so a 64-layer model amortises one
 * teardown over 64 transfers; load reopens whenever a block is read again. A
 * block-wise dump writes each file exactly once and gains nothing -- the cost
 * only moves from the transfer path to the retirement path.
 *
 * Handles held with no user are kept, not released: that is the whole point.
 * Capacity is the only thing that deregisters one, so the cost lands on a pool
 * that has filled up rather than on any particular transfer. Retirement runs on
 * a background thread because it blocks for seconds.
 */
class NdsHandlePool {
public:
    static constexpr size_t kDefaultCapacity = 1024;

    /**
     * @brief Borrowed access to a pooled file, released on destruction.
     *
     * Transfer paths bail out at several points, so ownership is scoped rather
     * than released by hand.
     */
    class Lease {
        NdsHandlePool* pool_{nullptr};
        std::shared_ptr<struct Entry> entry_{nullptr};

    public:
        Lease() = default;
        Lease(NdsHandlePool* pool, std::shared_ptr<struct Entry> entry)
            : pool_{pool}, entry_{std::move(entry)}
        {
        }
        ~Lease();
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&& other) noexcept
            : pool_{other.pool_}, entry_{std::move(other.entry_)}
        {
            other.pool_ = nullptr;
        }
        Lease& operator=(Lease&& other) noexcept;

        bool Valid() const noexcept { return entry_ != nullptr; }
        NdsFile& File() const noexcept;
    };

    /** @brief The process-wide pool; one driver, one set of registrations. */
    static NdsHandlePool& Instance();

    /**
     * @brief Raise the capacity to at least @p capacity entries.
     *
     * Raised, never lowered, so several store instances in one process cannot
     * shrink each other's pool.
     */
    void Reserve(size_t capacity);

    /**
     * @brief Borrow the file at @p path, opening and registering it if needed.
     *
     * @param flags Open flags, as NdsFile::Open takes them.
     * @param reserveSize Size to grow a freshly created file to; only consulted
     *        when this call is what opens the file.
     */
    Expected<Lease> Acquire(const std::string& path, uint32_t flags, size_t reserveSize);

    /**
     * @brief Drop @p path from the pool because the file it names is going away.
     *
     * Must be called before the file is renamed or unlinked, otherwise a later
     * Acquire of the same path hands out a handle onto the old inode. An entry
     * still in use is detached and retires once its last user leaves.
     *
     * Renames are why this exists: dump writes a `.tmp` file and commits it
     * under its final name, so the key stops matching the inode.
     *
     * TODO: the GC deletes committed files (shard_gc.cc RemoveFile) and does not
     * call this yet. A stale read-side handle returns a short read, which the
     * connector treats as a miss, so no wrong data is served -- but the open fd
     * keeps the inode alive, so the space the GC believes it reclaimed is not
     * actually freed.
     */
    void Invalidate(const std::string& path);

    /** @brief Entries currently tracked, for tests and diagnostics. */
    size_t Size() const;

private:
    NdsHandlePool();
    ~NdsHandlePool();
    NdsHandlePool(const NdsHandlePool&) = delete;
    NdsHandlePool& operator=(const NdsHandlePool&) = delete;

    void Release(const std::shared_ptr<struct Entry>& entry);
    void MakeRetirableLocked(std::shared_ptr<struct Entry> entry);
    void Retire(std::shared_ptr<struct Entry> entry);
    void EvictLocked();
    void RetireLoop();

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<struct Entry>> entries_;
    /** Idle entries, least recently released first; evicted from the front. */
    std::list<std::shared_ptr<struct Entry>> idle_;
    /**
     * Entries no longer reachable by path but still registered.
     *
     * Their fd stays open, so they count against capacity and are the first
     * thing evicted -- they can never be reused.
     */
    std::vector<std::shared_ptr<struct Entry>> retirable_;
    size_t capacity_{kDefaultCapacity};

    std::mutex retireMutex_;
    std::condition_variable retireCv_;
    std::vector<std::shared_ptr<struct Entry>> retireQueue_;
    std::thread retireThread_;
    std::atomic<bool> stop_{false};

    friend class Lease;
};

}  // namespace UC::PosixStore

#endif  // UCM_ENABLE_NDS

#endif
