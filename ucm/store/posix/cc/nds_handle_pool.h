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

class NdsHandlePool {
public:
    static constexpr size_t kDefaultCapacity = 1024;

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

    static NdsHandlePool& Instance();

    void Reserve(size_t capacity);

    Expected<Lease> Acquire(const std::string& path, uint32_t flags, size_t reserveSize);

    void Invalidate(const std::string& path);

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
    std::list<std::shared_ptr<struct Entry>> idle_;
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
