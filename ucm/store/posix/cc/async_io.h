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
#ifndef UNIFIEDCACHE_POSIX_STORE_CC_ASYNC_IO_H
#define UNIFIEDCACHE_POSIX_STORE_CC_ASYNC_IO_H

#include "status/status.h"
#include <liburing.h>
#include <vector>

namespace UC::PosixStore {

class AsyncIOQueue {
private:
    struct io_uring ring_;
    static constexpr unsigned int QUEUE_DEPTH = 256;
    bool initialized_{false};

public:
    AsyncIOQueue() = default;
    ~AsyncIOQueue();

    // Initialize io_uring queue
    Status Init();

    // Submit async read operation
    Status SubmitRead(int fd, void* buf, size_t size, off64_t offset);

    // Submit async write operation
    Status SubmitWrite(int fd, const void* buf, size_t size, off64_t offset);

    // Wait for all submitted operations to complete
    Status WaitAll();

    // Get number of completed operations
    int GetCompletions();

    // Check if initialized
    bool IsInitialized() const { return initialized_; }
};

}  // namespace UC::PosixStore

#endif
