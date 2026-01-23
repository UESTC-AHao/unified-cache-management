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
#include "async_io.h"
#include "logger/logger.h"
#include <fmt/format.h>
#include <cstring>

namespace UC::PosixStore {

AsyncIOQueue::~AsyncIOQueue()
{
    if (initialized_) {
        io_uring_queue_exit(&ring_);
        initialized_ = false;
    }
}

Status AsyncIOQueue::Init()
{
    if (initialized_) {
        return Status::OK();
    }

    int ret = io_uring_queue_init(QUEUE_DEPTH, &ring_, 0);
    if (ret < 0) [[unlikely]] {
        return Status::OsApiError(fmt::format("io_uring_queue_init failed: {}", -ret));
    }

    initialized_ = true;
    UC_DEBUG("AsyncIOQueue initialized with queue depth {}", QUEUE_DEPTH);
    return Status::OK();
}

Status AsyncIOQueue::SubmitRead(int fd, void* buf, size_t size, off64_t offset)
{
    if (!initialized_) [[unlikely]] {
        return Status::Error("AsyncIOQueue not initialized");
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) [[unlikely]] {
        return Status::Error("Failed to get SQE from io_uring");
    }

    io_uring_prep_pread(sqe, fd, buf, size, offset);
    return Status::OK();
}

Status AsyncIOQueue::SubmitWrite(int fd, const void* buf, size_t size, off64_t offset)
{
    if (!initialized_) [[unlikely]] {
        return Status::Error("AsyncIOQueue not initialized");
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) [[unlikely]] {
        return Status::Error("Failed to get SQE from io_uring");
    }

    io_uring_prep_pwrite(sqe, fd, const_cast<void*>(buf), size, offset);
    return Status::OK();
}

Status AsyncIOQueue::WaitAll()
{
    if (!initialized_) [[unlikely]] {
        return Status::Error("AsyncIOQueue not initialized");
    }

    // Submit all pending operations
    int ret = io_uring_submit(&ring_);
    if (ret < 0) [[unlikely]] {
        return Status::OsApiError(fmt::format("io_uring_submit failed: {}", -ret));
    }

    int submitted = ret;
    int completed = 0;

    // Wait for all completions
    while (completed < submitted) {
        struct io_uring_cqe* cqe;
        ret = io_uring_wait_cqe(&ring_, &cqe);
        if (ret < 0) [[unlikely]] {
            return Status::OsApiError(fmt::format("io_uring_wait_cqe failed: {}", -ret));
        }

        if (cqe->res < 0) [[unlikely]] {
            UC_ERROR("io_uring operation failed with result: {}", cqe->res);
            io_uring_cqe_seen(&ring_, cqe);
            return Status::OsApiError(fmt::format("io_uring operation failed: {}", -cqe->res));
        }

        io_uring_cqe_seen(&ring_, cqe);
        completed++;
    }

    return Status::OK();
}

int AsyncIOQueue::GetCompletions()
{
    if (!initialized_) {
        return 0;
    }

    struct io_uring_cqe* cqe;
    unsigned int head;
    int count = 0;

    io_uring_for_each_cqe(&ring_, head, cqe) {
        count++;
    }

    return count;
}

}  // namespace UC::PosixStore
