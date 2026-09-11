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
#ifndef UNIFIEDCACHE_POSIX_STORE_CC_NDS_FILE_H
#define UNIFIEDCACHE_POSIX_STORE_CC_NDS_FILE_H

#ifndef UCM_ENABLE_NDS
#define UCM_ENABLE_NDS 0
#endif

#if UCM_ENABLE_NDS

#include <fcntl.h>
#include <nds_api.h>
#include <string>
#include "status/status.h"

namespace UC::PosixStore {

static constexpr size_t kNdsIoAlignment = 4096;

inline constexpr bool IsNdsAligned(size_t value) noexcept
{
    return (value & (kNdsIoAlignment - 1)) == 0;
}

inline bool IsNdsAligned(const void* addr) noexcept
{
    return IsNdsAligned(reinterpret_cast<uintptr_t>(addr));
}

const char* NdsErrorName(NdsFileOpError err) noexcept;

class NdsDriver {
public:
    static Status Setup();
};

class NdsFile {
public:
    struct OpenFlag {
        static constexpr uint32_t READ_ONLY = O_RDONLY;
        static constexpr uint32_t WRITE_ONLY = O_WRONLY;
        static constexpr uint32_t READ_WRITE = O_RDWR;
        static constexpr uint32_t CREATE = O_CREAT;
        static constexpr uint32_t DIRECT = O_DIRECT;
    };

private:
    std::string path_{};
    int32_t handle_{-1};
    NdsFileHandle_t ndsHandle_{};
    bool registered_{false};

public:
    explicit NdsFile(std::string path) : path_{std::move(path)} {}
    ~NdsFile();
    NdsFile(const NdsFile&) = delete;
    NdsFile& operator=(const NdsFile&) = delete;
    const std::string& Path() const { return path_; }

    Status Open(uint32_t flags, size_t reserveSize = 0);
    void Close();
    Status Remove();

    void Abandon() noexcept
    {
        registered_ = false;
        handle_ = -1;
    }

    Status Read(void* devPtr, size_t size, off64_t fileOffset, off64_t ptrOffset = 0);

    Status Write(const void* devPtr, size_t size, off64_t fileOffset, off64_t ptrOffset = 0);

private:
    Status Reserve(size_t size);
};

}  // namespace UC::PosixStore

#endif  // UCM_ENABLE_NDS

#endif
