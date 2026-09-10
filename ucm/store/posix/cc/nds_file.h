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

/**
 * @brief NDS (direct HBM to storage) transfer alignment.
 *
 * NDS moves data straight between device memory and an O_DIRECT file
 * descriptor, so file offsets and I/O lengths carry the block-device
 * alignment requirement of direct I/O.
 */
static constexpr size_t kNdsIoAlignment = 4096;

/** @brief Whether a size or offset satisfies the NDS alignment requirement. */
inline constexpr bool IsNdsAligned(size_t value) noexcept
{
    return (value & (kNdsIoAlignment - 1)) == 0;
}

/** @brief Whether a device address satisfies the NDS alignment requirement. */
inline bool IsNdsAligned(const void* addr) noexcept
{
    return IsNdsAligned(reinterpret_cast<uintptr_t>(addr));
}

/**
 * @brief Name of an NdsFileOpError, for log messages.
 *
 * NdsFileRead/NdsFileWrite only ever return -1 on failure, so the numeric
 * result says nothing about the cause. The driver distinguishes ~30 conditions
 * (bad pointer, memory not registered, context mismatch, ...) and knowing which
 * one fired is the difference between a one-line fix and a blind bisect.
 */
const char* NdsErrorName(NdsFileOpError err) noexcept;

/**
 * @brief Process-wide NDS driver, opened once and closed at exit.
 *
 * NdsFileDriverOpen()/NdsFileDriverClose() are process scoped: the test kit
 * calls them once around the whole run, not per file. Setup() is idempotent
 * and thread-safe so every store instance in the process shares one driver.
 */
class NdsDriver {
public:
    /** Open the driver on first call; later calls return the first result. */
    static Status Setup();
};

/**
 * @brief One NDS-registered data file.
 *
 * Lifecycle mirrors the test kit: open the fd with O_DIRECT, register a
 * handle against it, do the transfers, then deregister before closing the fd.
 */
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

    /**
     * @brief Open the file with O_DIRECT and register an NDS handle for it.
     *
     * @param flags Open flags; DIRECT is added unconditionally.
     * @param reserveSize When non-zero, grow the file to this many bytes
     *        before registering. NDS writes land at explicit offsets, so a
     *        freshly created file must be sized up front.
     */
    Status Open(uint32_t flags, size_t reserveSize = 0);
    void Close();
    Status Remove();

    /**
     * @brief Read storage into device memory (S2H).
     *
     * @param devPtr Device (HBM) destination address.
     * @param size Bytes to move; must be a multiple of kNdsIoAlignment.
     * @param fileOffset Byte offset in the file; same alignment requirement.
     * @param ptrOffset Byte offset applied inside devPtr by the driver.
     */
    Status Read(void* devPtr, size_t size, off64_t fileOffset, off64_t ptrOffset = 0);

    /** @brief Write device memory into storage (H2S). See Read() for params. */
    Status Write(const void* devPtr, size_t size, off64_t fileOffset, off64_t ptrOffset = 0);

private:
    Status Reserve(size_t size);
};

}  // namespace UC::PosixStore

#endif  // UCM_ENABLE_NDS

#endif
