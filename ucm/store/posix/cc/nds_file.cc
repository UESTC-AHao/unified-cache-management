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
#include "nds_file.h"

#if UCM_ENABLE_NDS

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <sys/stat.h>
#include <unistd.h>
#include "logger/logger.h"
#include "time/now_time.h"

namespace UC::PosixStore {

static constexpr auto NewFilePerm = (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

const char* NdsErrorName(NdsFileOpError err) noexcept
{
    switch (err) {
        case NDS_FILE_SUCCESS: return "SUCCESS";
        case NDS_FILE_DRIVER_NOT_INITIALIZED: return "DRIVER_NOT_INITIALIZED";
        case NDS_FILE_DRIVER_ALREADY_OPEN: return "DRIVER_ALREADY_OPEN";
        case NDS_FILE_PLATFORM_NOT_SUPPORTED: return "PLATFORM_NOT_SUPPORTED";
        case NDS_FILE_DEVICE_NOT_SUPPORTED: return "DEVICE_NOT_SUPPORTED";
        case NDS_FILE_DEVICE_NOT_FOUND: return "DEVICE_NOT_FOUND";
        case NDS_FILE_CANN_POINTER_INVALID: return "CANN_POINTER_INVALID";
        case NDS_FILE_CANN_MEMORY_TYPE_INVALID: return "CANN_MEMORY_TYPE_INVALID";
        case NDS_FILE_CANN_POINTER_RANGE_ERROR: return "CANN_POINTER_RANGE_ERROR";
        case NDS_FILE_CANN_CONTEXT_MISMATCH: return "CANN_CONTEXT_MISMATCH";
        case NDS_FILE_MEMORY_NOT_REGISTERED: return "MEMORY_NOT_REGISTERED";
        case NDS_FILE_HANDLE_NOT_REGISTERED: return "HANDLE_NOT_REGISTERED";
        case NDS_FILE_HANDLE_ALREADY_REGISTERED: return "HANDLE_ALREADY_REGISTERED";
        case NDS_FILE_INVALID_FILE_TYPE: return "INVALID_FILE_TYPE";
        case NDS_FILE_INVALID_FILE_OPEN_FLAG: return "INVALID_FILE_OPEN_FLAG";
        case NDS_FILE_DIO_NOT_SET: return "DIO_NOT_SET";
        case NDS_FILE_INVALID_MAPPING_SIZE: return "INVALID_MAPPING_SIZE";
        case NDS_FILE_INVALID_MAPPING_RANGE: return "INVALID_MAPPING_RANGE";
        case NDS_FILE_PERMISSION_DENIED: return "PERMISSION_DENIED";
        case NDS_FILE_INTERNAL_ERROR: return "INTERNAL_ERROR";
        default: return "UNKNOWN";
    }
}

Status NdsDriver::Setup()
{
    static std::once_flag once;
    static Status result = Status::OK();
    std::call_once(once, [] {
        auto status = NdsFileDriverOpen();
        if (status.err != NDS_FILE_SUCCESS) {
            result = Status::OsApiError(fmt::format("NdsFileDriverOpen failed({}:{})",
                                                    NdsErrorName(status.err),
                                                    static_cast<int>(status.err)));
            UC_ERROR("Failed({}) to open NDS driver.", result);
            return;
        }
        UC_INFO("NDS driver opened.");
    });
    return result;
}

NdsFile::~NdsFile()
{
    if (handle_ != -1) { Close(); }
}

Status NdsFile::Reserve(size_t size)
{
    struct stat st{};
    if (fstat(handle_, &st) == 0 && static_cast<size_t>(st.st_size) >= size) {
        return Status::OK();
    }
    if (fallocate(handle_, 0, 0, static_cast<off_t>(size)) == 0) { return Status::OK(); }
    auto eno = errno;
    if ((eno == EOPNOTSUPP || eno == ENOSYS) && ftruncate(handle_, static_cast<off_t>(size)) == 0) {
        UC_WARN("fallocate unsupported(errno={}) on file({}), fell back to ftruncate.", eno, path_);
        return Status::OK();
    }
    return Status::OsApiError(fmt::format("reserve({}) failed({})", size, eno));
}

Status NdsFile::Open(uint32_t flags, size_t reserveSize)
{
    auto status = NdsDriver::Setup();
    if (status.Failure()) [[unlikely]] { return status; }

    auto tpOpen = NowTime::Now();
    handle_ = open(path_.c_str(), static_cast<int>(flags | OpenFlag::DIRECT), NewFilePerm);
    auto eno = errno;
    auto openMs = (NowTime::Now() - tpOpen) * 1e3;
    if (handle_ < 0) [[unlikely]] {
        handle_ = -1;
        if (eno == EEXIST) { return Status::DuplicateKey(); }
        if (eno == ENOENT) { return Status::NotFound(); }
        return Status::OsApiError(std::to_string(eno));
    }

    auto reserveMs = 0.0;
    if (reserveSize != 0) {
        auto tpReserve = NowTime::Now();
        status = Reserve(reserveSize);
        reserveMs = (NowTime::Now() - tpReserve) * 1e3;
        if (status.Failure()) [[unlikely]] {
            Close();
            return status;
        }
    }

    NdsFileDescr_t descr;
    memset(&descr, 0, sizeof(descr));
    descr.fd = handle_;
    auto tpRegister = NowTime::Now();
    auto ndsStatus = NdsFileHandleRegister(&ndsHandle_, &descr);
    auto registerMs = (NowTime::Now() - tpRegister) * 1e3;
    if (ndsStatus.err != NDS_FILE_SUCCESS) [[unlikely]] {
        Close();
        return Status::OsApiError(fmt::format("NdsFileHandleRegister failed({}:{})",
                                              NdsErrorName(ndsStatus.err),
                                              static_cast<int>(ndsStatus.err)));
    }
    registered_ = true;
    UC_DEBUG("Nds open({}) open={:.3f}ms reserve={:.3f}ms register={:.3f}ms.", path_, openMs,
             reserveMs, registerMs);
    return Status::OK();
}

void NdsFile::Close()
{
    if (registered_) {
        auto tp = NowTime::Now();
        NdsFileHandleDeregister(ndsHandle_);
        auto deregisterMs = (NowTime::Now() - tp) * 1e3;
        registered_ = false;
        UC_DEBUG("Nds deregister({}) took {:.3f}ms.", path_, deregisterMs);
    }
    if (handle_ != -1) {
        auto tp = NowTime::Now();
        close(handle_);
        auto closeMs = (NowTime::Now() - tp) * 1e3;
        handle_ = -1;
        UC_DEBUG("Nds close({}) took {:.3f}ms.", path_, closeMs);
    }
}

Status NdsFile::Remove()
{
    auto ret = remove(path_.c_str());
    auto eno = errno;
    if (ret == 0 || eno == ENOENT) { return Status::OK(); }
    return Status::OsApiError(std::to_string(eno));
}

Status NdsFile::Read(void* devPtr, size_t size, off64_t fileOffset, off64_t ptrOffset)
{
    errno = 0;
    auto nBytes = NdsFileRead(ndsHandle_, devPtr, size, fileOffset, ptrOffset);
    if (nBytes < 0) [[unlikely]] {
        auto eno = errno;
        return Status::OsApiError(
            fmt::format("NdsFileRead({}@{}) failed, errno={}", size, fileOffset, eno));
    }
    if (static_cast<size_t>(nBytes) != size) [[unlikely]] {
        UC_WARN("Nds short read({}/{}) at offset({}) of file({}).", nBytes, size, fileOffset,
                path_);
        return Status::NotFound();
    }
    return Status::OK();
}

Status NdsFile::Write(const void* devPtr, size_t size, off64_t fileOffset, off64_t ptrOffset)
{
    errno = 0;
    auto nBytes =
        NdsFileWrite(ndsHandle_, const_cast<void*>(devPtr), size, fileOffset, ptrOffset);
    if (nBytes < 0) [[unlikely]] {
        auto eno = errno;
        return Status::OsApiError(
            fmt::format("NdsFileWrite({}@{}) failed, errno={}", size, fileOffset, eno));
    }
    if (static_cast<size_t>(nBytes) != size) [[unlikely]] {
        return Status::OsApiError(
            fmt::format("NdsFileWrite short write({}/{}) at offset({})", nBytes, size, fileOffset));
    }
    return Status::OK();
}

}  // namespace UC::PosixStore

#endif  // UCM_ENABLE_NDS
