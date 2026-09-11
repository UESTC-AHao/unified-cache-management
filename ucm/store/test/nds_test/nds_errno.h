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
#ifndef _NDS_ERRNO_H_
#define _NDS_ERRNO_H_

#include <sys/types.h>

#ifdef __cplusplus
#if __cplusplus
extern "C" {
#endif /*  __cplusplus */
#endif /*  __cplusplus */
/*----------------------------------------------*
 * 宏定义                                       *
 *----------------------------------------------*/

enum NdsFileOpError {
    NDS_FILE_SUCCESS = 0,
    NDS_FILE_DRIVER_NOT_INITIALIZED = 5001,
    NDS_FILE_DRIVER_INVALID_PROPS,
    NDS_FILE_DRIVER_UNSUPPORTED_LIMIT,
    NDS_FILE_DRIVER_VERSION_MISMATCH,
    NDS_FILE_DRIVER_VERSION_READ_ERROR,
    NDS_FILE_DRIVER_CLOSING,
    NDS_FILE_PLATFORM_NOT_SUPPORTED,
    NDS_FILE_IO_NOT_SUPPORTED,
    NDS_FILE_DEVICE_NOT_SUPPORTED,
    NDS_FILE_CANN_DRIVER_ERROR = 5011,
    NDS_FILE_CANN_POINTER_INVALID,
    NDS_FILE_CANN_MEMORY_TYPE_INVALID,
    NDS_FILE_CANN_POINTER_RANGE_ERROR,
    NDS_FILE_CANN_CONTEXT_MISMATCH,
    NDS_FILE_INVALID_MAPPING_SIZE,
    NDS_FILE_INVALID_MAPPING_RANGE,
    NDS_FILE_INVALID_FILE_TYPE,
    NDS_FILE_INVALID_FILE_OPEN_FLAG,
    NDS_FILE_DIO_NOT_SET,
    NDS_FILE_INVALID_VALUE = 5022,
    NDS_FILE_MEMORY_ALREADY_REGISTERED,
    NDS_FILE_MEMORY_NOT_REGISTERED,
    NDS_FILE_PERMISSION_DENIED,
    NDS_FILE_DRIVER_ALREADY_OPEN,
    NDS_FILE_HANDLE_NOT_REGISTERED,
    NDS_FILE_HANDLE_ALREADY_REGISTERED,
    NDS_FILE_DEVICE_NOT_FOUND,
    NDS_FILE_INTERNAL_ERROR,
    NDS_FILE_NEWFD_FAILED,
    NDS_FILE_INIT_FAILED
};

/**
 * 通用错误码
 */
#define NDS_OK (0)
#define NDS_FAIL (1)

#ifdef __cplusplus
#if __cplusplus
}
#endif /*  __cplusplus */
#endif /*  __cplusplus */

#endif
