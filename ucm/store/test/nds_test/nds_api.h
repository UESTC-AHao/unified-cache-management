#ifndef _NDS_API_H_
#define _NDS_API_H_

#include "nds_errno.h"

#ifdef __cplusplus
#if __cplusplus
extern "C" {
#endif /*  __cpluscplus */
#endif /*  __cpluscplus */

typedef struct NdsFileError_s {
    enum NdsFileOpError err;  // ndsfile error
} NdsFileError_t;

typedef struct NdsFileDescr_s {
    int fd; /* Linux   */
} NdsFileDescr_t;

typedef void *NdsFileHandle_t;

/**
 * @ingroup NDS_API
 * @brief  NDS资源初始化
 *
 * @details
 *
 * @attention
 * @li 无
 *
 * @param[in]   void
 *
 * @retval NDS_OK,  成功
 * @retval 其他, 失败
 *
 * @par History
 * w00557841, 2024年7月27日
 */
NdsFileError_t NdsFileDriverOpen();

/**
 * @ingroup NDS_API
 * @brief  NDS资源释放
 *
 * @details
 *
 * @attention
 * @li 无
 *
 * @param[in]   void
 *
 * @retval NDS_OK,  成功
 * @retval 其他, 失败
 *
 * @par History
 * w00557841, 2024年7月27日
 */
NdsFileError_t NdsFileDriverClose();

/**
 * @ingroup NDS_API
 * @brief NDS文件描述符注册接口
 *
 * @details
 *
 * @attention
 * @li 无
 *
 * @param [out]  fh               类型 NdsFileHandle_t*. NDS句柄
 * @param [in]   descr            类型 NdsFileDescr_t*. 需要注册的文件描述符信息
 *
 * @retval NDS_OK,  成功
 * @retval 其他, 失败
 *
 * @par History
 * w00557841, 2024年7月27日
 */
NdsFileError_t NdsFileHandleRegister(NdsFileHandle_t *fh, NdsFileDescr_t *descr);

/**
 * @ingroup NDS_API
 * @brief NDS文件描述符解映射接口
 *
 * @details
 *
 * @attention
 * @li 无
 *
 * @param [in]   fh               类型 NdsFileHandle_t. NDS句柄
 *
 * @retval 无
 *
 * @par History
 * w00557841, 2024年7月27日
 */
void NdsFileHandleDeregister(NdsFileHandle_t fh);

/**
 * @ingroup NDS_API
 * @brief NDS从存储直通读
 *
 * @details
 *
 * @attention
 * @li 基地址、长度和偏移都需要按page size（4K）对齐
 *
 * @param [in]   fh               类型 NdsFileHandle_t. NDS句柄
 * @param [in]   ptr_base         类型 void*. 目的虚拟内存首地址
 * @param [in]   size             类型 size_t. 文件读取的数据长度，单位为字节
 * @param [in]   file_offset      类型 off_t. 读文件的偏移量，单位为字节
 * @param [in]   ptr_offset       类型 off_t. 相对于ptr_base的要写入的地址偏移，单位为字节
 *
 * @retval ssize_t, 成功读的字节数
 * @retval -1, 失败
 *
 * @par History
 * w00557841, 2024年7月27日
 */
ssize_t NdsFileRead(NdsFileHandle_t fh, void *ptr_base, size_t size, off_t file_offset, off_t ptr_offset);

/**
 * @ingroup NDS_API
 * @brief NDS往存储直通写
 *
 * @details
 *
 * @attention
 * @li 基地址、长度和偏移都需要按page size（4K）对齐
 *
 * @param [in]   fh               类型 NdsFileHandle_t. NDS句柄
 * @param [in]   ptr_base         类型 void*. 目的虚拟内存首地址
 * @param [in]   size             类型 size_t. 文件读取的数据长度，单位为字节
 * @param [in]   file_offset      类型 off_t. 读文件的偏移量，单位为字节
 * @param [in]   ptr_offset       类型 off_t. 相对于ptr_base的要写入的地址偏移，单位为字节
 *
 * @retval ssize_t, 成功写的字节数
 * @retval -1, 失败
 *
 * @par History
 * w00557841, 2024年7月27日
 */
ssize_t NdsFileWrite(NdsFileHandle_t fh, void *ptr_base, size_t size, off_t file_offset, off_t ptr_offset);


#ifdef __cplusplus
#if __cplusplus
}
#endif /*  __cpluscplus */
#endif /*  __cpluscplus */

#endif