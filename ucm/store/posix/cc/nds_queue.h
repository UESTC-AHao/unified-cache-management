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
#ifndef UNIFIEDCACHE_POSIX_STORE_CC_NDS_QUEUE_H
#define UNIFIEDCACHE_POSIX_STORE_CC_NDS_QUEUE_H

#include "nds_file.h"

#if UCM_ENABLE_NDS

#include "global_config.h"
#include "space_layout.h"
#include "template/hashset.h"
#include "thread/latch.h"
#include "thread/thread_pool.h"
#include "trans_task.h"

namespace UC::PosixStore {

/**
 * @brief Transfer queue that moves shards straight between HBM and storage.
 *
 * Same shape as TransQueue, but the shard addresses are device addresses fed
 * to the NDS driver instead of host buffers fed to pread/pwrite, so no host
 * staging copy happens on either direction.
 */
class NdsQueue {
    using TaskIdSet = HashSet<Detail::TaskHandle>;
    using TaskPtr = std::shared_ptr<TransTask>;
    using WaiterPtr = std::shared_ptr<Latch>;

private:
    struct IoUnit {
        TaskPtr task;
        Detail::Shard shard;
        std::shared_ptr<Latch> waiter;
        bool firstIo{false};
    };
    TaskIdSet* failureSet_;
    const SpaceLayout* layout_;
    ThreadPool<IoUnit> loadPool_;
    ThreadPool<IoUnit> dumpPool_;
    std::vector<size_t> tensorSizes_;
    int32_t deviceId_{-1};
    size_t shardSize_;
    size_t blockSize_;
    size_t nShardPerBlock_;
    size_t timeoutMs_;

public:
    Status Setup(const Config& config, TaskIdSet* failureSet, const SpaceLayout* layout);
    void Push(TaskPtr task, WaiterPtr waiter);
    void Cancel(TaskPtr task);

private:
    static Status CheckConfig(const Config& config);
    bool SetupWorkerDevice() const;
    void LoadWorker(IoUnit& ios);
    void DumpWorker(IoUnit& ios);
    void OnIoUnitTimeout(IoUnit& ios);
    Status Transfer(NdsFile& file, const Detail::Shard& shard, bool dump);
    Status H2S(IoUnit& ios);
    Status S2H(IoUnit& ios);
};

}  // namespace UC::PosixStore

#endif  // UCM_ENABLE_NDS

#endif
