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
#include "hotness_tracker.h"
#include <utime.h>
#include "logger/logger.h"
#include "shard_gc.h"

namespace UC::PosixStore {

HotnessTracker::~HotnessTracker()
{
    {
        std::lock_guard<std::mutex> lock(gcMtx_);
        stop_.store(true);
        gcCv_.notify_all();
    }
    if (gcWorker_.joinable()) { gcWorker_.join(); }
}

Status HotnessTracker::Setup(const SpaceLayout* layout, size_t gcCheckIntervalSeconds,
                             size_t utimeConcurrency)
{
    if (!layout) { return Status::InvalidParam("layout is null"); }
    layout_ = layout;
    gcCheckIntervalSeconds_ = gcCheckIntervalSeconds;
    stop_.store(false);

    auto success =
        utimePool_
            .SetWorkerFn([](UtimeTask& task, auto&) { utime(task.filePath.c_str(), nullptr); })
            .SetNWorker(utimeConcurrency)
            .Run();
    if (!success) { return Status::Error("failed to start utime thread pool"); }

    gcWorker_ = std::thread(&HotnessTracker::GCCheckLoop, this);
    return Status::OK();
}

void HotnessTracker::SetGCTrigger(ShardGarbageCollector* gc, size_t maxFileCount,
                                  double thresholdRatio)
{
    gc_ = gc;
    maxFileCount_ = maxFileCount;
    thresholdRatio_ = thresholdRatio;
}

void HotnessTracker::Touch(const Detail::BlockId& blockId)
{
    auto filePath = layout_->DataFilePath(blockId, false);
    utimePool_.Push({std::move(filePath)});
}

void HotnessTracker::GCCheckLoop()
{
    while (!stop_.load()) {
        {
            std::unique_lock<std::mutex> lock(gcMtx_);
            gcCv_.wait_for(lock, std::chrono::seconds(gcCheckIntervalSeconds_),
                           [this] { return stop_.load(); });
        }
        if (stop_.load()) { break; }
        if (gc_ && gc_->ShouldTrigger(maxFileCount_, thresholdRatio_)) { gc_->Trigger(); }
    }
}

}  // namespace UC::PosixStore
