# MIT License
#
# Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
import argparse
import multiprocessing
import secrets
import time

import numpy as np

from ucm.store.factory_v1 import UcmConnectorFactoryV1, UcmKVStoreBaseV1

NDS_ALIGNMENT = 4096

worker_number = 1
shard_size = 8 * 1024 * 1024
shard_number = 1
block_number = 64
dump_epoch_number = 32
load_epoch_number = 32
storage_backends = ["./build/data"]
device_offset = 0
posix_data_trans_concurrency = 32
bench = False
check_only = False
no_event_sync = False
reuse_block_ids = False
ACL_MEM_MALLOC_HUGE_FIRST = 0
ACL_MEM_MALLOC_HUGE_ONLY = 1
ACL_MEM_MALLOC_NORMAL_ONLY = 2
malloc_policy = -1


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run POSIX NDS store test")
    parser.add_argument("--worker-number", type=int, default=worker_number)
    parser.add_argument("--shard-size", type=int, default=shard_size)
    parser.add_argument("--shard-number", type=int, default=shard_number)
    parser.add_argument("--block-number", type=int, default=block_number)
    parser.add_argument("--dump-epoch-number", type=int, default=dump_epoch_number)
    parser.add_argument("--load-epoch-number", type=int, default=load_epoch_number)
    parser.add_argument(
        "--storage-backend",
        action="append",
        default=None,
        help="Storage backend path; may be repeated",
    )
    parser.add_argument(
        "--device-offset",
        type=int,
        default=device_offset,
        help="First NPU device id; worker i uses device_offset + i",
    )
    parser.add_argument(
        "--posix-data-trans-concurrency",
        type=int,
        default=posix_data_trans_concurrency,
        help="posix data transfer concurrency (nds worker count).",
    )
    parser.add_argument(
        "--malloc-policy",
        choices=["torch", "huge-only", "huge-first", "normal-only"],
        default="torch",
        help=(
            "Device memory allocator. 'torch' uses the torch caching allocator "
            "(what vLLM's KV cache goes through); the others call aclrtMalloc "
            "with that page policy, matching multi_test.cpp. Page size drives "
            "how many page-table entries the NDS DMA has to walk, so this is "
            "the knob to compare against the official kit"
        ),
    )
    parser.add_argument(
        "--bench",
        action="store_true",
        help="Run the throughput benchmark instead of the correctness check",
    )
    parser.add_argument(
        "--reuse-block-ids",
        action="store_true",
        help=(
            "Reuse one set of block ids for every epoch, so each epoch rewrites "
            "the same files. Default is fresh ids per epoch, which opens and "
            "registers block-number files that were never opened before"
        ),
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only report device address alignment, then exit without any I/O",
    )
    parser.add_argument(
        "--no-event-sync",
        action="store_true",
        help=(
            "Dump with prerequisite_handle=0. Default is to pass a real recorded "
            "event, matching what the connector does"
        ),
    )
    return parser.parse_args(argv)


def apply_args(args):
    global worker_number
    global shard_size
    global shard_number
    global block_number
    global dump_epoch_number
    global load_epoch_number
    global storage_backends
    global device_offset
    global posix_data_trans_concurrency
    global bench
    global check_only
    global no_event_sync
    global reuse_block_ids
    global malloc_policy

    worker_number = args.worker_number
    shard_size = args.shard_size
    shard_number = args.shard_number
    block_number = args.block_number
    dump_epoch_number = args.dump_epoch_number
    load_epoch_number = args.load_epoch_number
    if args.storage_backend is not None:
        storage_backends = args.storage_backend
    device_offset = args.device_offset
    posix_data_trans_concurrency = args.posix_data_trans_concurrency
    bench = args.bench
    check_only = args.check_only
    no_event_sync = args.no_event_sync
    reuse_block_ids = args.reuse_block_ids
    malloc_policy = {
        "torch": -1,
        "huge-first": ACL_MEM_MALLOC_HUGE_FIRST,
        "huge-only": ACL_MEM_MALLOC_HUGE_ONLY,
        "normal-only": ACL_MEM_MALLOC_NORMAL_ONLY,
    }[args.malloc_policy]

    if shard_size % NDS_ALIGNMENT != 0:
        raise SystemExit(
            f"--shard-size {shard_size} is not {NDS_ALIGNMENT}-aligned; "
            "NdsQueue::Setup would reject this config"
        )


def create_worker(device_id: int) -> UcmKVStoreBaseV1:
    module_path = "ucm.store.pipeline.connector"
    class_name = "UcmPipelineStore"
    config = {}
    config["store_pipeline"] = "Posix"
    config["posix_io_engine"] = "nds"
    config["storage_backends"] = storage_backends
    config["tensor_size"] = shard_size
    config["shard_size"] = shard_size
    config["block_size"] = shard_size * shard_number
    config["device_id"] = device_id
    config["io_direct"] = True
    config["posix_data_trans_concurrency"] = posix_data_trans_concurrency
    return UcmConnectorFactoryV1.create_connector(class_name, config, module_path)


class AclDeviceBuffer:

    def __init__(self, device_id: int, size: int, policy: int):
        import acl

        self._acl = acl
        self.ptr = 0
        self.size = size
        ptr, ret = acl.rt.malloc(size, policy)
        if ret != 0:
            raise RuntimeError(
                f"acl.rt.malloc({size}, policy={policy}) failed: ret={ret}"
            )
        self.ptr = ptr

    def fill_random(self):
        host = np.random.randint(0, 256, size=self.size, dtype=np.uint8)
        ret = self._acl.rt.memcpy(
            self.ptr, self.size, host.ctypes.data, self.size, 1
        )
        if ret != 0:
            raise RuntimeError(f"acl.rt.memcpy H2D failed: ret={ret}")

    def free(self):
        if self.ptr:
            self._acl.rt.free(self.ptr)
            self.ptr = 0

    def __del__(self):
        self.free()


def make_huge_page_buffer(device_id: int, size: int, fill_random: bool):
    import torch
    import torch_npu  # noqa: F401

    torch.npu.set_device(f"npu:{device_id}")
    buf = AclDeviceBuffer(device_id, size, malloc_policy)
    if fill_random:
        buf.fill_random()
    return buf, buf.ptr


def make_device_tensor(device_id: int, size: int, fill_random: bool):
    import torch
    import torch_npu  # noqa: F401  # 注册 torch.npu 后端

    device = f"npu:{device_id}"
    raw = torch.empty(size + NDS_ALIGNMENT, dtype=torch.uint8, device=device)
    pad = -raw.data_ptr() % NDS_ALIGNMENT
    view = raw[pad : pad + size]
    if fill_random:
        view.random_(0, 256)
    else:
        view.zero_()
    return view, view.data_ptr()


def record_event() -> int:
    if no_event_sync:
        return 0
    import acl
    import torch_npu

    stream = torch_npu.npu.current_stream().npu_stream
    event, ret = acl.rt.create_event()
    if ret != 0:
        raise RuntimeError(f"acl create_event failed: {ret}")
    ret = acl.rt.record_event(event, stream)
    if ret != 0:
        acl.rt.destroy_event(event)
        raise RuntimeError(f"acl record_event failed: {ret}")
    return int(event)


def destroy_event(event: int) -> None:
    if not event:
        return
    import acl

    acl.rt.destroy_event(event)


def assert_aligned(ptrs, label):
    bad = [(i, p) for i, p in enumerate(ptrs) if p % NDS_ALIGNMENT != 0]
    if bad:
        detail = ", ".join(f"[{i}]=0x{p:x}" for i, p in bad[:8])
        raise AssertionError(
            f"{len(bad)}/{len(ptrs)} {label} addrs are not {NDS_ALIGNMENT}-aligned: "
            f"{detail}. NDS cannot transfer these; the KV cache base itself is "
            "likely unaligned."
        )


def dump(epoch, device_id, worker, block_ids, block_ptr):
    total_size = shard_size * shard_number * block_number
    costs = []
    for i in range(shard_number):
        idxes = [i for _ in range(block_number)]
        ptrs = [[ptr + i * shard_size] for ptr in block_ptr]
        event = record_event()
        tp = time.perf_counter()
        try:
            task = worker.dump_data(block_ids, idxes, ptrs, event)
            worker.wait(task)
        finally:
            destroy_event(event)
        costs.append(time.perf_counter() - tp)
    total_cost = np.sum(costs)
    print(
        f"epoch={epoch:03}, worker={device_id:02}, "
        f"dump=[{shard_size} x {block_number} x {shard_number}], "
        f"avg_cost={np.average(costs) * 1e3:.3f}ms, "
        f"p99_cost={np.percentile(costs, 99) * 1e3:.3f}ms, "
        f"total_cost={total_cost * 1e3:.3f}ms, "
        f"bw={total_size / total_cost / 1e9:.3f}GB/s."
    )


def load(epoch, device_id, worker, block_ids, block_ptr):
    total_size = shard_size * shard_number * block_number
    costs = []
    for i in range(shard_number):
        idxes = [i for _ in range(block_number)]
        ptrs = [[ptr + i * shard_size] for ptr in block_ptr]
        tp = time.perf_counter()
        task = worker.load_data(block_ids, idxes, ptrs)
        worker.wait(task)
        costs.append(time.perf_counter() - tp)
    total_cost = np.sum(costs)
    print(
        f"epoch={epoch:03}, worker={device_id:02}, "
        f"load=[{shard_size} x {block_number} x {shard_number}], "
        f"avg_cost={np.average(costs) * 1e3:.3f}ms, "
        f"p99_cost={np.percentile(costs, 99) * 1e3:.3f}ms, "
        f"total_cost={total_cost * 1e3:.3f}ms, "
        f"bw={total_size / total_cost / 1e9:.3f}GB/s."
    )


def correctness_test(device_id, store, block_ids, src_tensors, src_ptrs):
    import torch

    block_size = shard_size * shard_number

    founds = store.lookup(block_ids)
    assert not any(founds), "blocks should not exist before dump"

    tp = time.perf_counter()
    for i in range(shard_number):
        idxes = [i for _ in range(block_number)]
        ptrs = [[p + i * shard_size] for p in src_ptrs]
        event = record_event()
        try:
            task = store.dump_data(block_ids, idxes, ptrs, event)
            store.wait(task)
        finally:
            destroy_event(event)
    cost_dump = time.perf_counter() - tp

    founds = store.lookup(block_ids)
    assert all(founds), "blocks should exist after dump"

    found_idx = store.lookup_on_prefix(block_ids)
    assert found_idx + 1 == block_number, (
        f"prefix lookup returned {found_idx}, expected {block_number - 1}"
    )

    dst = [make_device_tensor(device_id, block_size, False) for _ in range(block_number)]
    dst_tensors = [t for t, _ in dst]
    dst_ptrs = [p for _, p in dst]
    assert_aligned(dst_ptrs, "load dst")

    tp = time.perf_counter()
    for i in range(shard_number):
        idxes = [i for _ in range(block_number)]
        ptrs = [[p + i * shard_size] for p in dst_ptrs]
        task = store.load_data(block_ids, idxes, ptrs)
        store.wait(task)
    cost_load = time.perf_counter() - tp

    mismatch = 0
    for i, (src, dst_t) in enumerate(zip(src_tensors, dst_tensors)):
        if not torch.equal(src, dst_t):
            diff = (src != dst_t).nonzero().flatten()
            print(
                f"DIFF block[{i}]: {diff.numel()} of {block_size} bytes differ, "
                f"first at offset {diff[0].item()}"
            )
            mismatch += 1
    assert mismatch == 0, f"{mismatch}/{block_number} blocks came back corrupted"

    total_size = block_size * block_number
    print(
        f"worker={device_id:02}, correctness=PASS, "
        f"blocks={block_number} x {block_size}, "
        f"dump_cost={cost_dump * 1e3:.3f}ms "
        f"({total_size / cost_dump / 1e9:.3f}GB/s), "
        f"load_cost={cost_load * 1e3:.3f}ms "
        f"({total_size / cost_load / 1e9:.3f}GB/s)."
    )


def worker_loop(device_id, barrier):
    block_size = shard_size * shard_number
    allocate = make_device_tensor if malloc_policy < 0 else make_huge_page_buffer
    src = [allocate(device_id, block_size, True) for _ in range(block_number)]
    src_tensors = [t for t, _ in src]
    block_ptr = [p for _, p in src]

    assert_aligned(block_ptr, "dump src")
    print(
        f"worker={device_id:02}, allocated {block_number} x {block_size} bytes on npu, "
        f"all addrs {NDS_ALIGNMENT}-aligned, first=0x{block_ptr[0]:x}."
    )
    if check_only:
        return

    store = create_worker(device_id)
    barrier.wait()

    if not bench:
        if malloc_policy >= 0:
            raise SystemExit(
                "--malloc-policy other than 'torch' only supports --bench or "
                "--check-only: the correctness check compares with torch.equal, "
                "which needs tensors rather than raw acl pointers"
            )
        block_ids = [secrets.token_bytes(16) for _ in range(block_number)]
        correctness_test(device_id, store, block_ids, src_tensors, block_ptr)
        return

    if reuse_block_ids:
        shared = [secrets.token_bytes(16) for _ in range(block_number)]
        epoch_ids = [shared] * dump_epoch_number
    else:
        epoch_ids = [
            [secrets.token_bytes(16) for _ in range(block_number)]
            for _ in range(dump_epoch_number)
        ]
    for epoch in range(dump_epoch_number):
        dump(epoch, device_id, store, epoch_ids[epoch], block_ptr)
        barrier.wait()
    for epoch in range(load_epoch_number):
        load(epoch, device_id, store, epoch_ids[epoch % dump_epoch_number], block_ptr)
        barrier.wait()


if __name__ == "__main__":
    apply_args(parse_args())
    barrier = multiprocessing.Barrier(worker_number)
    workers = []
    for i in range(worker_number):
        p = multiprocessing.Process(
            target=worker_loop, args=(device_offset + i, barrier)
        )
        workers.append(p)
        p.start()
    failed = 0
    for w in workers:
        w.join()
        if w.exitcode != 0:
            failed += 1
    if failed:
        raise SystemExit(f"{failed}/{worker_number} worker(s) failed")
