# -*- coding: utf-8 -*-
#
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
import os
import secrets
import time
from typing import List

import numpy as np

from ucm.store.posix.connector import UcmPosixStore
from ucm.store.ucmstore import Task


class PosixStoreOnly:
    def __init__(
        self,
        block_size: int,
        storage_backends: List[str],
    ):
        posix_config = {}
        posix_config["storage_backends"] = storage_backends
        posix_config["device_id"] = 0
        posix_config["tensor_size"] = block_size
        posix_config["shard_size"] = block_size
        posix_config["block_size"] = block_size
        posix_config["io_direct"] = True
        posix_config["stream_number"] = 32
        self.posix = UcmPosixStore(posix_config)

    def lookup(self, block_ids: List[bytes]) -> List[bool]:
        return self.posix.lookup(block_ids)

    def prefetch(self, block_ids: List[bytes]) -> None:
        return self.posix.prefetch(block_ids)

    def load_data(
        self,
        block_ids: List[bytes],
        shard_index: List[int],
        dst_addr: List[List[int]],
    ) -> Task:
        return self.posix.load_data(block_ids, shard_index, dst_addr)

    def dump_data(
        self,
        block_ids: List[bytes],
        shard_index: List[int],
        src_addr: List[List[int]],
    ) -> Task:
        return self.posix.dump_data(block_ids, shard_index, src_addr)

    def wait(self, task: Task) -> None:
        return self.posix.wait(task)

    def check(self, task: Task) -> bool:
        return self.posix.check(task)


def e2e_test(
    store: PosixStoreOnly,
    block_size: int,
    block_num: int,
):
    block_ids = [secrets.token_bytes(16) for _ in range(block_num)]

    founds = store.lookup(block_ids)
    assert not all(founds), "Blocks should not exist before dump"

    shard_indexes = [0 for _ in range(block_num)]

    src_data = []
    src_arrays = []
    src_mems = []
    for i in range(block_num):
        arr = np.zeros(block_size, dtype=np.uint8)
        arr[:] = np.random.randint(0, 256, block_size, dtype=np.uint8)
        src_data.append([arr.ctypes.data])
        src_arrays.append(arr.copy())
        src_mems.append(arr)

    tp = time.perf_counter()
    task = store.dump_data(block_ids, shard_indexes, src_data)
    store.wait(task)
    cost_dump = time.perf_counter() - tp

    founds = store.lookup(block_ids)
    assert all(founds), "Blocks should exist after dump"

    dst_data = []
    dst_arrays = []
    dst_mems = []
    for i in range(block_num):
        arr = np.zeros(block_size, dtype=np.uint8)
        dst_data.append([arr.ctypes.data])
        dst_arrays.append(arr)
        dst_mems.append(arr)

    tp = time.perf_counter()
    task = store.load_data(block_ids, shard_indexes, dst_data)
    store.wait(task)
    cost_load = time.perf_counter() - tp

    for i, (src_arr, dst_arr) in enumerate(zip(src_arrays, dst_arrays)):
        if not np.array_equal(src_arr, dst_arr):
            diff_mask = src_arr != dst_arr
            num_diff = diff_mask.sum()
            print(f"DIFF at block {i}: {num_diff} bytes differ")
            print(f"  src sample: {src_arr[diff_mask][:10]}")
            print(f"  dst sample: {dst_arr[diff_mask][:10]}")
            assert False, f"Data mismatch at block {i}"

    data_size = block_size * block_num
    bw_dump = data_size / cost_dump if cost_dump > 0 else 0
    bw_load = data_size / cost_load if cost_load > 0 else 0
    print(f"dump={cost_dump * 1e3:.3f}ms, load={cost_load * 1e3:.3f}ms, "
          f"bw_dump={bw_dump / 1e9:.3f}GB/s, bw_load={bw_load / 1e9:.3f}GB/s")


def main():
    block_size = 1048576 * 16
    block_num = 256
    storage_backends = ["."]
    test_batch_number = 64

    store = PosixStoreOnly(block_size, storage_backends)

    for i in range(test_batch_number):
        e2e_test(store, block_size, block_num)
        print(f"[{i+1:03}/{test_batch_number:03}] completed")

    time.sleep(10)


if __name__ == "__main__":
    os.environ["UC_LOGGER_LEVEL"] = "debug"
    main()
