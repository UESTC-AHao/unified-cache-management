# NDS IO Engine 设计说明

将 KV cache 从 NPU HBM 直接读写后端存储，绕过 host 内存中转。以 PosixStore 的第三种
io engine 形态接入，通过 `posix_io_engine: "nds"` 启用。

## 1. 设计目的

### 现状：数据要在 host 内存里落一次脚

当前落盘链路是 `Cache|Posix` 两级 pipeline：

```
dump:  HBM --D2H(aclrtMemcpy)--> host buffer --pwrite--> 存储
load:  存储 --pread--> host buffer --H2D(aclrtMemcpy)--> HBM
```

- D2H / H2D 在 `ucm/store/cache/cc/dump_queue.cc:134` 和 `load_queue.cc:241`
- pread / pwrite 在 `ucm/store/posix/cc/trans_queue.cc:154`（H2S）和 `:179`（S2H）

host buffer 由 CacheStore 的 `BufferManager` 管理，默认要预留可观的物理内存
（`cache_buffer_capacity_gb`，共享 buffer 场景默认 128GB）。每个 block 的每一次
落盘和回读都要多走一次 HBM↔host 拷贝，这一跳既吃内存带宽，也吃内存容量。

### NDS 能省掉的正是这一跳

NDS（`libndsfs`）在设备内存和 `O_DIRECT` 文件描述符之间直接搬数据。而 UCM 的
`Detail::Shard::addrs` 里存的本来就是 connector 按 KV cache 布局算出的**设备地址**
（`ucm/integration/vllm/ucm_connector.py:1346` 的 `extract_block_addrs`），一路原样
传到 store 层。也就是说 NDS 需要的输入，UCM 已经现成具备：

```
dump:  HBM --NdsFileWrite--> 存储
load:  存储 --NdsFileRead--> HBM
```

收益有三点：

1. 去掉 D2H/H2D 拷贝，缩短传输路径
2. 不再需要 CacheStore 的 host buffer 池，省掉对应的物理内存预留
3. pipeline 从两级降为一级，少一层任务提交与等待的开销

代价是新增对 `libndsfs` 和昇腾环境的依赖，且 direct I/O 的对齐约束会传导到每一次
传输（见 §5.4）。

## 2. 设计方案

### 2.1 为什么做成 posix 的 io engine，而不是独立后端

PosixStore 里与"数据怎么搬"无关的部分占了绝大多数代码量，且全部可以原样复用：

| 组件 | 职责 | NDS 是否需要改 |
|---|---|---|
| `SpaceLayout` | block id → 文件路径、`.tmp` 提交语义 | 否 |
| `SpaceManager` | Lookup / LookupOnPrefix / Prefetch | 否 |
| `ShardGarbageCollector` | 容量回收 | 否 |
| `HotnessTracker` | 热度统计 | 否 |
| `CheckHealth` | 健康探测 | 否 |
| **H2S / S2H** | **实际数据传输** | **是，仅此一处** |

PosixStore 本来就有 `aio` / `psync` 两种 io engine 由 `posix_io_engine` 选择
（`trans_manager.h`）。NDS 作为第三种引擎接入，只替换传输实现，其余全部继承。
若另建 `ucm/store/nds/` 后端，则需照 ds3fs 的先例复制约 1000 行的 layout /
manager / GC 代码，且后续 posix 侧的修复要同步两处。

### 2.2 文件结构

新增（`ucm/store/posix/cc/`）：

| 文件 | 内容 |
|---|---|
| `nds_file.h/.cc` | `NdsDriver` 进程级驱动单例；`NdsFile` 单文件句柄封装 |
| `nds_queue.h/.cc` | load/dump 线程池，HBM 直通的 H2S/S2H |
| `io_engine_nds.h` | `TaskWrapper` 适配层，与 `io_engine_psync.h` 同构 |

修改：

| 文件 | 改动 |
|---|---|
| `trans_manager.h` | 增加 `"nds"` 分支；未编译时返回 InvalidParam 并说明原因 |
| `global_config.h` | 增加 `tensorSizes` 列表字段 |
| `posix_store.cc` | 解析 `tensor_size_list`；引擎名校验提前到 size 校验之前 |
| `posix/CMakeLists.txt` | 编译期探测 `libndsfs`，命中则定义 `UCM_ENABLE_NDS=1` |

Python 侧零改动。`_posix_pipeline_builder` 原样透传 config，`posix_io_engine` 自然
到达 PosixStore。

### 2.3 NDS 接口的生命周期映射

测试用例 `ucm/store/test/XDS_Kit/XDS_Kit.cc` 揭示了两层生命周期，实现按此对齐：

**进程级** —— `NdsFileDriverOpen` / `NdsFileDriverClose`。测试里在整个 run 前后各调
一次（`:662`、`:679`）。实现用 `std::call_once` + `std::atexit` 做单例
（`nds_file.cc:41`）：驱动开一次，进程退出时关。多个 store 实例共享同一驱动，因为
没有更早的安全关闭点。

**文件级** —— `O_DIRECT` open → `NdsFileHandleRegister` → 传输 → `NdsFileHandleDeregister`
→ close（测试 `:386-394`、`:446-447`）。封装为 `NdsFile`，`Close()` 保证先
deregister 再 close fd，因为句柄绑定的正是该描述符；析构兜底。

**数据面** —— `NdsFileRead/Write(fh, devPtr, size, fileOffset, ptrOffset)`，返回实际
字节数，`!= size` 即视为失败。

### 2.4 传输粒度：按 tensor 而非按 shard

`psync` 引擎按 `tensorSize` 逐 tensor 调用 pread/pwrite，偏移在 shard 内递增
（`trans_queue.cc:152-161`）。NDS 沿用同样的粒度（`nds_queue.cc:Transfer`），原因是
shard 内各 tensor 的设备地址来自不同的 KV cache 张量，在设备内存里并不连续 ——
`shard.addrs` 是一个地址数组，不是单个基址。因此每个 tensor 是一次独立的
`NdsFileRead/Write`。

这带来一个配置差异：`psync`/`aio` 只需要标量 `tensor_size`，而 NDS 需要知道每个
tensor 各自的大小。所以 `posix_store.cc` 的配置解析扩展为：

- 设了 `tensor_size`（均匀布局）→ 展开成 `shardSize / tensorSize` 个等长条目
- 未设 → 读取 `tensor_size_list`（异构布局，如 MLA 或 Indexer 模型）

connector 侧两个 key 都会传（`ucm_connector.py:951` 设 `tensor_size_list`），
`_posix_pipeline_builder` 不注入 `tensor_size`，因此 NDS 走 list 分支。

### 2.5 写路径的文件预分配

NDS 写入落在显式偏移，不会像 `write(2)` 那样扩展文件，所以新建文件必须先撑到
block 大小。测试用例用 `posix_fallocate`（`:246`），但那是单文件单线程场景。

UCM 里同一 block 的多个 shard 由不同 worker 并发处理，各自打开同一文件。
`posix_fallocate` 在不支持底层 `fallocate` 的文件系统上会退化为**写零**，可能覆盖
另一个 worker 刚写完的 shard。因此实现改用 `fallocate(2)`（只分配，不触碰已有数据），
已达到目标大小时直接跳过（`nds_file.cc` 的 `Reserve()`）。

回退策略只认 `EOPNOTSUPP` 和 `ENOSYS` 这两个明确表示"不支持"的 errno。`EINVAL`
**不**触发回退 —— `fallocate` 也用它表示长度非法，一并回退会把真实的参数错误静默
掩盖过去。回退到 `ftruncate` 时会打 WARN：这条路径本身不具备上述并发安全性，属
最后手段，落到这里的后端应当去确认为何不支持 `fallocate`。

### 2.6 设备上下文绑定

NDS 传输触及设备内存，每个 worker 线程都需先绑定设备上下文。线程池的
`SetWorkerInitFn` 里调 `Trans::Device::Setup(deviceId)`（`nds_queue.cc:SetupWorkerDevice`），
失败则该 worker 不启动，`Setup()` 返回错误。这也是 CMake 中 posixstore 需要链接
`trans` 的原因。

### 2.7 编译期裁剪与运行期行为

`libndsfs` 只存在于装了 NDS 的昇腾主机上，因此：

- CMake 探测 `nds_api.h` + `libndsfs`，且要求 `UCM_RUNTIME_ASCEND_FAMILY`
- 三者齐备 → 定义 `UCM_ENABLE_NDS=1`，链接 `ndsfs` 和 `trans`
- 否则 → `nds_*.cc` 整体被 `#if UCM_ENABLE_NDS` 包住，编译为空翻译单元

未编入 NDS 却配置了 `posix_io_engine: "nds"` 时，`TransManager::Setup` 返回
InvalidParam 并明确说明"nds io engine was not built, libndsfs was not found"。
**不做运行期自动回退到 host-bounce 路径** —— 静默降级会让性能表现不可预测，问题
也更难定位。

### 2.8 可观测性

复用 PosixStore 已有的 metric，无新增定义（均已在 `ucm/default_metrics_config.py`
注册）：

- 任务耗时：`posix_load_task_duration_ms` / `posix_dump_task_duration_ms`
- 带宽：`posix_s2h_bandwidth_gbps` / `posix_h2s_bandwidth_gbps`
- 字节数：`posix_s2h_bytes_total` / `posix_h2s_bytes_total`
- 排队等待：`posix_load_queue_wait_duration_ms` / `posix_dump_queue_wait_duration_ms`
- 错误：`posix_open_errors_total` / `posix_io_errors_total` / `posix_io_timeout_total`

日志中 NDS 任务以 `Nds task(...)` 前缀区分于 `Posix task(...)`。

## 3. 编译

```bash
cmake -B build \
    -DRUNTIME_ENVIRONMENT=ascend \
    -DBUILD_UCM_STORE=ON \
    -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

CMake 输出中确认这一行：

```
-- posixstore: nds io engine enabled (/path/to/libndsfs.so)
```

若显示 `disabled`，紧随其后的 `* Missing: ...` 会指出缺哪一项。头文件或库不在默认
搜索路径时显式指定：

```bash
cmake -B build -DRUNTIME_ENVIRONMENT=ascend \
    -DNDS_INCLUDE_DIR=/实际/头文件/目录 \
    -DNDS_LIBRARY=/实际/路径/libndsfs.so
```

默认探测路径为 `${NDS_ROOT}` 下的 `include` / `aarch64-linux/include` /
`x86_64-linux/include` 和 `lib64` / `aarch64-linux/lib64` / `x86_64-linux/lib64`，
`NDS_ROOT` 默认 `/usr/local/Ascend/ascend-toolkit/latest`。

## 4. 真机验证配置

### 4.1 最小配置

```yaml
ucm_connectors:
  - ucm_connector_name: "UcmPipelineStore"
    ucm_connector_config:
      store_pipeline: "Posix"
      posix_io_engine: "nds"
      storage_backends: "/mnt/nds_cache"
      io_direct: true
```

要点：

- `store_pipeline` 填 `"Posix"`，**不是** `"Cache|Posix"`。NDS 直接吃设备地址，
  前面挂 Cache 级反而把数据又拉回 host buffer，收益归零。
- `posix_io_engine: "nds"` 是唯一开关。
- `io_direct: true` 与 NDS 语义一致（NDS 内部本就要求 `O_DIRECT` fd）。
- `storage_backends` 指向的挂载点须支持 direct I/O。

### 4.2 对照基线

跑对比时用同一份配置，只改 `posix_io_engine`：

| 场景 | store_pipeline | posix_io_engine |
|---|---|---|
| NDS 直通 | `Posix` | `nds` |
| host 中转（基线） | `Cache\|Posix` | `psync` 或 `aio` |

对比指标看 `posix_h2s_bandwidth_gbps` / `posix_s2h_bandwidth_gbps` 和
`posix_dump_task_duration_ms` / `posix_load_task_duration_ms`。基线侧还应额外观察
`cache_d2h_duration_ms` / `cache_h2d_sync_ms`（这部分开销是 NDS 要消除的目标）。

### 4.3 启动自检

启动日志中应出现：

```
NDS driver opened.
Set PosixStore::IoEngine to nds.
Set PosixStore::TensorSizeCount to <非 0>.
Set PosixStore::ShardSize to <4096 的倍数>.
```

`TensorSizeCount` 为 0 说明 `tensor_size_list` 没传到，Setup 会直接失败。

## 5. 需要注意的点

以下几项仍需真机确认，按风险从高到低排列。

首版编写时没有 `nds_api.h` 实物，若干接口形态只能从测试用例反推。现在
`ucm/store/test/nds_test/nds_api.h` 已在仓库中，原先风险最高的两项推断
（§5.2、§5.3）已可直接对照头文件确认成立，均已降级。

### 5.1 头文件与库的实际安装路径（会导致编译失败）

CMake 里的搜索路径是按昇腾工具链的常见布局猜的。测试用例的 CMakeLists
（`XDS_Kit/CMakeLists.txt:57`）是从**源码目录**找 `./libndsfs.so`，并未反映系统安装
位置；`XDS_Kit.cc:49` 也只写了 `#include "nds_api.h"`，看不出安装前缀。

现象：CMake 报 `* Missing: nds_api.h` 或 `* Missing: libndsfs`。
处理：用 `-DNDS_INCLUDE_DIR` / `-DNDS_LIBRARY` 覆盖，并把真实路径反馈回来以修正默认值。

### 5.2 `NdsFileWrite` 第二参数的 const 属性（已确认）

头文件写明形参为 `void*`（非 const）：

```c
ssize_t NdsFileWrite(NdsFileHandle_t fh, void *ptr_base, size_t size,
                     off_t file_offset, off_t ptr_offset);
```

因此 `Write()` 里的 `const_cast` 是必要的 —— const 是我们自己在接口上加的，
这里只是把它摘掉。无需改动。

### 5.3 `NdsFileError_t` / `NdsFileHandle_t` 的实际形态（已确认）

头文件与首版的三处推断完全一致：

- `NdsFileError_t` 是含单个 `enum NdsFileOpError err` 成员的结构体
- `NdsFileHandle_t` 就是 `void*`，`NdsFileHandleDeregister(NdsFileHandle_t fh)` 按值收，
  返回 `void`（所以注销无法报错，`Close()` 里不检查返回值是对的）
- `NdsFileDescr_t` 含 `int fd`

`memset` 清零仍然保留：结构体目前只有 `fd` 一个成员，但清零可防止将来扩展字段时
残留栈上垃圾值。无需改动。

### 5.4 对齐约束可能拒绝某些模型配置（会导致启动失败）

`NdsQueue::CheckConfig` 在 Setup 阶段强制校验：

- `shardSize % 4096 == 0`
- 每个 `tensorSize % 4096 == 0`（size 为 0 的 ghost slot 不参与传输，天然满足）
- `sum(tensorSizes) <= shardSize`（与 CacheStore 一致，允许尾部 padding，见 §6.2）

另有一项在传输时校验：每个设备地址也须 4K 对齐（见 §6.4）。这条只能在真机上验证，
取决于 torch NPU 分配器的行为。

这些都是 direct I/O 的固有要求，且因为按 tensor 粒度传输，约束落到了**每个 tensor**
上，比 shard 整体对齐更严。某些模型的 tensor size 可能不是 4K 倍数，此时启动即报
InvalidParam。

这是有意的设计选择：启动失败比运行时静默截断或数据错位好定位。但如果真机上确实
撞到这个限制，有两条路可走 —— 改为按 shard 整体传输（要求 shard 内地址连续，需先
确认布局），或引入 padding。**遇到这种情况请把模型的 `tensor_size_list` 和
`shard_size` 实际值发我**，我据此判断该走哪条。

### 5.5 文件预分配在目标文件系统上的行为

§2.5 的并发覆盖分析是基于 `posix_fallocate` 的通用语义。若后端文件系统对
`fallocate(2)` 有特殊行为（例如返回成功但不真正预留，或在 direct I/O 下与写入交互
异常），这段逻辑需重新评估。

验证方法：跑一轮 dump 后检查落盘文件大小是否等于 `block_size`，且多次 load 回读的
数据与写入一致（尤其是同一 block 内非首个 shard 的内容）。

### 5.6 驱动生命周期与 fork

`NdsDriver` 用 `atexit` 关闭驱动。若 vLLM 的 worker 进程模型涉及在驱动打开后
`fork`，子进程继承的驱动状态是否可用需要确认。当前实现在每个进程首次使用时才打开
驱动（`call_once` 是进程内的），正常情况下 worker 各自独立打开，不应有问题 ——
但如果观察到 `NdsFileDriverOpen failed` 或句柄注册在部分 rank 上失败，这是排查方向
之一。

### 5.7 测试现状

已有 e2e 脚本 `ucm/store/test/e2e/posixstore_nds_test.py`（见 §6.9），覆盖地址对齐、
落盘读回的逐字节比对、以及 dump 路径的 event 同步。该脚本未经语法检查也未运行过。

尚缺单测。真机验证通过后建议补充：`NdsQueue::CheckConfig` 的对齐校验分支、
`NdsFile` 的注册/注销配对（可用 test hook 打桩 `NdsFileRead/Write`，不依赖真实硬件）、
`WaitPrerequisite` 的状态机（尤其失败路径只解析一次）。现有 `ucm/store/test/case/`
下已有 posix 的测试可作参照。

## 6. 评审修订记录

以下改动源自对首版实现的代码评审。按严重程度排列，第一项是正确性缺陷，其余为
健壮性与可诊断性。

### 6.1 dump 前补齐 compute event 同步（正确性缺陷）

**问题**：首版 `nds_queue.cc` 全文未出现 `prerequisiteHandle`，直接丢弃了
connector 传下来的计算完成事件。

链路是：connector 在 dump 前于计算流上记录一个 event
（`ucm_connector.py:1560` → `:1409` → `device.py` 的 `acl.rt.record_event`），
经 `dump_data(..., event_handle)` → `pipeline_store.py.cc:214` 存入
`desc.prerequisiteHandle`。这个 event 标记"这批 block 的 KV 已算完"。

现有两条路径都消费它：CacheStore 在 D2H 前 `stream.WaitEvent`
（`cache/cc/dump_queue.cc:114`），MooncakeStore 用 `aclrtSynchronizeEvent` 阻塞等
（`mooncakestore/cc/dump_queue.cc:113`）。

关键点是 `enable_event_sync` 默认为 `True`（`ucm_connector.py:798`），
此时 connector **不会**替下层 `synchronize()` —— 只有该开关为 `False` 或
`get_event_handle()` 返回 0 时才走 `device.synchronize()` 兜底（`:1405-1411`）。

所以首版的后果是：计算 kernel 可能仍在 NPU 上执行，`NdsFileWrite` 已经把那块 HBM
搬走，落盘的是**半成品 KV**。且不报任何错误，load 回来只表现为精度异常 ——
偶发、依赖时序、无日志线索。

**改动**：新增 `NdsQueue::WaitPrerequisite()`（`nds_queue.cc`），在 `H2S` 之前调用。

两个设计选择，都有理由：

1. **用 `aclrtSynchronizeEvent` 阻塞等，而非 `stream.WaitEvent`。**
   NDS 的 DMA 发生在内核态，不排在任何 ACL stream 上，因此 stream 层面的
   WaitEvent 约束不到它 —— 必须真正阻塞。这与 Mooncake 走 Mooncake 后端
   （非 backend 路径）时的选择一致。

2. **在 worker 线程等，而非 `Dispatch()` 里等。**
   `Dispatch()` 运行在调用方线程（vLLM 的 forward 线程）。在那里阻塞会把异步 dump
   变成同步，直接拖慢推理主路径。Mooncake 同样是在 worker 里等
   （`mooncakestore/cc/dump_queue.cc:264`）。

由此带来一个次生问题：一个 task 的 N 个 shard 分散在 N 个 worker 上，每个都会
调用一次 `aclrtSynchronizeEvent`。语义上无害（event 已完成时是空操作），但属
无谓的重复系统调用。为此在 `TransTask` 记录等待结果，让整个 task 只解析一次
（`trans_task.h`）：event 结果是全局设备状态，首个 worker 等待并发布结论，其余
worker 复用。该字段仅 nds 引擎使用，aio / psync 读的是 CacheStore 已同步过的
host buffer。

状态用四态枚举 `PrereqState{PENDING, RUNNING, DONE, FAILED}` 而非布尔量，原因是
布尔量无法区分"尚未等待"和"等过且失败"：

- 用 CAS 从 `PENDING` 抢到 `RUNNING` 的 worker 才发起系统调用，并发到达的其余
  worker 不会重复发起；
- **失败也要发布**（`FAILED`）。若失败时不落状态，每个 shard 都会重试一个必然
  失败的阻塞调用，并各自把同一个 task 标记失败 —— 结果是 N 次无谓阻塞和 N 条重复
  ERROR 日志，真实故障形态反被淹没。
- 未抢到的 worker 自旋等待终态；等待时间以首个 worker 的 event 同步为界，且整体
  受 worker 超时约束。

### 6.2 放宽 `sum(tensorSizes) == shardSize` 为 `<=`

**问题**：首版 `CheckConfig` 要求严格相等，比 CacheStore 的校验更严。
CacheStore 用的是 `>` 判断（`cache/cc/cache_store.cc:164-166`），即允许 shard 尾部
存在 padding。严格相等会拒绝 cache 路径能正常接受的布局，导致同一模型换
`posix_io_engine` 就起不来。

**改动**：改为 `total > config.shardSize` 才报错，与 CacheStore 对齐。
`Transfer()` 中 offset 仍按 `tensorSizes_` 逐个累加，尾部 padding 区间自然不被
触碰。

### 6.3 跳过 layerwise padding 的 ghost slot

**问题**：首版对每个 tensor 都要求 `tensorSize != 0`，且无条件对
`shard.addrs[i]` 发起传输。但 layerwise 布局会产生地址为 `nullptr` 或 size 为 0 的
占位 slot —— `trans/stream.h:59` 的 `HostToDeviceAsync` 明确跳过这类 slot 并注释
"skip zero-padded ghost slots"。首版会把 `nullptr` 交给驱动。

**改动**：`Transfer()` 中 `size == 0 || addr == nullptr` 时跳过传输，但**仍然累加
offset**，以保证落盘布局与 `tensorSizes_` 始终对应；`CheckConfig` 相应不再拒绝
size 为 0 的 slot（0 天然满足对齐）。

### 6.4 补充设备地址的对齐校验

**问题**：首版只在 Setup 阶段校验 `shardSize` 和各 `tensorSize` 的对齐，未校验
设备地址本身。而 `nds_api.h` 对 `NdsFileRead`/`NdsFileWrite` 的注释明确写着
"基地址、长度和偏移都需要按 page size（4K）对齐" —— 基地址也在其中。

**改动**：`nds_file.h` 新增 `IsNdsAligned()`（size 与指针两个重载），
`Transfer()` 在发起传输前校验地址。

这里的判断依据：地址是 `base_ptr + n * stride`，stride 已校验对齐，所以地址不对齐
只可能是 KV cache 基址本身不对齐 —— 属全局性配置问题，不是单个 block 的偶发问题。
因此给出明确报错优于让驱动返回一个笼统的指针错误码。这一条也正好是真机验证要
回答的问题之一（torch NPU 分配器给出的地址是否 4K 对齐）。

### 6.5 `Reserve()` 补全 fallocate 不支持时的 errno 判断

**问题**：首版只在 `errno == EOPNOTSUPP` 时回退到 `ftruncate`。但不支持
`fallocate(2)` 的文件系统报错并不统一：老内核可能返回 `ENOSYS`。漏判会导致预分配
直接失败，dump 全线报错。

**改动**：`EOPNOTSUPP` 和 `ENOSYS` 都回退，并在回退时打 WARN 日志。

评审初版还把 `EINVAL` 也算作"不支持"，复审时收窄掉了：`fallocate` 同样用 `EINVAL`
表示长度非法，一并回退会让真实的参数错误静默走进回退路径而不被发现。只保留两个
语义明确的 errno。

同时在注释里明确标注了一个**已知局限**：`fallocate` 并发调用同一区间是安全的
（幂等分配，不改动数据），但 `ftruncate` 回退路径**没有**做并发保护 —— 两个 worker
同时给同一 block 定尺寸时存在竞争窗口。这条路径是最后手段，生产环境的后端应确认
支持 `fallocate`。§2.5 原有的并发分析只覆盖了 `fallocate` 主路径，此处补全。

### 6.6 提升错误可诊断性

**问题**：`NdsFileRead`/`NdsFileWrite` 失败时只返回 `-1`，不返回
`NdsFileError_t`，所以首版日志里的 `failed(-1)` 不含任何原因信息。而驱动区分了
近 30 种失败条件（指针无效、显存未注册、context 不匹配……），这些正是适配期最可能
撞到的问题。

**改动**：

- 新增 `NdsErrorName()`（`nds_file.cc`），把 `NdsFileOpError` 映射为可读名称，
  用于 `NdsFileDriverOpen` / `NdsFileHandleRegister` 的失败日志 —— 这两个调用确实
  返回 `NdsFileError_t`。只枚举了 UCM 配置问题可能触发的条目，其余归入 `UNKNOWN`
  并照常打印原始码。
- 数据面（只有 `-1`）改为读取 `errno` 并连同 size / offset 一起上报。
- 短读补 WARN 日志，说明其映射为 `Status::NotFound()` 的理由：短读意味着落盘文件
  被截断或只写了一部分，返回 NotFound 可让 connector 按 miss 处理并重算
  （见 `rank_consistency.py:155` 对 `StoreNotFoundError` 的分类），而不是让整个
  请求失败。

### 6.7 关于 ACL 依赖的说明

`nds_queue.cc` 因 `aclrtSynchronizeEvent` 新增了 `#include <acl/acl.h>`。
CMake 无需改动：`posixstore` 已 PRIVATE 链接 `trans`（为绑定 worker 设备上下文），
而 `trans` 是 PUBLIC 链接 `Ascend::ascendcl` 并带 `INTERFACE_INCLUDE_DIRECTORIES`
（`shared/trans/ascend/CMakeLists.txt`），因此 include 路径可传递获得。

### 6.8 仍未解决的事项

- §5.1、§5.4~§5.6 仍需真机确认（§5.2、§5.3 已由头文件确认，不再是风险项）。
- `aclrtSynchronizeEvent` 在 worker 线程调用要求该线程已绑定设备上下文。
  `NdsQueue::Setup` 已通过 `SetupWorkerDevice()` 作为 `WorkerInitFn` 处理了这一点，
  与 Mooncake 的做法一致，但**未在真机验证过**。若出现
  `CANN_CONTEXT_MISMATCH`（现在会以名字打印出来），这是首要排查方向。
- 单测仍然缺失，见 §5.7。

### 6.9 复审：对评审改动本身的两处修正

评审的六条修订经复核全部成立，依据均已逐条对照代码确认。复审另发现两个问题：

**`prerequisiteDone` 在失败路径上重复劳动**（§6.1 改动引入）。原实现用
`std::atomic_bool` 记录"已完成"，失败时不落状态。后果是 event 同步失败时，该 task
的每个 shard 都会重试一次必然失败的阻塞调用，打出 N 条相同 ERROR。已改为四态枚举
`PrereqState`，用 CAS 抢占执行权，失败同样发布结论供其余 worker 复用。状态机的详细说明已并入 §6.1。

**`Reserve()` 把 `EINVAL` 当作"不支持 fallocate"**（§6.5 改动引入）。已收窄为只认
`EOPNOTSUPP` 和 `ENOSYS`，详见 §6.5 与 §2.5。

另外复审时移除了一处新加的 metric 上报（`posix_dump_prereq_wait_ms`）：该指标名未在
`default_metrics_config.py` 注册，且当前阶段以功能正确性为先，可观测性等真机验证
通过后再议。

### 6.10 新增 e2e 测试脚本

`ucm/store/test/e2e/posixstore_nds_test.py`，风格对齐 `posixstore_aio_test.py`
（同样的 argparse + 全局参数 + 多进程 barrier + 吞吐打印格式）。

与 aio 测试的三处必要差别：

1. **用 NPU 设备内存，而非 host 内存。** aio 测试用 `mmap` 分配 host 内存并手工
   对齐到 256K；NDS 只能吃设备地址，因此改用 `torch.randint(..., device="npu:N")`
   分配，取 `tensor.data_ptr()`。

2. **默认跑正确性校验，`--bench` 才跑压测。** aio 测试只测吞吐，不校验数据。
   但对 NDS 而言"数据是否正确"才是首要问题（尤其是 §6.1 那类静默错误），
   所以默认行为是：写入 → lookup → 读回**另一组**显存 → `torch.equal` 逐字节比对。
   读到独立的目标显存是刻意的，与源共用地址会让"根本没搬数据"这种错误也通过。

3. **dump 默认传入真实 event 句柄。** `dump_data` 的 `prerequisite_handle` 默认为 0，
   若不显式传，§6.1 修的 `WaitPrerequisite` 在测试里根本不会被执行。脚本按
   `device.py` 中 `NpuDevice.get_event_handle` 的同样方式
   （`acl.rt.create_event` + `record_event`）在计算流上记录 event 后传入。
   源数据由设备上的 `randint` kernel 产出，正是 event 要保护的场景。
   `--no-event-sync` 可退回 0 做对照。

另外提供 `--check-only`：只分配显存并检查地址对齐，不做任何 I/O，也不需要后端存储。
用于最快速地回答"torch NPU 分配器给的地址是否 4K 对齐"这个前置问题 —— 若这一步就
失败，§6.4 的设备地址校验会在真正传输时拦下所有请求，方案形态需要调整
（引入注册显存池 + 一次 D2D 拷贝，直通收益打折）。

主进程会检查子进程 exitcode 并在失败时以非 0 退出，便于接入 CI；aio 测试原本只
`join` 不检查返回值。

**未验证声明**：本机无可用 Python 解释器，该脚本仅做了接口签名的人工核对
（`dump_data` / `load_data` / `lookup` / `lookup_on_prefix` / `wait` 的形参与
`pipeline/connector.py` 一致），**未做语法检查，也未实际运行**。首次在真机执行时
若有低级错误（拼写、缩进、import），属预期范围。

### 6.11 复审：e2e 脚本的两处修正

**event 句柄泄漏**。`record_event()` 只创建不销毁，压测跑 32 个 epoch 会持续累积。
connector 侧本有配对释放（`ucm_connector.py` 的 `_release_dump_event_handle`），脚本
漏了。已补 `destroy_event()`，并在 dump 的 `wait()` 之后用 `try/finally` 配对调用。

**`--shard-number > 1` 时正确性测试必然失败**。`correctness_test` 原本固定
`idxes = [0] * block_number`，只写 shard 0，却比对整个 block。更关键的是
`CommitFile` 仅在最后一个 shard 完成时触发，少写任何 shard 都会让文件停在 `.tmp`，
随后的 `lookup` 直接失败。默认 `shard_number=1` 恰好掩盖了这个问题。已改为 dump 与
load 都遍历全部 shard。

