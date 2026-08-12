# P2P Master 合并执行方案与记录

> 目标仓库：`D:\mooncake_code\github\Mooncake`（main 分支 `0721main`）
> 参照仓库：`D:\mooncake_code\github2\Mooncake`（p2p 分支 `0721p2p`，只读参照）
> 工作分支：`feat/p2p-merge`（off `0721main`）
> 本轮范围：master 侧端到端（C1–C5），client 侧/测试后续

---

## 1. 总体策略

- **Central master 零改动**：main 的 `master_service.{h,cpp}`、`master_client.{h,cpp}`、`rpc_service.{h,cpp}` 原样保留。
- **取消 master 基类**：p2p 分支的抽象基类 `MasterService` 逻辑扁平化进 `P2PMasterService`，去 virtual/override。
- **P2P 代码迁入 `include/p2p/`、`src/p2p/`**，与 central 物理隔离。内部再按 master/client 分两个子目录（见 §10）。
- **共享头最小合并**：仅 `types.h`/`replica.h`/`master_config.h`/`default_config.h`/`master_metric_manager.h` 加必需符号；`rpc_types.h` 不动。

## 2. 根因与冲突点（已核实）

main 缺 p2p 管理层文件，是因为 **p2p 分支重构了 segment/client/元数据管理层 + RPC API**，非删除：

| 概念 | main（central） | p2p 分支 |
|------|----------------|----------|
| Segment 管理 | `segment.h:179` 非虚 `SegmentManager`（值成员） | `segment_manager.h` 虚基类 + `P2PSegmentManager` 子类 |
| Client 管理 | `ok_client_` UUID 集 | `ClientMeta`/`ClientManager` 虚基类 + P2P 子类 |
| 心跳/注册 | `Ping`/`PutStart` | `Heartbeat`/`RegisterClient`/`QueryClientStatus` |
| GetReplicaListResponse | `{replicas, lease_ttl_ms}` | `{replicas, centralized_extra}` |

**冲突点**：
1. `SegmentManager` 同名不同定义（main `segment.h` vs p2p `segment_manager.h`）→ ODR 冲突。
2. `GetReplicaListResponse` 同名不同定义（main vs p2p rpc_types）→ ODR 冲突。
3. p2p 分支 central 文件 `centralized_rpc_service.*` 已删但被 `master.cpp`/`ha_helper.cpp`/tests 引用 → 编译断裂。

**冲突解决方案**：
1. SegmentManager：p2p 侧扁平化 `SegmentManager`+`P2PSegmentManager` → 单一 `P2PSegmentManager`（无 `SegmentManager` 类），进 `include/p2p/`。main `segment.h::SegmentManager` 不动。
2. GetReplicaListResponse：p2p 不自定义该类型，**复用 main 共享的** `GetReplicaListResponse{replicas, lease_ttl_ms}`（p2p 设 `lease_ttl_ms=0`）；丢弃 `CentralizedGetReplicaListResponseExtra`（central 走 main 代码不需要）。
3. centralized_rpc_service：`master.cpp`/`ha_helper.cpp` central 分支改用 main 的 `rpc_service.h`/`WrappedMasterService`/`RegisterRpcService`。

## 3. 执行步骤

### C1 共享头最小合并
- `types.h`：+ `DeploymentMode`、`MemoryType` 枚举
- `replica.h`：+ `P2PProxyReplicaData`（variant 第4 alternative）+ ctor + `is_p2p_proxy_replica()`/`get_p2p_*()` 访问器
- `master_config.h`/`default_config.h`：+ `client_crashed_ttl_sec`/`max_replicas_per_key`/`deployment_mode`
- `master_metric_manager.h/.cpp`：+ P2P-RPC metric 方法
- `rpc_types.h`：不动

### C2 P2P 管理层（全进 `include/p2p/`）
- `p2p_segment_manager.h/.cpp`：扁平化 SegmentManager+P2PSegmentManager → `P2PSegmentManager`
- `heartbeat_type.h`：原样迁入
- `client_meta.h/.cpp` + `p2p_client_meta.h/.cpp`：迁入，`GetSegmentManager()` 返回 `shared_ptr<P2PSegmentManager>`
- `client_manager.h/.cpp` + `p2p_client_manager.h/.cpp`：迁入（保留基类）

### C3 P2P master 扁平化
- `p2p_rpc_types.h`：p2p RPC 类型（不含 GetReplicaListResponse，复用共享）
- `p2p_master_service.h/.cpp`：扁平化基类+子类 → 具体类，去 virtual

### C4 P2P 包装层
- `p2p_rpc_service.h/.cpp`：自包含 `WrappedP2PMasterService`

### C5 deploy mode + 入口分发 + CMake
- `master.cpp`/`ha_helper.cpp`/`src/CMakeLists.txt`

---

## 4. 执行记录（修改 / 冲突 / 解决）

> 以下按执行顺序追加。

### C1 执行中发现：共享类型深度分叉（重大冲突）

核实两个分支的 `types.h`/`replica.h` 后发现，p2p 分支并非"加法扩展"共享类型，而是**重设计**了 central 也依赖的核心类型：

| 类型 | main（central） | p2p 分支 | 能否加法合并 |
|------|----------------|----------|--------------|
| `Segment` | `{id,name,base,size,te_endpoint}` 平铺 | `{id,name,size, extra: variant<monostate,CentralizedSegmentExtraData,P2PSegmentExtraData>}` + `IsP2PSegment()`/`GetP2PExtra()` | ❌ 重设计，central 代码直接访问 `.base`/`.te_endpoint` |
| `ClientStatus` | `{UNDEFINED, OK, NEED_REMOUNT}` | `{UNDEFINED, HEALTH, DISCONNECTION, CRASHED}` | ❌ 状态机不同 |
| `Replica` | variant 3 alternative | 加 `P2PProxyReplicaData` 第4 alternative + 访问器 | ✅ 加法（central 不构造 p2p 副本） |
| `ErrorCode` | 较少 | 加 `REPLICA_ALREADY_EXISTS`/`REPLICA_NOT_FOUND`/`REPLICA_NUM_EXCEEDED`/`CLIENT_ALREADY_EXISTS`/`CLIENT_UNHEALTHY`/`CAS_FAILED`/`SHUTTING_DOWN`/`ASYNC_ENQUEUE_FAILED`/`INACCESSIBLE_MASTER`/`EMPTY_REPLICAS`/`TIER_NOT_FOUND`/`DATA_COPY_FAILED`/`KEYS_ULTRA_LIMIT` | ✅ 加法（central 不用新值） |
| `DeploymentMode`/`MemoryType` | 无 | 新增枚举 | ✅ 新增 |

**冲突本质**：central 与 p2p master 编译进同一 `mooncake_store` 库，`mooncake::Segment`/`ClientStatus` 只能有一份定义。p2p 的重设计版与 central 的平铺版不兼容 → "central 不动" 与 "复用 p2p 共享类型" 直接冲突。

**候选解法**：
- **加法隔离（保持 central 不动）**：main 的 `Segment` 保留平铺字段，**加法**追加 `std::optional<P2PSegmentExtraData> p2p_extra` + `IsP2PSegment()`/`GetP2PExtra()` 访问器（central 忽略空 optional）；`ClientStatus` p2p 侧改用独立 `P2PClientStatus`（p2p 路径内）；`Replica`/`ErrorCode` 加法合并。代价：p2p 代码需适配（Segment 构造改用 `p2p_extra`、`ClientStatus`→`P2PClientStatus` 改名、丢弃 `CentralizedSegmentExtraData`）。
- **统一到 p2p 类型（动 central）**：以 p2p 重设计类型为统一基线，central 代码适配（`.base`→`.GetCentralizedExtra().base`、`ClientStatus::OK`→`HEALTH` 等）。代价：大范围改 central，违背"central 不动"，高风险。

（待用户定夺策略后继续 C1 执行）

**策略定夺**：用户选 **A：加法隔离（central 不动）**。

### C1 执行完成（共享头加法合并）

**修改文件清单**：
1. `include/types.h`
   - + `DEFAULT_CLIENT_CRASHED_TTL_SEC = 30`
   - + `DeploymentMode` 枚举 + `operator<<`
   - + `MemoryType` 枚举 + `MemoryTypeToString`
   - + `P2PSegmentExtraData` 结构（priority/tags/memory_type/usage）
   - `Segment`：**加法**追加 `std::optional<P2PSegmentExtraData> p2p_extra` + `IsP2PSSegment()`/`GetP2PExtra()`；`YLT_REFL` 追加 `p2p_extra`。central 平铺字段 `base`/`te_endpoint` 保留不动。
   - `ErrorCode`：加法追加 `CLIENT_ALREADY_EXISTS`/`CLIENT_UNHEALTHY`/`CAS_FAILED`/`REPLICA_ALREADY_EXISTS`/`REPLICA_NOT_FOUND`/`REPLICA_NUM_EXCEEDED`/`EMPTY_REPLICAS`/`TIER_NOT_FOUND`/`DATA_COPY_FAILED`/`SHUTTING_DOWN`/`ASYNC_ENQUEUE_FAILED`/`INACCESSIBLE_MASTER`
2. `include/replica.h`
   - + `class P2PClientMeta;` 前向声明
   - `ReplicaType` + `P2P_PROXY` + 流输出映射补全（含 `LOCAL_DISK`）
   - + `P2PProxyReplicaData` / `P2PProxyDescriptor` 结构
   - + `Replica(P2PProxyReplicaData, ReplicaStatus)` 构造
   - + `is_p2p_proxy_replica()` / `get_p2p_tags()` / `get_p2p_priority()` / `get_p2p_memory_type()` / `get_p2p_client_id()`(声明) / `get_p2p_segment()` / `get_p2p_client()`
   - `ReplicaTypeVisitor` + `P2PProxyReplicaData` 分支；`data_` variant + 第4 alternative；`Descriptor` variant + `P2PProxyDescriptor` + `is_p2p_proxy_replica()`/`get_p2p_proxy_descriptor()` + `operator<<(Descriptor)` 友元
   - + `get_segment_id()`（memory + p2p proxy）
   - **移出**：`get_descriptor()` / `operator<<(Replica)` / `operator<<(Descriptor)` 由 inline 移到 `replica.cpp`（p2p proxy 分支需 `P2PClientMeta` 完整类型）
3. `src/replica.cpp`（**新建**，main 原无此文件）：含 memory/disk/local_disk/p2p_proxy 四分支的 `get_descriptor`/`operator<<`/`get_p2p_client_id`，`#include "p2p_client_meta.h"`
4. `include/master_config.h`：`MasterConfig`/`MasterServiceSupervisorConfig`/`WrappedMasterServiceConfig`/`MasterServiceConfig`/`MasterServiceConfigBuilder` 全面加 `client_crashed_ttl_sec`/`max_replicas_per_key`/`deployment_mode`（字段+构造拷贝+validate+builder setter）
5. `include/master_metric_manager.h` + `src/master_metric_manager.cpp`：+ P2P-RPC 指标（`heartbeat`/`get_write_route`/`add_replica`/`remove_replica` 及 batch 版本）声明+成员+构造初始化+inc 实现
6. `src/CMakeLists.txt`：`MOONCAKE_STORE_SOURCES` 加 `replica.cpp`（central 链接依赖）

**冲突点与解决**：
- `Segment` 重设计：p2p 用 `variant extra`，central 用平铺。→ 加法：main Segment 保留平铺 + 追加 `optional<P2PSegmentExtraData> p2p_extra` + 访问器。central 不读 p2p_extra，p2p 不读 base/te_endpoint。p2p 代码构造 Segment 时改用 `p2p_extra`（C2/C3 适配）。
- `ClientStatus` 分叉：central `{UNDEFINED,OK,NEED_REMOUNT}` vs p2p `{UNDEFINED,HEALTH,DISCONNECTION,CRASHED}`。→ p2p 侧改用独立 `P2PClientStatus`（C2 在 p2p 路径定义），main ClientStatus 不动。
- `replica.h` inline `get_descriptor`/`operator<<` 需 `P2PClientMeta` 完整类型 → 移到 `replica.cpp`，`replica.cpp` include `p2p_client_meta.h`（共享 cpp → p2p 头的构建期依赖，可接受）。
- `replica.cpp` main 原不存在 → 新建并加入 CMake。
- master_metric_manager 指标：`inc_mem_cache_nums` 等 main 已有，仅加 P2P-RPC 专属指标。serialize/summary 暂未追加（指标仍可用，仅不出现在 prometheus 输出——次要遗留）。

**遗留（C1 范围内次要）**：master_metric_manager 的 `serialize_metrics`/`get_summary_string` 未追加 p2p 指标输出；`InProcMasterConfig` 未加 `client_*_ttl_sec`（测试用，非必需）。

--- C1 完成。C2–C5 待续 ---

### C2 执行完成（P2P 管理层迁入 `include/p2p/`）

**新建文件**（10 头 + 7 cpp）：
- `include/p2p/p2p_types.h`：`P2PClientStatus` 枚举（HEALTH/DISCONNECTION/CRASHED/UNDEFINED）+ `operator<<`。替代 main 的 `ClientStatus`（p2p 专用）。
- `include/p2p/heartbeat_type.h`：原样迁入（HeartbeatTask/TierUsageInfo/SyncSegmentMeta* 等，p2p-only）。
- `include/p2p/p2p_segment_manager.h` + `src/p2p/p2p_segment_manager.cpp`：**扁平化** `SegmentManager`(基类) + `P2PSegmentManager`(子类) → 单一 `P2PMasterService`... → `P2PSegmentManager`（无 `SegmentManager` 类、去 virtual）。消除与 main `segment.h::SegmentManager` 同名冲突。
- `include/p2p/p2p_rpc_types.h`：合并 p2p 分支 `p2p_rpc_types.h`（WriteRoute/AddReplica/BatchSync 等）+ `rpc_types.h` 中 p2p-only 类型（`P2PGetReplicaListConfigExtra`/`GetReplicaListRequestConfig`/`Heartbeat*`/`RegisterClient*`/`QueryClientStatus*`）。`HeartbeatResponse.status`/`QueryClientStatusResponse.status` 用 `P2PClientStatus`。**不含** `GetReplicaListResponse`（复用 main 共享）、**不含** `CentralizedGetReplicaListResponseExtra`（central 走 main 代码）。
- `include/p2p/client_meta.h` + `src/p2p/client_meta.cpp`：迁入；`ClientStatus`→`P2PClientStatus`；`GetSegmentManager()` 返回 `shared_ptr<P2PSegmentManager>`；include `p2p_segment_manager.h`/`p2p_types.h`。
- `include/p2p/p2p_client_meta.h` + `src/p2p/p2p_client_meta.cpp`：迁入；override 返回 `shared_ptr<P2PSegmentManager>`。
- `include/p2p/client_manager.h` + `src/p2p/client_manager.cpp`：迁入；include 改 `p2p_rpc_types.h`（RPC 类型在 p2p 路径）；`ClientStatus`→`P2PClientStatus`。
- `include/p2p/p2p_client_manager.h` + `src/p2p/p2p_client_manager.cpp`：迁入（原样）。

**冲突点与解决**：
- `ClientStatus` 分叉 → p2p 全量改用 `P2PClientStatus`（client_meta/client_manager 的所有 `ClientStatus::` 引用改名）。
- `Segment` 构造适配：p2p 代码读 `segment.GetP2PExtra()`/`IsP2PSegment()`（C1 加法追加的访问器，可用）；p2p 不构造带 `extra` variant 的 Segment，改为 `p2p_extra`（P2P client 侧后续适配）。
- `client_manager.h` 依赖 RPC 类型 → 全部在 `p2p_rpc_types.h`（p2p 路径），不污染共享 `rpc_types.h`。

### C3 执行完成（P2P master 扁平化）

- `include/p2p/p2p_master_service.h`：**扁平化** p2p 分支 `master_service.h`(抽象基类) + `p2p_master_service.h`(子类) → 一个具体 `P2PMasterService`。去 `virtual`/`override`；`ObjectMetadata`/`MetadataShard`/`MetadataAccessor` 改私有嵌套具体类型；原纯虚 `GetShard`/`GetShardIndex`/`GetShardCount`/`GetClientManager`/`FilterReplicas`/各 hooks 改具体私有方法。成员：`P2PClientManager`、`metadata_shards_[1024]`、`max_replicas_per_key_`、`enable_ha_`、`view_version_`。
- `src/p2p/p2p_master_service.cpp`：合并 `master_service.cpp`(476 行基类逻辑) + `p2p_master_service.cpp`(480 行 P2P 逻辑)，`MasterService::`→`P2PMasterService::`。`GetReplicaList` 返回 main 共享 `GetReplicaListResponse`（`lease_ttl_ms=0`）。构造函数直接初始化 `enable_ha_`/`view_version_`/`max_replicas_per_key_` + 建 `P2PClientManager`。

**冲突点与解决**：
- `GetReplicaListResponse` 同名冲突（main `{replicas,lease_ttl_ms}` vs p2p `{replicas,centralized_extra}`）→ p2p 复用 main 共享类型，丢弃 `centralized_extra`，`resp.replicas = ...` 即可。
- main 的 `master_service.h/.cpp` **不动**（central 保持原样）；p2p 分支抽象基类 `master_service.h` 整个消失。

### C4 执行完成（P2P 包装层）

- `include/p2p/p2p_rpc_service.h` + `src/p2p/p2p_rpc_service.cpp`：自包含 `WrappedP2PMasterService`（持 `P2PMasterService master_service_`，**不继承** main `WrappedMasterService`）。合并 p2p 分支 `rpc_service.cpp`(公共 handler) + `p2p_rpc_service.cpp`(P2P handler)，`GetMasterService().`→`master_service_.`。`RegisterP2PRpcService` 注册全部（公共+P2P）handler。HTTP metrics/metric 线程自建。

### C5 执行完成（deploy mode + 入口分发 + CMake）

- `src/master.cpp`：+ `DEFINE_string(deployment_mode,...)`/`DEFINE_uint64(max_replicas_per_key,...)`/`DEFINE_int64(client_crashed_ttl,...)` + `InitMasterConf`/`LoadConfigFromCmdline` 对应项 + `client_crashed_ttl_sec` 默认 3x live ttl 逻辑 + deployment_mode 校验 + 非 HA 分支二选一分发（Centralization: main `WrappedMasterService`+`RegisterRpcService` 不变；P2P: `WrappedP2PMasterService`+`RegisterP2PRpcService`）。include `p2p_rpc_service.h`。
- `src/ha_helper.cpp`：include `p2p_rpc_service.h`；HA 启动按 `config_.deployment_mode` 二选一（central: main `WrappedMasterService`+`RegisterRpcService`；p2p: `WrappedP2PMasterService`+`RegisterP2PRpcService`）。
- `src/CMakeLists.txt`：`MOONCAKE_STORE_SOURCES` 加 7 个 p2p cpp。
- `mooncake-store/CMakeLists.txt`：`include_directories` 加 `include/p2p/`（使 `#include "p2p_xxx.h"` 可解析）。

**验证（grep，仅 review）**：
- `public MasterService` 在 `include/p2p/`+`src/p2p/` 为空 ✓（基类已取消）
- `centralized_rpc_service` 全仓引用为空 ✓
- p2p 目录 10 头 + 7 cpp ✓

---

## 5. 遗留与后续

**本轮未做（需后续）**：
1. **Client 侧**（`P2PMasterClient` 扁平化 + `p2p_client_service` + `real_client_main.cpp` deploy mode 分发）。
2. **测试迁移**（`tests/p2p/`：p2p_master_service_test 等；修复 central 测试引用已删 `centralized_rpc_service.h`——main 的 central 测试本就用 `rpc_service.h`，无需改）。
3. **master_metric_manager 的 `serialize_metrics`/`get_summary_string`** 追加 p2p 指标输出（当前指标可用但不出现在 prometheus 输出）。
4. **P2P client 侧 Segment 构造适配**：P2P client 构造 Segment 时改用 `p2p_extra`（C1 加法字段），而非 p2p 分支的 `extra` variant。
5. **Linux 编译验证 + 修复**（本轮无编译环境，全部为代码迁移，预期需少量编译修正）。
6. **`InProcMasterConfig`** 加 `client_*_ttl_sec`（测试用，非必需）。

**已知潜在编译风险点（待 Linux 验证）**：
- `replica.cpp` include `p2p_client_meta.h` → 共享 cpp 引入 p2p 头链（构建期依赖，应可编译）。
- `Segment` 的 `YLT_REFL` 追加 `p2p_extra` 字段：central client/master 序列化多一个空 optional 字段，需确认 coro_rpc/struct_json 兼容 optional 字段。
- p2p 分支部分宏（`NO_THREAD_SAFETY_ANALYSIS`/`GUARDED_BY`/`SharedMutexLocker`/`SpinRWLock`/`SpinRWLockLocker`）来自 main 的 `mutex.h`，需确认 main 已有（应已有，central 用同款）。
- `execute_rpc`/`ScopedVLogTimer` 来自 `rpc_helper.h`/`utils/scoped_vlog_timer.h`（共享），p2p wrapper 已 include。

---

## 6. C6 + C7（P2P client 侧）计划与风险

### 范围
- **C6** P2PMasterClient 扁平化（~865 行）
- **C7-leaves** client_rpc_types/task_handle/client_config_builder/peer_client/client_rpc_service/route_cache（~1826 行）
- **types.h 补枚举** HAClientState/HAEvent（additive）
- **C7-ha** async_metadata_notifier + ha_recovery_manager（~1159 行，依赖 C6）
- **C7-datapath** tiered_cache/ 整目录 + async_memcpy_executor + data_manager（~7036 行）

放置：全部进 `include/p2p/`+`src/p2p/`，tiered_cache 保留子目录（`include/p2p/tiered_cache/`、`src/p2p/tiered_cache/`）。

### 风险项（执行时重点验证）
1. **transfer_engine API 漂移**：`data_manager.cpp`(1170 行)、`async_memcpy_executor`、`tiered_cache` 重度依赖 transfer_engine/transport。若 main 的 transfer_engine 与 p2p 分支 API 不一致 → 编译期修复。**最大不确定项**，需 Linux 编译验证。
2. **`common.h` 依赖**：client_config_builder.h include `"common.h"`（外部共享头，非 mooncake-store/include）。需确认 main 仓库可解析（应在 mooncake-common 或 extern）。
3. **ascend_tier 条件编译**：`USE_ASCEND_CACHE_TIER` 控制 ascend_tier.cpp + Ascend ACL 库。默认 OFF 跳过；需在 main 的 `src/CMakeLists.txt` 加条件块（p2p 分支有，main 无）。
4. **无编译器**：~1 万行纯迁移，预期 Linux 编译期需少量修复（include 路径、API 漂移、宏差异）。
5. **C8 预告**（本轮不做）：`client_service.h` 同名冲突（main `class Client` vs P2P `class ClientService`）→ C8 时 P2P 版进 `include/p2p/client_service.h`，main 不动。
6. **共享类型兼容**：已确认 C7 文件不触 Segment 内部字段、不引 ClientStatus；用 MemoryType/DeploymentMode/P2PProxyDescriptor（C1 已加法兼容）。ha_recovery_manager 需 HAClientState/HAEvent（本轮加法补 types.h）。

### C6 关键改写
- 公共方法 RPC 指针 `&WrappedMasterService::*` → `&WrappedP2PMasterService::*`（P2P client 对接 P2P master）。
- RpcNameTraits 特化全指向 `&WrappedP2PMasterService::*`。
- main 的 `master_client.h/.cpp`（central 具体类）不动。

### C6 + C7 执行完成（P2P client 侧）

**C6：P2PMasterClient 扁平化**
- 新建 `include/p2p/p2p_master_client.h` + `src/p2p/p2p_master_client.cpp`：合并 `master_client.{h,cpp}`(基类) + `p2p_master_client.{h,cpp}`(子类) → 具体类 `P2PMasterClient`，去 virtual/继承。
- **关键改写**：公共方法 RPC 指针 `&WrappedMasterService::*` → `&WrappedP2PMasterService::*`（含 `ServiceReady`/`ExistKey`/`GetReplicaList`/`RegisterClient`/`Heartbeat` 等全部）；RpcNameTraits 特化全指向 `&WrappedP2PMasterService::*`（公共 + P2P 方法）。
- `src/CMakeLists.txt` 加 `p2p/p2p_master_client.cpp`。
- main 的 `master_client.h/.cpp` 不动。

**C7-leaves（原样迁入，无适配）**
- `include/p2p/`：`client_rpc_types.h`、`task_handle.h`、`client_config_builder.h`、`peer_client.h`、`client_rpc_service.h`、`route_cache.h`
- `src/p2p/`：`peer_client.cpp`、`client_rpc_service.cpp`、`route_cache.cpp`
- 验证：不引用 `rpc_types.h`/`master_client.h`/`client_service.h`（无共享头冲突）；`common.h` 在 `mooncake-transfer-engine/include/`（共享，已在 include 路径）。

**types.h 补枚举（additive）**
- 加 `HAClientState`（FULL/DEGRADED/SYNCING）+ `operator<<`/`toString` + `HAEvent`（MASTER_UNREACHABLE/MASTER_RECONNECTED）。central 忽略。

**C7-ha（原样迁入）**
- `include/p2p/async_metadata_notifier.h` + `src/p2p/async_metadata_notifier.cpp`、`include/p2p/ha_recovery_manager.h` + `src/p2p/ha_recovery_manager.cpp`：include `p2p_master_client.h`（经 include/p2p 路径解析）；用 `HAClientState`/`HAEvent`（已加）/`MemoryType`（C1 兼容）。

**C7-datapath（原样迁入）**
- `include/p2p/data_manager.h` + `src/p2p/data_manager.cpp`、`include/p2p/async_memcpy_executor.h` + `src/p2p/async_memcpy_executor.cpp`
- `include/p2p/tiered_cache/`（含 `scheduler/`、`tiers/` 子目录，13 头）+ `src/p2p/tiered_cache/`（9 cpp）整树迁入。

**CMake（`src/CMakeLists.txt`）**
- `MOONCAKE_STORE_SOURCES` 加 15 个 C7 cpp（leaves 3 + ha 2 + datapath 2 + tiered_cache 8）。
- 加 `USE_ASCEND_CACHE_TIER` 条件块：append `p2p/tiered_cache/tiers/ascend_tier.cpp` + ACL 库查找/链接/include（默认 OFF）。

**验证（grep）**
- `public MasterClient` 在 `include/p2p/` 为空 ✓
- p2p 目录 34 头 + 24 cpp ✓
- tiered_cache 子树（scheduler/ + tiers/）完整 ✓
- ha 文件 include `p2p_master_client.h` 经 include/p2p 路径可解析 ✓

**遗留/风险（待 Linux 编译验证）**
1. `data_manager.cpp`/`async_memcpy_executor`/`tiered_cache` 重度依赖 transfer_engine — API 漂移需编译期修。
2. C8（`client_service.h` 同名冲突 + `p2p_client_service` + `real_client_main` 分发）未做。
3. 整体 ~1 万行未编译验证。

---

## 7. C8（P2P client 侧）— 架构师要求与本轮执行

### 架构师要求（原话要点）
- `real_client_main.cpp`、`store_py.cpp` **对外接口相同**（不变）。
- `real_client.cpp` 负责业务逻辑包装；`client_service.cpp` 负责实现业务逻辑接口和资源初始化。
- **保持现状不变**（centralized client service 先保留）。合入冲突先解冲突。
- **但**：若 master 侧改数据结构（如 replica）导致 client service 接口变动 → **不保持 client service 继承关系，直接分家也可以**。

### 关键判断
- C1 的 replica/Segment 改动是**加法**，central `Client` 接口**未被强制改变** → 符合"保持现状"前提：central `Client` 不动。
- central `Client`（main `client_service.h`）与 p2p `ClientService`/`P2PClientService` 接口**根本性分叉**（Get/Put/Query 签名、config 类型、Ping vs Heartbeat）→ 确认"分家"成立：不共享基类。
- p2p 分支 `store_py.cpp`/`pyclient.h`/`real_client` 改了对外 API（删方法、改 config 类型、重命名）→ **必须保留 main 版本**，仅加法新增 P2P 入口。

### 本轮执行：P2PClientService 扁平化迁移（分家）

**决策点（架构师定）**：
1. **P2PClientService 迁移方式 = 扁平化（分家）**：吸收 p2p `ClientService` 基类 → 独立具体类 `P2PClientService`，无 `: public ClientService`。避免 `client_service.h` 同名冲突、避免基类 `GetMasterClient()`/`Create(CentralizedClientConfig)` 类型绑死，与 master 扁平化一致。
2. **config 翻译缝 = RealClient 合成默认**（下轮 RealClient 分发时实现）：外部 `ReplicateConfig` → RealClient 合成默认 `WriteRouteRequestConfig` 调 `p2p_->Put`。**风险**：丢弃 `ReplicateConfig` 语义（replica_num/with_soft_pin/preferred_segments 等），P2P 路径下这些参数不生效。需在 RealClient 分发实现时确认可接受，或后续给 P2PClientService 加 `ReplicateConfig` 重载。

**产出文件**：
- `include/p2p/p2p_client_service.h`（新建，扁平化）：合并 p2p `client_service.h`(基类 591) + `p2p_client_service.h`(子类 357) → 独立类。吸收 `WriteConfig` typedef + `QueryResult` 类 + 基类全部方法/成员（Stop/StopHeartbeat/Destroy/ConnectToMaster/InitTransferEngine/StartHeartbeat/HeartbeatThreadMain/HandleHeartbeatResponse/.../RegisterLocalMemory/InflightRequestGuard/...）+ P2P 方法/内部（Put/Get/BatchGet/Query/RouteIterator/...）。
- `src/p2p/p2p_client_service.cpp`（新建，扁平化）：合并 `client_service.cpp`(基类 716) + `p2p_client_service.cpp`(子类 1618)。`ClientService::`→`P2PClientService::`；删 `Create(CentralizedClientConfig)`+`#include "centralized_client_service.h"`；保留 `Create(P2PClientConfig)`；`HandleHeartbeatResponse` 的 `ClientStatus::`→`P2PClientStatus::`；`ClientService::Stop()/Destroy()` 调用内联为 `StopMetricsHttpServer();StopHeartbeat();` / `segment_ptrs_.clear();ascend_segment_ptrs_.clear();`。
- `src/CMakeLists.txt`：加 `p2p/p2p_client_service.cpp`。
- main 的 `client_service.h`(`Client`)/`real_client`/`store_py`/`pyclient.h` **不动**；p2p 基类 `client_service.h` 消失（无同名冲突）。

**关键改写（基类绑死点解除）**：
- `GetMasterClient()`：`virtual MasterClient& =0` → `P2PMasterClient& GetMasterClient()`（返回 `master_client_`，P2PMasterClient C6 已独立）。基类内联 `CalcCacheStats()`/`BatchQueryIp()`/`ConnectToMaster()`/`HeartbeatThreadMain()` 调 `GetMasterClient().Xxx()` — P2PMasterClient(C6) 有这些方法 ✓。
- 删 `Create(CentralizedClientConfig)` + `centralized_client_service.h` include。
- `ClientStatus::HEALTH/UNDEFINED` → `P2PClientStatus::HEALTH/UNDEFINED`。
- 删 `#include "master_client.h"`；改用 `p2p_master_client.h`。

**验证（grep）**：
- `public ClientService` 在 p2p 目录为空 ✓（分家成功）
- `centralized_client_service` 在 p2p_client_service.* 无引用 ✓
- `ClientStatus::` 无残留（仅 `P2PClientStatus::`）✓
- `master_client.h` include 不在 p2p_client_service.h ✓
- p2p 目录 35 头 + 25 cpp ✓

### 风险项（待 Linux 编译验证）
1. **transfer_engine API 漂移**：`InitTransferEngine`（基类吸收）+ `p2p_client_service.cpp` 数据通路重度用 transfer_engine（init/installTransport/registerLocalMemory/getLocalTopology/getLocalIpAndPort）。main 与 p2p 分支 API 可能漂移 → 编译期修。**最大不确定项**。
2. **`config.h`/`globalConfig()`**：基类 cpp include `"config.h"`（`globalConfig().max_mr_size`）。需确认 main 可解析（transfer-engine 共享头）。
3. **`GetMasterClient()` 返回 P2PMasterClient&**：基类内联方法调 `GetMasterClient().Connect/Heartbeat/BatchQueryIp/GetReplicaListByRegex/CalcCacheStats` — 需 P2PMasterClient(C6) 全部具备（已确认 ✓）。
4. **`calculate_total_size(replica)`**：`AsyncResolveRoutesFromMaster` 用此自由函数，需确认 main 的 replica.h/工具头具备（可能需补）。
5. **C7 依赖**：P2PClientService 依赖 data_manager/route_cache/peer_client/client_rpc_service/async_metadata_notifier/ha_recovery_manager/task_handle（C7 已迁）+ transfer_engine — 需 C7 + transfer_engine 编译通过。
6. **config 翻译缝风险**（决策点 2）：`ReplicateConfig` 语义在 P2P 路径丢弃，下轮 RealClient 实现时确认。

### C8 后续（未做）
- **RealClient 分发**：`real_client.cpp` 加 `mode_`(CENTRAL/P2P) + `setup_p2p(...)` + 逐方法分发（central `Client` vs `P2PClientService`）+ config 翻译缝实现。
- **`real_client_main.cpp`**：加 `--deployment_mode` flag（默认 Centralization）。
- **`store_py.cpp`**（mooncake-integration）：加 `setup_p2p_real_client`（加法，不动现有 API）。

---

## 8. C8 路径定夺：路径 X 不可行 → 确认路径 Y

### 探讨过的路径
- **路径 X**（central client = p2p 的 `CentralizedClientService`，采用 p2p config-templated RealClient）：被否决。
- **路径 Y**（central client = main 的 `Client`，RealClient 加法分发到 main Client / 扁平 P2PClientService）：**确认采用**。

### 路径 X 不可行的决定性证据（RPC 协议不匹配）
1. 恢复 p2p 历史中的 `centralized_master_client.h`（当前 p2p 分支被用户删除）：`CentralizedMasterClient final : public MasterClient`（p2p `MasterClient` 基类），加 `PutStart/PutEnd/GetFsdir/GetStorageConfig/CopyStart/MoveStart/...`。
2. p2p `MasterClient` 基类有 `Heartbeat(HeartbeatRequest)`（RPC `&WrappedMasterService::Heartbeat`）、`RegisterClient`、`QueryClientStatus` —— **p2p-era RPC**。
3. `CentralizedClientService` 继承 `ClientService` 基类 `HeartbeatThreadMain`，调 `GetMasterClient().Heartbeat(req)` → RPC `WrappedMasterService::Heartbeat`。
4. **合并仓 central master = main 的 `WrappedMasterService`**（C5 保留 main `rpc_service.h`），只有 `Ping`/`PutStart`/`PutEnd`（central-era），**无 `Heartbeat`/`RegisterClient`/`QueryClientStatus` RPC**。
5. → p2p `CentralizedMasterClient.Heartbeat` RPC 到 main central master 不存在的方法 → **协议断裂**。
6. p2p 当初是 master+client **双侧同时重构**到 Heartbeat（用户删的 `centralized_rpc_service.h` 即 p2p-era central master Heartbeat 版）。合并仓 central master 已定 main（Ping），故 central client 必须配 Ping → main 的 `Client`。**master/client RPC 协议必须配对**，非文件依赖问题。
7. "依赖改对接 main 代码"无法解决：心跳协议（Heartbeat vs Ping）是 `ClientService` 基类核心，改协议 = 回退 p2p 心跳重构 = central 恢复 main 逻辑 = main 的 `Client` = 路径 Y。

### 架构师细节标准 1 适用
"心跳逻辑变动大 → 把逻辑抽到 p2p 类，centralization 路径恢复 main 逻辑"。P2P 心跳逻辑在 `P2PClientService`（C8-partial 已迁），central 恢复 main 的 `Client`（Ping）。✓

### 路径 Y 最终方案（C8）
- **保留** C6（P2PMasterClient 扁平化）+ C8-partial（P2PClientService 扁平化）。
- **central client** = main 的 `Client`（Ping/PutStart，配 main central master ✓）—— 不动。
- **P2P client** = 扁平化 `P2PClientService`（Heartbeat/GetWriteRoute，配 P2P master ✓）—— C8-partial 已迁。
- **RealClient** 加法：`mode_`(CENTRAL/P2P) + `p2p_client_service_` 成员 + `setup_p2p()` + 逐 `_internal` 方法分发 + config 翻译缝。
- **`real_client_main.cpp`**：加 `--deployment_mode` flag。
- **`store_py.cpp`**：加 `setup_p2p_real_client`（加法，store_py 下转型；**pyclient.h 不动**）。

### C8-Y 执行完成（RealClient 加法分发）

**修改文件**：
1. `include/real_client.h`（加法）：
   - `#include "p2p_client_service.h"`
   - 加 `enum class ClientMode { CENTRAL, P2P } mode_`
   - 加 `std::shared_ptr<P2PClientService> p2p_client_service_`
   - 加 `int setup_p2p(...)` + `tl::expected<void,ErrorCode> setup_p2p_internal(...)`（public）
2. `src/real_client.cpp`（加法分发，23 个 P2P 分支）：
   - `setup_p2p`/`setup_p2p_internal`：`ClientConfigBuilder::build_p2p_real_client` → `P2PClientService::Create` → 存 `p2p_client_service_` + `mode_=P2P` + 建本地 `client_buffer_allocator_`（1GB）+ `RegisterLocalMemory` + 启 IPC。
   - 逐 `_internal` 方法早期 P2P 分支：`put_internal`/`put_from_internal`/`put_batch_internal`/`put_parts_internal`/`put_from_with_metadata`/`batch_put_from_internal`/`batch_put_from_multi_buffers_internal`/`get_into_internal`/`get_buffer_internal`/`batch_get_into_internal`/`batch_get_into_multi_buffers_internal`/`batch_get_buffer_internal`/`register_buffer_internal`/`unregister_buffer_internal`/`remove_internal`/`removeByRegex_internal`/`removeAll_internal`/`isExist_internal`/`batchIsExist_internal`/`getSize_internal`/`get_replica_desc`/`batch_get_replica_desc`/`tearDownAll_internal`。
   - config 翻译缝：写方法合成 `WriteConfig{WriteRouteRequestConfig{}}`（丢弃 ReplicateConfig 语义）；读方法合成 `ReadRouteConfig{}`。
   - `tearDownAll_internal` P2P 分支：`p2p_client_service_->Stop();Destroy();reset()` + `client_buffer_allocator_.reset()`。
   - `ping`/`map_shm_internal`/`unmap_shm_internal`/`setup_internal`/`initAll_internal` **不分发**（dummy TTL/IPC 在两模式共用；setup_internal/initAll 是 central 专用）。
3. `include/p2p/p2p_client_service.h` + `src/p2p/p2p_client_service.cpp`（C8-partial 微调）：`QueryResult` → `P2PQueryResult`（避免与 main `client_service.h::QueryResult` 同名冲突）。
4. `src/real_client_main.cpp`（加法）：`--deployment_mode` + P2P flags + main() 二选一分发（P2P→`setup_p2p_internal`；central→`setup_internal`）。
5. `mooncake-integration/store/store_py.cpp`（加法）：`setup_p2p_real_client` 方法 + `p2p_mode_` 标志 + `is_client_initialized()` 兼容 P2P（`store_->client_` 为 null 时 P2P 仍视为 initialized）。现有 `setup`/`setup_dummy`/`pub_tensor_with_tp` 系列等 **不动**（保 main Python API）。

**冲突点与解决**：
- `QueryResult` 同名冲突（main `client_service.h` vs p2p `p2p_client_service.h`）→ p2p 版重命名 `P2PQueryResult`（`Query`/`BatchQuery` 返回 `unique_ptr<P2PQueryResult>`）。
- `store_->client_` 为 null（P2P 模式）→ `is_client_initialized()` 加 `p2p_mode_` 短路。
- P2P `Get(key, allocator, ...)` 需非空 allocator → `setup_p2p_internal` 建 1GB `client_buffer_allocator_` + `RegisterLocalMemory`。

**风险项（待 Linux 编译验证）**：
1. **config 翻译缝**：`ReplicateConfig` 语义（replica_num/with_soft_pin/preferred_segments/prefer_alloc_in_same_node）在 P2P 路径**全部丢弃**（合成默认 `WriteRouteRequestConfig{}`）。P2P 副本数由 master `max_replicas_per_key` 管。**架构师已接受**。
2. **transfer_engine API 漂移**：`P2PClientService`（C8-partial）+ `setup_p2p_internal` 重度依赖 transfer_engine → 编译期修。
3. **P2P dummy client 语义**：`ping` 在 P2P 模式仍用本地 TTL 队列（不触 master），dummy client monitor 共用。P2P 模式下 dummy client 行为未完全验证。
4. **`store_py` 跨模块**：mooncake-integration 需 include `include/p2p/` 路径（CMake include 已加）。
5. **C7+C8-partial 依赖链**：P2PClientService → data_manager/route_cache/peer_client/.../transfer_engine 需全部编译通过。
6. **`put_internal` P2P 分支**：用 `client_buffer_allocator` 临时分配 buffer 持有 value bytes，`split_into_slices` 接管 handle；Put 同步 Wait 后 buffer 释放。若 P2P Put 异步持有 buffer 超出作用域需复核（P2PClientService::Put 内 `task_handle_ptr.value()->Wait()` 同步）。

**验证（grep）**：
- P2P 分支数 23，`p2p_client_service_->` 引用 24 ✓
- `P2PQueryResult` 重命名一致 ✓
- `pyclient.h` 未改（保 main Python API）✓

---

## 9. 路径 X-revised（保留 p2p client 架构 + 抽心跳 + 对接 main）

> 架构师最终决定：client service 保留 p2p 重构架构（ClientService 基类 + CentralizedClientService + P2PClientService 子类），把基类依赖的 p2p RPC 心跳逻辑抽到 P2PClientService，CentralizedClientService 对接 main 的 Ping master。RealClient 也换 p2p config-templated 版（反转 C8-Y），Python API 变化（已接受）。

### 决定性冲突与解决（执行前已核实）
1. **RegisterClient p2p RPC**（main master 无）：CentralizedClientService::RegisterClient 改 Ping+MountSegment（不调 master RegisterClient RPC）。
2. **task 方法 + task_manager.h**（main 无）：CentralizedMasterClient 删 task 方法 + include（已确认 CentralizedClientService 不调用，死代码）。
3. **GetMasterClient 返回类型 + MasterClient 同名**：引入 `MasterClientInterface`（4 方法：Connect/BatchQueryIp/GetReplicaListByRegex/CalcCacheStats）；基类 `GetMasterClient()` 返回 `MasterClientInterface&`；P2PMasterClient(C6 扁平)加继承；CentralizedMasterClient 组合包装 main MasterClient。**不恢复 p2p MasterClient 基类，不反转 C6**。
4. **CentralizedMasterClient RPC 目标**：纯委托 main MasterClient（invoke_rpc 已绑 main `&WrappedMasterService::*`），无需改指针。
5. **QueryResult lease**：main `Client::Get(key,QueryResult,slices)` 在 `:606` 调 `query_result.IsLeaseExpired()` → **lease 硬依赖**。p2p 基类 QueryResult 加 `lease_timeout` 字段（P2P 填 `time_point::max()`，Central 填 master lease）。
6. **Ping 线程移植**：CentralizedClientService 移植 main `Client::PingThreadMain`。
7. **view_version_/HA etcd**：移到 P2PClientService；Centralized 不做 HA view。

### 已执行：S1-S3
- **S1** `include/p2p/master_client_interface.h`（新建）：4 纯虚方法。
- **S2** `include/p2p/p2p_master_client.h`：`P2PMasterClient : public MasterClientInterface`，4 方法加 `override`（C6 扁平不反转）。
- **S3** `include/p2p/client_service.h` + `src/p2p/client_service.cpp`（p2p 分支迁入 + 抽心跳）：
  - 移除心跳逻辑（StartHeartbeat/HeartbeatThreadMain/HandleHeartbeatResponse/HandleHeartbeatTaskResult/ReconnectToMaster/WaitForNextHeartbeat/build_heartbeat_request）→ 纯虚 `StartKeepalive`/`StopHeartbeat`（子类实现）。
  - `GetMasterClient()` → `virtual MasterClientInterface& = 0`。
  - `ConnectToMaster` 去 etcd HA 分支（仅 `GetMasterClient().Connect`）。
  - 移除心跳成员（master_view_helper_/heartbeat_*/view_version_/connection_interrupted_）。
  - `QueryResult` 加 `lease_timeout` + `IsLeaseExpired()`（保 main Get 语义）。
  - 删 `#include "master_client.h"`（避免与 main 同名冲突），改 `master_client_interface.h`。
  - `GetViewVersion()` 改 virtual 默认返回 0。
- 验证：`public MasterClientInterface` ✓、`MasterClientInterface& GetMasterClient` ✓、`StartKeepalive=0` ✓、cpp 无心跳 impl ✓、QueryResult lease 7 处 ✓、无 master_client.h include ✓。

### 已执行：S4-S9（完成）
- **S4** `include/p2p/centralized_master_client.h`（新建，header-only 组合包装）：`: public MasterClientInterface`，持 `shared_ptr<mooncake::MasterClient>`（main 的），全方法委托。删 task 方法 + task_manager.h include。`GetReplicaList`/`BatchGetReplicaList` 接受 `ReadRouteConfig` 但丢弃（main master 不用 route config）。暴露 `Ping()` 供 CentralizedClientService Ping 线程用。
- **S5** `include/p2p/centralized_client_service.h` + `src/p2p/centralized_client_service.cpp`（p2p 分支迁入 + 改）：
  - `GetMasterClient()` 返回 `MasterClientInterface&`（协变 CentralizedMasterClient）。
  - `RegisterClient()` **改 Ping+返回 view_version**（不调 master RegisterClient RPC；main master 无此 RPC）。
  - 加 `StartKeepalive`/`StopHeartbeat`/`PingThreadMain`（移植 main Client 的 Ping 线程逻辑，调 `master_client_.Ping()`）。
  - 删 `build_heartbeat_request()` override。
  - `Query`/`BatchQuery` 的 `centralized_extra->lease_ttl_ms` 改为 main flat `lease_ttl_ms`（GetReplicaListResponse 是 main 版）。
  - 加 `ping_thread_`/`ping_running_` 成员。
- **S6** `include/p2p/p2p_client_service.h` + `src/p2p/p2p_client_service.cpp`（替换 C8-partial 扁平版为子类）：
  - `: public ClientService`（子类恢复）。
  - `GetMasterClient()` 返回 `MasterClientInterface&`（协变 P2PMasterClient）。
  - 心跳逻辑从基类移入：`StartKeepalive`/`StopHeartbeat`/`HeartbeatThreadMain`/`HandleHeartbeatResponse`/`HandleHeartbeatTaskResult`/`ReconnectToMaster`/`WaitForNextHeartbeat`/`build_heartbeat_request`。
  - 恢复 `master_view_helper_`/`heartbeat_*/view_version_/connection_interrupted_` 成员。
  - `HeartbeatThreadMain` 调 `master_client_.Heartbeat(req)`（直接调，不经接口）。
  - 删 `P2PQueryResult`（统一 p2p `QueryResult`，带 lease）。
- **S7** RealClient/pyclient/store_py/real_client_main/dummy_client 全换 p2p 版（反转 C8-Y）：从 p2p 分支复制替换。
- **S8** 删 main 孤儿：`include/client_service.h`(`Client`) + `src/client_service.cpp`。保留 `master_client.h/.cpp`（CentralizedMasterClient 包装它）。
- **S9** CMake：`MOONCAKE_STORE_SOURCES` 用扁平 `p2p/` 路径（非 `p2p/master/`+`p2p/client/` 子目录）；加 `p2p/client_service.cpp`+`p2p/centralized_client_service.cpp`+`p2p/p2p_client_service.cpp`；删 stale `p2p/master/`+`p2p/client/` 子目录。

**验证（grep）**：
- `public ClientService` 在 p2p_client_service.h ✓（子类恢复）
- `MasterClientInterface& GetMasterClient` 在 centralized + p2p client service ✓
- `PingThreadMain`/`StartKeepalive` in centralized ✓
- `master_client_.Ping()` in RegisterClient ✓（不调 RegisterClient RPC）
- main `client_service.h` 删除 ✓；`master_client.h/.cpp` 保留 ✓
- CMake 有 `centralized_client_service` + `client_service` + `p2p_client_service` ✓
- p2p 目录 27 cpp + 39 headers ✓

**风险项（待 Linux 编译验证）**：
1. **transfer_engine API 漂移**：CentralizedClientService(1938)+P2PClientService(1618)+RealClient(1710) 重度依赖 → 编译期修。
2. **GetReplicaListResponse 类型**：CentralizedClientService 用 main 版（flat lease_ttl_ms），已改 ✓。P2PClientService 用 p2p 版（无 centralized_extra）—— 但 p2p_rpc_types.h 已删 GetReplicaListResponse（复用 main），p2p master 返回的也是 main 版。需编译验证 P2P Query 路径。
3. **RealClient/pyclient/store_py 全换**：Python API 变化（WriteConfig/ReadRouteConfig/setup_p2p_real_client，删 setup_real/pub_tensor_with_tp 等）—— 已接受。
4. **main master_client.h 同名**：CentralizedMasterClient include main `master_client.h`；p2p 路径无 p2p `master_client.h`（已删）→ 无冲突 ✓。
5. **CentralizedQueryResult lease**：继承 p2p QueryResult（带 lease_timeout），构造调 `QueryResult(replicas, lease)` ✓。
6. **store_py 跨模块**：mooncake-integration 需 include `include/p2p/` 路径 ✓（CMake 已加）。

---

## 9. Linux 编译验证与修复记录 (2026-08-03 ~ 2026-08-04)

> 环境：`vllm18-mooncake` 容器 (vllm/vllm-openai:v0.18.0-aarch64, Ubuntu 22.04, CUDA 12.9, Python 3.12)
> 编译工具：cmake 4.4.0 (venv `/opt/mooncake-venv`), gcc 11, yalantinglibs (系统安装)
> 构建命令：`cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DPython3_EXECUTABLE=/opt/mooncake-venv/bin/python3 -DBUILD_UNIT_TESTS=OFF`
> 编译目标：`all` (mooncake_store + mooncake_master + mooncake_client + engine + store)

### 9.1 Merge 过程

- `git stash` 保存本地编译修复 → `git pull --ff-only origin feat/p2p-merge`（Fast-forward 到 `7880b8a0 realclient p2p distribution`）→ `git stash pop`
- 冲突文件（2 个）：`p2p_client_service.h`（注释冲突，取 upstream）、`real_client.cpp`（`remove_internal` P2P 分支，取 upstream 显式 error handling 写法）
- 残留 fix：`real_client.cpp` upstream 版本 `return {}` 缺分号（`;`），补上

### 9.2 编译错误分类与修复

#### 9.2.1 共享类型缺失（C1 遗留）

| 错误 | 修复 |
|------|------|
| `ErrorCode::NOT_IMPLEMENTED` 未定义 | `types.h` ErrorCode 枚举追加 `NOT_IMPLEMENTED = -1600` |
| P2P ErrorCode 值（`CLIENT_ALREADY_EXISTS`/`CLIENT_UNHEALTHY`/`CAS_FAILED`/`REPLICA_ALREADY_EXISTS`/`REPLICA_NOT_FOUND`/`REPLICA_NUM_EXCEEDED`/`EMPTY_REPLICAS`/`TIER_NOT_FOUND`/`DATA_COPY_FAILED`/`SHUTTING_DOWN`/`ASYNC_ENQUEUE_FAILED`/`INACCESSIBLE_MASTER`）在 `toString()` 中缺失 | `types.cpp` 补全所有 P2P ErrorCode 的 string 映射 |
| `ObjectIterateStrategy` 未声明 | `p2p_types.h` 新增枚举 `{ORDERED, RANDOM, CAPACITY_PRIORITY}` + `operator<<` |
| `ReplicaLocation` 未声明 | `p2p_types.h` 新增结构 `{std::string key; UUID tier_id; size_t size;}` |
| `SpinRWLock` / `SpinRWLockLocker` 不存在 | `mutex.h` 新增 spin 读写锁（`std::atomic<uint32_t>` 实现）+ RAII locker |
| `MutexLocker(Mutex*, bool)` 无匹配构造函数 | `mutex.h` 新增二参构造函数（`bool acquire` 控制是否立即 lock） |
| `MOONCAKE_CPU_RELAX` 未声明 | `mutex.h` 新增宏（x86→`_mm_pause()`，其他→`std::this_thread::yield()`） |

#### 9.2.2 Allocator 适配（C1 加法字段）

| 错误 | 修复 |
|------|------|
| `AllocatedBuffer::getSegmentId()` 不存在 | `allocator.h`：`AllocatedBuffer` 构造新增 `const UUID& segment_id` + `segment_id_` 成员 + `getSegmentId()` 方法；`BufferAllocatorBase` 新增 `virtual UUID getSegmentId() const = 0`；`CachelibBufferAllocator`/`OffsetBufferAllocator` 构造新增 `const UUID& segment_id` + `segment_id_` 成员 + `getSegmentId()` override |
| `allocator.cpp` 中 `make_unique<AllocatedBuffer>` 参数不足 | `allocator.cpp` 两处 allocate 传 `segment_id_` |
| `segment.cpp` 构造 allocator 参数不足 | 传 `segment.id` 作为第5参数 |

#### 9.2.3 P2P 代码适配

| 错误 | 修复 |
|------|------|
| `p2p_master_client.cpp:3` `ylt/coro_rpc/impl/coro_rpcclient.hpp` 不存在 | 改为 `ylt/coro_rpc/impl/coro_rpc_client.hpp`（匹配系统安装的 yalantinglibs 版本） |
| `QueryResult` 重定义（`client_service.h` vs `p2p_client_service.h`） | P2P 版重命名 `P2PQueryResult`（C8-Y upstream 已同步做此改名） |
| `tiered_backend.cpp` `Segment.extra` 不存在 | 改为 `Segment.p2p_extra`（C1 加法字段） |

#### 9.2.4 `std::unordered_map<UUID, ...>` 缺少 `boost::hash<UUID>`

| 错误 | 修复 |
|------|------|
| `client_scheduler.h/.cpp` 多处 `unordered_map<UUID,...>` 使用 deleted 构造函数 | 全部补 `boost::hash<UUID>` + `#include <boost/functional/hash.hpp>` |
| `tiered_backend.h/.cpp` 同上 | 补 `boost::hash<UUID>` |
| `SchedulerPolicy::Decide` / `LRUPolicy::Decide` / `SimplePolicy::Decide` / `BuildReclaimPlan` / `CollectTierStats` 签名不匹配 | 统一返回类型/参数类型为 `unordered_map<UUID, ..., boost::hash<UUID>>`，`scheduler_policy.h` 补 boost include |

#### 9.2.5 StorageBackend 接口补全

| 错误 | 修复 |
|------|------|
| `StorageBackendInterface` 无 `MarkKeyDeleted` | `storage_backend.h`：interface 新增 `virtual MarkKeyDeleted(key)` (默认 no-op)；`StorageBackendAdaptor` 新增 override；`BucketStorageBackend` 新增 override |
| `BucketStorageBackend` 无 `SelectBucketForEviction` / `EvictBucket` | `storage_backend.h`：新增两个 public 方法 + `bucket_valid_keys_` 成员；`storage_backend.cpp`：`BatchOffload` 中初始化 `bucket_valid_keys_`；`StorageBackendAdaptor::MarkKeyDeleted` 实现（删除文件+更新统计）；`BucketStorageBackend::MarkKeyDeleted`（移除 object_bucket_map_ 映射+递减 valid_keys）；`SelectBucketForEviction`（碎片率>50% 优先选最旧 bucket）；`EvictBucket`（移除 bucket 元数据+删除文件） |
| 初始编辑误将 Bucket 方法插入 `OffsetAllocatorStorageBackend` | 重新定位到 `BucketStorageBackend` 类正确位置 |

#### 9.2.6 RealClient / store_py 编译修复

| 错误 | 修复 |
|------|------|
| `real_client.cpp:843` `expected<void>::map([](auto){})` 非法 | Upstream 已改为显式 `auto r = ...Remove(key); if (!r) return ...; return {};`（补缺失分号 `;`） |
| `real_client_main.cpp:84` `std::optional<string>` 无法转为 `const string&` | `real_client.h` + `real_client.cpp` 中 `setup_p2p_internal` 的 `rdma_devices` 参数改为 `std::optional<std::string>`，内部使用改为 `rdma_devices && !rdma_devices->empty()` |
| `store_py.cpp:1048` `PyClient` 无 `setup_p2p` | `self.store_->setup_p2p(...)` → `std::static_pointer_cast<RealClient>(self.store_)->setup_p2p(...)`（`setup_p2p` 在 `RealClient` 上，不在 `PyClient` 基类） |
| `mooncake-integration` 缺少 `include/p2p/` include 路径 | `mooncake-integration/CMakeLists.txt` 新增 `include_directories("../mooncake-store/include/p2p")` |

### 9.3 编译结果

**全量编译通过，0 错误。** 产物：

| 产物 | 状态 |
|------|------|
| `libmooncake_store.a` | ✅ |
| `mooncake_master` | ✅ |
| `mooncake_client` | ✅ |
| `engine` (transfer engine) | ✅ |
| `store.cpython-312-aarch64-linux-gnu.so` (Python 模块) | ✅ |

### 9.4 说明

- 单元测试因容器未装 gtest 暂以 `-DBUILD_UNIT_TESTS=OFF` 构建。`BUILD_UNIT_TESTS` 在 `common.cmake` 中默认为 ON，但 gtest 缺失导致 `default_config_test` 编译失败。需安装 gtest 后开启测试。
- `extern/yalantinglibs` 非 git submodule，由 `dependencies.sh` 系统安装到 `/usr/local/include/ylt`。容器已预装，无需重新下载。
- `extern/pybind11` 为 git submodule，rsync 时需排除 `extern/` 目录以避免覆盖已 checkout 的子模块。
- `master_metric_manager` 的 `serialize_metrics`/`get_summary_string` 未追加 P2P 指标输出（C1 遗留），指标可用但不出现在 Prometheus 输出。
- `InProcMasterConfig` 未加 `client_*_ttl_sec`（C1 遗留），非必需。

---

## 10. P2P 目录重组：按 master/client 拆分 (2026-08-11)

### 动机

原始方案将所有 P2P 文件平铺在 `include/p2p/` 和 `src/p2p/` 下，随着文件增多（54 个文件），master 侧和 client 侧的代码混杂在一起，不利于维护和 code review。

### 重组方案

在 `include/p2p/` 和 `src/p2p/` 下新建 `master/` 和 `client/` 子目录，按**部署角色**分类：

```
mooncake-store/
├── include/p2p/
│   ├── heartbeat_type.h          # 共享
│   ├── p2p_rpc_types.h           # 共享
│   ├── p2p_types.h               # 共享
│   ├── master/
│   │   ├── client_manager.h
│   │   ├── client_meta.h
│   │   ├── p2p_client_manager.h
│   │   ├── p2p_client_meta.h
│   │   ├── p2p_master_service.h
│   │   ├── p2p_rpc_service.h
│   │   └── p2p_segment_manager.h
│   └── client/
│       ├── async_memcpy_executor.h
│       ├── async_metadata_notifier.h
│       ├── client_config_builder.h
│       ├── client_rpc_service.h
│       ├── client_rpc_types.h
│       ├── data_manager.h
│       ├── ha_recovery_manager.h
│       ├── p2p_client_service.h
│       ├── p2p_master_client.h
│       ├── peer_client.h
│       ├── route_cache.h
│       ├── task_handle.h
│       └── tiered_cache/          # 含 scheduler/、tiers/ 子目录
│
└── src/p2p/
    ├── master/
    │   ├── client_manager.cpp
    │   ├── client_meta.cpp
    │   ├── p2p_client_manager.cpp
    │   ├── p2p_client_meta.cpp
    │   ├── p2p_master_service.cpp
    │   ├── p2p_rpc_service.cpp
    │   └── p2p_segment_manager.cpp
    └── client/
        ├── async_memcpy_executor.cpp
        ├── async_metadata_notifier.cpp
        ├── client_rpc_service.cpp
        ├── data_manager.cpp
        ├── ha_recovery_manager.cpp
        ├── p2p_client_service.cpp
        ├── p2p_master_client.cpp
        ├── peer_client.cpp
        ├── route_cache.cpp
        └── tiered_cache/          # 含 scheduler/、tiers/ 子目录
```

### 分类标准

按代码**运行在哪个节点上**分类：

| 分类 | 文件数 | 说明 |
|------|--------|------|
| **master/** | 7 头 + 7 cpp | 运行在 master 节点：管理客户端注册/心跳、segment 挂载/卸载、副本分配、密钥操作。`client_manager`/`client_meta` 虽名字带 client，但是 master 侧管理已连接客户端的数据结构，归入 master。 |
| **client/** | 12 头 + 9 cpp | 运行在 client 节点：连接 master、buffer 注册、数据传输、tiered cache、HA 恢复、peer-to-peer RPC。 |
| **p2p/ (共享)** | 3 头 | 被 master 和 client 共同引用：`heartbeat_type.h`、`p2p_rpc_types.h`、`p2p_types.h`。 |

### 配套修改

1. **CMakeLists.txt**（`mooncake-store/CMakeLists.txt`）：`include_directories` 新增 `include/p2p/master/` 和 `include/p2p/client/`。
2. **CMakeLists.txt**（`mooncake-store/src/CMakeLists.txt`）：`MOONCAKE_STORE_SOURCES` 中 p2p 源文件路径从 `p2p/xxx.cpp` 改为 `p2p/master/xxx.cpp` 或 `p2p/client/xxx.cpp`。
3. **`#include` 路径**：无需修改。所有 p2p 内部 include 使用相对路径（如 `#include "client_manager.h"`），CMake 新增的 include 目录使其自动解析。

### 统计

| 目录 | 头文件 | 源文件 | 合计 |
|------|--------|--------|------|
| `include/p2p/` (共享) | 3 | 0 | 3 |
| `include/p2p/master/` | 7 | — | 7 |
| `include/p2p/client/` | 12 | — | 12 |
| `src/p2p/master/` | — | 7 | 7 |
| `src/p2p/client/` | — | 9 | 9 |
| **总计** | **22** | **16** | **38** |
