# 运行模式、本地存档与启动规范（Python 版）

> 对应根索引 [AGENTS.md](../../AGENTS.md)。本规范固化 Python 服务端平替 C++ 服务端的五大核心架构约束，适用于运行模式装配、本地持久化、集群存储接入及启动编排改动。

---

## 1. 平替 C++ 服务端定位

- **全量契约与行为对齐**：Python 服务端面向客户端与外部依赖，全协议（`.proto` / `message_id.txt` / `event_id.txt`）、全配置表、全微服务（Gate / Scene / Battle / Login / DB / DataService / Router / Match / SceneManager / PlayerLocator / PlayerInbox / Chat / Mail / Rank / Friend / Payment / Trade / Guild / MallGame / GmAdmin / ConfigNode / ContentGuard / SaTokenAuth）以及核心玩法与战斗逻辑均与 `xuanming-server-mmo-cpp` 保持契约与行为一致，客户端无需适配即可直接无缝平替。
- **权威基线追踪**：当前对齐 C++ 基线 Commit 为 `60b0bd6b30feb6fd60bc0654ebff73a4b5bdf11b`（短 SHA：`60b0bd6b3`，含工作区至消息号 `573` 与 83 张配表 v101）；增量同步流程见 [AGENTS.md §4](../../AGENTS.md#4-对照-c-仓增量移植流程)。

---

## 2. Windows 单进程本地开发范式

- **双击一键单进程启动**：保留面向 Windows 本地开发的「双击一键单进程启动」范式：
  - 一键脚本：`启动玄冥服务器.bat`
  - 标准 CLI 入口：`python -m xuanming run local --config config/local.toml`
- **全量进程内托管**：单进程同时托管 Gate + Scene + Battle 及 `LocalServiceHub`（进程内直接复用正式 Friend / Chat / Mail / Guild / Team / Match / Rank / Payment / Trade / ContentGuard 微服务与 `Memory*Store`），使用内存目录、ProtoJSON 角色存档与 `local_accounts.json` 本地角色名册，**零外部中间件依赖**（无需安装或启动 MySQL / TiDB / Redis / etcd / Kafka）。
- **回环绑定与日志诊断**：
  - 本地模式强制仅监听回环地址（`127.0.0.1` / `::1`），防止开发节点暴露至局域网或公网。
  - 静默控制台启动日志统一落盘至 `run/logs/server-console.log`；若进程异常退出，启动脚本必须自动展示日志末尾 **40 行**并执行 `pause`，避免窗口闪退丢失现场。

---

## 3. 本地 PB 直转 ProtoJSON 存档规范

- **官方 ProtoJSON 编解码**：本地模式 `JsonPlayerStore` 在传入 `protocol` 时，必须使用官方 `google.protobuf.json_format.MessageToDict(..., preserving_proto_field_name=True)` 与 `google.protobuf.json_format.ParseDict(...)` 将各行 `player_database` Protobuf 载荷直接持久化为人类可读的 UTF-8 格式化 JSON（`indent=2, ensure_ascii=False, sort_keys=True`）。
- **文档版本与分组结构**：
  - 写入版本为 `format_version = 2`，玩家记录通过 `rows_json` 按 `PlayerRowKind` 枚举名分组存储：`GROWTH`、`PROFILE`、`CHARACTER`、`WALLET`、`BAG`、`LOCATION`。
  - **向后兼容读取**：支持无缝读取历史 `format_version = 1`（`rows` 内为 base64 编码的二进制 PB 行）；首次写入时将文档规范化升级为 `format_version = 2`。
  - 若 `rows_json` 中出现非法字段名、未知 proto 字段或损坏 JSON，必须 fail-closed 抛出 `StoreUnavailable`，不得静默丢弃字段；若原始 PB 行包含前向兼容的未知 wire 字段，仅对该行附带可选 `rows_raw` 保真字节，正常行不输出 `rows_raw`。
- **原子落盘与跨进程串行化**：
  - 跨进程并发写入通过 `FileLock`（`<path>.lock`）串行化。
  - 落盘必须先写入同目录临时文件并执行 `flush` + `os.fsync`，再经 `os.replace` 原子替换目标文件；在 Windows 上针对杀毒/索引器短暂占用 `PermissionError` 执行退避重试（默认最多 12 次），禁止跳过原子替换直接截断原文件。
- **与集群存储语义一致**：本地 `JsonPlayerStore` 必须严格复用与集群 `db`（`PlayerStore` / MySQL / TiDB）完全一致的 `PlayerRowCodec` 分行契约、`owner_epoch` 写权围栏与 `save_seq` 单调递增幂等语义，确保本地跑通的业务在集群下行为一致。

---

## 4. 条件路由与严禁静默降级

- **统一工厂条件装配**：本地与集群运行模式（`local` vs `cluster`）通过统一存储工厂（如 `xuanming.data.create_player_store`）按运行模式条件选择进程内本地实现（`JsonPlayerStore`）或远程多节点服务（`GrpcClient` + `DataServicePlayerStore`），避免业务节点散落环境分支。
- **集群模式 Fail-Fast 拒启**：
  - `local=False`（集群模式）缺少必备端点（如 `settings.player_store`）、鉴权密钥（如 `MMORPG_PLAYER_STORE_SECRET_SCENE`，至少 32 字节）或数据库驱动时，**必须在启动装配阶段立即抛出异常拒启**。
  - **严禁静默降级**：任何集群节点在远程存储、配置或密钥缺失时，绝对禁止静默回退到本地 JSON 或 SQLite 存档，防止多节点脑裂或线上数据写散至本机磁盘。

---

## 5. 多节点集群一键极速并行启动器

- **纯标准库并行编排**：保留 `tools/supervisor.py` 作为本地多节点与集群联调的并行进程启动器，不新增第三方进程管理依赖：
  - **并行拉起与真实探活**：按进程表并行 `spawn` 各服务进程，并通过真实 TCP 端口连通性探活判定就绪状态（而非盲等固定秒数）。
  - **独立日志与退避重启**：每个受管进程输出独立落盘至 `run/logs/<name>.log`；异常退出时按指数退避策略重启并记录重启次数。
  - **Windows 优雅停机与进程树清理**：在 Windows 下以 `CREATE_NEW_PROCESS_GROUP` 启动子进程，停机时先发送 `CTRL_BREAK_EVENT` 触发节点优雅排空与存盘屏障，超时后使用 `taskkill /T /F` 清理残留进程树，避免孤儿进程占端口。
- **进程表与一键脚本**：
  - 进程清单：`config/processes.local.json`（本地多进程联调）与 `config/processes.cluster.json`（多节点集群）。
  - Windows 一键入口：`启动-本地存档.bat` 与 `启动-多节点集群.bat`。
