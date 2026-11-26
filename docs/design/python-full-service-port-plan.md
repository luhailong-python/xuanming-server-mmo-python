# Python 全业务服务去 Go 化移植设计方案

> 关联权威参考源：`xuanming-server-mmo-cpp/go/`（只读参照，不修改 C++/Go 仓库任何代码）
> 目标仓库：`xuanming-server-mmo-python`（全部在 `main` 分支推进，每阶段联调验证通过后提交并推送）

## 1. 需求分析

- **核心目标**：将 `xuanming-server-mmo-python` 集群模式目前仍依赖的外部 Go 业务服务全部移植为纯 Python 服务，使整个业务集群（Gate、Scene、Battle、Portal、Login、DataService 及所有微服务）均由 Python 实现；仅保留 Redis、etcd、Kafka、MySQL/TiDB 等基础设施组件。
- **兼容性要求**：
  - 沿用 C++/Go 权威 `proto/` 契约与 `tools/build_protocol.py` 代码/描述符生成机制；
  - 线协议、gRPC 路径、服务间 HMAC/Ed25519 鉴权、Redis 键与 Lua 脚本、MySQL/TiDB 表结构、etcd 注册键、Kafka 主题分区与 C++/Go 保持对齐，确保单服务独立替换与端到端联调一致；
  - 战斗玩法保持现有 Python 实现不变。
- **技术栈约束**：全部基于 Python 标准库与项目既有依赖（`asyncio`、`grpcio`/`grpc.aio`、`protobuf`、`aiomysql`、`redis.asyncio`、`aiokafka`、`cryptography`、`aiohttp`），不引入未经程序确认的新第三方库。

## 2. 思想分析

- **以契约和状态机为中心，不照搬 Go 样板**：
  - Go 侧因历史演进与 go-zero 脚手架存在大量生成包装、运维子命令与多套历史兼容路径；Python 版严格以**对外 RPC 契约、持久化状态机、并发防错（Fence/CAS/幂等键）**为单一事实源，用 Python `asyncio` + `dataclass(frozen=True)` + `Protocol` 表达同等语义。
- **Actor / 边界隔离原则**：
  - 跨服务、跨节点通信全部走不可变 Protobuf 消息（gRPC / Kafka）；服务内部按职责拆分为 `config`（配置校验）、`model`/`errors`（领域值对象与错误码）、`store`（SQL/Redis 持久化）、`service`（gRPC 适配与鉴权）、`node`（生命周期与 etcd 注册）。
  - 服务内部采用清晰直接的方法调用，避免无意义的内部消息化过度设计。
- **模式选型说明**：
  - **Repository / Adapter（仓储与适配器模式）**：将 SQL/Redis 存储抽象为 `Protocol`，在单元测试中注入内存双生体（in-memory twin）并在具备真实 MySQL/Redis 时无缝切换真实存储，解决外部基础设施不可达时无法回归测试的痛点。
  - **Chain of Responsibility / Decorator（鉴权包装器）**：统一复用于 `grpc_runtime.serve_grpc` 的服务间 HMAC 校验与 Gate Ed25519 会话校验，避免每个 RPC 重复编写验签逻辑且防止漏校验（fail-closed）。
  - **Strategy（路由与分派策略）**：`client_rpc_router` 与 `match` 房间分配将节点发现、释放轨道过滤与负载均衡策略解耦，便于水平扩展与独立测试。

## 3. 设计分析

### 3.1 服务分层与移植全量清单

| 阶段 | 服务 | 端口 | 核心存储 / 依赖 | 现状 |
|---|---|---|---|---|
| 已完成 | `login` | 53000 | LoginRedis / PlayerRedis / Kafka / DataService / PlayerLocator / SceneManager | 已移植主链路（`src/xuanming/login/`） |
| 已完成 | `data_service` | 9000 | MySQL/TiDB / etcd | 已移植主链路 11 RPC（`src/xuanming/data_service/`） |
| 阶段 1A | `db` (`player_store`) | 6000 | MySQL/TiDB / etcd | 待移植：玩家数据 Load/Save/CAS 与入场围栏 |
| 阶段 1A | `player_locator` | 50300 | PlayerRedis (Lua) / etcd | 待移植：玩家在线态、租约、世代与位置路由 |
| 阶段 1B | `client_rpc_router` | 50600 | etcd / 下游 gRPC | 待移植：Gate ForwardRequest 路由、元数据透传与白名单 |
| 阶段 1B | `player_inbox` | 50950 | MySQL/TiDB / etcd | 待移植：GM/战斗/邮件/交易收件箱 Enqueue/Fetch/Ack 游标 |
| 阶段 1C | `scene_manager` | 60000 | SceneControlRedis / etcd / Scene gRPC / Kafka | 待移植：场景注册、EnterScene、跨线/切图与负载调度 |
| 阶段 1C | `match` | 50500 | Redis / etcd / Battle gRPC / Scene gRPC | 待移植：组队、匹配、战斗房间分配与结算协调 |
| 阶段 2A | `content_guard` | 50700 | 词库 / 内存 / etcd | 待移植：敏感词检测、替换与限流 |
| 阶段 2A | `chat` | 50200 | MySQL/TiDB / Redis / Kafka / ContentGuard | 待移植：世界/区服/队伍/私聊发送、窗口与历史查询 |
| 阶段 2A | `mail` | 50400 | MySQL/TiDB / PlayerInbox / DataService | 待移植：系统与玩家邮件投递、读取、附件领取与删除 |
| 阶段 2A | `rank` | 50800 | Redis ZSET / MySQL/TiDB / Mail | 待移植：榜单更新、分页查询与结算快照 |
| 阶段 2B | `friend` | 50100 | MySQL/TiDB / Redis / PlayerLocator | 待移植：好友申请、关系维护、黑名单与在线态 |
| 阶段 2B | `payment` | 50900 | MySQL/TiDB / PlayerInbox / DataService | 待移植：订单创建、回调核销幂等与发货投递 |
| 阶段 2B | `trade` | 50920 | MySQL/TiDB / DataService / PlayerInbox | 待移植：上架/购买/撤销/托管与资产一致性围栏 |
| 阶段 2B | `guild` | 50930 | MySQL/TiDB / Redis / DataService | 待移植：公会创建、审批、权限、仓库与公告 |

### 3.2 统一工程规范

- **包结构**：每个独立服务位于 `src/xuanming/<service_name>/`，包含 `__init__.py`、`config.py`、`errors.py`、`model.py`、`store.py`（及 `sql_store.py`/`redis_store.py`）、`service.py`、`node.py`。
- **CLI 入口**：统一接入 `python -m xuanming <service-cmd> --config <toml>`（或 `run <role>`），配置段集中在 `config/cluster.example.toml`。
- **Schema 文件**：每个需要 MySQL/TiDB 的服务在 `config/<service>_schema.sql` 维护与 Go 一致的权威 DDL。
- **鉴权与协议**：复用 `src/xuanming/grpc_runtime.py` 的 `serve_grpc`、`GrpcClient`、`sign_service_call`、`verify_service_call` 与 `cluster.EtcdDirectory`。若发现 `proto/` 落后于 C++ 权威源，先同步 `.proto` 并通过 `python tools/build_protocol.py` 重新生成 `src/xuanming/contracts/schema.pb`。

## 4. 漏洞分析与风险控制

1. **鉴权与越权风险**：
   - 所有涉及资产/角色/状态的内部 RPC（`db`、`player_inbox`、`data_service`、`scene_manager`、`match`、`payment`、`trade`）必须强制 fail-closed：未配置密钥或验签失败直接返回 `PERMISSION_DENIED` / `UNAUTHENTICATED`，严禁开发默认放行。
   - `client_rpc_router` 必须严格校验客户端 `message_id` 是否允许外网路由（`client_protocol=True` 或显式允许的内部消息如 Gate 签名的 `RenewSession`），同时透传并校验 `x-session-detail-bin` 请求/响应头，防止伪造会话身份。
2. **并发与脏写风险（Fence / Epoch / CAS）**：
   - `db` 写入必须校验 `owner_epoch` / `version`（CAS），旧 Scene 迟到写包不得覆盖新主人的持久化快照。
   - `player_locator` 必须使用与 Go 字节级一致的 Redis Lua 原子脚本处理登录世代、租约续期与离线清理，防止并发登录导致的双在线或旧连接误踢新连接。
   - `player_inbox` 游标确认与 `payment` 回调核销必须使用数据库唯一键 + 事务状态机保证严格幂等，杜绝重复发奖/刷物。
3. **协议与依赖漂移风险**：
   - `aiomysql` 0.3.2 与 `PyMySQL` 1.2.4 存在 `bytes` 参数转义缺陷，SQL 二进制字段统一使用 `UNHEX(%s)` 或 `hex()` 安全编码，避免静默写坏 blob。
   - 任何从 C++ 同步的 proto 变更都需跑全量 `pytest`，确保不破坏已有 Gate/Scene/Battle/Login 测试。
4. **验证边界风险**：
   - 单元测试默认以内存双生体 + 真实 `grpc.aio` 环回验证；对 SQL/Redis/etcd 服务提供可选的真实基础设施集成测试，同时构建纯 Python 进程级主链路联调测试（Gate → Router → Login → DataService → PlayerLocator → SceneManager → Scene → DB → PlayerInbox → Match → Battle）。

## 5. 步骤分析

1. **阶段 1A（存储与定位底座）**：移植 `db`（`player_store`）与 `player_locator`，打通玩家数据持久化与在线态围栏；编写单测与全量回归，提交并推送。
2. **阶段 1B（路由与可靠投递）**：移植 `client_rpc_router` 与 `player_inbox`，打通 Gate 路由转发与 Scene 异步发奖/终局投递；编写单测与全量回归，提交并推送。
3. **阶段 1C（场景调度与匹配编排）**：移植 `scene_manager` 与 `match`，彻底闭合“登录 → 选区 → 进场景 → 切图 → 组队匹配 → 进战斗 → 结算回写”纯 Python 全集群主链路；补充端到端联调测试，提交并推送。
4. **阶段 2A（内容安全、聊天、邮件、排行）**：移植 `content_guard`、`chat`、`mail`、`rank`，提交并推送。
5. **阶段 2B（社交与经济系统）**：移植 `friend`、`payment`、`trade`、`guild`，完成全业务服务去 Go 化并跑通全量验证，最终提交并推送。

## 6. 执行步骤

每个阶段均严格执行以下闭环：
1. 只读分析 `xuanming-server-mmo-cpp/go/<service>` 契约、鉴权、DDL/Lua/etcd 细节并核对 `proto/` 差异；
2. 如有协议缺口，同步权威 `.proto` 并运行 `.venv\Scripts\python tools/build_protocol.py`；
3. 按 `src/xuanming/<service>/` 分层实现服务、配置、Schema、CLI 入口；
4. 编写面向真实 `serve_grpc` 环回与已有 Python 客户端对接的 `pytest` 回归测试（含异常/越权/并发围栏用例）；
5. 运行 `.venv\Scripts\python -m pytest -q` 全量通过后，在 `main` 分支按中文提交规范 `git commit` 并 `git push origin main`。

## 7. 阶段 3：全功能生产级补齐（零遗留停用面）

在 14 个核心与外围微服务主干全部跑通之后，阶段 3 彻底消除各服务的“可选/未移植/停用占位”缺口，分两批并行闭环：

- **Wave 3A**：
  1. `data_service`：补齐实名、账号注销、角色过户（Begin/Confirm/Cancel/Transfer/CheckSlot/AckDrain）、改名、外观同步、渠道身份、PlayerStore 写权核查与快照查询；
  2. `login`：补齐登录排队（ControlRedis 队列）、Gate 排空迁移、区服角色数查询、实名/防沉迷/账号注销、`LoginAdmin` 管理接口、生产口令认证与 `ContentGuard` 接入；
  3. `payment`：补齐微信支付、支付宝、微信小游戏、抖音小游戏、Apple AppStore、Google Play 六大生产渠道签名校验/加解密/查询退款适配器与 `MinorSpendingGate="realname"` 接入；
  4. `trade`：补齐官方商城 HTTP HMAC 轮询与支付撤销（`REVERSING`/`REVERSED`）、11 个 RMB 寄售/提现接口与角色过户协调流程；
  5. `guild`：补齐公会捐献/商店完整 `AssetOp` 扣发账本闭环与公会试炼大厅/匹配发车/战果结算；
  6. `match`：补齐战斗剧本 CRUD 与审核、Elo 积分与排行出箱推送、排位赛 V1 接口与队伍预设确认。
- **Wave 3B**：
  7. `content_guard`：补齐微信/抖音外部平台审核适配器与 `GetPlayerChannelIdentity` 缓存路由器；
  8. `player_locator`：补齐月卡托管恢复（`hosted_resume`）与死 Gate 清理对账器；
  9. `scene_manager`：补齐世界分线目录定时发布、空闲实例/拆场/死节点后台清理与死写权释放；
  10. `mail`：补齐 Kafka 邮件变更提醒推送（`event_id=39`, `msg_id=264`）与过期邮件清扫器；
  11. `ops` 运维工具：补齐 Python 版全库 Schema 校验迁移器（对应 Go `schemamigrate`）与配置表打包校验工具（对应 Go `table_sync`）。
- **阶段四（Java 剩余服务全量移植、门户补齐与运行时接线，已完成）**：
  1. `mall_game`（`src/xuanming/mall_game/`、`config/mall_game_schema.sql`）：完整移植 46 组官方商城/支付/修订/交付/退款/扣回/RMB 寄售/人工兜底/Mock HTTP 路由与 28 张表 DDL，并与 `portal_mall_admin`、`payment.mall_client`、`trade.mall_adapter` 真回环互通。
  2. `gm_admin`（`src/xuanming/gm_admin/`、`config/gm_admin_schema.sql`）：完整移植 23 组 GM 路由（概览、玩家查询、个人/系统/三阶段全服邮件活动、7 类在线玩家 GM 命令与审计流水）。
  3. `portal` 补齐与 `config_node`（`src/xuanming/portal_account.py`、`portal_admin_tables.py`、`config_node/`）：补齐 `/api/real-name`、`/api/account/delete`、`/api/account/delete/cancel` 与 `/admin/tables`（etcd CAS 配置表指针），并移植 `config_node` 只读查询与原子热重载服务。
  4. 运行时装配与配置表同步：`src/xuanming/runtime.py` `_login()` 接入排队、防沉迷、口令库（`config/login_schema.sql`）、注销、内容风控、合服刷新与 `KillSwitch`；`player_locator` 支持按区路由 `SceneManager`；同步 12 张缺失配置表（总计 61 张，含 `RankedPreset`/`RankedRule`）并接入 `match/ranked.py`；`schemamigrate` 覆盖全部 15 库 154 表。
- **阶段五（C++ Scene 新增系统 + Go 运维 CLI & 生产加固原语，已完成）**：
  1. 协议同步：补齐 `player_afk_pass`、`player_appearance`、`player_season`、`player_trial_journal`、`player_bag`（装备打造 5 RPC）、`player_pet`（宠物商店/词条洗炼 5 RPC）与 `message_id.txt`（453–458）并重编 `schema.pb`。
  2. Scene 新系统接入 `runtime.py`：`SceneGmCommands`（`SceneNodeGrpc.GmExecutePlayerCommand` 7 类在线 GM 命令与 `gm_admin` 字节级兼容）、`SceneAfkPass`、`SceneAppearance`（含试用自动到期）、`SceneSeason`、`SceneTrialJournal`、`SceneEquipCraft` 与 `ScenePets` 扩展。
  3. 生产加固与运维 CLI：`ops/resilience.py`（`ClockCheck`、`DbGuard`、`LiveViewGate`、`LoopLiveness`）、`grpc_runtime.py`（服务端 `MaxConnectionAge=15m` + 客户端 Keepalive）、`ops/dbcheck.py` 与 `ops/admin_cli.py`（`identitykeygen`、`password-admin`、`lexicon`、`merge-zone`、`data-consistency-check`），全部接入 `__main__.py`。
- **阶段六（角色改名、结算联动、纯 Python 压测机器人与容器/K8s 编排，已完成）**：
  1. `SceneRename`（`src/xuanming/nodes/scene_rename.py`）与 `BattleSettlement` 成长联动（`season` / `trial_journal` / `equip_craft`）全部接入 `runtime.py`；同步 `client_player_common.proto` 与 `transaction_log.proto` 并重编 `schema.pb`。
  2. 纯 Python `robot` 客户端（`src/xuanming/robot/`，支持 Portal HTTP + Gate TCP/WebSocket + Battle TCP、8 组全功能冒烟场景与并发压测器），已接入 `xuanming robot`。
  3. 纯 Python 容器与编排清单（`deploy/Dockerfile` 非 root UID 10001、`deploy/docker-compose.yml` 全 23 服务零 Go/Java、`deploy/k8s/` Kustomize 清单）与最后 3 项运维小工具（`kafka-dlq`、`devpay-notify`、`battle-script-review`）。
- **阶段七（全部离线工具、物理搬库与 Sa-Token 认证全量纯 Python 化，已完成）**：
  1. `table_exporter`（`src/xuanming/ops/table_exporter.py` + `tools/data_table_exporter/run.py`）：纯标准库 `.xlsx`/`.csv`/`.json` 导表器，支持 PK/FK/永久 ID/`bit_index`/`Tip.xlsx` 单轴校验、`_table_descriptors.pb` 编译与 `ConfigTables` 全兼容产物导出，接入 `xuanming table-export`。
  2. `dbmove`（`src/xuanming/ops/dbmove.py`）：物理分库在线搬迁状态机、分片回填、确定性 SHA-256 校验和核对、延迟/哨兵/防脑裂切流门禁，接入 `xuanming dbmove`。
  3. `satoken_auth`（`src/xuanming/satoken_auth/` + `config/satoken_auth_schema.sql`）：Sa-Token 兼容 HTTP/Redis/MySQL 认证与 OAuth/dev 门禁服务，接入 `xuanming satoken-auth` 与 `schemamigrate`（16 库 156 表）。
  4. `navmesh_baker`（`src/xuanming/ops/navmesh_baker.py`）与 `battle_art_gen`（`src/xuanming/ops/battle_art_gen.py`）：纯标准库 Detour MSET v1 导航网格/手绘平面烘焙、SHA-256 配对校验与寻路，以及零 Pillow 7 类 83 张确定性 PNG 战斗美术生成器，接入 `xuanming navmesh-baker` 与 `xuanming battle-art-gen`。

## C++ 仓库对账基线与后续增量移植约定

- **已校对 C++ 基线提交**：
  - 仓库：`xuanming-server-mmo-cpp`（分支 `main`）
  - Commit SHA：`1212e017ed1db628af1e3d2a335c0c884f5bf06a`（短哈希 `1212e017e`）
  - 追平范围：涵盖 `0f9bc81d6..1212e017e` 全部 16 笔提交（战斗 V5 战前 3 禁技能/战术标记 `532~533`/赛季与首领周词缀/天梯战中召唤灵宠、Match V4 禁灵脉 `534`/精彩集锦 `535`/宿敌连胜、Team V4 队形 `524`/一键召回 `525`、装备预设方案 `536~539`、技能书封印与解封 `543~544`、成长被动技能 V2 与战斗成长升级、挂机月卡托管设置与战报 `522~523`、合规激励视频 `540~542` 与 `CURRENCY_TYPE_BIND_GOLD=3` 绑金禁交易红线、82 张最新配表 version 93 及 516 条路由）。
- **本地/多节点运行范式（已写入 [docs/rules/runtime-modes-and-startup.md](../rules/runtime-modes-and-startup.md)）**：
  - Windows 单进程一键启动（`启动玄冥服务器.bat`）、PB 直转可读 ProtoJSON 本地存档（`JsonPlayerStore` v2 `rows_json`）、条件路由工厂（`create_player_store`，集群模式严禁静默降级）以及纯标准库并行多进程监管器（`tools/supervisor.py` + `启动-本地存档.bat` + `启动-多节点集群.bat`）。
- **后续增量移植流程**：
  1. 在 `xuanming-server-mmo-cpp` 查看该基线之后的新提交：`git log --oneline 1212e017ed1db628af1e3d2a335c0c884f5bf06a..HEAD`。
  2. 按需同步底层/框架及新增业务逻辑，确保 Python 端持续具备平替 C++ 服务端的能力；回归通过后同步更新本节、[README.md](../../README.md) 与 [AGENTS.md](../../AGENTS.md) 的基线 Commit SHA。
