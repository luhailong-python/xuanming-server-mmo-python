# 玄冥服务端 · Python

这是一个独立的 Python 项目，移植玄冥 C++ 运行层的 Gate、Scene、Battle。当前对照基线为 C++ 仓库 `main`（`d74bbeb1`）。协议快照、消息号、事件号和启动代码都在本项目内。运行和构建仅需此目录。

| 节点 | 当前运行路径 |
| --- | --- |
| Gate | TCP 票据、心跳和限流、登录态续租、场景交接期间的保序重投、直连 Go 服务或经 Router、Kafka 推送、世界聊天、跨区重定向和大厅观战 |
| Scene | RPC0、场景和角色归属代次、Redis 跨区交接、SQL 入场与封存、导航和组队跟随、断线保留和月卡托管、资产、属性、宠物、任务副本和实时技能 |
| Battle | gRPC 建房、TCP 票据直连和心跳、战斗自动剧本、原配表的职业技能和 Buff、PvE 奖励、战报和观战、Match SQL 终局、Agones 建房准入 |

原有 muduo 客户端帧与 RPC0 保持各自的二进制格式。Python 使用 `asyncio` 管理连接、时钟和退出，使用 Protobuf 描述符解析原协议，使用 `grpc.aio` 和 `aiokafka` 对接基础设施。配置采用标准库 `tomllib`。本地角色行存档采用带跨进程锁和原子替换的 JSON 文件；SQLite 继续保存本地收件箱与发号器。集群角色数据仍由 Go PlayerStore/DataService 写入现有 SQL/TiDB 路径。运行节点共享协议和引擎代码，玩法状态由各自的场景或房间持有。

## 本地启动

需要 Python 3.11 或更高版本。以下命令在本项目的 PowerShell 中运行。

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -e '.[dev]'
.venv/Scripts/python.exe -m xuanming smoke
.venv/Scripts/python.exe -m pytest -q
```

`smoke` 使用临时 JSON 角色存档、临时 SQLite 辅助账本和临时回环端口。持续运行时，设置开发登录令牌后可在同一进程启动三个角色。

```powershell
$env:XUANMING_DEV_LOGIN_TOKEN = '换成自己的本机开发令牌'
.venv/Scripts/python.exe -m xuanming run local --config config/local.toml
```

开发客户端先取得 Gate 票据，再发送 `ClientTokenVerifyRequest`、`ClientPlayerLogin.Login` 和 `EnterGame`。本机测试可调用 `Runtime.gate.issue_ticket()`。需要从另一终端签票时，在两边配置同一个至少 32 字节的 `MMORPG_GATE_TOKEN_SECRET`，然后执行 `xuanming-server gate-ticket --config config/local.toml`。返回的 `payload_b64` 先做 Base64 解码，`signature_hex` 按 ASCII 原样填入签名字段。开发账号 `local` 的角色 ID 为 1。SQLite 存档保存在 `run/`，自建角色列表保存在进程内。

## 单角色与全 Python 集群微服务启动

安装 `.[cluster]` 后，本项目已实现全套纯 Python 业务服务（无须任何 Go 业务进程，仅保留 MySQL/TiDB、Redis、etcd、Kafka 基础设施），配置模板见 [config/cluster.example.toml](config/cluster.example.toml)，各服务 DDL 位于 `config/*_schema.sql`：

| 服务 / 节点 | 启动命令 | 默认端口 | 说明 |
| --- | --- | --- | --- |
| Gate / Scene / Battle / Login | `xuanming-server run gate\|scene\|battle\|login --config <toml>` | 8610 / 8611 / 8613 / 53000 | 游戏主节点与登录认证会话服务 |
| Portal | `xuanming-server portal --config <toml>` | 8080 / 8082 | HTTP 门户与管理探针 |
| DataService | `xuanming-server data-service --config <toml>` | 9000 | 账号目录、角色路由/护栏/入场登记、名字与号段 |
| DB (PlayerStore) | `xuanming-server db --config <toml>` | 6000 | 玩家存档认领、CAS 存盘与审计出箱 |
| PlayerLocator | `xuanming-server player-locator --config <toml>` | 50300 | 玩家在线态、断线租约与 Redis Lua 围栏 |
| ClientRpcRouter | `xuanming-server router --config <toml>` | 50600 | Gate 客户端 RPC 路由、轨道隔离、子集与熔断 |
| PlayerInbox | `xuanming-server player-inbox --config <toml>` | 50950 | 战斗结算与 GM 制裁可靠收件箱 |
| SceneManager | `xuanming-server scene-manager --config <toml>` | 60000 | 世界线初始化、场景调度、跨区重定向与 2 步交接 |
| Match (含 Team) | `xuanming-server match --config <toml>` | 50500 | 组队、网络探测、匹配成局、观战与终局结算出箱 |
| ContentGuard | `xuanming-server content-guard --config <toml>` | 50250 | 敏感词 Trie、繁简归一、广告检测与词库热更 |
| Chat | `xuanming-server chat --config <toml>` | 50700 | 世界 8 分片与私聊纯 SQL 权威服务 |
| Mail | `xuanming-server mail --config <toml>` | 50900 | 个人/系统/公会邮件与 AssetOp 附件发放 |
| Rank | `xuanming-server rank --config <toml>` | 51100 | 纯 SQL 排行榜、分页与结算发奖 |
| Friend | `xuanming-server friend --config <toml>` | 50400 | 好友申请、关系图、黑名单与在线推荐 |
| Payment | `xuanming-server payment --config <toml>` | 51200 / 18130 | 充值意图、HTTP 渠道回调与退款核销 |
| Trade | `xuanming-server trade --config <toml>` | 50800 | 聚宝斋目录、寄售/P2P/市集/拍卖与资产交割 |
| Guild | `xuanming-server guild --config <toml>` | 50310 | 公会管理、审批、分区排行、AssetOp 经济与试炼活动 |
| Ops 运维工具 | `xuanming-server schemamigrate\|table-sync ...` | — | 全 12 库 123 表 Schema 校验迁移与配置表打包同步 |

设置 `GATE_CLIENT_RPC_ROUTER=0` 后，Gate 根据 etcd 登记直连 Login 和 Match；保留配置中的 `router` 地址或显式设置 `GATE_CLIENT_RPC_ROUTER=1` 可经 Python `client_rpc_router` 转发全部客户端业务服务。Gate 的 `rpc_port` 用于独立的内部 RPC0 监听。Scene 使用玩家域 Redis 做归属、战斗锁和进场同步摘要，使用 DataService、PlayerStore、SceneManager、Match 和 PlayerInbox 做控制面操作，神秘商人和资产补偿唤醒使用独立的 `scene_control_redis`。

Python Login（`src/xuanming/login`）完整实现 `ClientPlayerLogin`、`LoginPreGate` 与 `LoginAdmin` 全部 17 个 RPC，涵盖登录排队、Gate 排空迁移、实名/防沉迷、账号注销、生产口令认证（`scrypt` + 可选 `argon2`）、`ContentGuard` 与 `KillSwitch`，沿用 Go login 的 Redis 键、TTL 和 Lua 脚本，可与原版共用同一套 Redis。它使用 `[login]` 段和 `data_service`、`scene_manager`、`player_locator`、`redis`（玩家域）、`login_redis`（登录域，缺省同 `redis`）地址，并读取环境变量 `MMORPG_ACCOUNT_DIRECTORY_SECRET_LOGIN`、`MMORPG_GATE_TOKEN_SECRET`、`MMORPG_INTERNAL_AUTH_PUBLIC_KEYS`、`MMORPG_LOGIN_ADMIN_TOKEN`，开发口令登录读取 `LOGIN_DEV_PASSWORD_SHARED_SECRET`（账号前缀 `robot_`、`dev_`）。

Scene 和 Battle 的内部 Match 终局调用会发现 etcd 中的 stable Match 副本；终局提交和只读查询可在通信失败时切换副本。无登记或首次发现失败时使用配置中的 `match` 地址，因此该地址必须指向可信的 stable Match 服务；已有登记但没有合格 stable 副本时不会绕过轨道限制。

集群 Scene 接收停机请求后先关闭新建和入场准入、向注册中心发布关闭状态，在预算内等待场景迁移并疏散剩余玩家，再进行有界存档退出。`MMORPG_PLANNED_DRAIN_BUDGET_SECONDS` 默认 35 秒，`SCENE_PLANNED_DRAIN_SELF_EVACUATE_AFTER_SECONDS` 可调整自疏散启动时间；部署的终止宽限期还应为后续最多 15 秒的存档阶段留出时间。存储持续不可用或迁移预算耗尽时，停机会记录未完成的工作，不能将其当作成功迁移。

自疏散共用节点级限速，`SCENE_PLANNED_DRAIN_RELOCATIONS_PER_SECOND` 默认为 200，设为 0 表示不限制。

## HTTP 门户

Python 门户提供登录、Gate 分票、排队查询和令牌刷新，以及区服列表、公告、CDN 签名、热更检查和管理接口。安装 `.[cluster]`，按 [门户配置示例](config/portal.example.toml) 配置 MySQL、Redis、etcd 和 Login，设置密钥后启动：

```powershell
$env:XUANMING_PORTAL_MYSQL_PASSWORD = '数据库密码'
$env:XUANMING_PORTAL_CDN_SIGNING_SECRET = 'CDN 签名密钥'
$env:XUANMING_PORTAL_ADMIN_API_KEY = '管理接口密钥'
.venv/Scripts/xuanming-server.exe portal --config config/portal.example.toml
```

门户通过 HTTP 监听，应由可信反向代理提供 TLS；`trusted_proxies` 只填写可信代理网段。`home_zone_ids` 指定本实例负责发布策略、健康状态和检查角色容量的区服。管理接口使用 `X-Admin-Key`，未配置密钥时拒绝访问；商城代理另需配置目标服务及 `COMMERCE_ADMIN_SECRET`。CDN 密钥须与资源校验方一致。门户和游戏节点不需要 Java 运行环境，生产集群仍需要配置中的 Go 服务和中间件。

健康探针及 Prometheus 指标在独立的管理监听端口提供，默认绑定 `127.0.0.1:8082`；管理端口与业务端口共享同一 asyncio 事件循环。公开 HTTP 请求会生成新的追踪标识，传入的外部追踪头不作为内部调用链的来源。

新数据库需要先在与 Go Login 共用的数据库中执行 [门户建表脚本](config/portal_schema.sql)，并通过管理接口配置区服。脚本不会迁移已存在的旧表；已有部署应先核对 `zone_config`、`zone_whitelist` 和 `announcement` 的结构。

集群密钥沿用当前 C++ 配置名。Gate 使用 `MMORPG_GATE_TOKEN_SECRET`、`MMORPG_INTERNAL_AUTH_SIGNING_KEY` 和 `MMORPG_INTERNAL_AUTH_PUBLIC_KEYS`。内部鉴权私钥是 32 字节 Ed25519 种子的 64 位十六进制编码；公钥配置是逗号分隔的 64 位十六进制公钥列表，必须包含该私钥对应的公钥，轮换期间可配置多把公钥。Battle 使用 `MMORPG_BATTLE_TOKEN_SECRET` 和 `MMORPG_MATCH_OUTCOME_SECRET_BATTLE`。Scene 使用 `MMORPG_PLAYER_STORE_SECRET_SCENE` 和 `MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE`。资产调用方按需配置 `MMORPG_ASSET_OP_SECRET_GUILD`、`MMORPG_ASSET_OP_SECRET_TRADE`、`MMORPG_ASSET_OP_SECRET_MAIL`、`MMORPG_ASSET_OP_SECRET_DATA_SERVICE`。角色封存另用 `MMORPG_ACCOUNT_DIRECTORY_SECRET_TRADE`，大厅观战另用 `MMORPG_SPECTATE_TOKEN_SECRET`。本地客户端 GM 同时要求 `GATE_RUN_MODE=dev` 和 `SCENE_RUN_MODE=dev`。Gate 和 Scene 的内部停机 RPC 与 Scene 实体诊断 RPC 使用独立的 `GATE_GM_ADMIN_SECRET`。Scene 诊断列出 Python 运行时的组件和值，实体编号和组件描述使用 Python 实现的格式。

集群节点监听 etcd 的 `/mmorpg/killswitch/` 规则，可按实例、节点类型或全局短路 RPC。规则及优先级沿用 C++ 约定；etcd 长时间不可用时，超过 60 秒未同步的规则自动失效。本地模式不连接 etcd。

战斗奖励转邮件需要配置与 Mail 服务一致的 `MAIL_PRODUCER_SECRET_BATTLE`。密钥或 Mail 服务不可用时，无法入包的结算会保留并重试。

使用原版 Match 的地域探测时，Battle 和 Match 配置相同的 `MATCH_PROBE_SECRET`（至少 32 字节），并为 Battle 设置非零 `physical_region` 和客户端可访问的 `battle.client_host`、`battle.client_port`。设置 `GLOBAL_MATCH_ENABLED=true` 后，Battle 会在启动时检查探测密钥和机房编号。

Agones 部署设置 `AGONES_SDK_HTTP_PORT`，可同时设置 `CLIENT_ENDPOINT_SOURCE=agones` 从 GameServer 的 `client` 端口读取对外地址。Battle 注册后按 SDK 就绪、分配和排空状态更新 etcd 建房准入。停机时关闭准入，向正在观战的客户端通知离场，并在限定时间内补交已结束的战斗结局。Agones 生命周期目前用本地模拟 SDK HTTP 验证。

## 移植与验证范围

- 资产包括银两等币种、欠款、背包整理、装备穿脱、强化、宝石、临时格和神秘商人购买。内部资产操作保留签名、原请求去重和提交后回执。存档确认后发布审计记录，并按角色实体、币种或物品 ID 统计十分钟内的收益。超过原阈值时，集群模式向 `anomaly_alert_topic` 发布告警。
- 战斗终局由 Match 投递 PlayerInbox。Scene 将奖励、消耗、角色血蓝、任务进度、近期战报和消费账本一起提交，再确认收件箱。备战道具预留随角色存档保存，结算或终局复核后释放。背包和临时格都满时，战利品连同邮件待发记录存档，确认后再幂等投递。备战记录保留迟到确认所需的 Redis 锁，终局冻结沿用原版的投递宽限和续期上限。单机模式用 SQLite 终局队列走相同的应用路径。
- 任务保留稳定 ID、顺序目标、自动续接、旧存档兼容、奖励和活动活跃度。副本包含场景放置、入场以及原 Dungeon/Monster 表的回合制流程。
- 跨区保留原有 Redis 写权围栏、重定向、签名入场、封存与最终存盘顺序。回合技能保留原始配置指纹、重数、耗蓝、冷却、Buff、随机数和回放格式。
- Scene 实时技能按原配表执行施法、打断、伤害和 Buff。隐身影响 AOI，进入回合战斗时按原规则带入持续 Buff。实时技能可选取已在同场景注册的 NPC 网络号。原版运行路径的副本怪物由 Battle 根据 Dungeon 和 Monster 表生成。
- 角色属性包含加点、方案保存切换和洗点。宠物包含列表、出战、改名、加点、重置和等级、技能在战斗中的快照。进场同步摘要从原有 Redis 键读已知域，入场和战斗解冻会唤醒资产补偿。
- Scene 支持原版七份导航网格、月汐湾版本化导航和两份手绘平面导航。严格位图或手绘边界与网格文件按 SHA-256 配对校验；路径拐点偏出位图时，在 Detour 连通范围内按可行走格搜索并逐段复核。入场位置吸附、移动碰撞和纠正沿用原版坐标单位。六边形 AOI 维护有向可见性，通过列表通知同步可见变化，并合并发送角色的状态增量。实时技能造成伤害后会建立双方的高优先战斗关注；共同攻击 NPC 的玩家互相保持可见，最后一次交手 10 秒后解除关注。场景每秒按模拟步利用率调整 AOI 容量，使用平滑与滞回避免容量反复跳变，容量变化时按优先级和距离裁剪或恢复可见目标。组队跟随用 Go Team 签名读取和战斗锁围栏，在线队员可在同区跨节点跟随队长。场景保留原 25 秒断线窗口和月卡托管续期，掉线期间仍可应用合法的已完成战斗结算。

已用本地真实 TCP/gRPC 验证登录、副本、任务、资产、终局存档和实时技能选取 NPC，用隔离的 etcd、Kafka 和 Redis 验证基础设施协议，并和原生 C++ 对拍了指定回合、配表指纹及回放字节。

隔离整栈验收使用本地 MySQL、Redis、Kafka 和 etcd。Python 门户通过 `/api/login` 和 `/api/assign-gate` 调用 Go Login，完成 Argon2id 密码验证和分票。同一角色通过 Python Gate 和原版 Router 进入 1 区，再由原版 SceneManager 重定向至 2 区并返回 1 区，并直查 MySQL 核对写权代次和资产写入。

同一角色通过 Python Battle 的 TCP 端口完成三次延迟探测，提交回执并经原版 Go Match 正常入队。Match 创建单人 PvE 成局记录和 Python Battle 房间，客户端收到战斗票据和开战通知，进入战斗后以撤退结束。原版 Go Match 签名确认 `COMPLETE`，把终局原件写入 MySQL 并发布 Kafka。原版 PlayerInbox 投递并确认 `APPLIED`，原版 Go DB 可回读战报和对应的消费记录。

本机 Router 验收记录见 `build/interop-native-20261106/run-2eaf66f4/acceptance.json`（本地构建产物，不随仓库分发）。两个真实账号经修复后的 Go Match 完成申请、审批、离队、邀请和接受，无需注入申请记录；场景跟随、棋盘通关与重登、跨区和战斗落库也已验证。部署时 Go Match 需要包含对应的组队鉴权修复。

重新构建原生依赖并运行隔离联调的步骤见 [整栈验收工具](tools/interop/README.md)、[TiDB 全链工具](tools/tidb_full/README.md)、[TiDB 组件及商城验收工具](tools/tidb_interop/README.md) 和 [本机多副本故障演练](tools/tidb_ha/README.md)。这些工具只创建隔离测试资源，不能替代正式生产环境的容量、容灾或写者资格验收。

## WebSocket 客户端承载

Gate 和 Battle 保留原 TCP 监听，并可通过各自的 `websocket_port` 启用第二个客户端入口。每条 WebSocket 二进制消息必须恰好承载一条完整的原 muduo 帧，包括原长度头和校验和；票据、心跳、消息号和 protobuf 内容不变。文本消息、拼接帧、残帧都会被拒绝。Gate 和 Battle 的 WebSocket 端口单独配置，不改变原有节点发现字段。集群示例使用 Gate `8616`、Battle `8617`；未设置 `websocket_port` 时不会启动该监听器。

本地配置将角色存档写入 `../run/players.json`，锁文件为同目录的 `.lock` 文件，存档通过同目录临时文件和 `os.replace` 提交。首次认领时，如果 JSON 文件尚不存在而旧 SQLite 中已有角色表，会先导入原角色快照。SQLite 继续保存本地收件箱和号段。资产收益窗口和待发送告警写入 `../run/asset-anomaly.json`；Kafka 不可用时告警保留并退避重试。集群配置可将 `anomaly_journal` 指向持久化挂载路径，默认写入 `run/asset-anomaly.json`；多个 Scene 进程要共享监测窗口时，应挂载同一支持文件锁和原子替换的目录。告警投递为至少一次，进程若在 Kafka 确认后、清除本地 outbox 前退出，重启后可能重复发布。

## 玩法扩展

原始协议快照在 `proto/`。新 RPC 在此目录定义，并为客户端 RPC 在 `message_id.txt` 分配新编号。执行 `python tools/build_protocol.py` 重新生成独立运行包中的描述符。场景玩法可以通过 `SceneNode.register` 注册协议入口，把校验与状态转换写在场景系统内。需要房间级规则时，可沿用 Battle 的准入、直连和房间生命周期，把回合规则实现为独立的游戏模块。

大富翁首版在 Python Gate 和 Scene 上运行。入场后调用 `MonopolyClientPlayer.GetBoard`（消息号 `10000`），读取 12 个格子、棋子位置、余额、剩余骰子次数和 `next_roll_seq`。新角色有 20 次机会。每次调用 `MonopolyClientPlayer.RollDice`（`10001`）提交该序号，服务端掷两颗六面骰，按总点数走格并结算落点奖励。经过或到达起点另发 100，奖励使用现有钱包的金币槽位。

棋盘进度、钱包变动和最近一次回执随同一笔存档提交。遇到超时或服务暂时不可用时，客户端用原序号重试。最近一次已成功的请求返回原回执，过期序号和跳号请求都会被拒绝。运行 `python -m xuanming smoke` 可在本地通过真实 TCP 验证掷骰到存档，`python -m pytest -q tests/test_monopoly.py` 还覆盖重启恢复和提交后回包丢失。

`GetBoard` 同时返回钟楼、市集、花园的等级、上限、下一级价格和 `next_upgrade_seq`。提交 `MonopolyClientPlayer.UpgradeLandmark`（消息号 `10002`）时，填入地标 ID、当前等级 `expected_level` 和升级序号。等级 0 升到 1 是首次建造，之后逐级升到 3。以钟楼为例，三次扣款分别为 30、60、120 金币，直接使用掷骰收入所在的钱包。扣款、等级和回执一起落库。余额不足时等级和序号保持原值。最近一笔升级的重试需要保留原地标、等级和序号，可直接拿回原回执。

运行 `python -m pytest -q tests/test_monopoly_landmarks.py` 可复验掷骰赚钱、扣款建造、继续升级和重登读档。

三个地标都升到 3 级时，最后一次升级同时结算通关。起始小镇奖励 300 金币，随后自动进入海港小镇，从起点开始，地标从 0 级建造，掷骰次数补至 20。钱包余额和两类请求序号随角色保留。海港小镇通关奖励 450 金币，目前共有两张棋盘。

`GetBoard` 返回当前 `board_id`、通关奖励和最近一次通关记录。掷骰与升级请求携带 `board_id`，首张棋盘仍接受省略该字段的旧请求。最后一次升级的回执包含通关信息；超时重试应继续使用原棋盘、原地标、原等级和原序号。旧存档如果已有三个满级地标，首次读取棋盘时会补结算一次。运行 `python -m pytest -q tests/test_monopoly_boards.py` 可复验通关发奖、换板和重登恢复。

骰子低于 20 次时，每 5 分钟恢复 1 次，在线和离线都按服务端时间计算。满额时停止计时，消耗后重新起算。`GetBoard` 返回 `dice_cap`、`dice_refill_interval_ms` 和 `state.next_dice_at_ms`（UTC 毫秒，满额时为 0）。重试掷骰仍拿到原回执，当前余额和倒计时用 `GetBoard` 查询。旧存档沿用现有余额，在首次查棋盘或发起新掷骰时开始计时。运行 `python -m pytest -q tests/test_monopoly_dice.py` 可复验离线恢复、重登、满额和旧存档。
