# 玄冥服务端 · Python

这是一个独立的 Python 项目，移植玄冥 C++ 运行层的 Gate、Scene、Battle。当前对照基线为 C++ 仓库 `main`（`d74bbeb1`）。协议快照、消息号、事件号和启动代码都在本项目内。运行和构建仅需此目录。

| 节点 | 当前运行路径 |
| --- | --- |
| Gate | TCP 票据、心跳和限流、登录态续租、按会话保序、直连 Go 服务或经 Router、Kafka 推送、世界聊天、跨区重定向和大厅观战 |
| Scene | RPC0、场景和角色归属代次、Redis 跨区交接、SQL 入场与封存、导航和组队跟随、断线保留和月卡托管、资产、属性、宠物、任务副本和实时技能 |
| Battle | gRPC 建房、TCP 票据直连、原配表的职业技能和 Buff、PvE 奖励、战报和观战、Match SQL 终局、Agones 建房准入 |

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

## 单角色启动

安装 `.[cluster]` 后可用 `xuanming-server run gate|scene|battle --config <配置文件>` 单独起节点。用 `config/cluster.example.toml` 配置实例地址、原有 Kafka topic 代号和固定分区。设置 `GATE_CLIENT_RPC_ROUTER=0` 后，Gate 根据 etcd 登记直连原版 Go Login 和 Match。保留配置中的 `router` 地址或显式设置 `GATE_CLIENT_RPC_ROUTER=1` 可经 Router 转发，组队和其他 Go 客户端服务走此模式。Gate 的 `rpc_port` 用于独立的内部 RPC0 监听。Scene 使用玩家域 Redis 做归属、战斗锁和进场同步摘要，使用 DataService、PlayerStore、SceneManager、Match 和 PlayerInbox 做原有控制面操作。神秘商人和资产补偿唤醒使用独立的 `scene_control_redis`。Java 门户和 Go 业务服继续提供各自的集群服务。

集群密钥沿用原有配置名。Gate 使用 `MMORPG_GATE_TOKEN_SECRET` 和 `MMORPG_INTERNAL_AUTH_SECRET`。Battle 使用 `MMORPG_BATTLE_TOKEN_SECRET` 和 `MMORPG_MATCH_OUTCOME_SECRET_BATTLE`。Scene 使用 `MMORPG_PLAYER_STORE_SECRET_SCENE` 和 `MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE`。资产调用方按需配置 `MMORPG_ASSET_OP_SECRET_GUILD`、`MMORPG_ASSET_OP_SECRET_TRADE`、`MMORPG_ASSET_OP_SECRET_MAIL`、`MMORPG_ASSET_OP_SECRET_DATA_SERVICE`。角色封存另用 `MMORPG_ACCOUNT_DIRECTORY_SECRET_TRADE`，大厅观战另用 `MMORPG_SPECTATE_TOKEN_SECRET`。本地客户端 GM 同时要求 `GATE_RUN_MODE=dev` 和 `SCENE_RUN_MODE=dev`。Gate 和 Scene 的内部停机 RPC 与 Scene 实体诊断 RPC 使用独立的 `GATE_GM_ADMIN_SECRET`。Scene 诊断列出 Python 运行时的组件和值，实体编号和组件描述使用 Python 实现的格式。

使用原版 Match 的地域探测时，Battle 和 Match 配置相同的 `MATCH_PROBE_SECRET`（至少 32 字节），并为 Battle 设置非零 `physical_region` 和客户端可访问的 `battle.client_host`、`battle.client_port`。设置 `GLOBAL_MATCH_ENABLED=true` 后，Battle 会在启动时检查探测密钥和机房编号。

Agones 部署设置 `AGONES_SDK_HTTP_PORT`，可同时设置 `CLIENT_ENDPOINT_SOURCE=agones` 从 GameServer 的 `client` 端口读取对外地址。Battle 注册后按 SDK 就绪、分配和排空状态更新 etcd 建房准入。停机时关闭准入，向正在观战的客户端通知离场，并在限定时间内补交已结束的战斗结局。Agones 生命周期目前用本地模拟 SDK HTTP 验证。

## 移植与验证范围

- 资产包括银两等币种、欠款、背包整理、装备穿脱、强化、宝石、临时格和神秘商人购买。内部资产操作保留签名、原请求去重和提交后回执。存档确认后发布审计记录，并按角色实体、币种或物品 ID 统计十分钟内的收益。超过原阈值时，集群模式向 `anomaly_alert_topic` 发布告警。
- 战斗终局由 Match 投递 PlayerInbox。Scene 将奖励、消耗、角色血蓝、任务进度、近期战报和消费账本一起提交，再确认收件箱。备战记录保留迟到确认所需的 Redis 锁，终局冻结沿用原版的投递宽限和续期上限。单机模式用 SQLite 终局队列走相同的应用路径。
- 任务保留稳定 ID、顺序目标、自动续接、旧存档兼容、奖励和活动活跃度。副本包含场景放置、入场以及原 Dungeon/Monster 表的回合制流程。
- 跨区保留原有 Redis 写权围栏、重定向、签名入场、封存与最终存盘顺序。回合技能保留原始配置指纹、重数、耗蓝、冷却、Buff、随机数和回放格式。
- Scene 实时技能按原配表执行施法、打断、伤害和 Buff。隐身影响 AOI，进入回合战斗时按原规则带入持续 Buff。实时技能可选取已在同场景注册的 NPC 网络号。原版运行路径的副本怪物由 Battle 根据 Dungeon 和 Monster 表生成。
- 角色属性包含加点、方案保存切换和洗点。宠物包含列表、出战、改名、加点、重置和等级、技能在战斗中的快照。进场同步摘要从原有 Redis 键读已知域，入场和战斗解冻会唤醒资产补偿。
- Scene 支持原版七份导航网格、月汐湾版本化导航和两份手绘平面导航。严格位图或手绘边界与网格文件按 SHA-256 配对校验；路径拐点偏出位图时，在 Detour 连通范围内按可行走格搜索并逐段复核。入场位置吸附、移动碰撞和纠正沿用原版坐标单位。六边形 AOI 维护有向可见性，通过列表通知同步可见变化，并合并发送角色的状态增量。实时技能造成伤害后会建立双方的高优先战斗关注；共同攻击 NPC 的玩家互相保持可见，最后一次交手 10 秒后解除关注。场景每秒按模拟步利用率调整 AOI 容量，使用平滑与滞回避免容量反复跳变，容量变化时按优先级和距离裁剪或恢复可见目标。组队跟随用 Go Team 签名读取和战斗锁围栏，在线队员可在同区跨节点跟随队长。场景保留原 25 秒断线窗口和月卡托管续期，掉线期间仍可应用合法的已完成战斗结算。

已用本地真实 TCP/gRPC 验证登录、副本、任务、资产、终局存档和实时技能选取 NPC，用隔离的 etcd、Kafka 和 Redis 验证基础设施协议，并和原生 C++ 对拍了指定回合、配表指纹及回放字节。

隔离整栈验收使用本地 MySQL、Redis、Kafka 和 etcd。原版 Java 网关通过 `/api/login` 和 `/api/assign-gate` 调用 Go Login，完成 Argon2id 密码验证和分票。同一角色通过 Python Gate 和原版 Router 进入 1 区，再由原版 SceneManager 重定向至 2 区并返回 1 区。验收直查 MySQL 核对三次写权代次和两区资产写入，往返后余额从 0 增至 18。

同一角色通过 Python Battle 的 TCP 端口完成三次延迟探测，提交回执并经原版 Go Match 正常入队。Match 创建单人 PvE 成局记录和 Python Battle 房间，客户端收到战斗票据和开战通知，进入战斗后以撤退结束。原版 Go Match 签名确认 `COMPLETE`，把终局原件写入 MySQL 并发布 Kafka。原版 PlayerInbox 投递并确认 `APPLIED`，原版 Go DB 可回读战报和对应的消费记录。

本机 Router 验收记录见 `build/interop-native-20261106/run-e064fc92/acceptance.json`。有对应原版服务和中间件的联调环境可运行 `python build/interop-native-20261106/smoke_full.py --ingress java --scene-landing --scene-team --monopoly-dice-recovery`。两个真实账号组队时，原版 Go `ApplyJoinTeam` 返回 4030。验收在隔离 MySQL 写入一条待审申请，由原版 Go `HandleApplication` 审批并提交成员关系，Python 通过原版签名读取和真实 TCP 验证了同场景跟随、同节点换场景跟随与移动纠正。

Gate 直连验收记录见 `build/interop-native-20261106/run-ee0f1772/acceptance.json`。使用 `--ingress java --direct-gate --monopoly-board` 复验真实登录、棋盘通关与重登、跨区和战斗落库。

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
