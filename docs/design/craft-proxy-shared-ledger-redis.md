# 名匠代打共享摊位目录、口碑待回账与西市热榜改走按区 Redis（Python 版 r2-craft#4）

> 日期：2026-11-27。状态：代码与单测已写，未运行（pytest 交 Gemini，命令见 §6）。
> 权威参照：C++ 仓 `cpp/libs/services/scene/player/system/player_craft.{h,cpp}` 与设计稿 `docs/design/craft-proxy-reputation-digest-and-hot-ranking.md` §2.2～§2.5；发现经过见 C++ 仓 `docs/design/actor-isolation-review-20261127.md` §3、§6。
> 本仓实现：`src/xuanming/nodes/scene_skill_craft.py`（`CraftProxyStallDirectory`、`record_proxy_order_reputation`、`drain_pending_proxy_reputation`），接线在 `src/xuanming/runtime.py` 与 `src/xuanming/nodes/scene.py` `_load_player`。

## 1. 需求分析

改动前的问题：

1. `CraftProxyStallDirectory` 把共享摊位目录、口碑待回账、西市 24h 热榜都放在进程内存字典里。停机、或宗师换到别的 scene 进程，这些数据就丢了；多节点时各节点各看各的，离线宗师在别的节点成交的口碑永远回不了账。
2. 「宗师锁忙时记待回账并持久化」不成立：台账只在进程内存。
3. `record_proxy_order_reputation` 两条路并存：宗师锁空闲时，买家的任务当场拿宗师的锁、直接提交宗师存档（跨玩家写）；锁忙时记内存台账，再投宗师的串行任务。C++ 与 Java 都把这三样放在按区 Redis，Python 移植时漏了。

目标（与 C++ r2-craft#4 一致）：

- 键、字段与取走脚本和 C++ 逐字节一致。混跑时两版读写同一份数据。
- 买家只记账（`HINCRBY` / `HSET`），不拿宗师的锁，也不碰宗师存档。宗师在本节点在线时，只投他自己的串行任务去取走入账。
- 回账在宗师自己的玩家锁内做：Lua 原子取走（`HMGET` + `HDEL`）→ 入账 → `commit_player_data` 落盘。确定没写进去就原样归还；取走应答不明只记英文告警、什么都不入账。
- 摊位目录与热榜按 C++ 的方式走 Redis：写穿，加 5 秒整表刷新。西市列表叠加展示的待回账读刷新来的本地镜像。

不在范围：手工费（95% 净银两）仍走买家同事务出箱再经邮件发出，不改。

## 2. 设计分析

### 2.1 键与字段（与 C++ 一致）

| 键 | 字段 | 值 | 谁写 | 谁读 |
|---|---|---|---|---|
| `craft_proxy_stall:{ctl:<zone>}` | `<宗师 player_id 十进制>` | `CraftProxyStallInfo` 二进制 | 宗师所在节点开摊、收摊、回账后写穿（`HSET` / `HDEL`） | 各节点每 5 秒 `HGETALL` |
| `craft_proxy_reputation:{ctl:<zone>}` | `<M>:o` / `:s` / `:r` / `:g` | 待回账单数 / 极品数 / 稀世数 / 净银两（`HINCRBY`） | 买家所在节点；归还时宗师所在节点 | 刷新与取走 |
| 同上 | `<M>:b` | 最近一位买家角色名（`HSET` 覆盖） | 同上 | 同上 |
| `craft_proxy_hot:{ctl:<zone>}:<UTC 秒 // 3600>` | `<M>:o` / `:s` | 这一个小时的热榜单数 / 极品数（`HINCRBY`，每写一次 `EXPIRE` 续 25 小时） | 买家所在节点 | 刷新读当前小时与前 23 个小时共 24 个桶；回账不取 |

- 取走脚本 `TAKE_PROXY_REPUTATION_LUA` 与 C++ `kTakeProxyReputationLua` 同文，单测钉死文本。
- 解析口径照 C++：负数（旧版回账多扣出来的）与非数字按 0，超出 int64 封顶，u32 计数饱和；取走应答不是 5 项数组、或元素不是字符串 / nil，算结果未知。
- 所有键同一个 hash tag `{ctl:<zone>}`（同槽），与本仓 `scene_manager/keys.py` 的控制域一致。
- 待回账表里早先的 `<M>:24o` / `:24s` 已停用（只累加不归零，见 §2.4），读到按未知后缀跳过。

### 2.2 流程

1. **记账（买家的任务）**：`craft_skill_book` 与 `commit_equipment_craft` 在买家提交成功后调 `record_proxy_order_reputation`：
   - 本节点镜像先累加待回账，并在当前小时的热榜桶里加一笔；
   - 一次非事务 pipeline 发出待回账 `HINCRBY :o/:s/:r/:g`，买家名非空时再发 `HSET :b`；为 0 的项不发，`:g` 只写 `(0, int64]`。同一个 pipeline 里再给当前小时的热榜桶 `HINCRBY <M>:o 1`，有极品时加 `<M>:s`，最后对这个桶 `EXPIRE 90000`；
   - 等 pipeline 返回再往下走，保证随后投的回账能取到这一笔；
   - 宗师在本节点在线 → `scene.schedule_player_task(宗师, 回账)`；不在 → 什么都不做。他所在的节点在 5 秒内刷新时会看到，他上线时也会回账。
2. **回账（宗师自己的锁内）**：`drain_pending_proxy_reputation(scene, 宗师)`。调用点有：进场 `_load_player`，宗师自己的 `GetSkillCraftingState`、`SetCraftProxyStall`、`AckCraftProxyDigest`，记账投来的回账任务，以及刷新后投的回账任务。
   1. 只在宗师可写、没有待确认存档、且镜像提示他可能有待回账时发起；
   2. `EVAL` 取走。异常或应答畸形 → 英文 WARN，什么都不记，镜像不动；
   3. 取到全 0 → 别的节点已回过账或镜像过时，作废镜像里这一条；
   4. 取走后重查「可写且没有待确认存档」，不满足 → 原样归还；
   5. 在存档副本上入账（口碑、雅号位图、未读营业战报），再 `commit_player_data`；
   6. 提交抛错时，看待存盘行还在不在：不在（如 `StoreRejected`）说明确定没写进去，归还；还在（如 `StoreUnavailable`）说明结果未知，场景下一次落盘会重试这批行，所以不归还，只扣镜像；
   7. 成功 → 镜像扣掉取走的部分，再把摊位写穿（口碑与雅号变了）。
3. **刷新**：`refresh_loop` 在 runtime 配了 `scene_control_redis` 时启动，每 5 秒一次，启动时先刷一次。
   - 一次非事务 pipeline 发出三条命令：摊位目录 `HGETALL`、待回账表 `HGETALL`，以及求和脚本 `SUM_PROXY_HOT_LUA` 的 `EVAL`（KEYS 为当前小时与前 23 个小时共 24 个热榜桶）。用结果覆盖摊位、待回账、热榜三份镜像；
   - 三条命令各自成败：`execute(raise_on_error=False)`，与 C++ 三条命令各有回调相同。某条报错（比如键类型不对、脚本出错）或求和应答形状不对时，只保留那一份旧镜像，另外两份照常刷新。原先一条报错整次刷新作废，摊位目录和待回账会跟着停刷；
   - 给本节点在线、镜像里有待回账提示的宗师投回账任务；
   - Redis 故障只告警，保留旧镜像；
   - 刷新里出了意料之外的异常（非 Redis 故障，比如解析代码的缺陷），循环也只 `logger.exception` 一下，下一轮照刷。runtime 把任何后台任务结束都当成节点故障并停机，为展示用的目录停掉整个 scene 不值得。C++ 的定时器同样不会因为一次应答处理出错而停。
4. **写穿**：`sync_player` 在摊位合格时更新镜像并 `HSET`；不合格时，若镜像里原来有、或已收摊，就删镜像并 `HDEL`。条件同 C++ `SyncPlayerProxyStall`。
5. **买家解析摊位**：`find(master_id, scene=...)` 同 C++ `ResolveSharedProxyStall`。宗师在本节点在线时以他的存档为准（不可写就找不到），否则查镜像。补这一条是因为镜像现在会被整表刷新覆盖：宗师开摊那次写穿若失败，刷新会把他从镜像里抹掉，可他明明就在本节点，本节点的买家应该能找到他。
6. **西市列表**：`list_stalls` 取镜像摊位与本节点在线宗师的实时快照，叠加待回账镜像 `(o, s, r)` 与近 24 小时热榜（`hot_window()`），Top 3 评选与 C++ 一致。遍历在线玩家改为按 `locations`（player_id → 所在世界）一人一次：原先按世界去重遍历，每个在线玩家都把他所在世界的全部玩家扫一遍，开销是在线人数的平方。

### 2.4 热榜近 24 小时：按小时分桶（11-27，C++ 与 Python 同改）

- **问题**：原先热榜单数 / 极品数是待回账表里的 `<M>:24o` / `:24s`，只 `HINCRBY +`，从不过期也不扣减，「近 24 小时」实际是开摊以来的累计，几天后热榜就成了老宗师的累计榜。C++ 原实现如此，Python 移植时照搬，核对时发现。
- **方案**：与 C++ 设计稿 §2.6 相同。每个 UTC 小时一个桶 `craft_proxy_hot:{ctl:<zone>}:<hour>`，成交写当前小时，每写一次续 25 小时过期；近 24 小时 = 当前小时与前 23 个小时的桶之和，实际覆盖 23～24 小时。
- **读法与 C++ 相同**：用同一段只读求和脚本，`SUM_PROXY_HOT_LUA` 与 C++ `kSumProxyHotLua` 逐字节一致，单测钉文本。KEYS 取 `proxy_hot_window_bucket_keys(zone, 当前小时)`，同 C++ `ProxyHotWindowBucketKeys`。Redis 端按宗师求和，一次往返；数据量由桶的 TTL 封顶。
- **本节点镜像分两部分**：
  - `_hot_refreshed_totals`：上次整表刷新读回的近 24 小时之和；
  - `_hot_local_buckets`：之后本节点按 UTC 小时记下的成交。
  - `hot_window()` 返回两者之和，并丢掉比窗口更早的本地桶。下次刷新成功时，桶和接替整个镜像，本地桶清空：这些成交已在 Redis 里，不会重复计。
  - 本地单进程模式没有刷新，只有本地桶，窗口靠它按小时滚动，镜像也不会无限长。
- **11-27 中途改过一次**：起初 Python 用 pipeline 读回 24 个桶、在本地求和，数据格式与 C++ 相同。按统筹会话「Lua 与 C++ 逐字节一致」的要求改成现在这样：两边只维护一段读法，真实 Redis 上也只需验证一段脚本。
- **被否决的做法**：同 C++ §2.6，即表内按小时加字段由刷新方删、有序集合记每笔成交、自然日清零。

### 2.5 本轮顺带修掉的两处

- **刷新循环不因意外异常退出**：见上文 2.2 第 3 步。
- **西市列表的平方级遍历**：见上文 2.2 第 6 步。`_find_online_player_in_scene` 也改为按 `locations` O(1) 直取，刷新后给待回账宗师逐个判断在线不再遍历全部在线玩家。

### 2.3 运行模式

- **集群模式**：复用 runtime 已有的 `scene_control_redis` 客户端（`decode_responses=False`），世界频道目录、落地唤醒、神秘商人也用它。没配时打一条英文 WARN，目录与台账只留在本节点内存，与 C++ 控制 Redis 断连时只剩本地镜像同口径。这个地址在本仓集群配置里本就是可选项，`tools/interop/scene_full.py` 不开 landing 时也不配，所以没有改成缺省拒启。
- **本地单进程模式**（零中间件）：镜像就是台账。回账的「取走」是读一份镜像副本，落盘后再扣；宗师锁串行化了同一宗师的回账，单进程里也不会有第二个取走方。确定没写进去时什么都不用还，因为镜像从没扣过。

## 3. 思想分析

- **跨玩家只走消息**：买家的任务里只发 Redis 命令，再给宗师投一个任务；宗师存档只在他自己的串行任务里改（本仓 `docs/rules/engineering-principles.md` Actor 两层边界）。删掉「锁空闲就当场拿宗师锁提交」这条路后，A↔B 互相下单也就不存在锁序与死锁问题。
- **数额以原子取走为准**：镜像只是提示。重复入账的根因是「数额来自可能过时的副本，扣减在事后」，C++ 设计稿 §2.5 已论证。Python 直接按取走模型实现，不经过「照镜像数额入账、事后 `HINCRBY` 负值」的中间形态。
- **「没写进去」看待存盘行，不看异常类型**：本仓 `SceneNode.flush_player_rows` 遇 `StoreRejected` 会清掉待存盘行，遇其他异常则保留，并在下一次落盘（周期存盘、下一个改数据的请求、下线）时重试。如果在结果未知时归还，重试成功后同一批会记两次。所以判断依据是「这次的待存盘行还在不在」。扣镜像放在 `finally` 里，任务在提交途中被取消时同样成立。
- **为什么放现有控制 Redis**：与 C++ 同一份数据才能混跑。这些是展示计数（银两走邮件），用已有的控制 Redis 是最小改动，存档结构与位图都不变。
- **模式**：没有引入新模式。`CraftProxyStallDirectory` 本身就是仓储（本地镜像 + 远端哈希），控制 Redis 客户端由构造参数注入，单测注入 `FakeRedis`。

## 4. 漏洞分析

| 风险 | 处理 |
|---|---|
| 多个节点同时给同一离线宗师下单 | `HINCRBY` 原子累加，不整包覆盖 |
| 两个节点同时回账同一宗师，或本节点镜像过时 | Lua 一次执行里 `HMGET` + `HDEL`，只有一方拿到值；取到全 0 只作废提示 |
| 取走在途期间宗师离开或被冻结 | 重查可写，原样归还 |
| 落盘确定失败 | 原样归还，下一次再取 |
| 落盘结果未知 | 不归还（待存盘行会重试），镜像扣掉 |
| 取走的应答丢了（断连、超时） | 只告警，不入账。脚本若已执行，这一批展示口碑丢失（同 C++，不涉银两） |
| 取走之后、提交之前进程崩溃 | 这一批丢失（同 C++） |
| Redis 写失败 | 告警，不影响已提交的订单；本节点镜像照记，下一次整表刷新覆盖时消失（同 C++） |
| 在途的整表刷新带回取走前的值 | 西市列表最多 5 秒内多显示一次；之后回账取到全 0，只作废提示，不会记两次 |
| Redis 计数为负（旧版多扣） | 读时按 0，取走时一并删掉 |
| 计数溢出 | u32 饱和、int64 封顶；`:g` 只写 `(0, int64]` |
| 待回账宗师很多 | 刷新后按镜像逐个 O(1) 查是否在线，不遍历全部在线玩家 |
| Redis 卡住 | 客户端 3 秒超时；这期间买家的代打请求在订单已提交后最多多等 3 秒，展示计数照丢 |
| 热榜只累加不归零 | 按 UTC 小时分桶、桶带 25 小时过期，只加近 24 个桶（§2.4） |
| 本地单进程热榜镜像无限长 | `hot_window()` 求和时丢掉比窗口更早的桶 |
| 刷新代码出意料之外的异常 | 循环记日志后继续，不让后台任务结束（否则 runtime 判节点故障停机） |
| 西市列表在线人数大时变慢 | 按 `locations` 一人一次遍历，不再是在线人数的平方 |

## 5. 步骤分析

1. 对照 C++ `player_craft.cpp` 的 `RedisHIncrByReputation`、`RedisHSetReputationBuyer`、`kTakeProxyReputationLua`、`ParseTakenReputation`、`GiveBackProxyReputation`、`ApplyTakenProxyReputation`、`OnProxyReputationReply`、`SyncPlayerProxyStall`、`ResolveSharedProxyStall`，以及 Java `CraftProxyStallDirectory`，确认键、字段、脚本与解析口径。
2. 改写 `scene_skill_craft.py` 里的目录与回账，删掉买家任务里当场提交宗师存档的分支和不再使用的 `_get_player_lock`。
3. 调整调用点：`scene.py` 进场写穿改成 `await`；`scene_equip_craft.py` 与制书路径解析摊位时带上 `scene`。
4. `runtime.py` 按 `scene_control_redis` 建目录，配了就起刷新循环。
5. 单测：仓内 `FakeRedis` 补通用哈希命令与 `expire`，取走脚本写成 Python 双生体。
6. 热榜窗口：C++（权威）与 Python 同改为小时分桶，C++ 设计稿 §2.6 写定键、写法、读法与被否决的做法；Java 登记 `PARITY.md` 待做。

## 6. 执行步骤

- [x] 代码：`src/xuanming/nodes/scene_skill_craft.py`、`src/xuanming/nodes/scene.py`、`src/xuanming/nodes/scene_equip_craft.py`、`src/xuanming/runtime.py`
- [x] 单测：新增 `tests/test_craft_proxy_reputation_ledger.py`（14 例，含热榜窗口滚动、刷新循环不退出、刷新三条命令各自成败）；改 `tests/test_actor_isolation.py`（锁忙转投那条改走 `FakeRedis` 台账）、`tests/test_cpp_60b0_skill_wanted_and_rent.py`（`record_pending_reputation` → `record_order`）；`tests/login_fakes.py` 的 `FakeRedis` 补 `hset` / `hdel` / `hgetall` / `hincrby` / `expire`
- [x] C++ 热榜窗口：`cpp/libs/services/scene/player/system/player_craft.{h,cpp}` 与 `cpp/tests/bag_test/player_craft_quality_test.cpp`（C++ 仓，编译与 GTest 交 Gemini，命令见 C++ 仓 `docs/PROGRESS.md` 2026-11-27「热榜读取改走台账接缝」一条）
- [ ] Gemini 运行（仓库根目录）：
  - `.venv\Scripts\python -m pytest -q tests/test_craft_proxy_reputation_ledger.py tests/test_actor_isolation.py`
  - `.venv\Scripts\python -m pytest -q tests/test_cpp_60b0_skill_wanted_and_rent.py tests/test_latest_cpp_sync_systems.py`
  - `.venv\Scripts\python -m pytest -q tests/test_login_flow.py tests/test_login_full.py tests/test_login_state.py tests/test_cellroute_and_owner_authority.py tests/test_cpp_6b4a_trade_login_dualcontrol.py`（`FakeRedis` 的其他使用方）
  - `.venv\Scripts\python -m pytest -q`（全量）
- [ ] 真实 Redis 上的取走脚本与 C++ 同区混跑冒烟：未做

## 7. 已知边界

- 热榜窗口按整小时分桶，实际覆盖 23～24 小时；Redis 断连期间热榜不衰减（保留旧镜像），恢复后第一次刷新即纠正。
- Java 版用自己的键（`<ns>:world:{z:<zone>}:craft-proxy-reputation`）。双版本只共享客户端契约，Java 不与 C++/Python 同区混跑；Java 热榜仍是累计，已登记 Java 仓 `PARITY.md` 待做，本任务不写 Java 代码。
