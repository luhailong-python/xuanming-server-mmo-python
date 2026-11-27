"""名匠代打共享摊位目录、口碑待回账与西市 24h 热榜改走按区控制 Redis 的回归测试（r2-craft#4 Python 版）。

Redis 用仓内既有替身 ``login_fakes.FakeRedis``；取走脚本按 C++ ``kTakeProxyReputationLua`` 逐行写成 Python 双生体
（同一次执行里 HMGET 五项再 HDEL）。真实 Redis 上的脚本字节兼容不在这里验证。
"""

from __future__ import annotations

import asyncio

import pytest

import xuanming.nodes.scene_skill_craft as craft_module
from login_fakes import FakeRedis
from xuanming.data import PlayerRowCodec, SqlitePlayerStore, StoreRejected, StoreUnavailable
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_skill_craft import (
    SUM_PROXY_HOT_LUA,
    TAKE_PROXY_REPUTATION_LUA,
    CraftProxyStallDirectory,
    _parse_hot_bucket,
    _parse_reputation_ledger,
    _parse_taken_reputation,
    drain_pending_proxy_reputation,
    proxy_hot_bucket_key,
    proxy_hot_window_bucket_keys,
    proxy_reputation_ledger_key,
    proxy_stall_directory_key,
    record_proxy_order_reputation,
)
from xuanming.protocol import Protocol

_ZONE_ID = 1
_LEDGER = proxy_reputation_ledger_key(_ZONE_ID).encode()
_STALLS = proxy_stall_directory_key(_ZONE_ID).encode()
_HOUR_MS = 3_600_000
_START_HOUR = 500_000  # 固定时钟：UTC 1_800_000_000_000 毫秒所在小时


def _hot_key(hour: int) -> bytes:
    return proxy_hot_bucket_key(_ZONE_ID, hour).encode()


def craft_ledger_redis() -> FakeRedis:
    """带两段脚本双生体的 FakeRedis：取走缺的字段回 None（同 Lua 里 HMGET 的 false 回成 nil）；求和跨 KEYS 按字段累加。"""
    redis = FakeRedis()

    def take(keys_, argv):
        record = redis.data.get(keys_[0], {})
        fields = [argv[0] + suffix for suffix in (b":o", b":s", b":r", b":g", b":b")]
        values = [record.get(field) for field in fields]
        for field in fields:
            record.pop(field, None)
        return values

    def sum_hot(keys_, _argv):
        acc: dict[bytes, int] = {}
        for key in keys_:
            alive = redis._alive(key)
            for field, value in (redis.data[alive].items() if alive is not None else ()):
                if value.lstrip(b"-").isdigit():  # Lua tonumber 认不出的跳过
                    acc[field] = acc.get(field, 0) + int(value)
        return [item for field, total in acc.items() for item in (field, str(total).encode())]

    redis.scripts[TAKE_PROXY_REPUTATION_LUA] = take
    redis.scripts[SUM_PROXY_HOT_LUA] = sum_hot
    return redis


async def _scene_with_players(tmp_path, redis, *player_ids, clock=None):
    protocol = Protocol()
    store = SqlitePlayerStore(tmp_path / "players.sqlite")
    rows = PlayerRowCodec(protocol)
    info = protocol.message("NodeInfo", node_id=7, node_uuid="craft-ledger-scene", zone_id=_ZONE_ID)
    scene = SceneNode(protocol, info, store, rows, lambda *_: None)
    clock = clock if clock is not None else [_START_HOUR * _HOUR_MS]
    scene.craft_proxy_directory = CraftProxyStallDirectory(
        protocol, control_redis=redis, zone_id=_ZONE_ID, clock_ms=lambda: clock[0],
    )
    scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))
    for pid in player_ids:
        seed = await store.load_and_claim(pid, f"seed-{pid}".encode())
        data = protocol.message("player_database", player_id=pid)
        data.profile_component.name = f"宗师{pid}"
        data.craft_component.SetInParent()
        await store.commit(seed, rows.project(data), release=True)
        await scene.enter(protocol.message(
            "PlayerEnterGameNodeRequest", player_id=pid, session_id=(pid << 17) | 7, scene_id=1,
            enter_gs_type=1, owner_revision=1,
        ), "gate")
    return scene


def _player(scene, pid):
    return scene.locations[pid].players[pid]


def _craft(scene, pid):
    return scene.rows.assemble(_player(scene, pid).snapshot).craft_component


async def _drain_as_master(scene, pid) -> bool:
    async with scene.player_lock(pid):
        return await drain_pending_proxy_reputation(scene, _player(scene, pid))


def test_keys_and_take_script_match_cpp():
    # 与 C++ PlayerCraftSystem::ProxyStallDirectoryKey / ProxyReputationLedgerKey / ProxyHotBucketKey / kTakeProxyReputationLua 同文。
    assert proxy_stall_directory_key(7) == "craft_proxy_stall:{ctl:7}"
    assert proxy_reputation_ledger_key(7) == "craft_proxy_reputation:{ctl:7}"
    assert proxy_hot_bucket_key(7, 491234) == "craft_proxy_hot:{ctl:7}:491234"
    window = proxy_hot_window_bucket_keys(7, 1000)
    assert len(window) == 24 and window[0] == "craft_proxy_hot:{ctl:7}:1000" and window[-1] == "craft_proxy_hot:{ctl:7}:977"
    assert TAKE_PROXY_REPUTATION_LUA == (
        "local k=KEYS[1] local p=ARGV[1] "
        "local v=redis.call('HMGET',k,p..':o',p..':s',p..':r',p..':g',p..':b') "
        "redis.call('HDEL',k,p..':o',p..':s',p..':r',p..':g',p..':b') "
        "return v"
    )
    assert SUM_PROXY_HOT_LUA == (  # C++ kSumProxyHotLua
        "local acc={} "
        "for i=1,#KEYS do "
        "local kv=redis.call('HGETALL',KEYS[i]) "
        "for j=1,#kv,2 do local n=tonumber(kv[j+1]) if n then acc[kv[j]]=(acc[kv[j]] or 0)+n end end "
        "end "
        "local out={} "
        "for f,n in pairs(acc) do out[#out+1]=f out[#out+1]=string.format('%d',n) end "
        "return out"
    )


def test_take_reply_and_ledger_parsing_follow_cpp():
    taken = _parse_taken_reputation([b"3", b"-2", None, b"99999999999999999999", "乙".encode()])
    assert (taken.orders, taken.supreme, taken.rare, taken.net_gold, taken.last_buyer) == (3, 0, 0, (1 << 63) - 1, "乙")
    assert _parse_taken_reputation([b"1", b"0"]) is None  # 形状不对 = 结果未知
    assert _parse_taken_reputation([b"1", 2, None, None, None]) is None

    pending = _parse_reputation_ledger({
        b"2:o": b"3", b"2:s": b"-1", b"2:g": b"40", b"2:b": "丙".encode(), b"2:24o": b"5",
        b"9:b": b"only-buyer", b"abc:o": b"1", b"0:o": b"1", b"5:": b"1", b"5:x": b"1",
    })
    assert set(pending) == {2}  # 只剩买家名、没有计数的不留；旧版热榜字段 :24o 不再认
    assert (pending[2].orders, pending[2].supreme, pending[2].net_gold, pending[2].last_buyer) == (3, 0, 40, "丙")

    bucket = _parse_hot_bucket({b"2:o": b"4", b"2:s": b"1", b"300:o": b"-3", b"7:b": b"x", b"x:o": b"1"})
    assert set(bucket) == {2} and (bucket[2].orders, bucket[2].supreme) == (4, 1)


def test_record_order_writes_cpp_ledger_fields_and_survives_redis_failure(tmp_path):
    async def run():
        redis = craft_ledger_redis()
        scene = await _scene_with_players(tmp_path, redis, 1)
        try:
            # 宗师 200 不在本节点：只记账，不投回账任务。
            await record_proxy_order_reputation(scene, 200, supreme_delta=1, net_gold_earned=95, buyer_name="甲侠士")
            await record_proxy_order_reputation(scene, 200, rare_delta=1, net_gold_earned=76, buyer_name="乙侠士")
            assert redis.data[_LEDGER] == {
                b"200:o": b"2", b"200:s": b"1", b"200:r": b"1", b"200:g": b"171", b"200:b": "乙侠士".encode(),
            }
            # 热榜进当前小时的桶，并带 25 小时过期。
            assert redis.data[_hot_key(_START_HOUR)] == {b"200:o": b"2", b"200:s": b"1"}
            assert 24 * _HOUR_MS < redis.ttl_ms(_hot_key(_START_HOUR)) <= 25 * _HOUR_MS
            assert not scene._actor_tasks

            # 控制 Redis 写失败只告警：买家的订单已提交，不能因展示计数失败；本节点镜像照记。
            redis.fail.add("hincrby")
            await record_proxy_order_reputation(scene, 201, supreme_delta=0, net_gold_earned=0, buyer_name="")
            assert scene.craft_proxy_directory.has_pending(201)
            assert not any(field.startswith(b"201:") for field in redis.data[_LEDGER])
        finally:
            await scene.close()

    asyncio.run(run())


def test_online_master_takes_atomically_in_own_actor_and_keeps_hot_counters(tmp_path):
    async def run():
        redis = craft_ledger_redis()
        scene = await _scene_with_players(tmp_path, redis, 1, 2)
        try:
            # 宗师 2 正在自己的回合里：买家记账不等他的锁、也不写他的存档，回账任务排在他后面。
            async with scene.player_lock(2):
                await asyncio.wait_for(record_proxy_order_reputation(
                    scene, 2, supreme_delta=1, net_gold_earned=95, buyer_name="甲侠士",
                ), timeout=1.0)
                assert _craft(scene, 2).proxy_orders_completed == 0
                assert redis.data[_LEDGER][b"2:o"] == b"1"
            await asyncio.gather(*tuple(scene._actor_tasks))

            craft = _craft(scene, 2)
            assert (craft.proxy_orders_completed, craft.supreme_forged_count) == (1, 1)
            assert craft.unread_proxy_digest_net_gold == 95 and craft.unread_proxy_digest_last_buyer == "甲侠士"
            assert redis.data[_LEDGER] == {}  # 待回账已全部取走
            assert redis.data[_hot_key(_START_HOUR)] == {b"2:o": b"1", b"2:s": b"1"}  # 热榜桶不受回账影响
            assert not scene.craft_proxy_directory.has_pending(2)
        finally:
            await scene.close()

    asyncio.run(run())


def test_stale_mirror_takes_nothing_and_never_double_counts(tmp_path):
    async def run():
        redis = craft_ledger_redis()
        scene = await _scene_with_players(tmp_path, redis, 2)
        directory = scene.craft_proxy_directory
        try:
            await directory.record_order(2, 0, 0, 50, "甲侠士")
            assert await _drain_as_master(scene, 2)
            # 在途的整表刷新带回取走前的值（或别的节点已回过账）：镜像又说「有待回账」，台账里却已经没了。
            await directory.record_order(2, 0, 0, 50, "甲侠士")
            for suffix in (b":o", b":g", b":b"):
                redis.data[_LEDGER].pop(b"2" + suffix)
            assert not await _drain_as_master(scene, 2)
            assert _craft(scene, 2).proxy_orders_completed == 1
            assert not directory.has_pending(2)
        finally:
            await scene.close()

    asyncio.run(run())


def test_definitive_commit_failure_gives_taken_back(tmp_path):
    async def run():
        redis = craft_ledger_redis()
        scene = await _scene_with_players(tmp_path, redis, 2)
        directory = scene.craft_proxy_directory
        real_commit = scene.store.commit
        try:
            await directory.record_order(2, 1, 0, 95, "甲侠士")

            async def reject(*_args, **_kwargs):
                raise StoreRejected("SAVE_PLAYER_NOT_HOLDER")

            scene.store.commit = reject
            assert not await _drain_as_master(scene, 2)
            assert _player(scene, 2).pending_save is None
            assert _craft(scene, 2).proxy_orders_completed == 0
            assert {f: redis.data[_LEDGER][f] for f in (b"2:o", b"2:s", b"2:g", b"2:b")} == {
                b"2:o": b"1", b"2:s": b"1", b"2:g": b"95", b"2:b": "甲侠士".encode(),
            }

            scene.store.commit = real_commit
            assert await _drain_as_master(scene, 2)
            assert (_craft(scene, 2).proxy_orders_completed, _craft(scene, 2).unread_proxy_digest_net_gold) == (1, 95)
        finally:
            scene.store.commit = real_commit
            await scene.close()

    asyncio.run(run())


def test_unknown_commit_outcome_keeps_batch_in_pending_save_without_give_back(tmp_path):
    async def run():
        redis = craft_ledger_redis()
        scene = await _scene_with_players(tmp_path, redis, 2)
        directory = scene.craft_proxy_directory
        real_commit = scene.store.commit
        try:
            await directory.record_order(2, 0, 1, 76, "乙侠士")

            async def in_doubt(*_args, **_kwargs):
                raise StoreUnavailable("save outcome unknown")

            scene.store.commit = in_doubt
            assert not await _drain_as_master(scene, 2)
            master = _player(scene, 2)
            assert master.pending_save is not None  # 这批口碑在待确认存档里，场景会重试落盘
            assert not any(field.endswith((b":o", b":r", b":g")) for field in redis.data[_LEDGER])
            assert not directory.has_pending(2)

            scene.store.commit = real_commit
            await scene.flush_player_rows(master)
            assert not await _drain_as_master(scene, 2)
            craft = _craft(scene, 2)
            assert (craft.proxy_orders_completed, craft.rare_books_crafted_count) == (1, 1)
            assert craft.unread_proxy_digest_net_gold == 76
        finally:
            scene.store.commit = real_commit
            await scene.close()

    asyncio.run(run())


def test_master_frozen_while_take_in_flight_gets_reputation_given_back(tmp_path):
    async def run():
        redis = craft_ledger_redis()
        scene = await _scene_with_players(tmp_path, redis, 2)
        directory = scene.craft_proxy_directory
        try:
            await directory.record_order(2, 0, 0, 30, "甲侠士")
            frozen: list[bool] = []
            real_mutable = scene.mutable
            scene.mutable = lambda player: not frozen and real_mutable(player)
            take = redis.scripts[TAKE_PROXY_REPUTATION_LUA]

            def take_then_freeze(keys_, argv):
                frozen.append(True)
                return take(keys_, argv)

            redis.scripts[TAKE_PROXY_REPUTATION_LUA] = take_then_freeze
            assert not await _drain_as_master(scene, 2)
            assert _craft(scene, 2).proxy_orders_completed == 0
            assert (redis.data[_LEDGER][b"2:o"], redis.data[_LEDGER][b"2:g"]) == (b"1", b"30")
            assert directory.has_pending(2)
        finally:
            await scene.close()

    asyncio.run(run())


def test_take_failure_or_malformed_reply_applies_nothing(tmp_path):
    async def run():
        redis = craft_ledger_redis()
        scene = await _scene_with_players(tmp_path, redis, 2)
        directory = scene.craft_proxy_directory
        try:
            await directory.record_order(2, 0, 0, 20, "甲侠士")
            redis.fail.add("eval")
            assert not await _drain_as_master(scene, 2)
            redis.fail.discard("eval")
            take = redis.scripts[TAKE_PROXY_REPUTATION_LUA]
            redis.scripts[TAKE_PROXY_REPUTATION_LUA] = lambda _keys, _argv: [b"1"]
            assert not await _drain_as_master(scene, 2)
            assert _craft(scene, 2).proxy_orders_completed == 0
            assert redis.data[_LEDGER][b"2:o"] == b"1" and directory.has_pending(2)

            redis.scripts[TAKE_PROXY_REPUTATION_LUA] = take
            assert await _drain_as_master(scene, 2)
            assert _craft(scene, 2).proxy_orders_completed == 1
        finally:
            await scene.close()

    asyncio.run(run())


def test_stall_publish_refresh_and_live_lookup_follow_cpp(tmp_path):
    async def run():
        redis = craft_ledger_redis()
        scene = await _scene_with_players(tmp_path, redis, 1, 2)
        directory = scene.craft_proxy_directory
        protocol = scene.protocol
        try:
            master = _player(scene, 2)
            data = scene.rows.assemble(master.snapshot)
            data.craft_component.scroll_mastery_exp = 60
            data.craft_component.proxy_stall_open = True
            data.craft_component.proxy_scroll_fee_gold = 80
            await scene.commit_player_data(master, data)

            # 开摊写穿：field 是十进制 player_id，value 是 CraftProxyStallInfo 二进制（C++ 照此解析）。
            await directory.sync_player(2, data)
            published = protocol.parse("CraftProxyStallInfo", redis.data[_STALLS][b"2"])
            assert (published.master_player_id, published.proxy_scroll_fee_gold) == (2, 80)

            # 别的节点（含 C++）发布的摊位、待回账与热榜整表刷新进镜像；本节点在线的宗师 2 被投回账任务。
            remote = protocol.message(
                "CraftProxyStallInfo", master_player_id=300, master_name="远端宗师",
                scroll_mastery_tier=3, proxy_scroll_fee_gold=90, viewer_gets_free_proxy=True,
            )
            redis.data[_STALLS][b"300"] = remote.SerializeToString()
            redis.data[_STALLS][b"301"] = b"\xff\xff"  # 坏数据跳过
            redis.data[_LEDGER] = {b"2:o": b"3", b"2:g": b"40", b"2:b": "丙".encode()}
            redis.data[_hot_key(_START_HOUR - 1)] = {b"300:o": b"2"}
            await directory.refresh(scene)
            assert directory.find(300).viewer_gets_free_proxy is False
            await asyncio.gather(*tuple(scene._actor_tasks))
            craft = _craft(scene, 2)
            assert (craft.proxy_orders_completed, craft.unread_proxy_digest_net_gold) == (3, 40)
            assert craft.unread_proxy_digest_last_buyer == "丙"
            assert redis.data[_LEDGER] == {}
            listed = {s.master_player_id: s for s in directory.list_stalls(scene=scene, viewer_player_id=1)}
            assert listed[2].proxy_orders_completed == 3 and listed[300].recent_24h_orders == 2

            # 镜像里没有本节点在线宗师时，买家仍以他的存档为准找到摊位；宗师不可写则找不到。
            redis.data[_STALLS].pop(b"2")
            await directory.refresh(scene)
            assert directory.find(2) is None
            assert directory.find(2, scene=scene).proxy_scroll_fee_gold == 80
            real_mutable = scene.mutable
            scene.mutable = lambda player: player.player_id != 2 and real_mutable(player)
            assert directory.find(2, scene=scene) is None
            scene.mutable = real_mutable

            # 重新写穿后收摊：HDEL。
            await directory.sync_player(2, scene.rows.assemble(master.snapshot))
            assert b"2" in redis.data[_STALLS]
            data = scene.rows.assemble(master.snapshot)
            data.craft_component.proxy_stall_open = False
            await directory.sync_player(2, data)
            assert b"2" not in redis.data[_STALLS]
        finally:
            await scene.close()

    asyncio.run(run())


def test_hot_ranking_counts_only_the_last_24_hourly_buckets(tmp_path):
    async def run():
        redis = craft_ledger_redis()
        clock = [_START_HOUR * _HOUR_MS + 59 * 60_000]  # 第 H 小时的最后一分钟
        scene = await _scene_with_players(tmp_path, redis, 1, clock=clock)
        try:
            await scene.craft_proxy_directory.record_order(300, 1, 0, 95, "甲侠士")
            clock[0] = (_START_HOUR + 1) * _HOUR_MS
            await scene.craft_proxy_directory.record_order(300, 0, 0, 76, "乙侠士")
            assert redis.data[_hot_key(_START_HOUR)] == {b"300:o": b"1", b"300:s": b"1"}
            assert redis.data[_hot_key(_START_HOUR + 1)] == {b"300:o": b"1"}

            # 另一个节点靠整表刷新看热榜（求和脚本读 24 个桶）。
            other = CraftProxyStallDirectory(
                scene.protocol, control_redis=redis, zone_id=_ZONE_ID, clock_ms=lambda: clock[0],
            )

            def counts():
                window = other.hot_window().get(300)
                return None if window is None else (window.orders, window.supreme)

            clock[0] = (_START_HOUR + 23) * _HOUR_MS
            await other.refresh(scene)
            assert counts() == (2, 1)  # 第 H+23 小时：两单都在窗口里
            # 两次刷新之间本节点又成交一单：先在本地小时桶里加上；下次刷新由桶和接替，不重复计。
            await other.record_order(300, 0, 0, 0, "")
            assert counts() == (3, 1)
            await other.refresh(scene)
            assert counts() == (3, 1)

            # 求和应答畸形（结果未知）：保留上次的桶和。
            sum_hot = redis.scripts[SUM_PROXY_HOT_LUA]
            redis.scripts[SUM_PROXY_HOT_LUA] = lambda _keys, _argv: [b"300:o"]
            clock[0] = (_START_HOUR + 24) * _HOUR_MS
            await other.refresh(scene)
            assert counts() == (3, 1)
            redis.scripts[SUM_PROXY_HOT_LUA] = sum_hot

            expected = {24: (2, 0), 25: (1, 0), 47: None}  # H 那单出窗 → H+1 那单出窗 → 全部出窗
            for hours_later, want in expected.items():
                clock[0] = (_START_HOUR + hours_later) * _HOUR_MS
                await other.refresh(scene)
                assert counts() == want, hours_later
        finally:
            await scene.close()

    asyncio.run(run())


def test_hot_ranking_window_without_redis_drops_stale_hours():
    clock = [_START_HOUR * _HOUR_MS]
    directory = CraftProxyStallDirectory(Protocol(), clock_ms=lambda: clock[0])

    async def run():
        await directory.record_order(300, 1, 0, 0, "")
        clock[0] = (_START_HOUR + 23) * _HOUR_MS
        assert directory.hot_window()[300].orders == 1
        clock[0] = (_START_HOUR + 24) * _HOUR_MS
        assert directory.hot_window() == {}
        assert directory._hot_local_buckets == {}  # 出窗的桶丢掉，本地单进程镜像不会无限长

    asyncio.run(run())


def test_refresh_loop_survives_unexpected_errors(monkeypatch):
    # runtime 把后台任务结束当成节点故障：展示用的目录刷新出意料之外的错，只记日志、下一轮再刷。
    directory = CraftProxyStallDirectory(Protocol(), control_redis=craft_ledger_redis())
    calls: list[int] = []

    async def flaky_refresh(_scene):
        calls.append(len(calls))
        if len(calls) == 1:
            raise RuntimeError("unexpected parse bug")
        raise asyncio.CancelledError  # 能走到第二轮就说明第一轮的异常没让循环退出；借取消结束循环

    monkeypatch.setattr(craft_module, "PROXY_DIRECTORY_REFRESH_INTERVAL_SEC", 0)
    monkeypatch.setattr(directory, "refresh", flaky_refresh)

    async def run():
        with pytest.raises(asyncio.CancelledError):
            await directory.refresh_loop(None)

    asyncio.run(run())
    assert calls == [0, 1]


def test_refresh_parts_fail_independently(tmp_path):
    # 同 C++ 三条刷新命令各自回调：求和脚本报错只保留旧热榜，摊位目录与待回账照常刷新。
    async def run():
        redis = craft_ledger_redis()
        scene = await _scene_with_players(tmp_path, redis, 1)
        directory = scene.craft_proxy_directory
        try:
            await directory.record_order(300, 1, 0, 0, "")
            await directory.refresh(scene)
            assert directory.hot_window()[300].orders == 1

            def broken_sum(_keys, _argv):
                raise RuntimeError("ERR user_script: simulated Lua failure")

            redis.scripts[SUM_PROXY_HOT_LUA] = broken_sum
            remote = scene.protocol.message(
                "CraftProxyStallInfo", master_player_id=301, master_name="远端宗师",
                scroll_mastery_tier=3, proxy_scroll_fee_gold=90,
            )
            redis.data[_STALLS] = {b"301": remote.SerializeToString()}
            redis.data[_LEDGER][b"301:o"] = b"2"
            await directory.refresh(scene)
            assert directory.hot_window()[300].orders == 1
            assert directory.find(301) is not None
            assert directory.has_pending(301)
        finally:
            await scene.close()

    asyncio.run(run())
