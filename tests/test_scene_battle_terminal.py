import asyncio
from types import SimpleNamespace

from xuanming.nodes.battle_control import service_digest, sign_service_call, verify_service_call
from xuanming.nodes.scene_battle import TERMINAL, Freeze, SceneBattle, _READ, _REMOVE, _RENEW
from xuanming.protocol import Protocol


class _BattleRedis:
    def __init__(self, context, ttl):
        self.value = str(context.battle_id)
        self.context = context.SerializeToString()
        self.ttl = ttl
        self.fail_remove = False
        self.renewals = []

    async def eval(self, script, keys, lock, ctx, *args):
        assert keys == 2 and lock == "battle:lock:{101}" and ctx == "battle:ctx:{101}"
        if script == _READ:
            return (self.value, self.context, self.ttl) if self.value else None
        if not self.value or args[0] != self.value:
            return 0
        if script == _RENEW:
            _, ttl, self.context = args
            self.renewals.append(ttl)
            self.ttl = max(ttl, self.ttl)
            return 1
        assert script == _REMOVE and args[1] == b""
        if self.fail_remove:
            raise OSError("simulated Redis timeout")
        self.value = None
        return 2


def _setup(state, deadline=10_000):
    protocol = Protocol()
    clock = SimpleNamespace(now=3000)
    player = SimpleNamespace(
        player_id=101, session_id=7, disconnect_at=None, battle_expired_id=0,
        snapshot=protocol.message("player_database"),
    )
    lock = asyncio.Lock()
    pushed = []
    scene = SimpleNamespace(
        protocol=protocol, _lock=lambda _: lock, _push=lambda *args: pushed.append(args),
        locations={101: SimpleNamespace(players={101: player})},
        rows=SimpleNamespace(assemble=lambda snapshot: snapshot),
    )
    context = protocol.message("InBattleComp", battle_id=40, battle_node_id=2,
                               deadline_ms=deadline, prepare_deadline_ms=3000, state=state)
    redis = _BattleRedis(context, ttl=60)
    return protocol, clock, player, lock, pushed, scene, context, redis


def test_prepare_expiry_keeps_redis_lock_for_late_confirmation():
    async def exercise():
        protocol, clock, player, _, pushed, scene, context, redis = _setup(1)
        battle = SceneBattle(scene, None, now_ms=lambda: clock.now, redis=redis)
        battle._freezes[101] = Freeze(context, player.session_id)
        battle.reap()
        assert battle.frozen(101) and not battle._reaping
        clock.now += 1
        battle.reap()
        await battle._reaping[101]
        assert not battle.frozen(101) and redis.value == "40"
        await battle.cancel(protocol.message("CancelBattlePrepareRequest", player_id=101, battle_id=40))
        assert redis.value == "40"

        event = protocol.message("BattleConfirmedEvent", player_id=101, battle_id=40, deadline_ms=10_000)
        await battle.confirm(event)
        entry = battle._freezes[101]
        assert (entry.due, entry.hold_until) == (130_000, 1_030_000)
        assert redis.renewals == [(entry.hold_until - clock.now) // 1000 + 60]
        assert protocol.parse("InBattleComp", redis.context).state == 2
        assert len(pushed) == 1 and pushed[0][2].battle_id == 40
        event.deadline_ms += 120_000
        await battle.confirm(event)
        assert entry.context.deadline_ms == 10_000 and len(pushed) == 1

    asyncio.run(exercise())


def test_terminal_complete_holds_but_cap_releases_even_with_query_in_flight():
    async def exercise():
        protocol, clock, player, lock, _, scene, context, redis = _setup(2)
        secret = b"x" * 32
        sent = 0
        second_query = asyncio.Event()

        async def match(path, body):
            nonlocal sent
            request = protocol.parse("match.GetBattleTerminalDecisionRequest", body)
            assert path == TERMINAL and request.battle_id == 40
            assert verify_service_call(path, request, "scene", clock.now, secret)
            sent += 1
            if sent > 1:
                second_query.set()
                await asyncio.Future()
            response = protocol.message("match.GetBattleTerminalDecisionResponse", decision=1,
                                        request_digest=service_digest(request))
            sign_service_call(path + "/reply", response, "match", clock.now, secret)
            return response.SerializeToString()

        battle = SceneBattle(scene, None, now_ms=lambda: clock.now, redis=redis, match_call=match,
                             terminal_secret=secret)
        entry = battle._freezes[101] = Freeze(context, player.session_id)
        clock.now = entry.due
        battle.reap()
        assert not battle._reaping
        clock.now += 1
        battle.reap()
        await battle._reaping[101]
        assert battle.frozen(101) and sent == 1
        assert redis.renewals == [(entry.hold_until - clock.now) // 1000 + 60]

        clock.now = entry.retry_at
        battle.reap()
        lookup = battle._reaping[101]
        await second_query.wait()
        redis.fail_remove = True
        async with lock:
            clock.now = entry.hold_until + 1
            battle.reap()
            cleanup = battle._reaping[101]
            assert cleanup is not lookup
            battle.reap()
            assert battle._reaping[101] is cleanup
        await cleanup
        assert lookup.cancelled() and sent == 2 and not battle.frozen(101)
        assert player.battle_expired_id == 40 and redis.value == "40"

        await battle.restore(player)
        assert not battle.frozen(101)
        redis.fail_remove = False
        await battle.confirm(protocol.message("BattleConfirmedEvent", player_id=101, battle_id=40, deadline_ms=10_000))
        assert not battle.frozen(101) and redis.value is None

    asyncio.run(exercise())


def test_applied_ledger_releases_without_match_query():
    async def exercise():
        _, clock, player, _, _, scene, context, redis = _setup(2)
        cursor = player.snapshot.inbox_ledger.cursors.add(domain=2, decided_through_seq=1)
        cursor.recent.add(seq=1, msg_type=4, request_id=40, outcome=1)

        async def unexpected(*_):
            raise AssertionError("durable local APPLIED is conclusive")

        battle = SceneBattle(scene, None, now_ms=lambda: clock.now, redis=redis, match_call=unexpected,
                             terminal_secret=b"x" * 32)
        entry = battle._freezes[101] = Freeze(context, player.session_id)
        clock.now = entry.due + 1
        battle.reap()
        await battle._reaping[101]
        assert not battle.frozen(101) and redis.value is None

    asyncio.run(exercise())
