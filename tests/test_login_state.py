import asyncio

import pytest

from login_fakes import FakeRedis
from xuanming.login import keys, login_session, session_fence, tokens
from xuanming.login.session_fence import Outcome


def run(coroutine):
    return asyncio.run(coroutine)


def test_key_builders_match_go_golden_vectors():
    assert keys.crc16("123456789") == 0x31C3
    expected = {0: "afps", 1: "emj", 2: "ahwf", 100: "aotc", 12739: "aarc", 16383: "hia"}
    assert {slot: keys.slot_tag(slot) for slot in expected} == expected
    tag = keys.account_tag("123456789")
    assert tag == "aarc" and keys.valid_account_tag(tag) and not keys.valid_account_tag("zzzz")
    assert keys.account_device_set_key("123456789") == "login_account_sessions:{aarc}:123456789"
    assert keys.session_fence_key(keys.connection_id("gi", 9)) == "login:sessfence:{gi:9}"
    assert keys.login_generation_key(42) == "player:{42}:login_generation"
    assert keys.player_locker_key(42) == "player_locker:{42}"


def test_token_family_issue_rotate_replay_and_reuse_revocation():
    async def exercise():
        redis = FakeRedis()
        manager = tokens.TokenManager(redis)
        pair = await manager.issue("robot_1", "password")
        tag, _, _ = pair.access_token.partition(".")
        assert tag == keys.account_tag("robot_1")
        assert (await manager.validate_access(pair.access_token)).account == "robot_1"
        with pytest.raises(tokens.AccessRejected):
            await manager.validate_access(pair.refresh_token)  # wrong kind bit, rejected before Redis
        with pytest.raises(tokens.RefreshRejected):
            await manager.refresh(pair.access_token)

        rotated = await manager.refresh(pair.refresh_token)
        assert rotated.refresh_token != pair.refresh_token
        assert (await manager.validate_access(pair.access_token)).account == "robot_1"  # same family stays valid
        assert await manager.refresh(pair.refresh_token) == rotated  # lost-reply retry inside the grace window

        await manager.refresh(rotated.refresh_token)
        with pytest.raises(tokens.RefreshRejected):
            await manager.refresh(pair.refresh_token)  # successor already rotated: reuse revokes the family
        with pytest.raises(tokens.AccessRejected):
            await manager.validate_access(rotated.access_token)

    run(exercise())


def test_session_fence_handshake_leave_and_renew_restore():
    async def exercise():
        redis = FakeRedis()
        conn = keys.connection_id("gate-uuid", 7)
        assert await session_fence.commit_login(redis, conn, "robot_1", 60_000)
        assert await session_fence.check_enter_game(redis, conn, 42) == (Outcome.OK, 0)
        assert await session_fence.bind_player(redis, conn, 42, 0) is Outcome.OK
        assert await session_fence.check_enter_game(redis, conn, 43) == (Outcome.OTHER_PLAYER, 0)
        assert redis.ttl_ms(keys.session_fence_key(conn)) > session_fence.SESSION_TTL_MS  # bound record lifetime

        await redis.delete(keys.login_session_key(conn))  # login state expired while still bound
        assert await login_session.renew(redis, conn, 60_000)
        assert await login_session.get_account(redis, conn) == "robot_1"
        assert await redis.smembers(keys.account_device_set_key("robot_1")) == {conn.encode()}

        assert await session_fence.leave(redis, conn) == 42
        assert await session_fence.leave(redis, conn) == 42  # replay returns the same player to settle
        assert await session_fence.recheck(redis, conn, 0) is Outcome.LEFT
        assert await session_fence.close(redis, conn) == 0
        assert not await session_fence.commit_login(redis, conn, "robot_1", 60_000)  # closed: nothing written
        assert await session_fence.renew(redis, conn, 60_000) == (False, "", False)

    run(exercise())


def test_disconnect_before_bind_is_seen_by_the_enter_chain():
    async def exercise():
        redis = FakeRedis()
        conn = keys.connection_id("gate-uuid", 8)
        _, gen = await session_fence.check_enter_game(redis, conn, 42)
        assert await session_fence.close(redis, conn) == 0
        assert await session_fence.bind_player(redis, conn, 42, gen) is Outcome.CLOSED

    run(exercise())
