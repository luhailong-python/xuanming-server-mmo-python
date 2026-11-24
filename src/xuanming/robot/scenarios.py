"""机器人冒烟场景套件与并发压测执行器。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass, replace
from inspect import isawaitable
import logging
import math
from pathlib import Path
import secrets
from tempfile import TemporaryDirectory
import time
from typing import Any

from ..protocol import Protocol
from ..runtime import Runtime
from ..settings import Settings
from .client import GateRobotClient, RobotError

logger = logging.getLogger(__name__)


def _check(condition: bool, reason: str) -> None:
    if not condition:
        raise RobotError(reason)


def _check_reply_ok(reply: Any, label: str) -> None:
    if hasattr(reply, "error_message") and reply.error_message.id:
        params = list(reply.error_message.parameters)
        raise RobotError(f"{label} rejected with error_id={reply.error_message.id} params={params}")


def _percentile(sorted_samples: list[float], pct: float) -> float:
    if not sorted_samples:
        return 0.0
    index = max(0, min(len(sorted_samples) - 1, math.ceil(len(sorted_samples) * pct / 100.0) - 1))
    return round(sorted_samples[index], 3)


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    """单个冒烟场景的执行结果。"""

    name: str
    passed: bool
    duration_ms: float
    detail: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class StressReport:
    """并发压测汇总报告。"""

    concurrency: int
    rounds: int
    total_ops: int
    errors: int
    throughput_ops_sec: float
    p50_ms: float
    p95_ms: float
    p99_ms: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


async def smoke_monopoly(client: GateRobotClient) -> str:
    """校验大富翁棋盘查询、掷骰走格、幂等重放与地标升级（或状态一致性）。"""
    board = await client.rpc("monopoly.MonopolyClientPlayer", "GetBoard", {}, "monopoly.GetBoardResponse")
    _check_reply_ok(board, "Monopoly.GetBoard")
    tile_count = len(board.tiles)
    _check(tile_count > 0, "Monopoly board returned empty tiles")

    state = board.state
    board_id = board.board_id or 1
    first_req = client.protocol.message(
        "monopoly.RollDiceRequest",
        roll_seq=state.next_roll_seq,
        board_id=board_id,
    )
    roll = await client.rpc("monopoly.MonopolyClientPlayer", "RollDice", first_req, "monopoly.RollDiceResponse")
    _check_reply_ok(roll, "Monopoly.RollDice")
    _check(roll.durable and not roll.replayed, "Monopoly first roll was not durably applied")
    result = roll.result
    expected_pos = (state.position + result.die_one + result.die_two) % tile_count
    _check(result.to_position == expected_pos, "Monopoly roll destination mismatch")
    _check(result.state.dice_remaining == state.dice_remaining - 1, "Monopoly dice decrement mismatch")
    _check(result.state.coins == state.coins + result.coins_credited, "Monopoly coin delta mismatch")

    replay = await client.rpc("monopoly.MonopolyClientPlayer", "RollDice", first_req, "monopoly.RollDiceResponse")
    _check_reply_ok(replay, "Monopoly.RollDice(replay)")
    _check(replay.replayed and replay.durable, "Monopoly roll retry was not marked replayed+durable")
    _check(replay.result == result, "Monopoly roll replay payload diverged")

    state = result.state
    upgradable = [lm for lm in board.landmarks if lm.level < lm.max_level and lm.next_cost > 0]
    min_cost = min((lm.next_cost for lm in upgradable), default=0)

    rolls_done = 1
    while min_cost and state.coins < min_cost and state.dice_remaining > 0 and rolls_done < 8:
        next_req = client.protocol.message(
            "monopoly.RollDiceRequest",
            roll_seq=state.next_roll_seq,
            board_id=board_id,
        )
        step = await client.rpc("monopoly.MonopolyClientPlayer", "RollDice", next_req, "monopoly.RollDiceResponse")
        _check_reply_ok(step, "Monopoly.RollDice(extra)")
        _check(step.durable and not step.replayed, "Monopoly extra roll was not durable")
        state = step.result.state
        rolls_done += 1

    target = next((lm for lm in upgradable if state.coins >= lm.next_cost), None)
    if target is not None:
        up_req = client.protocol.message(
            "monopoly.UpgradeLandmarkRequest",
            landmark_id=target.landmark_id,
            expected_level=target.level,
            upgrade_seq=board.next_upgrade_seq or 1,
            board_id=board_id,
        )
        upgrade = await client.rpc(
            "monopoly.MonopolyClientPlayer",
            "UpgradeLandmark",
            up_req,
            "monopoly.UpgradeLandmarkResponse",
        )
        _check_reply_ok(upgrade, "Monopoly.UpgradeLandmark")
        _check(upgrade.durable and not upgrade.replayed, "Monopoly landmark upgrade was not durable")
        _check(upgrade.result.level == target.level + 1, "Monopoly landmark level did not advance")
        return (
            f"board={board_id} rolls={rolls_done} replayed=ok "
            f"upgraded_landmark={target.landmark_id}->lv{upgrade.result.level}"
        )

    refreshed = await client.rpc("monopoly.MonopolyClientPlayer", "GetBoard", {}, "monopoly.GetBoardResponse")
    _check_reply_ok(refreshed, "Monopoly.GetBoard(verify)")
    _check(refreshed.state.position == state.position, "Monopoly refreshed board position mismatch")
    return f"board={board_id} rolls={rolls_done} replayed=ok coins={refreshed.state.coins}"


async def smoke_bag_and_equip_craft(client: GateRobotClient) -> str:
    """校验背包查询、装备打造规则与每日材料自选奖励快照。"""
    bag = await client.rpc("SceneBagClientPlayer", "GetBag", {"bag_type": 0}, "GetBagResponse")
    _check_reply_ok(bag, "SceneBag.GetBag")
    _check(bag.bag.layout.capacity > 0, "GetBag returned non-positive capacity")

    rules = await client.rpc(
        "SceneBagClientPlayer",
        "GetEquipmentCraftRules",
        {},
        "GetEquipmentCraftRulesResponse",
    )
    _check_reply_ok(rules, "SceneBag.GetEquipmentCraftRules")

    reward = await client.rpc(
        "SceneBagClientPlayer",
        "GetEquipmentMaterialReward",
        {},
        "GetEquipmentMaterialRewardResponse",
    )
    _check_reply_ok(reward, "SceneBag.GetEquipmentMaterialReward")
    return (
        f"bag_cap={bag.bag.layout.capacity} recipes={len(rules.recipes)} "
        f"affixes={len(rules.affixes)} mat_avail={reward.snapshot.available_units}"
    )


async def smoke_attributes_and_skills(client: GateRobotClient) -> str:
    """校验属性面板与技能列表查询。"""
    panel = await client.rpc(
        "SceneAttributeClientPlayer",
        "GetAttributePanel",
        {},
        "GetAttributePanelResponse",
    )
    _check_reply_ok(panel, "SceneAttribute.GetAttributePanel")

    skills = await client.rpc("SceneSkillClientPlayer", "ListSkills", {}, "ListSkillsResponse")
    _check_reply_ok(skills, "SceneSkill.ListSkills")
    return f"schemes={len(panel.panel.schemes)} catalog_skills={len(skills.catalog)}"


async def smoke_pets(client: GateRobotClient) -> str:
    """校验宠物列表与宠物商店查询。"""
    pet_list = await client.rpc("ScenePetClientPlayer", "GetPetList", {}, "GetPetListResponse")
    _check_reply_ok(pet_list, "ScenePet.GetPetList")

    pet_shop = await client.rpc("ScenePetClientPlayer", "GetPetShop", {}, "GetPetShopResponse")
    _check_reply_ok(pet_shop, "ScenePet.GetPetShop")
    return f"pets={len(pet_list.pets.pets)} shop_offers={len(pet_shop.offers)}"


async def smoke_afk_pass_and_appearance(client: GateRobotClient) -> str:
    """校验挂机月卡与角色外观信息查询。"""
    afk_pass = await client.rpc("SceneAfkPassClientPlayer", "GetAfkPass", {}, "GetAfkPassResponse")
    _check_reply_ok(afk_pass, "SceneAfkPass.GetAfkPass")

    appearance = await client.rpc(
        "SceneAppearanceClientPlayer",
        "GetAppearanceInfo",
        {},
        "GetAppearanceInfoResponse",
    )
    _check_reply_ok(appearance, "SceneAppearance.GetAppearanceInfo")
    afk_active = afk_pass.info.expires_at > afk_pass.info.server_now
    return f"afk_active={afk_active} appearances={len(appearance.info.appearances)}"


async def smoke_season_and_trial_journal(client: GateRobotClient) -> str:
    """校验赛季状态与五行试炼志快照查询。"""
    season = await client.rpc("SceneSeasonClientPlayer", "GetSeasonState", {}, "GetSeasonStateResponse")
    _check_reply_ok(season, "SceneSeason.GetSeasonState")

    journal = await client.rpc(
        "SceneTrialJournalClientPlayer",
        "GetTrialJournal",
        {},
        "GetTrialJournalResponse",
    )
    _check_reply_ok(journal, "SceneTrialJournal.GetTrialJournal")
    return (
        f"season_id={season.state.season_id} "
        f"journal_chapters={len(journal.snapshot.chapters)}"
    )


async def smoke_mission_and_activity(client: GateRobotClient) -> str:
    """校验任务列表与日常活动列表查询。"""
    missions = await client.rpc("SceneMissionClientPlayer", "GetMissionList", {}, "GetMissionListResponse")
    _check_reply_ok(missions, "SceneMission.GetMissionList")

    activities = await client.rpc("SceneActivityClientPlayer", "GetActivityList", {}, "GetActivityListResponse")
    _check_reply_ok(activities, "SceneActivity.GetActivityList")
    return f"missions={len(missions.missions)} activities={len(activities.activities)}"


async def smoke_heartbeat_idle(client: GateRobotClient) -> str:
    """校验 Gate 心跳往返与短暂空闲后的连接活性。"""
    first = await client.heartbeat(101)
    _check(first.seq == 101, f"Heartbeat seq mismatch: expected 101, got {first.seq}")
    await asyncio.sleep(0.05)
    second = await client.heartbeat(102)
    _check(second.seq == 102, f"Heartbeat seq mismatch: expected 102, got {second.seq}")
    return "heartbeat_seqs=101,102"


SMOKE_SCENARIOS: dict[str, Callable[[GateRobotClient], Awaitable[str]]] = {
    "smoke_monopoly": smoke_monopoly,
    "smoke_bag_and_equip_craft": smoke_bag_and_equip_craft,
    "smoke_attributes_and_skills": smoke_attributes_and_skills,
    "smoke_pets": smoke_pets,
    "smoke_afk_pass_and_appearance": smoke_afk_pass_and_appearance,
    "smoke_season_and_trial_journal": smoke_season_and_trial_journal,
    "smoke_mission_and_activity": smoke_mission_and_activity,
    "smoke_heartbeat_idle": smoke_heartbeat_idle,
}


async def run_smoke_suite(
    client: GateRobotClient,
    scenarios: Sequence[str] | None = None,
) -> list[ScenarioResult]:
    """按顺序在已入场的 ``GateRobotClient`` 上执行指定（或全部）冒烟场景。"""
    selected = list(SMOKE_SCENARIOS.keys()) if scenarios is None else list(scenarios)
    unknown = [name for name in selected if name not in SMOKE_SCENARIOS]
    if unknown:
        raise ValueError(f"Unknown smoke scenarios: {', '.join(unknown)}")

    results: list[ScenarioResult] = []
    for name in selected:
        fn = SMOKE_SCENARIOS[name]
        started = time.perf_counter()
        try:
            detail = await fn(client)
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
            results.append(ScenarioResult(name=name, passed=True, duration_ms=elapsed_ms, detail=detail))
            logger.info("Smoke scenario %s passed in %.2fms (%s)", name, elapsed_ms, detail)
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
            results.append(ScenarioResult(name=name, passed=False, duration_ms=elapsed_ms, error=str(exc)))
            logger.warning("Smoke scenario %s failed in %.2fms: %s", name, elapsed_ms, exc)
    return results


def _ephemeral_local_settings(folder: str, *, use_websocket: bool = False) -> Settings:
    settings = Settings()
    return replace(
        settings,
        database=Path(folder) / "players.sqlite3",
        gate=replace(settings.gate, port=0, websocket_port=0 if use_websocket else None),
        scene=replace(settings.scene, port=0, grpc_port=0),
        battle=replace(settings.battle, port=0, grpc_port=0),
    )


def _runtime_ws_url(runtime: Runtime) -> str:
    if not runtime._websocket_servers:
        raise RobotError("Local runtime has no WebSocket listener configured")
    ws_port = runtime._websocket_servers[0].sockets[0].getsockname()[1]
    return f"ws://{runtime.settings.gate.host}:{ws_port}"


async def run_local_smoke(
    use_websocket: bool = False,
    *,
    scenarios: Sequence[str] | None = None,
) -> list[ScenarioResult]:
    """启动临时隔离的进程内 ``Runtime('local')`` 并跑完整个冒烟套件。"""
    with TemporaryDirectory(prefix="xuanming-robot-smoke-") as folder:
        settings = _ephemeral_local_settings(folder, use_websocket=use_websocket)
        dev_token = secrets.token_hex(16)
        async with Runtime(settings, "local", dev_token=dev_token) as runtime:
            async with GateRobotClient(runtime.protocol) as client:
                if use_websocket:
                    await client.connect_websocket(_runtime_ws_url(runtime))
                else:
                    endpoint = runtime.gate.info.endpoint
                    await client.connect_tcp(endpoint.ip, endpoint.port)
                ticket = runtime.gate.issue_ticket()
                await client.verify_ticket(ticket.payload, ticket.signature)
                await client.login_and_enter("local", dev_token)
                results = await run_smoke_suite(client, scenarios=scenarios)
                await client.leave_game()
    return results


async def _issue_ticket_pair(ticket_issuer: Callable[[], Any]) -> tuple[bytes, bytes]:
    ticket = ticket_issuer()
    if isawaitable(ticket):
        ticket = await ticket
    if isinstance(ticket, tuple) and len(ticket) == 2:
        return bytes(ticket[0]), bytes(ticket[1])
    if hasattr(ticket, "payload") and hasattr(ticket, "signature"):
        return bytes(ticket.payload), bytes(ticket.signature)
    raise RobotError("ticket_issuer must return (payload, signature) or ClientTokenVerifyRequest")


# 压测轮次使用的标准只读 Scene RPC（在 MessageLimiter 中单连接限流为 10 req/s）。
_STRESS_OPS: tuple[tuple[str, str, dict[str, Any], str], ...] = (
    ("SceneBagClientPlayer", "GetBag", {"bag_type": 0}, "GetBagResponse"),
    ("SceneAttributeClientPlayer", "GetAttributePanel", {}, "GetAttributePanelResponse"),
    ("ScenePetClientPlayer", "GetPetList", {}, "GetPetListResponse"),
)


async def run_stress(
    *,
    host: str,
    port: int,
    ticket_issuer: Callable[[], Any],
    access_token: str,
    concurrency: int = 4,
    rounds: int = 3,
    use_websocket: bool = False,
    websocket_url: str = "",
    protocol: Protocol | None = None,
) -> StressReport:
    """并发启动多条机器人会话执行多轮 Scene RPC 压测并汇总吞吐与分位延迟。"""
    if concurrency < 1 or rounds < 1:
        raise ValueError("concurrency and rounds must be >= 1")
    shared_protocol = protocol or Protocol()
    latencies_ms: list[float] = []
    total_ops = 0
    errors = 0

    async def worker(worker_idx: int) -> tuple[list[float], int, int]:
        local_latencies: list[float] = []
        local_ops = 0
        local_errors = 0
        account = f"stress-{worker_idx}-{secrets.token_hex(4)}"
        window_start = time.monotonic()
        window_rounds = 0
        try:
            async with GateRobotClient(shared_protocol) as client:
                if use_websocket:
                    url = websocket_url or f"ws://{host}:{port}"
                    await client.connect_websocket(url)
                else:
                    await client.connect_tcp(host, port)
                payload, signature = await _issue_ticket_pair(ticket_issuer)
                await client.verify_ticket(payload, signature)
                await client.login_and_enter(account, access_token)
                for _ in range(rounds):
                    # 单会话 1s 内同消息上限为 10 次，高轮次时按窗口节流避免误触限流。
                    if window_rounds >= 8:
                        elapsed = time.monotonic() - window_start
                        if elapsed < 1.05:
                            await asyncio.sleep(1.05 - elapsed)
                        window_start = time.monotonic()
                        window_rounds = 0
                    for service, method, req_fields, resp_name in _STRESS_OPS:
                        local_ops += 1
                        op_start = time.perf_counter()
                        try:
                            reply = await client.rpc(service, method, req_fields, resp_name)
                            _check_reply_ok(reply, f"{service}.{method}")
                            local_latencies.append((time.perf_counter() - op_start) * 1000.0)
                        except Exception as exc:
                            local_errors += 1
                            logger.warning("Stress worker %d op %s.%s failed: %s", worker_idx, service, method, exc)
                    window_rounds += 1
                await client.leave_game()
        except Exception as exc:
            # 建连或登录阶段失败，按剩余未执行的轮次计错。
            remaining = max(1, rounds * len(_STRESS_OPS) - local_ops)
            local_ops += remaining
            local_errors += remaining
            logger.warning("Stress worker %d aborted: %s", worker_idx, exc)
        return local_latencies, local_ops, local_errors

    started = time.perf_counter()
    worker_results = await asyncio.gather(*(worker(idx) for idx in range(concurrency)))
    wall_seconds = max(time.perf_counter() - started, 1e-6)

    for w_latencies, w_ops, w_errors in worker_results:
        latencies_ms.extend(w_latencies)
        total_ops += w_ops
        errors += w_errors

    latencies_ms.sort()
    throughput = round((total_ops - errors) / wall_seconds, 2)
    return StressReport(
        concurrency=concurrency,
        rounds=rounds,
        total_ops=total_ops,
        errors=errors,
        throughput_ops_sec=throughput,
        p50_ms=_percentile(latencies_ms, 50.0),
        p95_ms=_percentile(latencies_ms, 95.0),
        p99_ms=_percentile(latencies_ms, 99.0),
    )


async def run_local_stress(
    concurrency: int = 4,
    rounds: int = 2,
    *,
    use_websocket: bool = False,
) -> StressReport:
    """启动临时隔离的进程内 ``Runtime('local')`` 并执行并发压测。"""
    with TemporaryDirectory(prefix="xuanming-robot-stress-") as folder:
        settings = _ephemeral_local_settings(folder, use_websocket=use_websocket)
        dev_token = secrets.token_hex(16)
        async with Runtime(settings, "local", dev_token=dev_token) as runtime:
            endpoint = runtime.gate.info.endpoint
            ws_url = _runtime_ws_url(runtime) if use_websocket else ""
            return await run_stress(
                host=endpoint.ip,
                port=endpoint.port,
                ticket_issuer=runtime.gate.issue_ticket,
                access_token=dev_token,
                concurrency=concurrency,
                rounds=rounds,
                use_websocket=use_websocket,
                websocket_url=ws_url,
                protocol=runtime.protocol,
            )
