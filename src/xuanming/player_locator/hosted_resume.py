"""挂机月卡离线托管归位（逐项对齐 Go ``hosted_resume.go``）。

开启挂机月卡的玩家断线后，角色由 scene 节点离线托管并每 20 秒续期 ``player:{<pid>}:hosted_presence``；
本服务在每 300 秒续租断线租约后核对期望状态与实际心跳：
1. 先按当前月卡剩余秒数对齐 ``player:{<pid>}:hosted_return`` 的 TTL（即使此刻仍有在场心跳）；
2. 若在场心跳缺失且月卡剩余不少于 60 秒、归位记录属于本次登录，则向 ``SceneManager.EnterScene``
   发起 ``hosted_placement=True`` 的托管归位；
3. 若 ``SceneManager`` 返回 ``FAILED_PRECONDITION``（归属修订失效等），按值守卫删除该归位记录。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

import grpc
from google.protobuf.message import DecodeError

from ..protocol import Protocol
from .store import SessionStore

logger = logging.getLogger(__name__)

# 月卡剩余不足 60 秒不再发起归位：角色落地即面临月卡到期退场，避免无效调度
HOSTED_RESUME_MIN_PASS_REMAINING_SEC = 60.0
HOSTED_RESUME_TIMEOUT_SEC = 5.0
HOSTED_RETURN_RECORD_VERSION = "v1"

_FIELD_BITS = (32, 64, 32, 64)


@dataclass(frozen=True, slots=True)
class HostedReturnRecord:
    """scene 节点写入的托管归位记录（``v1:<zone>:<owner_revision>:<session_id>:<login_generation>``）。"""

    zone_id: int
    owner_revision: int
    session_id: int
    login_generation: int


def parse_hosted_return_record(value: str) -> HostedReturnRecord:
    """严格解析托管归位记录：版本必须为 v1，四个字段必须为无前导零的非零十进制无符号整数。"""
    parts = value.split(":")
    if len(parts) != 5 or parts[0] != HOSTED_RETURN_RECORD_VERSION:
        raise ValueError(f"unexpected hosted return record {value!r}")
    fields: list[int] = []
    for raw, bits in zip(parts[1:], _FIELD_BITS):
        if not raw or not raw.isascii() or not raw.isdigit() or raw[0] == "0":
            raise ValueError(f"unexpected hosted return record {value!r}")
        parsed = int(raw)
        if parsed <= 0 or parsed >= (1 << bits):
            raise ValueError(f"unexpected hosted return record {value!r}")
        fields.append(parsed)
    return HostedReturnRecord(
        zone_id=fields[0],
        owner_revision=fields[1],
        session_id=fields[2],
        login_generation=fields[3],
    )


class HostedResumeVerdict(str, Enum):
    """续租时的托管归位判定结论（7 种，与 Go ``hostedResumeVerdict`` 同口径）。"""

    PRESENT = "present"
    PASS_ENDING = "pass_ending"
    NO_RECORD = "no_record"
    RECORD_MALFORMED = "record_malformed"
    OTHER_LOGIN = "other_login"
    NO_ZONE = "no_zone"
    SEND = "send"


@dataclass(frozen=True, slots=True)
class HostedResumeFacts:
    """``decide_hosted_resume`` 的纯数据输入快照。"""

    presence_present: bool = False
    pass_remaining_sec: float = 0.0
    record_found: bool = False
    record_error: str | None = None
    record: HostedReturnRecord | None = None
    session_id: int = 0
    login_generation: int = 0
    location_zone: int = 0


def record_belongs_to_session(facts: HostedResumeFacts) -> bool:
    """判断归位记录是否合法且归属于当前这条断线会话对应的登录代次。"""
    return (
        facts.record_found
        and facts.record_error is None
        and facts.record is not None
        and facts.login_generation != 0
        and facts.record.session_id == facts.session_id
        and facts.record.login_generation == facts.login_generation
    )


def decide_hosted_resume(facts: HostedResumeFacts) -> tuple[HostedResumeVerdict, int]:
    """按固定顺序做无副作用归位判定；优先取 location 权威 zone，缺失时回退归位记录中的 zone。"""
    if facts.presence_present:
        return HostedResumeVerdict.PRESENT, 0
    if facts.pass_remaining_sec < HOSTED_RESUME_MIN_PASS_REMAINING_SEC:
        return HostedResumeVerdict.PASS_ENDING, 0
    if not facts.record_found:
        return HostedResumeVerdict.NO_RECORD, 0
    if facts.record_error is not None or facts.record is None:
        return HostedResumeVerdict.RECORD_MALFORMED, 0
    if not record_belongs_to_session(facts):
        return HostedResumeVerdict.OTHER_LOGIN, 0
    zone = facts.location_zone or facts.record.zone_id
    if zone == 0:
        return HostedResumeVerdict.NO_ZONE, 0
    return HostedResumeVerdict.SEND, zone


def return_record_ttl_alignment(facts: HostedResumeFacts) -> int | None:
    """计算归位记录应与挂机月卡对齐的 TTL 秒数；不属于本次登录或不足 1 秒时返回 None。"""
    if not record_belongs_to_session(facts) or facts.pass_remaining_sec < 1.0:
        return None
    return int(facts.pass_remaining_sec)


def _grpc_status_code(error: Exception):
    code_fn = getattr(error, "code", None)
    return code_fn() if callable(code_fn) else None


async def read_hosted_resume_facts(
    protocol: Protocol,
    store: SessionStore,
    session,
    now_unix_sec: float,
) -> tuple[HostedResumeFacts, str]:
    """读取托管归位判定所需的四个同槽键；任一存储错误直接上抛（fail-closed 不发归位）。"""
    player_id = int(session.player_id)
    presence_raw = await store.get_hosted_presence(player_id)
    record_raw = await store.get_hosted_return(player_id)
    pass_raw = await store.get_afk_pass_bytes(player_id)
    location_raw = await store.get_location_bytes(player_id)

    pass_remaining_sec = 0.0
    if pass_raw is not None:
        try:
            expiry_unix_sec = int(pass_raw.decode("utf-8"))
            pass_remaining_sec = float(expiry_unix_sec) - float(now_unix_sec)
        except (UnicodeDecodeError, ValueError):
            pass_remaining_sec = 0.0

    record_found = record_raw is not None
    raw_record = ""
    parsed_record: HostedReturnRecord | None = None
    record_error: str | None = None
    if record_raw is not None:
        try:
            raw_record = record_raw.decode("utf-8")
            parsed_record = parse_hosted_return_record(raw_record)
        except (UnicodeDecodeError, ValueError) as exc:
            record_error = str(exc)

    location_zone = 0
    if location_raw is not None:
        try:
            loc = protocol.parse("storage.PlayerLocation", location_raw)
        except DecodeError as exc:
            raise RuntimeError(f"decode SceneManager location for player {player_id}: {exc}") from exc
        location_zone = int(loc.zone_id)

    facts = HostedResumeFacts(
        presence_present=presence_raw is not None,
        pass_remaining_sec=pass_remaining_sec,
        record_found=record_found,
        record_error=record_error,
        record=parsed_record,
        session_id=int(session.session_id),
        login_generation=int(session.login_generation),
        location_zone=location_zone,
    )
    return facts, raw_record


async def resume_hosted_if_lost(
    protocol: Protocol,
    store: SessionStore,
    scene_manager,
    session,
    now_unix_sec: float,
) -> HostedResumeVerdict | None:
    """AFK 续租成功后检查离线托管心跳并在失联时触发托管归位；错误只记日志不中断续租路径。"""
    player_id = int(session.player_id)
    try:
        facts, raw_record = await read_hosted_resume_facts(protocol, store, session, now_unix_sec)
    except Exception as exc:
        logger.error("LeaseMonitor: hosted resume check skipped player=%d error=%s", player_id, exc)
        return None

    # 无论在场心跳是否仍在，均先对齐归位记录 TTL 到当前月卡到期时间；失败不阻塞后续归位判定
    ttl_sec = return_record_ttl_alignment(facts)
    if ttl_sec is not None:
        try:
            await store.expire_hosted_return_if_value(player_id, raw_record, ttl_sec)
        except Exception as exc:
            logger.error("LeaseMonitor: align hosted return record ttl failed player=%d error=%s", player_id, exc)

    verdict, zone = decide_hosted_resume(facts)
    if verdict in (HostedResumeVerdict.PRESENT, HostedResumeVerdict.NO_RECORD, HostedResumeVerdict.PASS_ENDING):
        return verdict
    if verdict != HostedResumeVerdict.SEND:
        logger.error(
            "LeaseMonitor: hosted character has no responsible scene node but cannot be resumed "
            "player=%d verdict=%s session=%d login_generation=%d record_err=%s",
            player_id, verdict.value, session.session_id, session.login_generation, facts.record_error,
        )
        return verdict

    if scene_manager is None:
        logger.error("LeaseMonitor: hosted resume not sent player=%d zone=%d reason=scene_manager_not_configured",
                     player_id, zone)
        return verdict

    assert facts.record is not None
    logger.info(
        "LeaseMonitor: requesting hosted resume player=%d zone=%d session=%d login_generation=%d owner_revision=%d",
        player_id, zone, session.session_id, session.login_generation, facts.record.owner_revision,
    )
    try:
        resp = await scene_manager.enter_scene(
            player_id=player_id,
            session_id=int(session.session_id),
            login_generation=int(session.login_generation),
            owner_revision=facts.record.owner_revision,
            gate_zone_id=zone,
            hosted_placement=True,
        )
    except Exception as exc:
        if _grpc_status_code(exc) == grpc.StatusCode.FAILED_PRECONDITION:
            dropped = False
            del_err: Exception | None = None
            try:
                dropped = await store.delete_hosted_return_if_value(player_id, raw_record)
            except Exception as derr:
                del_err = derr
            logger.info(
                "LeaseMonitor: hosted resume refused by scene_manager (ownership revision invalid) "
                "player=%d dropped=%s del_err=%s error=%s",
                player_id, dropped, del_err, exc,
            )
            return verdict
        logger.error(
            "LeaseMonitor: hosted resume failed (next lease renewal retries) player=%d error=%s",
            player_id, exc,
        )
        return verdict

    error_code = int(getattr(resp, "error_code", 0) or 0)
    if error_code != 0:
        error_msg = str(getattr(resp, "error_message", "") or "")
        logger.info(
            "LeaseMonitor: hosted resume not placed yet (next lease renewal retries) "
            "player=%d error_code=%d msg=%r",
            player_id, error_code, error_msg,
        )
        return verdict

    logger.info("LeaseMonitor: hosted resume placed player=%d zone=%d", player_id, zone)
    return verdict
