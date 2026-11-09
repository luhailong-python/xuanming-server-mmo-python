"""Scene 入场资格、同区换图及跨区交接。"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from itertools import count
import logging
import os
import time
from uuid import uuid4
from weakref import WeakKeyDictionary

from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import GrpcClient
from ..settings import secret
from . import _handoff_lua as lua
from .battle_control import service_digest, sign_service_call, verify_service_call

_LOG = logging.getLogger(__name__)
_DATA = "/data_service.DataService/"
_MANAGER = "/scene_manager.SceneManager/"
_TRADE_SEAL = "/scene_node.SceneNodeGrpc/SealAccountRoleTrade"
_TEAM_MEMBERSHIP = "/teampb.TeamAuthority/ReadMembership"
_ENTER_FAILED = 3023
_ZONE_BUSY = 3027
_HANDOFF_PENDING = 18
_LATE_DISPATCH = 35.0
_REPLY_WATCHDOG = 30.0
_FREEZE_CAP = 70.0
_RECOVERY_TICK = 5.0


def _keys(player_id: int) -> tuple[str, str, str, str]:
    prefix = f"player:{{{player_id}}}:"
    return tuple(prefix + name for name in ("owner_epoch", "handoff_fence", "handoff", "location"))


def _rotate(fence: str, state: str, *, epoch: int = 0) -> str:
    prior, scene, generation, admitted, _, _ = fence.split("|")
    return f"{epoch or prior}|{scene}|{generation}|{admitted}|{state}|{uuid4().hex}"


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


class AdmissionError(ValueError):
    """入场资格没有得到权威确认。"""


@dataclass(slots=True)
class Admission:
    identity: Message
    fence: str
    released: str = ""
    revoked: str = ""
    release_stamp: int = 0
    retired: bool = False


@dataclass(slots=True)
class _Travel:
    player: object
    zone: int
    scene_id: int
    scene_config: int
    gate_uuid: str
    session_id: int
    generation: int
    owner_revision: int
    mirror_config: int = 0
    silent: bool = False
    source_scene: int = 0
    client_pick: bool = False
    frozen: bool = False
    marked: bool = False
    started_at: float = 0
    enter_sent_at: float = 0
    stamp: int = 0
    held: str = ""
    published: str = ""
    revoked: str = ""
    epoch: int = 0
    resume: tuple[int, bytes, str] | None = None
    task: asyncio.Task | None = None


@dataclass(slots=True)
class _TradeDrain:
    seal: Message
    player: object
    saved: bool = False
    floor: tuple[int, int] | None = None


class SceneTravel:
    def __init__(self, scene, tables, coordinator, redis, admission_client, admission_secret: bytes,
                 *, team_client=None, trade_secret: bytes = b"") -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self._coordinator = coordinator
        self._redis = redis
        self._admission_client = admission_client
        self._team_client = team_client
        self._secret = admission_secret
        self._trade_secret = trade_secret
        self._world_configs = {row.scene_id for row in tables["World"].rows()}
        self._mirrors = tables["Mirror"]
        self._admission_ids = count(1)
        self._correlations = count(1)
        self._last_stamp = 0
        self._trips: dict[int, _Travel] = {}
        self._tasks: set[asyncio.Task] = set()
        self._trade_tasks: set[asyncio.Task] = set()
        self._trade_drains: dict[int, _TradeDrain] = {}
        self._trade_completed: OrderedDict[tuple[int, int], bytes] = OrderedDict()
        self._drain_after = WeakKeyDictionary()
        self._stopping = False
        scene.register("SceneSceneClientPlayer", "EnterScene", self.change_scene)
        scene.register("SceneSceneClientPlayer", "TravelToZone", self.travel_to_zone)

    @property
    def redis(self):
        return self._redis

    async def _eval(self, script: str, player_id: int, *args, location: bool = False):
        keys = _keys(player_id) if location else _keys(player_id)[:3]
        return await self._redis.eval(script, len(keys), *keys, *args)

    async def _data(self, method: str, **fields):
        path = _DATA + method
        request = self.protocol.message(f"data_service.{method}Request", **fields)
        sign_service_call(path, request, "scene", _now_ms(), self._secret)
        response = await self._admission_client.call(path, request.SerializeToString(deterministic=True))
        response_type = "google.protobuf.Empty" if method == "AckRoleTradeDrain" else f"data_service.{method}Response"
        return self.protocol.parse(response_type, response)

    async def admit(self, request) -> Admission:
        if not all((request.player_id, request.owner_revision, request.owner_epoch,
                    request.login_generation, request.session_id)):
            raise AdmissionError("场景入场缺少原账号修订、会话或归属 epoch")
        if self.sealed(request.player_id):
            raise AdmissionError("角色原入场世代正在封存")
        info = self.scene.info
        identity = self.protocol.message(
            "common.base.RoleAdmissionIdentity", player_id=request.player_id, scene_instance_id=info.node_uuid,
            generation=next(self._admission_ids), owner_revision=request.owner_revision,
            owner_epoch=request.owner_epoch, scene_node_id=str(info.node_id), scene_id=request.scene_id,
            zone_id=info.zone_id, session_id=request.session_id, login_generation=request.login_generation,
        )
        token = uuid4().hex
        admission = Admission(identity, f"{request.owner_epoch}|{info.node_uuid}|{identity.generation}|{token}|H|{uuid4().hex}")
        prior = None
        try:
            for attempt in range(3):
                try:
                    reply = await self._data("RegisterRoleAdmission", identity=identity)
                    if reply.identity != identity or reply.closed_through >= identity.generation:
                        raise AdmissionError("DataService 未确认本次入场世代")
                    break
                except AdmissionError:
                    raise
                except Exception:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1)

            prior = await self._redis.hget(_keys(request.player_id)[1], "handoff_fence") or b""
            for attempt in range(3):
                try:
                    result = await self._eval(lua.REGISTER, request.player_id, request.owner_epoch, admission.fence, prior)
                    if result != 1:
                        raise AdmissionError("场景入场的 Redis 写权已被替换")
                    return admission
                except AdmissionError:
                    raise
                except Exception:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1)
        except BaseException:
            # SQL / Redis 请求可能已送达。撤销原世代和原候选，绝不读取新持有者补授权。
            if prior is not None:
                try:
                    await self._eval(lua.CANCEL_REGISTRATION, request.player_id, request.owner_epoch,
                                     admission.fence, prior, _rotate(admission.fence, "R"))
                except Exception:
                    _LOG.exception("撤销未完成的 Redis 入场资格失败 player=%d", request.player_id)
            try:
                await self._close_admission(admission)
            except Exception:
                _LOG.exception("关闭未完成的 SQL 入场世代失败 player=%d", request.player_id)
            raise

    async def confirm(self, admission: Admission) -> None:
        identity = admission.identity
        if await self._eval(lua.CONFIRM, identity.player_id, identity.owner_epoch, admission.fence) != 0:
            raise AdmissionError("角色加载完成时已失去入场资格")

    async def _close_admission(self, admission: Admission, snapshot=None) -> None:
        identity = admission.identity
        save_seq = snapshot.save_seq if snapshot is not None else 0
        response = await self._data(
            "CloseRoleAdmissions", player_id=identity.player_id, scene_instance_id=identity.scene_instance_id,
            owner_revision=identity.owner_revision, through_generation=identity.generation,
            required_owner_epoch=snapshot.owner_epoch if save_seq else 0, required_save_seq=save_seq,
        )
        if response.closed_through < identity.generation:
            raise AdmissionError("DataService 未确认入场世代已关闭")

    async def retire(self, admission: Admission, snapshot=None, *, release: bool = False, transferred: bool = False) -> None:
        if admission.retired:
            return
        identity = admission.identity
        if release and snapshot is not None and not snapshot.released:
            raise AdmissionError("只能为已提交的最终存盘发布释放标记")
        if release and snapshot is not None:
            if not admission.released:
                admission.released = _rotate(admission.fence, "F")
                admission.release_stamp = self._stamp()
            epoch = snapshot.owner_epoch
            await self._eval(lua.RELEASE, identity.player_id, epoch, f"{epoch}:{admission.release_stamp}",
                             300, admission.fence, admission.released)
        elif not transferred:
            if not admission.revoked:
                admission.revoked = _rotate(admission.fence, "R")
            await self._eval(lua.REVOKE, identity.player_id, admission.fence, admission.fence, admission.revoked, "")
        await self._close_admission(admission, snapshot)
        admission.retired = True

    async def release_credential(self, player_id: int, expected_epoch: int) -> bool:
        """调用方持角色锁。只为本进程原先提交过的当前代补释放凭证。"""
        if not expected_epoch or player_id in self.scene.locations or player_id in self._trips or self.sealed(player_id):
            return False
        observed = await self._redis.hget(_keys(player_id)[1], "handoff_fence")
        if not observed:
            return False
        fence = observed.decode("ascii")
        fields = fence.split("|")
        if len(fields) != 6 or fields[0] != str(expected_epoch) or fields[1] != self.scene.info.node_uuid:
            return False
        if player_id in self.scene.locations or player_id in self._trips:
            return False
        release_detached = getattr(self.scene.store, "release_detached", None)
        if release_detached is None or not await release_detached(
            player_id, self.scene.info.node_uuid.encode(), expected_epoch,
        ):
            return False
        stamp = self._stamp()
        result = await self._eval(lua.CREDENTIAL, player_id, expected_epoch, f"{expected_epoch}:{stamp}",
                                  300, fence, _rotate(fence, "F"))
        return result == 1

    def _stamp(self) -> int:
        self._last_stamp = max(_now_ms(), self._last_stamp + 1)
        return self._last_stamp

    def frozen(self, player_id: int) -> bool:
        trip = self._trips.get(player_id)
        return self.sealed(player_id) or trip is not None and trip.frozen

    def sealed(self, player_id: int) -> bool:
        return player_id in self._trade_drains

    async def seal(self, request) -> Message:
        if len(self._trade_secret) < 32 or not verify_service_call(
            _TRADE_SEAL, request, "trade", _now_ms(), self._trade_secret,
        ):
            raise PermissionError("角色封存调用签名无效")
        seal = request.seal
        info = self.scene.info
        if (not all((seal.player_id, seal.listing_id, seal.through_generation, seal.owner_revision))
                or (seal.scene_instance_id, seal.scene_node_id, seal.zone_id)
                != (info.node_uuid, str(info.node_id), info.zone_id)):
            raise AdmissionError("角色封存没有指向本节点的原世代")
        player_id = seal.player_id
        key = player_id, seal.through_generation
        raw = seal.SerializeToString(deterministic=True)
        async with self.scene._lock(player_id):
            current = self._trade_drains.get(player_id)
            previous = self._trade_completed.get(key)
            if previous is not None or current is not None:
                original = previous if previous is not None else current.seal.SerializeToString(deterministic=True)
                if original != raw:
                    raise AdmissionError("角色封存原件不一致")
                return self.protocol.message("Empty")
            world = self.scene.locations.get(player_id)
            player = world.players.get(player_id) if world is not None else None
            if (self._stopping or player is None or player.admission is None or self.published(player_id)
                    or len(self._trade_drains) >= 1024):
                raise AdmissionError("角色尚无法安全封存")
            identity = player.admission.identity
            if (identity.generation, identity.owner_revision, identity.scene_instance_id) != (
                seal.through_generation, seal.owner_revision, seal.scene_instance_id,
            ):
                raise AdmissionError("角色当前入场世代与封存原件不一致")
            self.superseded(player)
            self._trade_drains[player_id] = drain = _TradeDrain(type(seal).FromString(raw), player)
            player.motion.velocity = (0., 0., 0.)
            self.scene._push(player, "SceneClientPlayerCommonSendTipToClient",
                             self.protocol.message("TipInfoMessage", id=_ENTER_FAILED))
            self.scene._push(player, "SceneClientPlayerCommonKickPlayer",
                             self.protocol.message("GameKickPlayerRequest", reason={"id": _ENTER_FAILED}))
            task = asyncio.create_task(self._drain_trade(drain), name=f"scene-trade-drain-{player_id}")
            self._trade_tasks.add(task)
            task.add_done_callback(self._trade_tasks.discard)
        return self.protocol.message("Empty")

    async def _drain_trade(self, drain: _TradeDrain) -> None:
        player = drain.player
        player_id = drain.seal.player_id
        while True:
            try:
                if drain.floor is None:
                    async with self.scene._lock(player_id):
                        world = self.scene.locations.get(player_id)
                        if world is None or world.players.get(player_id) is not player:
                            raise AdmissionError("原封存实体在最终存盘前离开")
                        if not drain.saved:
                            await self.scene.flush_player_rows(player)
                            await self.scene.commit_player_rows(player, self.scene._position_rows(player), force=True)
                            drain.saved = True
                        if not player.snapshot.released:
                            player.snapshot = await self.scene.store.commit(player.snapshot, player.snapshot.rows, release=True)
                        admission = player.admission
                        if not admission.revoked:
                            admission.revoked = _rotate(admission.fence, "R")
                        await self._eval(lua.REVOKE, player_id, admission.fence, admission.fence, admission.revoked, "")
                        self.scene._evict(world, player)
                        drain.floor = player.snapshot.owner_epoch, player.snapshot.save_seq
                await self._data("AckRoleTradeDrain", seal=drain.seal,
                                 required_owner_epoch=drain.floor[0], required_save_seq=drain.floor[1])
            except Exception:
                _LOG.warning("角色封存尚未完成 player=%d", player_id, exc_info=True)
                if self._stopping:
                    return
                await asyncio.sleep(1)
                continue
            self._trade_completed[player_id, drain.seal.through_generation] = drain.seal.SerializeToString(deterministic=True)
            if len(self._trade_completed) > 1024:
                self._trade_completed.popitem(last=False)
            del self._trade_drains[player_id]
            return

    def published(self, player_id: int) -> bool:
        trip = self._trips.get(player_id)
        return trip is not None and trip.marked

    def superseded(self, player) -> None:
        """新的权威入场已取代原实体，不允许原交接回调再处理它。"""
        trip = self._trips.get(player.player_id)
        if trip is not None and trip.player is player:
            del self._trips[player.player_id]
            if trip.task is not None:
                trip.task.cancel()

    def drain(self, world) -> None:
        """疏散目标由 SceneManager 选择。每拍限量，仍在战斗的玩家等下一拍。"""
        now = self._clock
        if now < self._drain_after.get(world, 0):
            return
        self._drain_after[world] = now + 1
        started = 0
        for player in tuple(world.players.values()):
            if (player.player_id in self._trips or not self._ready(player)
                    or self.scene.battle and self.scene.battle.frozen(player.player_id)):
                continue
            self._start(player, self.scene.info.zone_id, 0, 0, frozen=True)
            started += 1
            if started == 32:
                break

    async def read_team_membership(self, player) -> Message:
        if self._team_client is None or player.admission is None:
            raise AdmissionError("队伍权威服务未配置")
        request = self.protocol.message("teampb.ReadTeamMembershipRequest", admission=player.admission.identity)
        sign_service_call(_TEAM_MEMBERSHIP, request, "scene", _now_ms(), self._secret)
        data = await self._team_client.call(_TEAM_MEMBERSHIP, request.SerializeToString(deterministic=True))
        reply = self.protocol.parse("teampb.ReadTeamMembershipResponse", data)
        if (reply.retry or reply.request_digest != service_digest(request) or not verify_service_call(
            _TEAM_MEMBERSHIP + "/reply", reply, "team", _now_ms(), self._secret,
        )):
            raise AdmissionError("队伍权威未确认本次入场身份的组队状态")
        if reply.team_id:
            member_ids = {member.player_id for member in reply.record.members}
            if (not reply.membership_epoch or not reply.version or reply.record.team_id != reply.team_id
                    or not 1 <= len(reply.record.members) <= 5 or not reply.record.leader_id
                    or not {player.player_id, reply.record.leader_id} <= member_ids):
                raise AdmissionError("队伍权威返回的成员原件无效")
        elif reply.record.team_id:
            raise AdmissionError("队伍权威返回了冲突的队伍身份")
        return reply

    async def _team_membership(self, player) -> bool:
        return bool((await self.read_team_membership(player)).team_id)

    async def team_leader_location(self, player_id: int, leader_id: int) -> Message | None:
        if await self._redis.get(f"battle:lock:{{{player_id}}}") is not None:
            return None
        raw = await self._redis.get(_keys(leader_id)[3])
        return self.protocol.parse("storage.PlayerLocation", raw) if raw else None

    def follow_scene(self, player, scene_id: int) -> None:
        if self._ready(player) and player.player_id not in self._trips:
            self._start(player, self.scene.info.zone_id, scene_id, 0, silent=True)

    async def travel_to_zone(self, world, player, route, request) -> Message:
        zone = request.target_zone_id
        config = request.scene_config_id
        if not zone or zone == self.scene.info.zone_id:
            code = 3024
        elif config and self._world_configs and config not in self._world_configs:
            code = 3007
        elif self.scene.battle and self.scene.battle.frozen(player.player_id):
            code = 3025
        elif player.player_id in self._trips:
            code = 13000 if self.frozen(player.player_id) else 3014
        elif not self._ready(player):
            code = _ZONE_BUSY
        else:
            try:
                grouped = await self._team_membership(player)
            except Exception:
                _LOG.warning("跨区前未能确认组队状态 player=%d", player.player_id, exc_info=True)
                code = _ZONE_BUSY
            else:
                if grouped:
                    code = 3026
                elif not self._ready(player) or player.player_id in self._trips:
                    code = _ZONE_BUSY
                else:
                    self._start(player, zone, 0, config, frozen=True)
                    code = 0
        return self._response("TravelToZoneResponse", code)

    def change_scene(self, world, player, route, request) -> Message:
        target = request.scene_info
        mirror = target.mirror_config_id if not target.scene_id else 0
        if self.scene.info.scene_node_type in (1, 3):
            code = 3004
        elif self.scene.battle and self.scene.battle.frozen(player.player_id):
            code = _ENTER_FAILED
        elif player.player_id in self._trips:
            code = 3014
        elif not (target.scene_id or target.scene_config_id or mirror):
            code = 3005
        elif target.scene_id == world.info.scene_id:
            code = 3008
        elif mirror and (world.info.mirror_config_id or self._mirrors.get(mirror) is None):
            code = 3005
        elif not self._ready(player):
            code = 3028
        else:
            self._start(player, self.scene.info.zone_id, target.scene_id,
                        world.info.scene_config_id if mirror else target.scene_config_id,
                        mirror_config=mirror, source_scene=world.info.scene_id, client_pick=bool(target.scene_id))
            code = 0
        return self._response("EnterSceneC2SResponse", code)

    def _response(self, name: str, code: int) -> Message:
        reply = self.protocol.message(name)
        if code:
            reply.error_message.id = code
        return reply

    def _ready(self, player) -> bool:
        return bool(not self._stopping and player.admission is not None and player.gate_uuid
                    and player.session_id >> 17 and player.owner_revision and player.disconnect_at is None
                    and player.snapshot.owner_epoch and not player.snapshot.released and not self.sealed(player.player_id))

    def _start(self, player, zone: int, scene_id: int, config: int, **options) -> None:
        trip = _Travel(player, zone, scene_id, config, player.gate_uuid, player.session_id,
                       player.generation, player.owner_revision, **options)
        if trip.frozen:
            trip.started_at = asyncio.get_running_loop().time()
            player.motion.velocity = (0., 0., 0.)
        self._trips[player.player_id] = trip
        task = asyncio.create_task(self._drive(trip), name=f"scene-travel-{player.player_id}")
        trip.task = task
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _world(self, trip: _Travel):
        player = trip.player
        world = self.scene.locations.get(player.player_id)
        if world is not None and world.players.get(player.player_id) is player and (
            player.session_id, player.gate_uuid, player.generation, player.owner_revision
        ) == (trip.session_id, trip.gate_uuid, trip.generation, trip.owner_revision):
            return world
        return None

    def _tip(self, trip: _Travel, code: int, *, kick: bool = False) -> None:
        player = trip.player
        if (trip.silent and not trip.frozen or self._world(trip) is None or player.disconnect_at is not None):
            return
        self.scene._push(player, "SceneClientPlayerCommonSendTipToClient", self.protocol.message("TipInfoMessage", id=code))
        if kick:
            self.scene._push(player, "SceneClientPlayerCommonKickPlayer",
                             self.protocol.message("GameKickPlayerRequest", reason={"id": code}))

    @property
    def _clock(self) -> float:
        return asyncio.get_running_loop().time()

    async def _call_manager(self, method: str, **fields):
        request = self.protocol.message(f"scene_manager.{method}Request", **fields)
        body = await self._coordinator.call(_MANAGER + method, request.SerializeToString())
        return self.protocol.parse(f"scene_manager.{method}Response", body)

    async def _enter(self, trip: _Travel, *, client_pick: bool = False):
        player = trip.player
        correlation = next(self._correlations)
        reply = await self._call_manager(
            "EnterScene", player_id=player.player_id, scene_id=trip.scene_id, scene_conf_id=trip.scene_config,
            session_id=trip.session_id, gate_id=str(trip.session_id >> 17), gate_instance_id=trip.gate_uuid,
            gate_zone_id=self.scene.info.zone_id, zone_id=trip.zone, correlation_id=correlation,
            login_generation=trip.generation, owner_revision=trip.owner_revision, client_channel_pick=client_pick,
        )
        if reply.player_id != player.player_id or reply.correlation_id not in (0, correlation):
            raise ValueError("SceneManager 应答与本次入场不匹配")
        return reply

    async def _drive(self, trip: _Travel) -> None:
        player = trip.player
        failure = _ZONE_BUSY if trip.zone != self.scene.info.zone_id else _ENTER_FAILED
        try:
            if not trip.frozen:
                if trip.mirror_config:
                    reply = await self._call_manager(
                        "CreateScene", scene_conf_id=trip.scene_config, scene_type=2,
                        creator_ids=[player.player_id], zone_id=trip.zone, source_scene_id=trip.source_scene,
                        mirror_config_id=trip.mirror_config,
                    )
                    if reply.error_code or not reply.scene_id:
                        self._tip(trip, failure)
                        return
                    trip.scene_id = reply.scene_id
                reply = await self._enter(trip, client_pick=trip.client_pick)
                if trip.silent:
                    # 自动跟随只换本节点场景。被拒后释放请求槽位，等下一次队伍信号。
                    if reply.error_code:
                        _LOG.info("Team-follow scene change rejected player_id=%d scene_id=%d code=%d",
                                  player.player_id, trip.scene_id, reply.error_code)
                    return
                if reply.error_code != _HANDOFF_PENDING:
                    if reply.error_code:
                        self._tip(trip, failure)
                    return
                async with self.scene._lock(player.player_id):
                    if (self._world(trip) is None or not self._ready(player)
                            or self.scene.battle and self.scene.battle.frozen(player.player_id)):
                        self._tip(trip, failure)
                        return
                    trip.frozen = True
                    trip.started_at = self._clock
                    player.motion.velocity = (0., 0., 0.)
            await self._handoff(trip, failure)
        except Exception:
            if trip.silent and not trip.frozen:
                _LOG.info("Team-follow scene change failed player_id=%d scene_id=%d",
                          player.player_id, trip.scene_id, exc_info=True)
                return
            _LOG.exception("场景切换失败 player=%d", player.player_id)
            if trip.marked:
                if trip.enter_sent_at:
                    # gRPC 失败不能证明协调器已经放弃；给权威路由提交或回滚留足窗口。
                    await asyncio.sleep(max(0, trip.enter_sent_at + _REPLY_WATCHDOG - self._clock))
                await self._settle(trip, failure, "no_reply" if trip.enter_sent_at else "mark_unknown")
            else:
                self._tip(trip, failure)
        finally:
            try:
                if trip.marked and asyncio.current_task().cancelling():
                    await self._depose(trip)
            finally:
                if self._trips.get(player.player_id) is trip:
                    del self._trips[player.player_id]

    async def _handoff(self, trip: _Travel, failure: int) -> None:
        player = trip.player
        async with asyncio.timeout_at(trip.started_at + _LATE_DISPATCH):
            async with self.scene._lock(player.player_id):
                if self._world(trip) is None or not self._ready(player):
                    return
                await self.scene.flush_player_rows(player)
                snapshot = await self.scene.commit_player_rows(player, self.scene._position_rows(player), force=True)
                if self._clock >= trip.started_at + _LATE_DISPATCH:
                    raise TimeoutError("交接写入窗口已结束")
                trip.epoch = snapshot.owner_epoch
                trip.held = player.admission.fence
                trip.published = _rotate(trip.held, "F")
                trip.revoked = _rotate(trip.held, "R")
                trip.stamp = self._stamp()
                trip.marked = True
            result = await self._eval(lua.PUBLISH, player.player_id, trip.epoch, f"{trip.epoch}:{trip.stamp}",
                                      300, trip.held, trip.published)
        if result != 1:
            await self._depose(trip, failure)
            return
        if self._world(trip) is None or not self._ready(player) or self._clock >= trip.started_at + _LATE_DISPATCH:
            await self._settle(trip, failure, "mark_unknown")
            return
        trip.enter_sent_at = self._clock
        reply = await self._enter(trip)
        if not reply.error_code and reply.HasField("redirect") and trip.zone != self.scene.info.zone_id:
            await self._depose(trip)
            return
        evidence = "failed" if reply.error_code else "same_zone" if trip.zone == self.scene.info.zone_id else "anomalous"
        await self._settle(trip, failure, evidence)

    async def _settle(self, trip: _Travel, failure: int, evidence: str) -> None:
        deadline = trip.started_at + _FREEZE_CAP
        player = trip.player
        while self._world(trip) is not None and self._clock < deadline:
            try:
                async with asyncio.timeout_at(deadline):
                    if trip.resume is None:
                        epoch, raw = await self._eval(lua.JUDGE, player.player_id, trip.held, trip.published,
                                                      trip.revoked, trip.stamp, location=True)
                        if not await self._classify(trip, failure, evidence, int(epoch), raw):
                            return
                    await self._resume(trip)
                if evidence != "same_zone":
                    self._tip(trip, failure)
                return
            except Exception:
                _LOG.warning("交接未能取得结论，保持冻结 player=%d", player.player_id, exc_info=True)
                await asyncio.sleep(max(0, min(_RECOVERY_TICK, deadline - self._clock)))
        await self._depose(trip, failure)

    async def _classify(self, trip: _Travel, failure: int, evidence: str, epoch: int, raw: bytes | None) -> bool:
        try:
            location = self.protocol.parse("storage.PlayerLocation", raw) if raw else None
        except DecodeError:
            location = None
        info = self.scene.info
        same_receipt = bool(location and location.rollback_receipt == f"{trip.epoch}:{trip.stamp}")
        rolled_back = bool(
            location and epoch >= trip.epoch + 2 and location.owner_epoch == epoch
            and location.zone_id == info.zone_id and location.node_id == str(info.node_id)
            and same_receipt
        )
        if epoch == trip.epoch or rolled_back:
            trip.resume = (epoch, raw or b"", _rotate(trip.revoked, "H", epoch=epoch))
            return True
        needs_reset = bool(
            evidence in ("failed", "no_reply") and trip.zone != info.zone_id and location
            and not location.node_id and location.zone_id == trip.zone and location.owner_epoch == epoch
        )
        await self._depose(trip, failure if needs_reset or same_receipt else 0)
        return False

    async def _resume(self, trip: _Travel) -> None:
        player = trip.player
        epoch, raw, held = trip.resume
        async with self.scene._lock(player.player_id):
            world = self._world(trip)
            if world is None:
                return
            result = await self._eval(lua.RESUME, player.player_id, trip.revoked, held, epoch, raw, location=True)
            if result != 1:
                raise AdmissionError("原节点恢复写权时归属已再次改变")
            if epoch != player.snapshot.owner_epoch:
                player.snapshot = await self.scene.store.load_and_claim(player.player_id, player.snapshot.owner, epoch)
            player.admission.fence = held
            if player.admission.identity.scene_id != world.info.scene_id or player.admission.identity.owner_epoch != epoch:
                former = player.admission
                request = self.protocol.message(
                    "PlayerEnterGameNodeRequest", player_id=player.player_id, session_id=player.session_id,
                    scene_id=world.info.scene_id, owner_epoch=epoch, owner_revision=player.owner_revision,
                    login_generation=player.generation,
                )
                admitted = await self.admit(request)
                try:
                    await self.confirm(admitted)
                except BaseException:
                    await self.retire(admitted)
                    raise
                player.admission = admitted
                try:
                    await self.retire(former, transferred=True)
                except Exception:
                    _LOG.exception("关闭恢复前的 SQL 入场世代失败 player=%d", player.player_id)

    async def _depose(self, trip: _Travel, kick_code: int = 0) -> None:
        player = trip.player
        async with self.scene._lock(player.player_id):
            world = self._world(trip)
            if world is None:
                return
            if kick_code:
                self._tip(trip, kick_code, kick=True)
            self.scene._evict(world, player)
        try:
            await self.retire(player.admission, player.snapshot, transferred=True)
        except Exception:
            _LOG.exception("关闭已交接的 SQL 入场世代失败 player=%d", player.player_id)

    async def stop(self) -> None:
        self._stopping = True
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*tuple(self._trade_tasks), return_exceptions=True)

    async def close(self) -> None:
        await self.stop()
        closers = [self._redis.aclose(), self._admission_client.close(), self._coordinator.close()]
        if self._team_client is not None:
            closers.append(self._team_client.close())
        await asyncio.gather(*closers)


async def create_scene_travel(scene, tables, settings) -> SceneTravel:
    from redis.asyncio import Redis, RedisCluster

    admission_secret = secret("MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE")
    redis_type = RedisCluster if settings.redis_cluster else Redis
    redis = redis_type.from_url(settings.redis, decode_responses=False, socket_connect_timeout=3, socket_timeout=3)
    coordinator = GrpcClient(settings.scene_manager, timeout=12)
    data = GrpcClient(settings.data_service, timeout=5)
    team = GrpcClient(settings.match, timeout=4)
    try:
        await redis.ping()
        return SceneTravel(scene, tables, coordinator, redis, data, admission_secret,
                           team_client=team, trade_secret=os.getenv("MMORPG_ACCOUNT_DIRECTORY_SECRET_TRADE", "").encode())
    except BaseException:
        await asyncio.gather(redis.aclose(), coordinator.close(), data.close(), team.close(), return_exceptions=True)
        raise
