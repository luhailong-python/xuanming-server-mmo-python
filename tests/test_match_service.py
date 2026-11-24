"""match 与 team 服务全链路测试：覆盖 5 组 gRPC 服务、真 BattleNode / PlayerInbox 联调、签名鉴权与补偿回滚。"""

from __future__ import annotations

import asyncio
import hmac
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from xuanming.grpc_runtime import (
    GrpcClient,
    serve_grpc,
    service_digest,
    session_metadata,
    sign_service_call,
    verify_service_call,
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.match import (
    BattleNodeEntry,
    GatherPipeline,
    GrpcEndpointPool,
    MATCH_NODE_TYPE,
    MatchConfig,
    MatchGrpcService,
    MatchServer,
    Matchmaker,
    MemoryMatchStore,
    MemoryPresenceProvider,
    OutcomeOutboxWorker,
    PlayerPresence,
    SpectateManager,
    StaticBattleNodeResolver,
    TEAM_NODE_TYPE,
    TeamGrpcService,
)
from xuanming.match.errors import (
    ERR_MATCH_BATTLE_REPORT_NOT_FOUND,
    ERR_MATCH_CANCEL_TOO_LATE,
    ERR_MATCH_CHALLENGE_SELF,
    ERR_MATCH_IN_BATTLE,
    ERR_MATCH_MODE_NOT_OPEN,
    ERR_MATCH_SPECTATE_WHILE_QUEUED,
    ERR_TEAM_NOT_LEADER,
    ERR_TEAM_NO_TEAM,
)
from xuanming.match.model import (
    MODE_1V1,
    MODE_3V3,
    MODE_PVE_SOLO,
    MODE_PVE_TEAM,
    OUTCOME_DECISION_ABORT,
    OUTCOME_DECISION_COMPLETE,
    OUTCOME_DECISION_UNKNOWN,
    PROBE_GRANT_DOMAIN,
    PROBE_RECEIPT_DOMAIN,
    QUEUE_STATE_NOT_QUEUED,
    QUEUE_STATE_QUEUED,
    QUEUE_STATE_READY,
    SPECTATE_DELIVERY_DIRECT,
    SPECTATE_DELIVERY_LOBBY,
    SPECTATE_TIER_BROADCAST,
    verify_spectate_ticket,
)
from xuanming.match.sql_store import REQUIRED_TABLES
from xuanming.nodes.battle import BattleNode
from xuanming.nodes.battle_tcp import _PROBE_GRANT_DOMAIN, _PROBE_RECEIPT_DOMAIN
from xuanming.player_inbox import InboxRegistry, MemoryInboxStore, PlayerInboxService
from xuanming.protocol import Protocol

BATTLE_SECRET = b"battle-outcome-secret-32-bytes-!!"
SCENE_SECRET = b"scene-directory-secret-32-bytes-!"
COORD_SECRET = b"coordinator-fence-secret-32-byte!"
INBOX_SECRET = b"match-player-inbox-secret-32byte!"
GUILD_SECRET = b"guild-internal-secret-32-bytes-!!"
OPS_SECRET = b"match-ops-featured-secret-32byte!"
MANUAL_SECRET = b"match-manual-abort-secret-32byte!"
PROBE_SECRET = b"match-network-probe-secret-32byt!"
SPECTATE_SECRET = b"match-spectate-ticket-secret-32b!"


class _FakeClock:
    def __init__(self, start_ms: int | None = None) -> None:
        self.now = start_ms or (time.time_ns() // 1_000_000)

    def __call__(self) -> int:
        return self.now

    def advance(self, delta_ms: int) -> None:
        self.now += delta_ms


class _RecordingTeamNotifier:
    def __init__(self) -> None:
        self.pushes: list[tuple[int, int, object]] = []
        self.scene_refreshes: list[int] = []

    async def push_to_player(self, player_id: int, message_id: int, payload_msg) -> None:
        self.pushes.append((player_id, message_id, payload_msg))

    async def refresh_scene(self, player_id: int) -> None:
        self.scene_refreshes.append(player_id)


class _StubInboxDirectory:
    """配合 MemoryInboxStore 的本地角色目录桩，返回与 match 侧一致的归属修订。"""

    def __init__(self, protocol: Protocol, roles: dict[int, tuple[str, int, int]]) -> None:
        self.protocol = protocol
        self.roles = roles  # player_id -> (account, owner_revision, home_zone)

    async def player_exists(self, player_id: int) -> bool:
        return player_id in self.roles

    async def get_role_fence_state(self, player_id: int):
        acct, rev, zone = self.roles.get(player_id, (f"acct:{player_id}", 1, 1))
        return self.protocol.message(
            "rolefence.RoleFenceState",
            player_id=player_id,
            account=acct,
            owner_revision=rev,
            version=1,
            home_zone=zone,
        )

    async def verify_role_admission(self, admission) -> bool:
        return True

    async def read_inbox_ledger(self, player_id: int):
        return None, False

    async def read_item_gain_block(self, player_id: int):
        return None, False


class _TestSceneServer:
    """提供真实 SceneNodeGrpc.PrepareBattle / CancelBattlePrepare 的测试场景服。"""

    def __init__(self, protocol: Protocol, *, scene_node_id: int = 2, scene_uuid: str = "scene-uuid-1") -> None:
        self.protocol = protocol
        self.scene_node_id = scene_node_id
        self.scene_uuid = scene_uuid
        self.reject_players: dict[int, int] = {}
        self.prepared: list[tuple[int, int]] = []
        self.cancelled: list[tuple[int, int]] = []
        self.fingerprint = "fp-v1"

    def services(self):
        return {
            "scene_node.SceneNodeGrpc": {
                "PrepareBattle": self.prepare_battle,
                "CancelBattlePrepare": self.cancel_battle_prepare,
            }
        }

    def prepare_battle(self, request):
        resp = self.protocol.message("PrepareBattleResponse")
        pid = int(request.player_id)
        if pid in self.reject_players:
            resp.error_message.id = self.reject_players[pid]
            return resp
        self.prepared.append((pid, int(request.battle_id)))
        snap = resp.snapshot
        snap.player_id = pid
        snap.player_name = f"Hero{pid}"
        snap.level = 30
        snap.class_id = 1
        snap.max_health = 100
        snap.max_mana = 50
        snap.physical_attack = 120
        snap.magic_attack = 80
        snap.defense = 10
        snap.base_attributes.health = 100
        snap.base_attributes.mana = 50
        snap.base_attributes.speed = 50 + pid
        snap.base_attributes.strength = 20
        snap.table_fingerprint = self.fingerprint
        snap.routing.session_id = (1 << 17) | pid
        snap.routing.gate_node_id = 1
        snap.routing.gate_instance_id = "gate-uuid-1"
        snap.routing.scene_node_id = self.scene_node_id
        snap.routing.scene_instance_id = self.scene_uuid
        snap.routing.zone_id = 1
        resp.table_fingerprint = self.fingerprint
        return resp

    def cancel_battle_prepare(self, request):
        self.cancelled.append((int(request.player_id), int(request.battle_id)))
        return self.protocol.message("Empty")


class _TestBattleGrpcAdapter:
    """将真实 BattleNode 包装为真实 gRPC 服务，并支持注入 battle_not_allocatable 故障。"""

    def __init__(self, protocol: Protocol, battle: BattleNode) -> None:
        self.protocol = protocol
        self.battle = battle
        self.force_not_allocatable = False
        self.destroyed: list[tuple[int, str]] = []

    def services(self):
        return {
            "BattleNode": {
                "QueryBattleCreation": self.query_creation,
                "CreateBattle": self.create_battle,
                "DestroyBattle": self.destroy_battle,
                "AddObserver": self.add_observer,
                "RemoveObserver": self.remove_observer,
                "QueryObserver": self.query_observer,
                "IssueBattleTicket": self.issue_ticket,
                "AttachRelayGate": self.attach_relay_gate,
                "PromoteSpectateBroadcast": self.promote_spectate_broadcast,
            }
        }

    def query_creation(self, request, context, path):
        return self.battle.query_creation(request)

    async def create_battle(self, request, context, path):
        if self.force_not_allocatable:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "battle_not_allocatable")
        return self.battle.create(request)

    def destroy_battle(self, request, context, path):
        self.destroyed.append((int(request.battle_id), request.reason))
        return self.battle.destroy(request)

    def add_observer(self, request, context, path):
        return self.battle.add_observer(request)

    def remove_observer(self, request, context, path):
        return self.battle.remove_observer(request)

    def query_observer(self, request, context, path):
        return self.battle.query_observer(request)

    def issue_ticket(self, request, context, path):
        return self.battle.issue_ticket(request)

    def attach_relay_gate(self, request, context, path):
        return self.battle.attach_relay_gate(request)

    def promote_spectate_broadcast(self, request, context, path):
        return self.battle.promote_spectate_broadcast(request)


@asynccontextmanager
async def _match_harness(
    *,
    guild_secret: bytes = GUILD_SECRET,
    require_probe: bool = False,
    inline_gather: bool = True,
    release_track: str = "stable",
    manual_env: str = "staging-test",
):
    protocol = Protocol()
    clock = _FakeClock()
    gate_key = Ed25519PrivateKey.generate()
    gate_pub = gate_key.public_key()
    verifier = CallerVerifier([gate_pub], allowed_callers=("gate",))

    # 1. 启动测试 Scene gRPC 服务
    scene_stub = _TestSceneServer(protocol)
    scene_srv, scene_port = await serve_grpc(protocol, "127.0.0.1:0", scene_stub.services())
    scene_ep = f"127.0.0.1:{scene_port}"

    # 2. 启动真实 BattleNode gRPC 服务（附带最小 PVE 怪物工厂以支持 PVE/活动开局）
    def _monster_factory(_request):
        m = protocol.message("BattleActorState", name="Slime", level=10, max_health=50, max_mana=10, defense=5)
        m.attributes.health = 50
        m.attributes.mana = 10
        m.attributes.speed = 20
        m.monster_table_id = 101
        return [m]

    battle_outcomes: list[tuple[int, int]] = []
    spectate_frames: list[tuple] = []
    battle_node = BattleNode(
        protocol,
        node_id=30,
        instance_id="battle-inst-1",
        physical_region=1,
        host="127.0.0.1",
        port=19030,
        ticket_secret=b"battle-direct-ticket-secret-32b!",
        monster_factory=_monster_factory,
        on_outcome=lambda bid, outcome: battle_outcomes.append((bid, outcome)),
        on_spectate_frame=lambda ch, gid, bid, ev: spectate_frames.append((ch, gid, bid, ev)),
        now_ms=clock,
    )
    battle_adapter = _TestBattleGrpcAdapter(protocol, battle_node)
    battle_srv, battle_port = await serve_grpc(
        protocol, "127.0.0.1:0", battle_adapter.services(), pass_context=True
    )
    battle_ep = f"127.0.0.1:{battle_port}"

    # 3. 启动真实 PlayerInboxService gRPC 服务
    inbox_roles = {
        pid: (f"acct:{pid}", 1, 1) for pid in (1001, 1002, 1003, 1004, 1005, 1006, 2001)
    }
    inbox_store = MemoryInboxStore(
        protocol,
        InboxRegistry(protocol),
        _StubInboxDirectory(protocol, inbox_roles),
        allowed_types=frozenset((4,)),
        clock_ms=clock,
    )
    inbox_svc = PlayerInboxService(
        protocol,
        inbox_store,
        scene_secret=SCENE_SECRET,
        coordinator_secret=COORD_SECRET,
        match_secret=INBOX_SECRET,
        clock_ms=clock,
    )
    inbox_srv, inbox_port = await serve_grpc(
        protocol, "127.0.0.1:0", inbox_svc.services(), pass_context=True
    )
    inbox_client = GrpcClient(f"127.0.0.1:{inbox_port}", timeout=5.0)

    # 4. 装配并启动 Match（含 Team）统一 gRPC 服务（5 组服务共端口）
    cfg = MatchConfig(
        node_id=101,
        team_node_id=102,
        grpc_port=0,
        release_track=release_track,
        require_network_probe=require_probe,
        battle_outcome_secret=BATTLE_SECRET,
        scene_secret=SCENE_SECRET,
        coordinator_secret=COORD_SECRET,
        player_inbox_secret=INBOX_SECRET,
        guild_secret=guild_secret,
        ops_secret=OPS_SECRET,
        manual_resolution_secret=MANUAL_SECRET,
        manual_resolution_env=manual_env,
        probe_secret=PROBE_SECRET,
        spectate_secret=SPECTATE_SECRET,
        pve_team_sizes={1: 5, 2: 3},
    )
    store = MemoryMatchStore(protocol, clock_ms=clock)
    presence = MemoryPresenceProvider()
    for pid in (1001, 1002, 1003, 1004, 1005, 1006, 2001):
        presence.set_presence(
            PlayerPresence(
                player_id=pid,
                online=True,
                zone_id=1,
                scene_node_id=2,
                scene_instance_id="scene-uuid-1",
                scene_grpc_endpoint=scene_ep,
                session_id=(1 << 17) | pid,
                gate_node_id=1,
                gate_instance_id="gate-uuid-1",
                name=f"Hero{pid}",
                level=30,
                class_id=1,
            )
        )
    battle_resolver = StaticBattleNodeResolver(
        [
            BattleNodeEntry(
                node_id=30,
                node_uuid="battle-inst-1",
                grpc_endpoint=battle_ep,
                physical_region=1,
                client_host="127.0.0.1",
                client_port=19030,
                release_track="stable",
            )
        ]
    )
    rpc_pool = GrpcEndpointPool(default_timeout=5.0)
    notifier = _RecordingTeamNotifier()
    published_results: list[tuple[int, bytes]] = []

    async def _pub_result(battle_id: int, raw_payload: bytes) -> None:
        published_results.append((battle_id, raw_payload))

    gather = GatherPipeline(protocol, store, cfg, rpc_pool, presence, battle_resolver)
    matchmaker = Matchmaker(protocol, store, cfg, gather, battle_resolver)
    outbox = OutcomeOutboxWorker(
        protocol,
        store,
        inbox_call=inbox_client.call,
        inbox_secret=INBOX_SECRET,
        publish_result=_pub_result,
        clock_ms=clock,
    )
    spectate = SpectateManager(protocol, store, cfg, rpc_pool, presence)
    team_svc = TeamGrpcService(
        protocol,
        store,
        cfg,
        gate_verifier=verifier,
        presence=presence,
        battle_nodes=battle_resolver,
        gather=gather,
        notifier=notifier,
        inline_gather=inline_gather,
        clock_ms=clock,
    )
    match_svc = MatchGrpcService(
        protocol,
        store,
        cfg,
        gate_verifier=verifier,
        presence=presence,
        battle_nodes=battle_resolver,
        gather=gather,
        spectate=spectate,
        inline_gather=inline_gather,
        clock_ms=clock,
    )
    match_server = MatchServer(
        protocol=protocol,
        store=store,
        config=cfg,
        match_service=match_svc,
        team_service=team_svc,
        gather=gather,
        matchmaker=matchmaker,
        outbox=outbox,
        spectate=spectate,
    )
    grpc_srv, match_port = await serve_grpc(
        protocol, "127.0.0.1:0", match_server.services(), pass_context=True
    )
    client = GrpcClient(f"127.0.0.1:{match_port}", timeout=5.0)

    def gate_md(
        player_id: int,
        method: str,
        *,
        account: str | None = None,
        owner_revision: int = 1,
        release_track_hdr: str = "",
    ) -> tuple:
        details = protocol.message(
            "SessionDetails",
            session_id=(1 << 17) | player_id,
            player_id=player_id,
            gate_node_id=1,
            gate_instance_id="gate-uuid-1",
            session_incarnation=100 + player_id,
            owner_revision=owner_revision,
            authenticated_account=f"acct:{player_id}" if account is None else account,
        )
        md = session_metadata(details.SerializeToString(deterministic=True), method, gate_key)
        if release_track_hdr:
            md = md + (("x-release-track", release_track_hdr),)
        return md

    try:
        yield {
            "protocol": protocol,
            "clock": clock,
            "client": client,
            "gate_md": gate_md,
            "gate_key": gate_key,
            "store": store,
            "presence": presence,
            "notifier": notifier,
            "scene_stub": scene_stub,
            "battle_node": battle_node,
            "battle_adapter": battle_adapter,
            "battle_resolver": battle_resolver,
            "matchmaker": matchmaker,
            "outbox": outbox,
            "published_results": published_results,
            "inbox_client": inbox_client,
            "inbox_store": inbox_store,
            "config": cfg,
        }
    finally:
        await client.close()
        await grpc_srv.stop(1.0)
        await match_server.close()
        await rpc_pool.close()
        await inbox_client.close()
        await inbox_srv.stop(1.0)
        await battle_srv.stop(1.0)
        await scene_srv.stop(1.0)


async def _call(client: GrpcClient, protocol: Protocol, path: str, req, resp_type: str, *, metadata: tuple = ()):
    raw = await client.call(path, req.SerializeToString(deterministic=True), metadata=metadata)
    return protocol.parse(resp_type, raw)


# -- 1. Team 全生命周期 + TeamAuthority.ReadMembership + Kafka 钩子 ------------------------------------


def test_team_full_lifecycle_and_scene_authority_read():
    async def run():
        async with _match_harness() as h:
            p, c, md = h["protocol"], h["client"], h["gate_md"]
            notifier: _RecordingTeamNotifier = h["notifier"]

            # CreateTeam（1001 建队）
            path = "/teampb.ClientPlayerTeam/CreateTeam"
            req_id = str(uuid.uuid4())
            r1 = await _call(c, p, path, p.message("teampb.CreateTeamRequest", request_id=req_id), "teampb.TeamResponse", metadata=md(1001, path))
            assert r1.error_message.id == 0
            tid = r1.team.team_id
            assert tid > 0 and r1.team.leader_id == 1001 and r1.team.version == 1
            ep1 = r1.team.membership_epoch
            assert ep1 == 1
            assert 1001 in notifier.scene_refreshes

            # 幂等重放同一 request_id 返回同一队伍与版本
            r1_replay = await _call(c, p, path, p.message("teampb.CreateTeamRequest", request_id=req_id), "teampb.TeamResponse", metadata=md(1001, path))
            assert r1_replay.team.team_id == tid and r1_replay.team.version == 1

            # InviteToTeam -> ListMyInvites -> RespondInvite(accept=True)
            inv_path = "/teampb.ClientPlayerTeam/InviteToTeam"
            r_inv = await _call(
                c, p, inv_path,
                p.message("teampb.InviteToTeamRequest", target_player_id=1002, expected_team_id=tid, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1001, inv_path),
            )
            assert r_inv.error_message.id == 0 and len(r_inv.team.pending_invites) == 1
            assert any(pid == 1002 and mid == 215 for pid, mid, _ in notifier.pushes)

            list_path = "/teampb.ClientPlayerTeam/ListMyInvites"
            r_list = await _call(c, p, list_path, p.message("teampb.ListMyInvitesRequest"), "teampb.ListMyInvitesResponse", metadata=md(1002, list_path))
            assert len(r_list.invites) == 1 and r_list.invites[0].team_id == tid

            resp_path = "/teampb.ClientPlayerTeam/RespondInvite"
            r_join = await _call(
                c, p, resp_path,
                p.message("teampb.RespondInviteRequest", team_id=tid, accept=True, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1002, resp_path),
            )
            assert r_join.error_message.id == 0 and len(r_join.team.members) == 2
            ep2_join = r_join.team.membership_epoch
            assert ep2_join == 1

            # ApplyJoinTeam(1003) -> HandleApplication(approve=True)
            app_path = "/teampb.ClientPlayerTeam/ApplyJoinTeam"
            r_app = await _call(
                c, p, app_path,
                p.message("teampb.ApplyJoinTeamRequest", target_player_id=1001, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1003, app_path),
            )
            assert r_app.error_message.id == 0

            handle_path = "/teampb.ClientPlayerTeam/HandleApplication"
            # 非队长审批必须拒绝
            r_nonleader = await _call(
                c, p, handle_path,
                p.message("teampb.HandleApplicationRequest", applicant_id=1003, approve=True, expected_team_id=tid, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1002, handle_path),
            )
            assert r_nonleader.error_message.id == ERR_TEAM_NOT_LEADER

            r_ok = await _call(
                c, p, handle_path,
                p.message("teampb.HandleApplicationRequest", applicant_id=1003, approve=True, expected_team_id=tid, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1001, handle_path),
            )
            assert r_ok.error_message.id == 0 and len(r_ok.team.members) == 3

            # TeamAuthority.ReadMembership（与 nodes/scene_travel.py:409-415 签名口径逐字对齐）
            rm_path = "/teampb.TeamAuthority/ReadMembership"
            rm_req = p.message("teampb.ReadTeamMembershipRequest")
            rm_req.admission.player_id = 1002
            rm_req.admission.owner_revision = 1
            rm_req.admission.owner_epoch = 5
            rm_req.admission.scene_instance_id = "scene-uuid-1"
            rm_digest = service_digest(rm_req)
            sign_service_call(rm_path, rm_req, "scene", h["clock"](), SCENE_SECRET)
            rm_resp = await _call(c, p, rm_path, rm_req, "teampb.ReadTeamMembershipResponse")
            assert not rm_resp.retry
            assert rm_resp.request_digest == rm_digest
            assert verify_service_call(rm_path + "/reply", rm_resp, "team", h["clock"](), SCENE_SECRET)
            assert rm_resp.team_id == tid and rm_resp.membership_epoch == ep2_join and rm_resp.record.leader_id == 1001

            # GetMyTeam(regroup=True) 触发 Scene 刷新
            notifier.scene_refreshes.clear()
            gmt_path = "/teampb.ClientPlayerTeam/GetMyTeam"
            r_gmt = await _call(c, p, gmt_path, p.message("teampb.GetMyTeamRequest", regroup=True), "teampb.TeamResponse", metadata=md(1002, gmt_path))
            assert r_gmt.team.team_id == tid and 1002 in notifier.scene_refreshes

            # TransferLeader(1001 -> 1002) -> KickMember(1003) -> LeaveTeam(1001) -> DisbandTeam(1002)
            tr_path = "/teampb.ClientPlayerTeam/TransferLeader"
            r_tr = await _call(
                c, p, tr_path,
                p.message("teampb.TransferLeaderRequest", target_player_id=1002, expected_team_id=tid, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1001, tr_path),
            )
            assert r_tr.error_message.id == 0 and r_tr.team.leader_id == 1002

            kick_path = "/teampb.ClientPlayerTeam/KickMember"
            r_kick = await _call(
                c, p, kick_path,
                p.message("teampb.KickMemberRequest", target_player_id=1003, expected_team_id=tid, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1002, kick_path),
            )
            assert r_kick.error_message.id == 0 and len(r_kick.team.members) == 2

            leave_path = "/teampb.ClientPlayerTeam/LeaveTeam"
            r_leave = await _call(
                c, p, leave_path,
                p.message("teampb.LeaveTeamRequest", expected_team_id=tid, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1001, leave_path),
            )
            assert r_leave.error_message.id == 0 and r_leave.team.team_id == 0
            assert r_leave.team.membership_epoch > ep1  # 单调递增

            # expected_team_id 不匹配拒绝
            disband_path = "/teampb.ClientPlayerTeam/DisbandTeam"
            r_wrong_tid = await _call(
                c, p, disband_path,
                p.message("teampb.DisbandTeamRequest", expected_team_id=999999, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1002, disband_path),
            )
            assert r_wrong_tid.error_message.id == ERR_TEAM_NO_TEAM

            r_dis = await _call(
                c, p, disband_path,
                p.message("teampb.DisbandTeamRequest", expected_team_id=tid, request_id=str(uuid.uuid4())),
                "teampb.TeamResponse", metadata=md(1002, disband_path),
            )
            assert r_dis.error_message.id == 0 and r_dis.team.team_id == 0
            assert r_dis.team.membership_epoch > ep2_join

            # 3 个推送占位 RPC 空操作返回 Empty
            for push_name, msg_type in (
                ("NotifyTeamSnapshot", "teampb.TeamSnapshotS2C"),
                ("NotifyTeamInvite", "teampb.TeamInviteS2C"),
                ("NotifyTeamEvent", "teampb.TeamEventS2C"),
            ):
                pp = f"/teampb.ClientPlayerTeam/{push_name}"
                await _call(c, p, pp, p.message(msg_type), "Empty", metadata=md(1001, pp))

    asyncio.run(run())


# -- 2. Network Probe 签发与回执核验（与 nodes/battle_tcp.py 域常数字节对齐）----------------------------


def test_network_probe_grant_and_commit_contract():
    assert PROBE_GRANT_DOMAIN == _PROBE_GRANT_DOMAIN
    assert PROBE_RECEIPT_DOMAIN == _PROBE_RECEIPT_DOMAIN

    async def run():
        async with _match_harness(require_probe=True) as h:
            p, c, md = h["protocol"], h["client"], h["gate_md"]

            # 未探测前入队被拒（ERR_MATCH_PROBE_REQUIRED = 16022）
            join_path = "/match.MatchService/JoinQueue"
            r_no_probe = await _call(
                c, p, join_path,
                p.message("match.JoinQueueRequest", mode=MODE_PVE_SOLO, battle_config_id=1, request_id=str(uuid.uuid4())),
                "match.JoinQueueResponse", metadata=md(1001, join_path),
            )
            assert r_no_probe.error_code == 16022

            # 获取探测计划
            plan_path = "/match.MatchService/GetNetworkProbePlan"
            plan = await _call(
                c, p, plan_path, p.message("match.GetNetworkProbePlanRequest"),
                "match.GetNetworkProbePlanResponse", metadata=md(1001, plan_path),
            )
            assert plan.plan_id and len(plan.targets) == 1
            target = plan.targets[0]
            assert target.physical_region == 1 and target.port == 19030

            # 照 nodes/battle_tcp.py:214-247 验签 grant 并签发 receipt
            expected_sig = hmac.digest(PROBE_SECRET, _PROBE_GRANT_DOMAIN + bytes(target.grant.payload), "sha256")
            assert hmac.compare_digest(bytes(target.grant.signature), expected_sig)
            grant = p.parse("match.NetworkProbeGrant", bytes(target.grant.payload))
            assert grant.player_id == 1001 and grant.target_instance_id == "battle-inst-1"

            receipt = p.message(
                "match.NetworkProbeReceipt",
                grant=grant, rtt_ms=18, sample_count=3,
                measured_at_ms=h["clock"](), expires_at_ms=grant.expires_at_ms,
            )
            r_payload = receipt.SerializeToString(deterministic=True)
            r_sig = hmac.digest(PROBE_SECRET, _PROBE_RECEIPT_DOMAIN + r_payload, "sha256")

            commit_path = "/match.MatchService/CommitNetworkProbe"
            commit_req = p.message("match.CommitNetworkProbeRequest", plan_id=plan.plan_id)
            commit_req.receipts.add(payload=r_payload, signature=r_sig)
            commit_resp = await _call(c, p, commit_path, commit_req, "match.CommitNetworkProbeResponse", metadata=md(1001, commit_path))
            assert commit_resp.expires_at_ms == grant.expires_at_ms

            # 篡改签名必须拒绝
            bad_req = p.message("match.CommitNetworkProbeRequest", plan_id=plan.plan_id)
            bad_req.receipts.add(payload=r_payload, signature=b"\x00" * 32)
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await _call(c, p, commit_path, bad_req, "match.CommitNetworkProbeResponse", metadata=md(1001, commit_path))
            assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

            # 探测提交后入队成功
            r_ok = await _call(
                c, p, join_path,
                p.message("match.JoinQueueRequest", mode=MODE_PVE_SOLO, battle_config_id=1, request_id=str(uuid.uuid4()), network_plan_id=plan.plan_id),
                "match.JoinQueueResponse", metadata=md(1001, join_path),
            )
            assert r_ok.error_code == 0 and r_ok.queue_ticket

    asyncio.run(run())


# -- 3. 排队 + 取消墓碑 + 切磋 + 真实 Gather + PrepareBattle/battle_not_allocatable 回滚 --------------


def test_queue_challenge_gather_and_rollback_matrix():
    async def run():
        async with _match_harness() as h:
            p, c, md = h["protocol"], h["client"], h["gate_md"]
            scene_stub: _TestSceneServer = h["scene_stub"]
            battle_adapter: _TestBattleGrpcAdapter = h["battle_adapter"]

            # 未开放模式拒绝
            join_path = "/match.MatchService/JoinQueue"
            r_bad_mode = await _call(
                c, p, join_path,
                p.message("match.JoinQueueRequest", mode=MODE_3V3, battle_config_id=1, request_id=str(uuid.uuid4())),
                "match.JoinQueueResponse", metadata=md(1001, join_path),
            )
            assert r_bad_mode.error_code == ERR_MATCH_MODE_NOT_OPEN

            # 先取消再迟到入队：持久墓碑阻止复活，GetQueueStatus.request_cancelled = True
            tomb_req_id = str(uuid.uuid4())
            cancel_path = "/match.MatchService/CancelQueue"
            await _call(c, p, cancel_path, p.message("match.CancelQueueRequest", request_id=tomb_req_id), "Empty", metadata=md(1001, cancel_path))

            status_path = "/match.MatchService/GetQueueStatus"
            st = await _call(c, p, status_path, p.message("match.GetQueueStatusRequest", request_id=tomb_req_id), "match.GetQueueStatusResponse", metadata=md(1001, status_path))
            assert st.state == QUEUE_STATE_NOT_QUEUED and st.request_cancelled is True

            r_late = await _call(
                c, p, join_path,
                p.message("match.JoinQueueRequest", mode=MODE_1V1, battle_config_id=1, request_id=tomb_req_id),
                "match.JoinQueueResponse", metadata=md(1001, join_path),
            )
            assert r_late.error_code == ERR_MATCH_CANCEL_TOO_LATE

            # 1v1 排队：1002 在 PrepareBattle 被拒 -> 1001 收到 CancelBattlePrepare 回滚并保留回队，1002 肇事被终结
            scene_stub.reject_players[1002] = 1006
            j1 = await _call(c, p, join_path, p.message("match.JoinQueueRequest", mode=MODE_1V1, battle_config_id=1, request_id=str(uuid.uuid4())), "match.JoinQueueResponse", metadata=md(1001, join_path))
            j2 = await _call(c, p, join_path, p.message("match.JoinQueueRequest", mode=MODE_1V1, battle_config_id=1, request_id=str(uuid.uuid4())), "match.JoinQueueResponse", metadata=md(1002, join_path))
            assert j1.error_code == 0 and j2.error_code == 0

            formed = await h["matchmaker"].run_once()
            assert len(formed) == 1
            assert any(pid == 1001 for pid, _ in scene_stub.cancelled)
            st1 = await _call(c, p, status_path, p.message("match.GetQueueStatusRequest"), "match.GetQueueStatusResponse", metadata=md(1001, status_path))
            st2 = await _call(c, p, status_path, p.message("match.GetQueueStatusRequest"), "match.GetQueueStatusResponse", metadata=md(1002, status_path))
            assert st1.state == QUEUE_STATE_QUEUED  # 无辜者回队
            assert st2.state == QUEUE_STATE_NOT_QUEUED  # 肇事者终止

            # battle_not_allocatable 回滚：DestroyBattle + CancelBattlePrepare + 双方回队
            scene_stub.reject_players.clear()
            scene_stub.cancelled.clear()
            battle_adapter.force_not_allocatable = True
            j2_again = await _call(c, p, join_path, p.message("match.JoinQueueRequest", mode=MODE_1V1, battle_config_id=1, request_id=str(uuid.uuid4())), "match.JoinQueueResponse", metadata=md(1002, join_path))
            assert j2_again.error_code == 0
            await h["matchmaker"].run_once()
            assert len(battle_adapter.destroyed) == 1
            assert {pid for pid, _ in scene_stub.cancelled} == {1001, 1002}
            # 节点进入冷却后可恢复
            battle_adapter.force_not_allocatable = False
            h["battle_resolver"]._cooldown_until.clear()

            # 取消双方排队，转测切磋 ChallengePlayer -> RespondChallenge(accept=True) -> 真建房
            await _call(c, p, cancel_path, p.message("match.CancelQueueRequest"), "Empty", metadata=md(1001, cancel_path))
            await _call(c, p, cancel_path, p.message("match.CancelQueueRequest"), "Empty", metadata=md(1002, cancel_path))

            ch_path = "/match.MatchService/ChallengePlayer"
            ch_self = await _call(c, p, ch_path, p.message("match.ChallengePlayerRequest", target_player_id=1001), "match.ChallengePlayerResponse", metadata=md(1001, ch_path))
            assert ch_self.error_message.id == ERR_MATCH_CHALLENGE_SELF

            ch_ok = await _call(c, p, ch_path, p.message("match.ChallengePlayerRequest", target_player_id=1004, battle_config_id=1), "match.ChallengePlayerResponse", metadata=md(1003, ch_path))
            assert ch_ok.error_message.id == 0 and ch_ok.challenge_id > 0

            rc_path = "/match.MatchService/RespondChallenge"
            rc_ok = await _call(c, p, rc_path, p.message("match.RespondChallengeRequest", challenge_id=ch_ok.challenge_id, accept=True), "match.RespondChallengeResponse", metadata=md(1004, rc_path))
            assert rc_ok.error_message.id == 0
            assert len(h["battle_node"].rooms) == 1

    asyncio.run(run())


# -- 4. CommitBattleOutcome + GetBattleTerminalDecision + Outbox -> 真 PlayerInbox 销账释票 + 战报回放 --


def test_outcome_terminal_decision_outbox_inbox_settlement_and_reports():
    async def run():
        async with _match_harness() as h:
            p, c, md = h["protocol"], h["client"], h["gate_md"]
            battle_node: BattleNode = h["battle_node"]
            clock: _FakeClock = h["clock"]

            # 1001 与 1002 走 1v1 排队 -> Matchmaker + Gather 在真实 BattleNode 建房
            join_path = "/match.MatchService/JoinQueue"
            await _call(c, p, join_path, p.message("match.JoinQueueRequest", mode=MODE_1V1, battle_config_id=1, request_id=str(uuid.uuid4())), "match.JoinQueueResponse", metadata=md(1001, join_path))
            await _call(c, p, join_path, p.message("match.JoinQueueRequest", mode=MODE_1V1, battle_config_id=1, request_id=str(uuid.uuid4())), "match.JoinQueueResponse", metadata=md(1002, join_path))
            formed = await h["matchmaker"].run_once()
            assert len(formed) == 1
            battle_id = formed[0]
            assert battle_id in battle_node.rooms

            # 终局提交前：Scene 查 GetBattleTerminalDecision 应回 UNKNOWN + terminal_expected=True
            td_path = "/match.MatchInternal/GetBattleTerminalDecision"
            td_req = p.message("match.GetBattleTerminalDecisionRequest", battle_id=battle_id)
            td_digest = service_digest(td_req)
            sign_service_call(td_path, td_req, "scene", clock(), SCENE_SECRET)
            td_resp = await _call(c, p, td_path, td_req, "match.GetBattleTerminalDecisionResponse")
            assert not td_resp.retry and td_resp.decision == OUTCOME_DECISION_UNKNOWN and td_resp.terminal_expected is True
            assert td_resp.request_digest == td_digest
            assert verify_service_call(td_path + "/reply", td_resp, "match", clock(), SCENE_SECRET)

            # 驱动真实 BattleNode 打完对局并冻结 outcome_request（含战报回放）
            room = battle_node.rooms[battle_id]
            while not room.proposed_outcome:
                battle_node._resolve(room)
            commit_req = battle_node.outcome_request(battle_id)
            assert commit_req is not None

            # Battle 提交 CommitBattleOutcome（签名 caller="battle" -> 回包 caller="match"）
            co_path = "/match.MatchInternal/CommitBattleOutcome"
            co_digest = service_digest(commit_req)
            sign_service_call(co_path, commit_req, "battle", clock(), BATTLE_SECRET)
            co_resp = await _call(c, p, co_path, commit_req, "match.CommitBattleOutcomeResponse")
            assert co_resp.decision == OUTCOME_DECISION_COMPLETE
            assert co_resp.request_digest == co_digest
            assert verify_service_call(co_path + "/reply", co_resp, "match", clock(), BATTLE_SECRET)

            # 幂等重放原件仍回 COMPLETE
            co_replay = p.message("match.CommitBattleOutcomeRequest")
            co_replay.CopyFrom(commit_req)
            co_replay.ClearField("auth")
            sign_service_call(co_path, co_replay, "battle", clock(), BATTLE_SECRET)
            co_resp2 = await _call(c, p, co_path, co_replay, "match.CommitBattleOutcomeResponse")
            assert co_resp2.decision == OUTCOME_DECISION_COMPLETE

            # 篡改原件重放拒绝（FAILED_PRECONDITION）
            co_bad = p.message("match.CommitBattleOutcomeRequest")
            co_bad.CopyFrom(commit_req)
            co_bad.ClearField("auth")
            co_bad.raw_result_payload = b"corrupt"
            sign_service_call(co_path, co_bad, "battle", clock(), BATTLE_SECRET)
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await _call(c, p, co_path, co_bad, "match.CommitBattleOutcomeResponse")
            assert exc_info.value.code() == grpc.StatusCode.FAILED_PRECONDITION

            # 终局提交后：Scene 查 GetBattleTerminalDecision 应回 COMPLETE
            td_req2 = p.message("match.GetBattleTerminalDecisionRequest", battle_id=battle_id)
            sign_service_call(td_path, td_req2, "scene", clock(), SCENE_SECRET)
            td_resp2 = await _call(c, p, td_path, td_req2, "match.GetBattleTerminalDecisionResponse")
            assert td_resp2.decision == OUTCOME_DECISION_COMPLETE and td_resp2.terminal_expected is False

            # Outbox 第一轮：发布 match-results Kafka + EnqueueBattleSettlement -> PENDING；票据绝不提前释放！
            stats1 = await h["outbox"].run_once()
            assert stats1.results_published == 1
            assert stats1.settlements_pending == 2
            assert stats1.tickets_released == 0
            assert len(h["published_results"]) == 1 and h["published_results"][0][0] == battle_id

            status_path = "/match.MatchService/GetQueueStatus"
            st_mid = await _call(c, p, status_path, p.message("match.GetQueueStatusRequest"), "match.GetQueueStatusResponse", metadata=md(1001, status_path))
            assert st_mid.state == QUEUE_STATE_READY  # 尚未 APPLIED，严禁释放票据

            # Scene 消费并 Ack(APPLIED=1) 真实 PlayerInbox
            inbox_c: GrpcClient = h["inbox_client"]
            for pid in (1001, 1002):
                fetch_path = "/player_inbox.PlayerInbox/FetchItems"
                f_req = p.message(
                    "player_inbox.FetchPlayerInboxRequest",
                    player_id=pid, domain=2, owner_epoch=1, correlation_id=1, max_items=16,
                )
                f_req.admission.player_id = pid
                f_req.admission.scene_instance_id = "11111111-1111-4111-8111-111111111111"
                f_req.admission.generation = 1
                f_req.admission.owner_revision = 1
                f_req.admission.owner_epoch = 1
                f_req.admission.scene_node_id = "2"
                f_req.admission.scene_id = 1
                f_req.admission.zone_id = 1
                f_req.admission.session_id = (1 << 17) | pid
                f_req.admission.login_generation = 1
                sign_service_call(fetch_path, f_req, "scene", clock(), SCENE_SECRET)
                f_resp = await _call(inbox_c, p, fetch_path, f_req, "player_inbox.FetchPlayerInboxResponse")
                assert len(f_resp.items) == 1
                seq = f_resp.items[0].seq

                ack_path = "/player_inbox.PlayerInbox/AckItems"
                a_req = p.message(
                    "player_inbox.AckPlayerInboxRequest",
                    player_id=pid, domain=2, owner_epoch=1, correlation_id=2,
                )
                a_req.admission.CopyFrom(f_req.admission)
                a_req.decisions.add(seq=seq, outcome=1, request_id=battle_id)  # OUTCOME_APPLIED
                sign_service_call(ack_path, a_req, "scene", clock(), SCENE_SECRET)
                a_resp = await _call(inbox_c, p, ack_path, a_req, "player_inbox.AckPlayerInboxResponse")
                assert a_resp.error_message.id == 0 and a_resp.results[0].state == 2

            # 推进时钟过 5s 退避窗口，Outbox 第二轮 Lookup -> 见到 APPLIED(2) -> 释放双方匹配票！
            clock.advance(6000)
            stats2 = await h["outbox"].run_once()
            assert stats2.settlements_applied == 2 and stats2.tickets_released == 2
            st_done = await _call(c, p, status_path, p.message("match.GetQueueStatusRequest"), "match.GetQueueStatusResponse", metadata=md(1001, status_path))
            assert st_done.state == QUEUE_STATE_NOT_QUEUED

            # 战报列表与回放正文：参战者可见，局外人 1005 不可见
            lr_path = "/match.MatchService/ListBattleReports"
            lr_resp = await _call(c, p, lr_path, p.message("match.ListBattleReportsRequest"), "match.ListBattleReportsResponse", metadata=md(1001, lr_path))
            assert len(lr_resp.reports) == 1 and lr_resp.reports[0].battle_id == battle_id and lr_resp.reports[0].has_replay

            gr_path = "/match.MatchService/GetBattleReport"
            gr_resp = await _call(c, p, gr_path, p.message("match.GetBattleReportRequest", battle_id=battle_id), "match.GetBattleReportResponse", metadata=md(1001, gr_path))
            assert gr_resp.error_message.id == 0 and len(gr_resp.replay.payload) > 0

            gr_other = await _call(c, p, gr_path, p.message("match.GetBattleReportRequest", battle_id=battle_id), "match.GetBattleReportResponse", metadata=md(1005, gr_path))
            assert gr_other.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND

    asyncio.run(run())


# -- 5. 观战（直连 AddObserver + 大厅 SpectateTicket 验签）+ 丢票补签 + 赛事广播 ------------------------


def test_spectate_direct_lobby_ticket_and_featured():
    async def run():
        async with _match_harness() as h:
            p, c, md = h["protocol"], h["client"], h["gate_md"]
            clock: _FakeClock = h["clock"]

            # 建一场 1v1 公开对局
            join_path = "/match.MatchService/JoinQueue"
            await _call(c, p, join_path, p.message("match.JoinQueueRequest", mode=MODE_1V1, battle_config_id=1, request_id=str(uuid.uuid4())), "match.JoinQueueResponse", metadata=md(1001, join_path))
            await _call(c, p, join_path, p.message("match.JoinQueueRequest", mode=MODE_1V1, battle_config_id=1, request_id=str(uuid.uuid4())), "match.JoinQueueResponse", metadata=md(1002, join_path))
            battle_id = (await h["matchmaker"].run_once())[0]

            # ListWatchableBattles 可见该局
            lw_path = "/match.MatchService/ListWatchableBattles"
            lw = await _call(c, p, lw_path, p.message("match.ListWatchableBattlesRequest", limit=10), "match.ListWatchableBattlesResponse", metadata=md(1005, lw_path))
            assert len(lw.battles) == 1 and lw.battles[0].battle_id == battle_id and not lw.battles[0].featured

            # WatchBattle(L0 DIRECT) 走真实 BattleNode.AddObserver
            wb_path = "/match.MatchService/WatchBattle"
            wb_direct = await _call(c, p, wb_path, p.message("match.WatchBattleRequest", battle_id=battle_id), "match.WatchBattleResponse", metadata=md(1005, wb_path))
            assert wb_direct.error_message.id == 0 and wb_direct.delivery == SPECTATE_DELIVERY_DIRECT and wb_direct.spectator_operation_id

            # RequestBattleTicket 补签：参战者 1001 与观众 1005 均能拿到真实签名票据
            rt_path = "/match.MatchService/RequestBattleTicket"
            rt_p = await _call(c, p, rt_path, p.message("RequestBattleTicketRequest", battle_id=battle_id), "RequestBattleTicketResponse", metadata=md(1001, rt_path))
            assert rt_p.error_message.id == 0 and rt_p.assignment.battle_id == battle_id and rt_p.assignment.role == 1

            rt_obs = await _call(c, p, rt_path, p.message("RequestBattleTicketRequest", battle_id=battle_id), "RequestBattleTicketResponse", metadata=md(1005, rt_path))
            assert rt_obs.error_message.id == 0 and rt_obs.assignment.role == 2

            # SetSpectateFeatured 标成赛事局 -> 升级广播（SPECTATE_TIER_BROADCAST=3），后续观众 1006 走 LOBBY 拿 SpectateTicket
            feat_path = "/match.MatchInternal/SetSpectateFeatured"
            feat_req = p.message("match.SetSpectateFeaturedRequest", battle_id=battle_id, featured=True)
            sign_service_call(feat_path, feat_req, "ops", clock(), OPS_SECRET)
            feat_resp = await _call(c, p, feat_path, feat_req, "match.SetSpectateFeaturedResponse")
            assert feat_resp.reject == 0 and feat_resp.tier == SPECTATE_TIER_BROADCAST

            wb_lobby = await _call(c, p, wb_path, p.message("match.WatchBattleRequest", battle_id=0), "match.WatchBattleResponse", metadata=md(1006, wb_path))
            assert wb_lobby.error_message.id == 0 and wb_lobby.delivery == SPECTATE_DELIVERY_LOBBY and wb_lobby.spectate_ticket

            # 按 nodes/gate_spectate.py:88-94 完全一致的逻辑验签 SpectateTicket
            payload = verify_spectate_ticket(p, SPECTATE_SECRET, bytes(wb_lobby.spectate_ticket))
            assert payload is not None
            assert payload.battle_id == battle_id and payload.player_id == 1006 and payload.battle_instance_id == "battle-inst-1"
            assert payload.expire_at_ms > clock()

    asyncio.run(run())


# -- 6. Guild 活动开局/占用 + 滞留成局人工处置 + RoleFence --------------------------------------------


def test_guild_activity_stranded_resolution_and_role_fence():
    async def run():
        async with _match_harness() as h:
            p, c, md = h["protocol"], h["client"], h["gate_md"]
            clock: _FakeClock = h["clock"]

            # AcquireTrialOccupancy -> ReleaseTrialOccupancy -> StartActivityBattle -> CancelActivityBattle -> GetBattleCompletion
            acq_path = "/match.MatchInternal/AcquireTrialOccupancy"
            acq_req = p.message("match.AcquireTrialOccupancyRequest", owner_id=9001, ttl_ms=30_000)
            acq_req.roster.add(player_id=1001, account="acct:1001", owner_revision=1)
            sign_service_call(acq_path, acq_req, "guild", clock(), GUILD_SECRET)
            acq_resp = await _call(c, p, acq_path, acq_req, "match.AcquireTrialOccupancyResponse")
            assert acq_resp.reject == 0

            # 占用期间玩家不可排队
            join_path = "/match.MatchService/JoinQueue"
            r_occ = await _call(c, p, join_path, p.message("match.JoinQueueRequest", mode=MODE_PVE_SOLO, battle_config_id=1, request_id=str(uuid.uuid4())), "match.JoinQueueResponse", metadata=md(1001, join_path))
            assert r_occ.error_code == ERR_MATCH_IN_BATTLE

            # RoleFence 预检识别占用阻塞
            rf_prep = "/rolefence.RoleFenceParticipant/PrepareRoleFreeze"
            prep_req = p.message("rolefence.PrepareRoleFreezeRequest", dry_run=True)
            prep_req.state.player_id = 1001
            prep_req.state.account = "acct:1001"
            prep_req.state.owner_revision = 1
            prep_req.state.freeze_token = 77
            prep_req.state.version = 2
            sign_service_call(rf_prep, prep_req, "coordinator", clock(), COORD_SECRET)
            prep_resp = await _call(c, p, rf_prep, prep_req, "rolefence.PrepareRoleFreezeResponse")
            assert not prep_resp.clean and prep_resp.blocker == "match_role_occupancy"

            # 释放占用后 RoleFence 冻结干净通过
            rel_path = "/match.MatchInternal/ReleaseTrialOccupancy"
            rel_req = p.message("match.ReleaseTrialOccupancyRequest", owner_id=9001, player_ids=[1001])
            sign_service_call(rel_path, rel_req, "guild", clock(), GUILD_SECRET)
            rel_resp = await _call(c, p, rel_path, rel_req, "match.ReleaseTrialOccupancyResponse")
            assert rel_resp.reject == 0

            prep_req2 = p.message("rolefence.PrepareRoleFreezeRequest", dry_run=False)
            prep_req2.state.CopyFrom(prep_req.state)
            sign_service_call(rf_prep, prep_req2, "coordinator", clock(), COORD_SECRET)
            prep_resp2 = await _call(c, p, rf_prep, prep_req2, "rolefence.PrepareRoleFreezeResponse")
            assert prep_resp2.clean and not prep_resp2.blocker

            # 冻结后客户端请求被 fail-closed 拒绝（PERMISSION_DENIED）
            with pytest.raises(grpc.aio.AioRpcError) as exc_frozen:
                await _call(c, p, join_path, p.message("match.JoinQueueRequest", mode=MODE_PVE_SOLO, battle_config_id=1, request_id=str(uuid.uuid4())), "match.JoinQueueResponse", metadata=md(1001, join_path))
            assert exc_frozen.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 解冻并过户到新 owner_revision=2，随后 PurgeRemovedRole 删角清理
            rf_apply = "/rolefence.RoleFenceParticipant/ApplyRoleFence"
            app_req = p.message("rolefence.ApplyRoleFenceRequest")
            app_req.state.player_id = 1001
            app_req.state.account = "acct:new-owner"
            app_req.state.owner_revision = 2
            app_req.state.freeze_token = 0
            app_req.state.version = 3
            sign_service_call(rf_apply, app_req, "coordinator", clock(), COORD_SECRET)
            await _call(c, p, rf_apply, app_req, "rolefence.ApplyRoleFenceResponse")

            # 旧修订 1 仍被拒，新修订 2 放行
            with pytest.raises(grpc.aio.AioRpcError):
                await _call(c, p, join_path, p.message("match.JoinQueueRequest", mode=MODE_PVE_SOLO, battle_config_id=1, request_id=str(uuid.uuid4())), "match.JoinQueueResponse", metadata=md(1001, join_path, account="acct:1001", owner_revision=1))

            rf_purge = "/rolefence.RoleFenceParticipant/PurgeRemovedRole"
            purge_req = p.message("rolefence.PurgeRemovedRoleRequest")
            purge_req.state.player_id = 1001
            purge_req.state.account = ""
            purge_req.state.owner_revision = 2
            purge_req.state.freeze_token = 0
            purge_req.state.freeze_final = True
            purge_req.state.version = 4
            sign_service_call(rf_purge, purge_req, "coordinator", clock(), COORD_SECRET)
            await _call(c, p, rf_purge, purge_req, "rolefence.PurgeRemovedRoleResponse")

            # StartActivityBattle（1003, 1004）
            act_path = "/match.MatchInternal/StartActivityBattle"
            act_req = p.message("match.StartActivityBattleRequest", battle_config_id=1, member_player_ids=[1003, 1004], request_id=7788)
            act_req.activity_context.kind = 1
            act_req.activity_context.guild_id = 500
            act_req.activity_context.activity_id = 10
            act_req.activity_context.period_key = 202611
            act_req.activity_context.guild_period_key = 202611
            act_req.activity_context.initiator_player_id = 1003
            sign_service_call(act_path, act_req, "guild", clock(), GUILD_SECRET)
            act_resp = await _call(c, p, act_path, act_req, "match.StartActivityBattleResponse")
            assert act_resp.reject == 0 and act_resp.battle_id > 0

            # 模拟一条坏形状滞留成局（outcome_protocol_version=0）过期 -> ListStrandedFormations + ResolveStrandedFormation(SHOW -> DRY_RUN -> ABORT)
            bid = act_resp.battle_id
            h["store"]._state.formations[bid].outcome_protocol_version = 0
            clock.advance(1_000_000)

            ls_path = "/match.MatchInternal/ListStrandedFormations"
            ls_req = p.message("match.ListStrandedFormationsRequest", after_battle_id=0, limit=10)
            sign_service_call(ls_path, ls_req, "ops-formation", clock(), MANUAL_SECRET)
            ls_resp = await _call(c, p, ls_path, ls_req, "match.ListStrandedFormationsResponse")
            assert len(ls_resp.formations) == 1 and ls_resp.formations[0].battle_id == bid
            digest = ls_resp.formations[0].digest

            rs_path = "/match.MatchInternal/ResolveStrandedFormation"
            for mode in (1, 2, 3):  # SHOW -> DRY_RUN -> ABORT
                rs_req = p.message(
                    "match.ResolveStrandedFormationRequest",
                    battle_id=bid, mode=mode, expect_digest=digest, expect_target="staging-test",
                    operator_name="oncall", reason="ticket-42",
                )
                sign_service_call(rs_path, rs_req, "ops-formation", clock(), MANUAL_SECRET)
                rs_resp = await _call(c, p, rs_path, rs_req, "match.ResolveStrandedFormationResponse")
                assert rs_resp.verdict == 1  # ELIGIBLE / COMMITTED

            # GetBattleCompletion 读到 ABORT
            gc_path = "/match.MatchInternal/GetBattleCompletion"
            gc_req = p.message("match.GetBattleCompletionRequest", battle_id=bid)
            sign_service_call(gc_path, gc_req, "guild", clock(), GUILD_SECRET)
            gc_resp = await _call(c, p, gc_path, gc_req, "match.GetBattleCompletionResponse")
            assert gc_resp.reject == 0 and gc_resp.decision == OUTCOME_DECISION_ABORT

    asyncio.run(run())


# -- 7. Fail-Closed 鉴权、Canary 轨道与 SQL Schema 完整性 ---------------------------------------------


def test_fail_closed_auth_canary_and_schema_ddl():
    assert MATCH_NODE_TYPE == 16 and TEAM_NODE_TYPE == 10
    assert MatchConfig(release_track="stable").runs_global_background is True
    assert MatchConfig(release_track="canary").runs_global_background is False

    ddl = (Path(__file__).resolve().parents[1] / "config" / "match_schema.sql").read_text(encoding="utf-8")
    for table in REQUIRED_TABLES:
        assert re.search(rf"CREATE TABLE IF NOT EXISTS `{table}`", ddl), f"missing table {table} in match_schema.sql"

    async def run():
        # guild_secret 留空，验证 Guild 内部 RPC fail-closed
        async with _match_harness(guild_secret=b"") as h:
            p, c, md = h["protocol"], h["client"], h["gate_md"]
            clock: _FakeClock = h["clock"]

            # 1) 客户端 RPC 缺失签名 / 缺 owner_revision / 缺 authenticated_account -> PERMISSION_DENIED
            for path, req in (
                ("/match.MatchService/GetQueueStatus", p.message("match.GetQueueStatusRequest")),
                ("/teampb.ClientPlayerTeam/GetMyTeam", p.message("teampb.GetMyTeamRequest")),
            ):
                with pytest.raises(grpc.aio.AioRpcError) as e_none:
                    await c.call(path, req.SerializeToString())
                assert e_none.value.code() == grpc.StatusCode.PERMISSION_DENIED

                with pytest.raises(grpc.aio.AioRpcError) as e_no_rev:
                    await c.call(path, req.SerializeToString(), metadata=md(1001, path, owner_revision=0))
                assert e_no_rev.value.code() == grpc.StatusCode.PERMISSION_DENIED

                with pytest.raises(grpc.aio.AioRpcError) as e_no_acct:
                    await c.call(path, req.SerializeToString(), metadata=md(1001, path, account=""))
                assert e_no_acct.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 2) 全部内部服务拒绝携带 x-session-detail-bin（即使携带合法 HMAC！）
            internal_cases = [
                ("/match.MatchInternal/GetBattleTerminalDecision", p.message("match.GetBattleTerminalDecisionRequest", battle_id=1), "scene", SCENE_SECRET),
                ("/match.MatchInternal/CommitBattleOutcome", p.message("match.CommitBattleOutcomeRequest", battle_id=1), "battle", BATTLE_SECRET),
                ("/teampb.TeamAuthority/ReadMembership", p.message("teampb.ReadTeamMembershipRequest"), "scene", SCENE_SECRET),
                ("/rolefence.RoleFenceParticipant/ApplyRoleFence", p.message("rolefence.ApplyRoleFenceRequest"), "coordinator", COORD_SECRET),
            ]
            for path, req, caller, sec in internal_cases:
                sign_service_call(path, req, caller, clock(), sec)
                with pytest.raises(grpc.aio.AioRpcError) as e_sess:
                    await c.call(path, req.SerializeToString(deterministic=True), metadata=md(1001, path))
                assert e_sess.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 3) 错误 HMAC 密钥拒绝
            bad_td = p.message("match.GetBattleTerminalDecisionRequest", battle_id=1)
            sign_service_call("/match.MatchInternal/GetBattleTerminalDecision", bad_td, "scene", clock(), b"wrong-secret-32-bytes-0000000000")
            with pytest.raises(grpc.aio.AioRpcError) as e_hmac:
                await c.call("/match.MatchInternal/GetBattleTerminalDecision", bad_td.SerializeToString(deterministic=True))
            assert e_hmac.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 4) 未配置 MMORPG_MATCH_INTERNAL_SECRET_GUILD 时 StartActivityBattle fail-closed
            act_path = "/match.MatchInternal/StartActivityBattle"
            act_req = p.message("match.StartActivityBattleRequest", battle_config_id=1, request_id=1)
            sign_service_call(act_path, act_req, "guild", clock(), GUILD_SECRET)
            with pytest.raises(grpc.aio.AioRpcError) as e_guild:
                await c.call(act_path, act_req.SerializeToString(deterministic=True))
            assert e_guild.value.code() in (grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.UNAVAILABLE)

    asyncio.run(run())


def test_start_team_match_immediate_pve_and_backfill():
    async def run():
        async with _match_harness() as h:
            p, c, md = h["protocol"], h["client"], h["gate_md"]

            # 1001 建队并拉入 1002
            cp = "/teampb.ClientPlayerTeam/CreateTeam"
            r_c = await _call(c, p, cp, p.message("teampb.CreateTeamRequest", request_id=str(uuid.uuid4())), "teampb.TeamResponse", metadata=md(1001, cp))
            tid = r_c.team.team_id
            ip = "/teampb.ClientPlayerTeam/InviteToTeam"
            await _call(c, p, ip, p.message("teampb.InviteToTeamRequest", target_player_id=1002, expected_team_id=tid, request_id=str(uuid.uuid4())), "teampb.TeamResponse", metadata=md(1001, ip))
            rp = "/teampb.ClientPlayerTeam/RespondInvite"
            await _call(c, p, rp, p.message("teampb.RespondInviteRequest", team_id=tid, accept=True, request_id=str(uuid.uuid4())), "teampb.TeamResponse", metadata=md(1002, rp))

            # StartTeamMatch 即时 PVE（2 人队伍 target_team_size=0 -> 直接开战，真实 BattleNode 建房）
            sp = "/teampb.ClientPlayerTeam/StartTeamMatch"
            req_id = str(uuid.uuid4())
            r_s = await _call(
                c, p, sp,
                p.message("teampb.StartTeamMatchRequest", battle_config_id=1, expected_team_id=tid, request_id=req_id, match_mode=MODE_PVE_TEAM),
                "teampb.TeamResponse", metadata=md(1001, sp),
            )
            assert r_s.error_message.id == 0 and r_s.match_intent.battle_id > 0
            assert r_s.match_intent.battle_id in h["battle_node"].rooms

            # GetMyTeam(match_request_id=req_id) 返回原开战意图
            gp = "/teampb.ClientPlayerTeam/GetMyTeam"
            r_g = await _call(c, p, gp, p.message("teampb.GetMyTeamRequest", match_request_id=req_id), "teampb.TeamResponse", metadata=md(1001, gp))
            assert r_g.match_intent.battle_id == r_s.match_intent.battle_id and r_g.match_intent.state == 4

    asyncio.run(run())
