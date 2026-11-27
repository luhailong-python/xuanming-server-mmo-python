"""guild 生产对等端到端测试：AssetOp 经济闭环（捐献/商店/粘性 27007/对账回档）与同道历练（大厅/MatchInternal 双向验签/战斗结算/owed 溢出/护栏阻断）。"""

from __future__ import annotations

import asyncio
import functools
from contextlib import AsyncExitStack
from dataclasses import replace

import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from xuanming.data.asset_ledger import Outcome, authorized as ledger_authorized, classify as ledger_classify, record as ledger_record
from xuanming.grpc_runtime import (
    GrpcClient,
    serve_grpc,
    service_digest,
    session_metadata,
    sign_service_call,
    verify_service_call,
)
from xuanming.guild import (
    ACTIVITY_TYPE_TRIAL,
    ASSET_STREAM_GUILD_CREDIT,
    ASSET_STREAM_GUILD_DEBIT,
    AssetOpRow,
    ERR_GUILD_ASSET_PENDING,
    ERR_GUILD_ASSET_REJECTED,
    ERR_GUILD_CURRENCY_INSUFFICIENT,
    ERR_GUILD_SHOP_LIMIT,
    ERR_GUILD_TRIAL_TEAM_INVALID,
    ERR_INVALID_PARAMETER,
    ERR_OK,
    GrpcDataServiceLedgerReader,
    GuildAssetOpCaller,
    GuildAssetOpLoop,
    GuildConfig,
    GuildService,
    GuildTrialCoordinator,
    MATCH_INTERNAL_SECRET_ENV,
    MatchInternalClient,
    MemoryGuildRankStore,
    MemoryGuildStore,
    OP_KIND_ACTIVITY_REWARD,
    OP_KIND_DONATE,
    OP_KIND_SHOP,
    OP_STATUS_ABORTED,
    OP_STATUS_APPLIED,
    OP_STATUS_APPLIED_PARTIAL,
    OP_STATUS_PENDING,
    OP_STATUS_REJECTED,
    ROLE_BLOCKER_ASSET_OP,
    ROLE_BLOCKER_TRIAL_LAUNCH_ROLE,
    ROLE_BLOCKER_TRIAL_LOBBY_SLOT,
    ROLE_BLOCKER_TRIAL_REWARD_OWED,
    SERVICE_GUILD,
    SERVICE_GUILD_INTERNAL,
    SERVICE_ROLE_FENCE,
    SceneTarget,
    removal_token,
)
from xuanming.guild.errors import (
    ERR_ASSET_CURRENCY_INSUFFICIENT,
    ERR_ASSET_PARTIAL_APPLIED,
    TrialMatchUnavailableError,
)
from xuanming.guild.model import (
    ACTIVITY_BATTLE_REJECT_MEMBER_IN_BATTLE,
    ACTIVITY_BATTLE_REJECT_NONE,
    ASSET_OP_MAX_PENDING,
    BATTLE_ACTIVITY_KIND_GUILD_TRIAL,
    BATTLE_OUTCOME_SIDE_A_WIN,
    MS_PER_HOUR,
    ORDER_STATUS_APPLIED,
    ORDER_STATUS_APPLIED_PARTIAL,
    ORDER_STATUS_PENDING,
    ORDER_STATUS_REJECTED,
    OUTCOME_DECISION_COMPLETE,
    TRIAL_BATTLE_STATE_SETTLED,
    TRIAL_BATTLE_STATE_STARTED,
    TRIAL_LAUNCH_ROLE_ACTIVE,
    TRIAL_LAUNCH_ROLE_RELEASED,
    TRIAL_LAUNCH_STATE_LAUNCHED,
    TRIAL_LAUNCH_STATE_PREPARED,
    TRIAL_LAUNCH_STATE_TERMINAL,
    TRIAL_LOBBY_STATE_ENDED,
    TRIAL_LOBBY_STATE_LAUNCHED,
    TRIAL_LOBBY_STATE_PENDING,
    TRIAL_OCCUPANCY_REJECT_NONE,
    TRIAL_SETTLE_RESULT_WIN,
    TX_GUILD_ACTIVITY_REWARD,
    TrialBattleKey,
    TrialLaunchRecord,
    TrialLaunchRoleRecord,
    TrialLobbyRecord,
    TrialLobbySlotRecord,
    TrialOwedRewardRecord,
    day_key,
    guild_period_key,
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.protocol import Protocol

PROTOCOL = Protocol()

SECRET_COORDINATOR = b"role-fence-coordinator-secret-01234567"
SECRET_ASSET_GUILD = b"asset-op-secret-for-guild-caller-01234"
SECRET_MATCH_GUILD = b"match-internal-secret-for-guild-012345"


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class FakeClock:
    def __init__(self, start_ms: int = 1_750_000_000_000) -> None:
        self.now_ms = start_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, delta_ms: int) -> int:
        self.now_ms += delta_ms
        return self.now_ms


class FakeHomeZones:
    def __init__(self, zones: dict[int, int] | None = None) -> None:
        self.zones = dict(zones or {})

    async def get_home_zone(self, player_id: int) -> int:
        return self.zones.get(player_id, 1)


class FakeOnline:
    def __init__(self, online: set[int] | None = None) -> None:
        self.online = set(online or ())

    async def batch_online(self, player_ids, *, strict: bool = False) -> dict[int, bool]:
        return {int(pid): int(pid) in self.online for pid in player_ids}


class RecordingNotifier:
    def __init__(self) -> None:
        self.events: list[tuple[int, int, int, int, tuple[int, ...]]] = []

    async def notify(self, kind: int, guild_id: int, actor_player_id: int, target_player_id: int, recipients) -> None:
        self.events.append((kind, guild_id, actor_player_id, target_player_id, tuple(recipients)))


class FakeSceneAssetServer:
    """严格遵循 nodes/scene_assets.py + data/asset_ledger.py 签名与收据契约的真实 gRPC Scene 资产服务。"""

    def __init__(self, secret: bytes, clock: FakeClock) -> None:
        self.secret = secret
        self.clock = clock
        self.secrets = {"guild": secret}
        self.currencies: dict[tuple[int, int], int] = {}  # (player_id, currency_type) -> balance
        self.items: dict[tuple[int, int], int] = {}  # (player_id, config_id) -> count
        self.online: set[int] = set()
        self.ledgers: dict[int, object] = {}  # player_id -> PlayerAssetOpLedgerComp (内存态)
        self.sql_ledgers: dict[int, object] = {}  # player_id -> PlayerAssetOpLedgerComp (SQL 已提交态)
        self.player_released: set[int] = set()

        # 可编程注入开关
        self.delay_sql_commit: bool = False
        self.force_credit_reject_reason: int = 0
        self.force_credit_partial: bool = False
        self.calls: list[tuple[str, int, int, int]] = []  # (rpc, player_id, stream, seq)

    def set_currency(self, player_id: int, currency_type: int, amount: int) -> None:
        self.currencies[(player_id, currency_type)] = amount

    def flush_sql_ledgers(self) -> None:
        for pid, comp in self.ledgers.items():
            cloned = PROTOCOL.message("PlayerAssetOpLedgerComp")
            cloned.CopyFrom(comp)
            self.sql_ledgers[pid] = cloned

    def _get_ledger(self, player_id: int):
        comp = self.ledgers.get(player_id)
        if comp is None:
            comp = PROTOCOL.message("PlayerAssetOpLedgerComp")
            self.ledgers[player_id] = comp
        return comp

    def _commit_if_immediate(self, player_id: int) -> bool:
        if self.delay_sql_commit:
            return False
        cloned = PROTOCOL.message("PlayerAssetOpLedgerComp")
        cloned.CopyFrom(self._get_ledger(player_id))
        self.sql_ledgers[player_id] = cloned
        return True

    def _resp(self, outcome: Outcome, *, reason: int = 0, durable: bool = True, partial: bool = False):
        msg = PROTOCOL.message("AssetOpResponse", outcome=int(outcome), durable=durable, partial=partial)
        if reason:
            msg.reason.id = reason
        return msg

    async def _handle_scene_rpc(self, rpc: str, request, context, path: str):
        pid = int(request.player_id)
        stream = int(request.stream)
        seq = int(request.seq)
        self.calls.append((rpc, pid, stream, seq))

        # 复用生产 data/asset_ledger.authorized 强制校验签名与调用方白名单
        if not ledger_authorized(rpc, request, self.secrets, self.clock()):
            return self._resp(Outcome.REJECTED, reason=27008, durable=False)

        if pid not in self.online:
            return self._resp(Outcome.NOT_HERE, reason=27006, durable=False)

        comp = self._get_ledger(pid)
        existing_stream = next((st for st in comp.streams if int(st.stream) == stream), None)
        if existing_stream is not None:
            prev = ledger_classify(existing_stream, int(request.stream_epoch), seq)
            if prev in (Outcome.APPLIED, Outcome.REJECTED):
                durable = pid in self.sql_ledgers and not self.delay_sql_commit
                partial = seq in existing_stream.partial_seqs
                reason = ERR_ASSET_PARTIAL_APPLIED if partial else next(
                    (int(r.reason_tip_id) for r in existing_stream.rejections if int(r.seq) == seq), 0
                )
                return self._resp(prev, reason=reason, durable=durable, partial=partial)

        if rpc == "debit":
            if stream != ASSET_STREAM_GUILD_DEBIT or int(request.tx_type) != 24:
                return self._resp(Outcome.REJECTED, reason=27004, durable=False)
            for cur in request.bundle.currencies:
                bal = self.currencies.get((pid, int(cur.currency_type)), 0)
                if bal < int(cur.amount):
                    ledger_record(comp, request, Outcome.REJECTED, reason=ERR_ASSET_CURRENCY_INSUFFICIENT)
                    durable = self._commit_if_immediate(pid)
                    return self._resp(Outcome.REJECTED, reason=ERR_ASSET_CURRENCY_INSUFFICIENT, durable=durable)
            for cur in request.bundle.currencies:
                key = (pid, int(cur.currency_type))
                self.currencies[key] = self.currencies.get(key, 0) - int(cur.amount)
            ledger_record(comp, request, Outcome.APPLIED, 0)
            durable = self._commit_if_immediate(pid)
            return self._resp(Outcome.APPLIED, durable=durable)

        if rpc == "abort_debit":
            if stream != ASSET_STREAM_GUILD_DEBIT:
                return self._resp(Outcome.REJECTED, reason=27004, durable=False)
            ledger_record(comp, request, Outcome.REJECTED, 0)
            durable = self._commit_if_immediate(pid)
            return self._resp(Outcome.REJECTED, reason=0, durable=durable)

        if rpc == "credit":
            if stream != ASSET_STREAM_GUILD_CREDIT or int(request.tx_type) not in (25, 26):
                return self._resp(Outcome.REJECTED, reason=27004, durable=False)
            if self.force_credit_reject_reason:
                reason = self.force_credit_reject_reason
                ledger_record(comp, request, Outcome.REJECTED, reason)
                durable = self._commit_if_immediate(pid)
                return self._resp(Outcome.REJECTED, reason=reason, durable=durable)
            for it in request.bundle.items:
                key = (pid, int(it.config_id))
                self.items[key] = self.items.get(key, 0) + int(it.count)
            partial = self.force_credit_partial
            ledger_record(comp, request, Outcome.APPLIED, ERR_ASSET_PARTIAL_APPLIED if partial else 0, partial=partial)
            durable = self._commit_if_immediate(pid)
            return self._resp(
                Outcome.APPLIED,
                reason=ERR_ASSET_PARTIAL_APPLIED if partial else 0,
                durable=durable,
                partial=partial,
            )

        return self._resp(Outcome.UNKNOWN, durable=False)

    async def asset_debit(self, request, context, path: str):
        return await self._handle_scene_rpc("debit", request, context, path)

    async def asset_credit(self, request, context, path: str):
        return await self._handle_scene_rpc("credit", request, context, path)

    async def asset_abort_debit(self, request, context, path: str):
        return await self._handle_scene_rpc("abort_debit", request, context, path)

    async def get_player_asset_op_ledger(self, request, context, path: str):
        pid = int(request.player_id)
        comp = self.sql_ledgers.get(pid)
        resp = PROTOCOL.message(
            "data_service.GetPlayerAssetOpLedgerResponse",
            found=comp is not None,
            player_released=pid in self.player_released,
        )
        if comp is not None:
            resp.ledger.CopyFrom(comp)
        return resp

    def services(self) -> dict:
        return {
            "scene_node.SceneNodeGrpc": {
                "AssetDebit": self.asset_debit,
                "AssetCredit": self.asset_credit,
                "AssetAbortDebit": self.asset_abort_debit,
            },
            "data_service.DataService": {
                "GetPlayerAssetOpLedger": self.get_player_asset_op_ledger,
            },
        }


class DirectSceneResolver:
    def __init__(self, endpoint: str, client: GrpcClient, online_set: set[int]) -> None:
        self._endpoint = endpoint
        self._client = client
        self._online = online_set

    async def resolve(self, player_id: int) -> tuple[SceneTarget | None, bool]:
        if player_id not in self._online:
            return None, True
        return SceneTarget(endpoint=self._endpoint, call=self._client.call), False


class FakeMatchInternalServer:
    """严格遵循 match/match_service.py HMAC 双向签名契约的真实 gRPC MatchInternal 服务。"""

    def __init__(self, secret: bytes, clock: FakeClock) -> None:
        self.secret = secret
        self.clock = clock
        self.occupancies: dict[int, set[int]] = {}  # owner_id -> set(player_ids)
        self.started_battles: dict[int, int] = {}  # request_id -> battle_id
        self.next_battle_id: int = 80001
        self.completions: dict[int, tuple[int, bytes]] = {}  # battle_id -> (decision, raw_payload)
        self.calls: list[str] = []

        # 可编程注入开关
        self.force_start_reject: int = ACTIVITY_BATTLE_REJECT_NONE
        self.tamper_cancel_signature: bool = False
        self.tamper_cancel_digest: bool = False

    def _verify(self, path: str, request) -> bool:
        return verify_service_call(path, request, "guild", self.clock(), self.secret)

    async def acquire_trial_occupancy(self, request, context, path: str):
        self.calls.append("AcquireTrialOccupancy")
        resp = PROTOCOL.message("match.AcquireTrialOccupancyResponse")
        if not self._verify(path, request):
            resp.reject = 3
            return resp
        owner_id = int(request.owner_id)
        pids = {int(r.player_id) for r in request.roster}
        for other_owner, held in self.occupancies.items():
            if other_owner != owner_id:
                conflict = held & pids
                if conflict:
                    resp.reject = 1
                    resp.offender_player_id = min(conflict)
                    return resp
        self.occupancies[owner_id] = pids
        resp.reject = TRIAL_OCCUPANCY_REJECT_NONE
        return resp

    async def release_trial_occupancy(self, request, context, path: str):
        self.calls.append("ReleaseTrialOccupancy")
        resp = PROTOCOL.message("match.ReleaseTrialOccupancyResponse")
        if not self._verify(path, request):
            resp.reject = 3
            return resp
        owner_id = int(request.owner_id)
        held = self.occupancies.get(owner_id)
        if held is not None:
            held -= {int(p) for p in request.player_ids}
            if not held:
                del self.occupancies[owner_id]
        resp.reject = TRIAL_OCCUPANCY_REJECT_NONE
        return resp

    async def start_activity_battle(self, request, context, path: str):
        self.calls.append("StartActivityBattle")
        resp = PROTOCOL.message("match.StartActivityBattleResponse")
        if not self._verify(path, request):
            resp.reject = 5
            return resp
        if self.force_start_reject != ACTIVITY_BATTLE_REJECT_NONE:
            resp.reject = self.force_start_reject
            resp.offender_player_id = int(request.member_player_ids[0]) if request.member_player_ids else 0
            return resp
        req_id = int(request.request_id)
        bid = self.started_battles.get(req_id)
        if bid is None:
            bid = self.next_battle_id
            self.next_battle_id += 1
            self.started_battles[req_id] = bid
        resp.reject = ACTIVITY_BATTLE_REJECT_NONE
        resp.battle_id = bid
        resp.formation_state = 2
        return resp

    async def cancel_activity_battle(self, request, context, path: str):
        self.calls.append("CancelActivityBattle")
        if not self._verify(path, request):
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "bad guild hmac")
        req_id = int(request.original.request_id)
        bid = self.started_battles.get(req_id, 0)
        reply = PROTOCOL.message(
            "match.CancelActivityBattleResponse",
            battle_id=bid,
            request_cancelled=(bid == 0),
            formation_state=5 if bid == 0 else 2,
            terminal_decision=0,
        )
        reply.request_digest = "0" * 64 if self.tamper_cancel_digest else service_digest(request)
        sign_secret = (b"X" * 32) if self.tamper_cancel_signature else self.secret
        sign_service_call(path + "/reply", reply, "match", self.clock(), sign_secret)
        return reply

    async def get_battle_completion(self, request, context, path: str):
        self.calls.append("GetBattleCompletion")
        resp = PROTOCOL.message("match.GetBattleCompletionResponse")
        if not self._verify(path, request):
            resp.reject = 3
            return resp
        bid = int(request.battle_id)
        entry = self.completions.get(bid)
        if entry is not None:
            resp.decision, resp.raw_payload = entry
        return resp

    def services(self) -> dict:
        return {
            "match.MatchInternal": {
                "AcquireTrialOccupancy": self.acquire_trial_occupancy,
                "ReleaseTrialOccupancy": self.release_trial_occupancy,
                "StartActivityBattle": self.start_activity_battle,
                "CancelActivityBattle": self.cancel_activity_battle,
                "GetBattleCompletion": self.get_battle_completion,
            }
        }


class EconomyTrialHarness:
    """全链路真实 gRPC 集成脚手架：Guild + Scene/DataService + MatchInternal。"""

    def __init__(self, *, asset_op_enabled: bool = True, trials_enabled: bool = True) -> None:
        self.clock = FakeClock()
        self.gate_key = Ed25519PrivateKey.generate()
        self.verifier = CallerVerifier((self.gate_key.public_key(),), allowed_callers=("gate",))
        self.home_zones = FakeHomeZones()
        self.online = FakeOnline()
        self.notifier = RecordingNotifier()
        self.scene_server = FakeSceneAssetServer(SECRET_ASSET_GUILD, self.clock)
        self.match_server = FakeMatchInternalServer(SECRET_MATCH_GUILD, self.clock)
        self.store = MemoryGuildStore(PROTOCOL, clock_ms=self.clock)
        self.rank_store = MemoryGuildRankStore(merged_zones=self.store.merged_zones, clock_ms=self.clock)
        self._asset_op_enabled = asset_op_enabled
        self._trials_enabled = trials_enabled
        self.stack = AsyncExitStack()

        self.guild_client: GrpcClient
        self.scene_client: GrpcClient
        self.match_client_grpc: GrpcClient
        self.asset_loop: GuildAssetOpLoop
        self.trials: GuildTrialCoordinator
        self.service: GuildService

    async def __aenter__(self) -> "EconomyTrialHarness":
        await self.stack.__aenter__()

        scene_srv, scene_port = await serve_grpc(
            PROTOCOL, "127.0.0.1:0", self.scene_server.services(), pass_context=True
        )
        self.stack.push_async_callback(scene_srv.stop, 0)
        scene_endpoint = f"127.0.0.1:{scene_port}"
        self.scene_client = GrpcClient(scene_endpoint, timeout=3.0, enable_retries=False)
        self.stack.push_async_callback(self.scene_client.close)

        match_srv, match_port = await serve_grpc(
            PROTOCOL, "127.0.0.1:0", self.match_server.services(), pass_context=True
        )
        self.stack.push_async_callback(match_srv.stop, 0)
        self.match_client_grpc = GrpcClient(f"127.0.0.1:{match_port}", timeout=3.0, enable_retries=False)
        self.stack.push_async_callback(self.match_client_grpc.close)

        resolver = DirectSceneResolver(scene_endpoint, self.scene_client, self.scene_server.online)
        caller = GuildAssetOpCaller(
            PROTOCOL,
            resolver,
            SECRET_ASSET_GUILD,
            requery_delays_sec=(0.0,),
            clock_ms=self.clock,
        )
        ledger_reader = GrpcDataServiceLedgerReader(PROTOCOL, self.scene_client.call)
        self.asset_loop = GuildAssetOpLoop(
            PROTOCOL,
            self.store,
            caller,
            ledger_reader,
            lease_ms=10_000,
            base_backoff_ms=1_000,
            max_backoff_ms=5_000,
            await_durable_delay_ms=100,
            clock_ms=self.clock,
            rnd=lambda: 0.5,
        )

        match_client = MatchInternalClient(
            PROTOCOL, self.match_client_grpc.call, SECRET_MATCH_GUILD, clock_ms=self.clock
        )
        self.trials = GuildTrialCoordinator(
            PROTOCOL,
            self.store,
            match_client,
            online_resolver=self.online,
            notifier=self.notifier,
            asset_loop=self.asset_loop if self._asset_op_enabled else None,
            clock_ms=self.clock,
        )

        self.service = GuildService(
            PROTOCOL,
            self.store,
            rank_store=self.rank_store,
            caller_verifier=self.verifier,
            coordinator_secret=SECRET_COORDINATOR,
            home_zones=self.home_zones,
            online_resolver=self.online,
            notifier=self.notifier,
            asset_loop=self.asset_loop if self._asset_op_enabled else None,
            trials=self.trials if self._trials_enabled else None,
            asset_op_enabled=self._asset_op_enabled,
            trials_enabled=self._trials_enabled,
            clock_ms=self.clock,
        )

        guild_srv, guild_port = await serve_grpc(
            PROTOCOL, "127.0.0.1:0", self.service.services(), pass_context=True
        )
        self.stack.push_async_callback(guild_srv.stop, 0)
        self.guild_client = GrpcClient(f"127.0.0.1:{guild_port}", timeout=3.0, enable_retries=False)
        self.stack.push_async_callback(self.guild_client.close)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stack.__aexit__(exc_type, exc, tb)

    def add_player(self, player_id: int, *, zone_id: int = 1, online: bool = True) -> None:
        self.home_zones.zones[player_id] = zone_id
        if online:
            self.online.online.add(player_id)
            self.scene_server.online.add(player_id)

    async def call_guild(self, method: str, req_type: str, resp_type: str, player_id: int, **fields):
        path = f"/{SERVICE_GUILD}/{method}"
        subject = PROTOCOL.message(
            "SessionDetails",
            player_id=player_id,
            authenticated_account=f"acc_{player_id}",
            owner_revision=1,
            session_id=player_id * 10,
            gate_node_id=1,
            gate_instance_id="gate-1",
            session_incarnation=1,
        ).SerializeToString(deterministic=True)
        md = session_metadata(subject, path, self.gate_key)
        req = PROTOCOL.message(req_type, **fields)
        raw = await self.guild_client.call(path, req.SerializeToString(deterministic=True), metadata=md)
        return PROTOCOL.parse(resp_type, raw)

    async def call_internal(self, method: str, req_type: str, resp_type: str, **fields):
        path = f"/{SERVICE_GUILD_INTERNAL}/{method}"
        req = PROTOCOL.message(req_type, **fields)
        raw = await self.guild_client.call(path, req.SerializeToString(deterministic=True))
        return PROTOCOL.parse(resp_type, raw)

    async def call_role_fence(self, method: str, req_type: str, resp_type: str, **fields):
        path = f"/{SERVICE_ROLE_FENCE}/{method}"
        req = PROTOCOL.message(req_type, **fields)
        sign_service_call(path, req, "coordinator", self.clock(), SECRET_COORDINATOR)
        raw = await self.guild_client.call(path, req.SerializeToString(deterministic=True))
        return PROTOCOL.parse(resp_type, raw)

    async def create_guild_with_members(self, name: str, leader_id: int, member_ids: list[int]) -> int:
        self.add_player(leader_id)
        resp = await self.call_guild(
            "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", leader_id, name=name
        )
        assert resp.error_message.id == ERR_OK
        gid = int(resp.guild.guild_id)
        for pid in member_ids:
            self.add_player(pid)
            ar = await self.call_guild(
                "ApplyJoinGuild", "guildpb.ApplyJoinGuildRequest", "guildpb.ApplyJoinGuildResponse", pid, guild_id=gid
            )
            assert ar.error_message.id == ERR_OK
            rv = await self.call_guild(
                "ReviewGuildApplication",
                "guildpb.ReviewGuildApplicationRequest",
                "guildpb.ReviewGuildApplicationResponse",
                leader_id,
                applicant_player_id=pid,
                approve=True,
            )
            assert rv.error_message.id == ERR_OK
        return gid


@_async_test
async def test_donate_idempotency_scene_debit_ledger_proof_and_rejection_refund():
    async with EconomyTrialHarness(asset_op_enabled=True, trials_enabled=False) as h:
        gid = await h.create_guild_with_members("玄冥捐献堂", 1001, [])
        donate_cfg = next(iter(h.store.catalog.sorted_donates()))
        h.scene_server.set_currency(1001, donate_cfg.currency_type, donate_cfg.cost_amount * 5)

        # 1. 正常捐献：同步经 Scene AssetDebit 签名扣款 + DataService SQL 账本核验 -> 终结 APPLIED -> 资金与帮贡到账
        r1 = await h.call_guild(
            "DonateToGuild",
            "guildpb.DonateToGuildRequest",
            "guildpb.DonateToGuildResponse",
            1001,
            donate_id=donate_cfg.donate_id,
            client_request_id=9001,
            expected_guild_id=gid,
        )
        assert r1.error_message.id == ERR_OK
        assert r1.donation.status == ORDER_STATUS_APPLIED
        assert r1.guild.funds == donate_cfg.funds_gain
        contrib = await h.store.get_member_contribution(gid, 1001)
        assert contrib == (donate_cfg.contribution_gain, donate_cfg.contribution_gain)
        assert h.scene_server.currencies[(1001, donate_cfg.currency_type)] == donate_cfg.cost_amount * 4

        # 2. 相同 client_request_id 幂等重放：不重复扣款、不重复发资金/帮贡
        r1_replay = await h.call_guild(
            "DonateToGuild",
            "guildpb.DonateToGuildRequest",
            "guildpb.DonateToGuildResponse",
            1001,
            donate_id=donate_cfg.donate_id,
            client_request_id=9001,
            expected_guild_id=gid,
        )
        assert r1_replay.error_message.id == ERR_OK
        assert r1_replay.donation.op_id == r1.donation.op_id
        assert r1_replay.donation.status == ORDER_STATUS_APPLIED
        assert h.scene_server.currencies[(1001, donate_cfg.currency_type)] == donate_cfg.cost_amount * 4

        # 同 client_request_id 换参数 -> ERR_INVALID_PARAMETER
        r1_bad = await h.call_guild(
            "DonateToGuild",
            "guildpb.DonateToGuildRequest",
            "guildpb.DonateToGuildResponse",
            1001,
            donate_id=donate_cfg.donate_id + 1,
            client_request_id=9001,
            expected_guild_id=gid,
        )
        assert r1_bad.error_message.id == ERR_INVALID_PARAMETER

        # 3. 货币不足被 Scene 拒绝（27000）：返回 ERR_GUILD_CURRENCY_INSUFFICIENT 且退回每日捐献次数
        h.scene_server.set_currency(1001, donate_cfg.currency_type, 0)
        usage_before = await h.store.get_counter_usage(1001, 1, (day_key(h.clock()),))
        r_rej = await h.call_guild(
            "DonateToGuild",
            "guildpb.DonateToGuildRequest",
            "guildpb.DonateToGuildResponse",
            1001,
            donate_id=donate_cfg.donate_id,
            client_request_id=9002,
            expected_guild_id=gid,
        )
        assert r_rej.error_message.id == ERR_GUILD_CURRENCY_INSUFFICIENT
        assert r_rej.donation.status == ORDER_STATUS_REJECTED
        usage_after = await h.store.get_counter_usage(1001, 1, (day_key(h.clock()),))
        assert usage_after == usage_before

        # 4. 超期走 AssetAbortDebit 终止并退回计数器
        h.scene_server.online.discard(1001)  # 模拟离线，首次投递挂起 PENDING
        r_off = await h.call_guild(
            "DonateToGuild",
            "guildpb.DonateToGuildRequest",
            "guildpb.DonateToGuildResponse",
            1001,
            donate_id=donate_cfg.donate_id,
            client_request_id=9003,
            expected_guild_id=gid,
        )
        assert r_off.error_message.id == ERR_OK
        assert r_off.donation.status == ORDER_STATUS_PENDING
        abort_op_id = int(r_off.donation.op_id)

        # 推进时钟超过 deadline 后玩家重新上线，后台 tick 发送 AssetAbortDebit -> OP_STATUS_ABORTED 并退计数
        h.clock.advance((h.store.catalog.rule.asset_op_deadline_seconds + 10) * 1000)
        h.scene_server.online.add(1001)
        processed = await h.asset_loop.tick()
        assert processed == 1
        aborted = await h.store.get_asset_op(abort_op_id)
        assert aborted is not None and aborted.status == OP_STATUS_ABORTED
        assert any(call[0] == "abort_debit" for call in h.scene_server.calls)
        usage_after_abort = await h.store.get_counter_usage(1001, 1, (day_key(h.clock() - (h.store.catalog.rule.asset_op_deadline_seconds + 10) * 1000),))
        assert usage_after_abort == usage_before

    # 5. 显式关闭 asset_op_enabled=False 时仍返回 kGuildAssetPending (14026)
    async with EconomyTrialHarness(asset_op_enabled=False, trials_enabled=False) as h_off:
        gid = await h_off.create_guild_with_members("禁用通道帮", 1099, [])
        donate_cfg = next(iter(h_off.store.catalog.sorted_donates()))
        r_disabled = await h_off.call_guild(
            "DonateToGuild",
            "guildpb.DonateToGuildRequest",
            "guildpb.DonateToGuildResponse",
            1099,
            donate_id=donate_cfg.donate_id,
            client_request_id=1,
            expected_guild_id=gid,
        )
        assert r_disabled.error_message.id == ERR_GUILD_ASSET_PENDING


@_async_test
async def test_buy_shop_goods_credit_rejection_refund_and_sticky_partial_27007():
    async with EconomyTrialHarness(asset_op_enabled=True, trials_enabled=False) as h:
        gid = await h.create_guild_with_members("玄冥商行", 2001, [])
        goods = next(g for g in h.store.catalog.sorted_shops() if g.required_guild_level <= 1)
        init_balance = goods.cost_contribution * 10
        h.store.set_member_contribution_for_test(gid, 2001, init_balance, init_balance)

        # 1. 正常购买：预扣帮贡 -> Scene AssetCredit (tx_type=25) -> SQL 账本核验 -> APPLIED
        r_ok = await h.call_guild(
            "BuyGuildShopGoods",
            "guildpb.BuyGuildShopGoodsRequest",
            "guildpb.BuyGuildShopGoodsResponse",
            2001,
            goods_id=goods.goods_id,
            count=2,
            client_request_id=3001,
            expected_guild_id=gid,
        )
        assert r_ok.error_message.id == ERR_OK
        assert r_ok.order.status == ORDER_STATUS_APPLIED
        assert r_ok.contribution_balance == init_balance - goods.cost_contribution * 2
        assert h.scene_server.items[(2001, goods.item_id)] == goods.item_count * 2

        # 超出单次最大购买上限（min(20, max_stack // item_count)）被拒绝
        r_cap = await h.call_guild(
            "BuyGuildShopGoods",
            "guildpb.BuyGuildShopGoodsRequest",
            "guildpb.BuyGuildShopGoodsResponse",
            2001,
            goods_id=goods.goods_id,
            count=21,
            client_request_id=3002,
            expected_guild_id=gid,
        )
        assert r_cap.error_message.id == ERR_GUILD_SHOP_LIMIT

        # 2. Scene 拒绝发奖：自动退还帮贡余额与限购计数
        bal_before_rej = r_ok.contribution_balance
        h.scene_server.force_credit_reject_reason = 27004
        r_rej = await h.call_guild(
            "BuyGuildShopGoods",
            "guildpb.BuyGuildShopGoodsRequest",
            "guildpb.BuyGuildShopGoodsResponse",
            2001,
            goods_id=goods.goods_id,
            count=1,
            client_request_id=3003,
            expected_guild_id=gid,
        )
        h.scene_server.force_credit_reject_reason = 0
        assert r_rej.error_message.id == ERR_GUILD_ASSET_REJECTED
        assert r_rej.order.status == ORDER_STATUS_REJECTED
        assert r_rej.contribution_balance == bal_before_rej

        # 3. 粘性 27007 部分发放：先延迟 SQL 提交（校验重排保留 last_reason=27007），再落盘终结为 APPLIED_PARTIAL 且不退帮贡
        h.scene_server.force_credit_partial = True
        h.scene_server.delay_sql_commit = True
        r_part = await h.call_guild(
            "BuyGuildShopGoods",
            "guildpb.BuyGuildShopGoodsRequest",
            "guildpb.BuyGuildShopGoodsResponse",
            2001,
            goods_id=goods.goods_id,
            count=1,
            client_request_id=3004,
            expected_guild_id=gid,
        )
        assert r_part.error_message.id == ERR_OK
        assert r_part.order.status == ORDER_STATUS_PENDING
        part_op = await h.store.get_asset_op(int(r_part.order.op_id))
        assert part_op is not None
        assert part_op.last_reason == ERR_ASSET_PARTIAL_APPLIED

        # 落盘 SQL 账本并取消 partial 标志（测试粘性 27007），后台 tick 推进终结为 APPLIED_PARTIAL
        h.scene_server.delay_sql_commit = False
        h.scene_server.force_credit_partial = False
        h.scene_server.flush_sql_ledgers()
        h.clock.advance(500)
        assert await h.asset_loop.tick() == 1
        finalized_part = await h.store.get_asset_op(int(r_part.order.op_id))
        assert finalized_part is not None
        assert finalized_part.status == OP_STATUS_APPLIED_PARTIAL
        # APPLIED_PARTIAL 绝不做对侧退款
        contrib_after_part = await h.store.get_member_contribution(gid, 2001)
        assert contrib_after_part is not None
        assert contrib_after_part[1] == bal_before_rej - goods.cost_contribution

        # 4. ListAppliedAssetOpsSince 返回 APPLIED 与 APPLIED_PARTIAL（排除 REJECTED）
        since_ms = h.clock() - 60_000
        applied_resp = await h.call_internal(
            "ListAppliedAssetOpsSince",
            "guildpb.ListAppliedAssetOpsSinceRequest",
            "guildpb.ListAppliedAssetOpsSinceResponse",
            zone_id=1,
            player_ids=[2001],
            since_ms=since_ms,
            limit=10,
        )
        op_statuses = {int(o.op_id): int(o.status) for o in applied_resp.ops}
        assert op_statuses == {
            int(r_ok.order.op_id): OP_STATUS_APPLIED,
            int(r_part.order.op_id): OP_STATUS_APPLIED_PARTIAL,
        }


@_async_test
async def test_guild_trial_full_lifecycle_win_settlement_owed_overflow_and_fallback_sweep():
    async with EconomyTrialHarness(asset_op_enabled=True, trials_enabled=True) as h:
        # 成员入会需满 activity_join_min_hours（推进时钟 48h 满足条件）
        gid = await h.create_guild_with_members("同道历练宗", 3001, [3002, 3003])
        h.clock.advance(48 * MS_PER_HOUR)

        trial_row = next(r for r in h.store.catalog.activities if r.activity_type == ACTIVITY_TYPE_TRIAL)
        # 实表默认关闭日常帮会历练（反上班化）；本用例显式启用内存行以继续覆盖历练状态机与对账。
        trial_row = replace(trial_row, enabled=True)
        h.store.catalog = replace(
            h.store.catalog,
            activities=tuple(trial_row if r.activity_id == trial_row.activity_id else r for r in h.store.catalog.activities),
        )

        # 1. StartGuildTrial -> AcquireTrialOccupancy -> SQL lobby PENDING
        st = await h.call_guild(
            "StartGuildTrial",
            "guildpb.StartGuildTrialRequest",
            "guildpb.StartGuildTrialResponse",
            3001,
            activity_id=trial_row.activity_id,
            member_player_ids=[3001, 3002, 3003],
        )
        assert st.error_message.id == ERR_OK
        assert st.activity.trial_lobby.state == TRIAL_LOBBY_STATE_PENDING
        lobby_id = int(st.activity.trial_lobby.lobby_id)
        assert lobby_id > 0
        assert lobby_id in h.match_server.occupancies
        assert h.match_server.occupancies[lobby_id] == {3001, 3002, 3003}

        # 2. 队友 3002 同意（仍在等待 3003）
        r2 = await h.call_guild(
            "RespondGuildTrialInvite",
            "guildpb.RespondGuildTrialInviteRequest",
            "guildpb.RespondGuildTrialInviteResponse",
            3002,
            lobby_id=lobby_id,
            accept=True,
        )
        assert r2.error_message.id == ERR_OK
        assert r2.activity.trial_lobby.state == TRIAL_LOBBY_STATE_PENDING
        assert set(r2.activity.trial_lobby.accepted_player_ids) == {3001, 3002}

        # 3. 队友 3003 同意 -> 齐人发战 -> StartActivityBattle -> 登记 STARTED battle & LAUNCHED lobby
        r3 = await h.call_guild(
            "RespondGuildTrialInvite",
            "guildpb.RespondGuildTrialInviteRequest",
            "guildpb.RespondGuildTrialInviteResponse",
            3003,
            lobby_id=lobby_id,
            accept=True,
        )
        assert r3.error_message.id == ERR_OK
        assert r3.activity.trial_lobby.state == TRIAL_LOBBY_STATE_LAUNCHED
        battle_id = int(r3.activity.trial_lobby.battle_id)
        assert battle_id > 0
        battle_rec = await h.store.get_trial_battle(battle_id)
        assert battle_rec is not None and battle_rec.state == TRIAL_BATTLE_STATE_STARTED

        # 给 3002 预填满 16 条 GUILD_CREDIT pending 槽位，制造 guild_trial_reward_owed 溢出场景
        _, epoch_3002 = await h.store._run_tx(
            lambda tx: tx.ensure_and_lock_seq(3002, ASSET_STREAM_GUILD_CREDIT, h.clock())
        )
        filler_op_ids: list[int] = []
        for idx in range(ASSET_OP_MAX_PENDING):
            oid = await h.store.allocate_op_id()
            seq, ep = await h.store._run_tx(lambda tx: tx.allocate_seq(3002, ASSET_STREAM_GUILD_CREDIT, h.clock()))
            filler_op_ids.append(oid)
            h.store.seed_asset_op_for_test(
                AssetOpRow(
                    op_id=oid,
                    player_id=3002,
                    stream=ASSET_STREAM_GUILD_CREDIT,
                    seq=seq,
                    guild_id=gid,
                    kind=OP_KIND_SHOP,
                    status=OP_STATUS_PENDING,
                    durable=0,
                    attempts=0,
                    next_attempt_ms=h.clock() + 60_000,
                    deadline_ms=0,
                    payload=b"",
                    ref_id=1,
                    ref_count=1,
                    period_key=day_key(h.clock()),
                    contribution_delta=0,
                    funds_delta=0,
                    reason_tip_id=0,
                    created_ms=h.clock(),
                    updated_ms=h.clock(),
                    lease_until_ms=0,
                    lease_token=0,
                    tx_type=25,
                    last_outcome=0,
                    last_reason=0,
                    stream_epoch=ep,
                )
            )

        # 4. 消费 BattleResultEvent (WIN)：3001 存活、3002 阵亡（仍领奖且溢出 owed）、3003 逃跑（排除领奖）
        ev = PROTOCOL.message(
            "contracts.kafka.BattleResultEvent",
            battle_id=battle_id,
            outcome=BATTLE_OUTCOME_SIDE_A_WIN,
            winner_team_index=0,
            finished_at_ms=h.clock(),
            fled_player_ids=[3003],
            dead_player_ids=[3002],
        )
        ev.teams.add(team_index=0, player_ids=[3001, 3002, 3003])
        ev.activity_context.kind = BATTLE_ACTIVITY_KIND_GUILD_TRIAL
        ev.activity_context.guild_id = gid
        ev.activity_context.activity_id = trial_row.activity_id
        ev.activity_context.period_key = battle_rec.period_key
        ev.activity_context.guild_period_key = battle_rec.guild_period_key
        ev.activity_context.initiator_player_id = 3001
        ev.activity_context.launch_request_id = lobby_id

        settle_res = await h.service.handle_battle_result_event(ev)
        assert settle_res is not None
        assert settle_res.status == "settled"
        assert settle_res.result == TRIAL_SETTLE_RESULT_WIN
        assert set(settle_res.rewarded) == {3001, 3002}
        assert settle_res.funds_granted is True
        assert settle_res.owed == (3002,)
        # 占用已释放、launch 终结、大厅关闭
        assert lobby_id not in h.match_server.occupancies
        settled_b = await h.store.get_trial_battle(battle_id)
        assert settled_b is not None and settled_b.state == TRIAL_BATTLE_STATE_SETTLED
        launch_after = await h.store.get_trial_launch(lobby_id)
        assert launch_after is not None and launch_after.state == TRIAL_LAUNCH_STATE_TERMINAL

        # 3001 的道具奖励已同步经 Scene AssetCredit (tx_type=26) 发放；3003 未获得帮贡
        guild_after = await h.store.get_guild(gid)
        assert guild_after is not None and guild_after.funds == trial_row.guild_funds
        c1 = await h.store.get_member_contribution(gid, 3001)
        c2 = await h.store.get_member_contribution(gid, 3002)
        c3 = await h.store.get_member_contribution(gid, 3003)
        assert c1 == (trial_row.personal_contribution, trial_row.personal_contribution)
        assert c2 == (trial_row.personal_contribution, trial_row.personal_contribution)
        assert c3 == (0, 0)
        for item_id, cnt in h.store.catalog.reward_items(trial_row.reward_id):
            assert h.scene_server.items[(3001, item_id)] == cnt

        # 5. 清空 3002 挂起槽后，convert_owed_rewards_once 将 owed 转为 asset_op 并经 Scene 发放
        for oid in filler_op_ids:
            h.store.state.asset_ops[oid] = replace(h.store.state.asset_ops[oid], status=OP_STATUS_APPLIED)
        assert await h.trials.convert_owed_rewards_once() == 1
        assert await h.store.list_trial_rewards_owed() == []
        for item_id, cnt in h.store.catalog.reward_items(trial_row.reward_id):
            assert h.scene_server.items[(3002, item_id)] == cnt

        # 6. 兜底对账：GetBattleCompletion 补结算超时未收 Kafka 事件的 STARTED 对局
        overdue_bid = 90001
        await h.store.register_trial_battle(
            TrialBattleKey(
                battle_id=overdue_bid,
                guild_id=gid,
                activity_id=trial_row.activity_id,
                period_key=day_key(h.clock()),
                guild_period_key=guild_period_key(trial_row, h.clock()),
                initiator_player_id=3001,
                launch_request_id=0,
            ),
            h.clock(),
        )
        ev2 = PROTOCOL.message(
            "contracts.kafka.BattleResultEvent",
            battle_id=overdue_bid,
            outcome=BATTLE_OUTCOME_SIDE_A_WIN,
            winner_team_index=0,
            finished_at_ms=h.clock(),
        )
        ev2.teams.add(team_index=0, player_ids=[3001, 3003])
        ev2.activity_context.kind = BATTLE_ACTIVITY_KIND_GUILD_TRIAL
        ev2.activity_context.guild_id = gid
        ev2.activity_context.activity_id = trial_row.activity_id
        ev2.activity_context.period_key = day_key(h.clock())
        ev2.activity_context.guild_period_key = guild_period_key(trial_row, h.clock())
        ev2.activity_context.initiator_player_id = 3001
        h.match_server.completions[overdue_bid] = (
            OUTCOME_DECISION_COMPLETE,
            ev2.SerializeToString(deterministic=True),
        )
        h.clock.advance(400_000)
        assert await h.trials.sweep_overdue_battles_once(overdue_ms=300_000) == 1
        overdue_after = await h.store.get_trial_battle(overdue_bid)
        assert overdue_after is not None and overdue_after.state == TRIAL_BATTLE_STATE_SETTLED
        assert (await h.store.get_member_contribution(gid, 3003))[0] == trial_row.personal_contribution


@_async_test
async def test_guild_trial_cancel_activity_battle_and_reply_signature_verification():
    async with EconomyTrialHarness(asset_op_enabled=True, trials_enabled=True) as h:
        gid = await h.create_guild_with_members("取消验签堂", 4001, [4002])
        h.clock.advance(48 * MS_PER_HOUR)
        trial_row = next(r for r in h.store.catalog.activities if r.activity_type == ACTIVITY_TYPE_TRIAL)
        # 实表默认关闭日常帮会历练（反上班化）；本用例显式启用内存行以继续覆盖历练状态机与对账。
        trial_row = replace(trial_row, enabled=True)
        h.store.catalog = replace(
            h.store.catalog,
            activities=tuple(trial_row if r.activity_id == trial_row.activity_id else r for r in h.store.catalog.activities),
        )

        # 1. 发起历练并模拟 StartActivityBattle 被拒 + CancelActivityBattle 回包签名被篡改（fail-closed）
        st = await h.call_guild(
            "StartGuildTrial",
            "guildpb.StartGuildTrialRequest",
            "guildpb.StartGuildTrialResponse",
            4001,
            activity_id=trial_row.activity_id,
            member_player_ids=[4001, 4002],
        )
        assert st.error_message.id == ERR_OK
        lobby_id = int(st.activity.trial_lobby.lobby_id)

        h.match_server.force_start_reject = ACTIVITY_BATTLE_REJECT_MEMBER_IN_BATTLE
        h.match_server.tamper_cancel_signature = True
        r_fail = await h.call_guild(
            "RespondGuildTrialInvite",
            "guildpb.RespondGuildTrialInviteRequest",
            "guildpb.RespondGuildTrialInviteResponse",
            4002,
            lobby_id=lobby_id,
            accept=True,
        )
        assert r_fail.error_message.id == ERR_GUILD_TRIAL_TEAM_INVALID
        # 签名篡改被拒 -> launch 仍保持 PREPARED（绝不无证释放）
        launch_stuck = await h.store.get_trial_launch(lobby_id)
        assert launch_stuck is not None and launch_stuck.state == TRIAL_LAUNCH_STATE_PREPARED
        assert lobby_id in h.match_server.occupancies

        # 2. 篡改 request_digest 同样被 fail-closed 拒绝
        h.match_server.tamper_cancel_signature = False
        h.match_server.tamper_cancel_digest = True
        h.clock.advance(15_000)
        await h.trials.recover_launches_once()
        launch_still_stuck = await h.store.get_trial_launch(lobby_id)
        assert launch_still_stuck is not None and launch_still_stuck.state == TRIAL_LAUNCH_STATE_PREPARED

        # 3. 恢复正常 "match" 签名与摘要 -> recover_launches_once 凭终态回执释放 launch、角色与占用
        h.match_server.tamper_cancel_digest = False
        h.clock.advance(60_000)
        assert await h.trials.recover_launches_once() == 1
        launch_done = await h.store.get_trial_launch(lobby_id)
        assert launch_done is not None and launch_done.state == TRIAL_LAUNCH_STATE_TERMINAL
        assert lobby_id not in h.match_server.occupancies
        lobby_done = await h.store.get_trial_lobby(lobby_id)
        assert lobby_done is not None and lobby_done.state == TRIAL_LOBBY_STATE_ENDED

        # 4. 邀请拒绝路径：释放占用并立即关闭大厅
        h.match_server.force_start_reject = ACTIVITY_BATTLE_REJECT_NONE
        h.clock.advance(30_000)
        st2 = await h.call_guild(
            "StartGuildTrial",
            "guildpb.StartGuildTrialRequest",
            "guildpb.StartGuildTrialResponse",
            4001,
            activity_id=trial_row.activity_id,
            member_player_ids=[4001, 4002],
        )
        assert st2.error_message.id == ERR_OK
        lobby_id_2 = int(st2.activity.trial_lobby.lobby_id)
        assert lobby_id_2 in h.match_server.occupancies
        r_decl = await h.call_guild(
            "RespondGuildTrialInvite",
            "guildpb.RespondGuildTrialInviteRequest",
            "guildpb.RespondGuildTrialInviteResponse",
            4002,
            lobby_id=lobby_id_2,
            accept=False,
        )
        assert r_decl.error_message.id == ERR_OK
        assert lobby_id_2 not in h.match_server.occupancies
        lob2 = await h.store.get_trial_lobby(lobby_id_2)
        assert lob2 is not None and lob2.state == TRIAL_LOBBY_STATE_ENDED


@_async_test
async def test_role_fence_prepare_freeze_blocks_all_four_pending_blockers():
    async with EconomyTrialHarness(asset_op_enabled=True, trials_enabled=True) as h:
        pid = 5001
        tok = removal_token(1)

        async def _prepare(ver: int):
            state = PROTOCOL.message(
                "rolefence.RoleFenceState",
                player_id=pid,
                account=f"acc_{pid}",
                owner_revision=1,
                freeze_token=tok,
                freeze_final=False,
                version=ver,
                home_zone=1,
            )
            return await h.call_role_fence(
                "PrepareRoleFreeze",
                "rolefence.PrepareRoleFreezeRequest",
                "rolefence.PrepareRoleFreezeResponse",
                state=state,
                dry_run=False,
            )

        # 1. 非终态 guild_asset_op（PENDING 与 APPLIED_PARTIAL 均阻断）
        op_row = AssetOpRow(
            op_id=70001,
            player_id=pid,
            stream=ASSET_STREAM_GUILD_DEBIT,
            seq=1,
            guild_id=100,
            kind=OP_KIND_DONATE,
            status=OP_STATUS_PENDING,
            durable=0,
            attempts=0,
            next_attempt_ms=0,
            deadline_ms=0,
            payload=b"",
            ref_id=1,
            ref_count=1,
            period_key=20261123,
            contribution_delta=10,
            funds_delta=10,
            reason_tip_id=0,
            created_ms=h.clock(),
            updated_ms=h.clock(),
            lease_until_ms=0,
            lease_token=0,
            tx_type=24,
            last_outcome=0,
            last_reason=0,
            stream_epoch=1,
        )
        h.store.seed_asset_op_for_test(op_row)
        r1 = await _prepare(1)
        assert r1.clean is False and r1.blocker == ROLE_BLOCKER_ASSET_OP

        h.store.seed_asset_op_for_test(replace(op_row, status=OP_STATUS_APPLIED_PARTIAL))
        r1b = await _prepare(2)
        assert r1b.clean is False and r1b.blocker == ROLE_BLOCKER_ASSET_OP
        h.store.seed_asset_op_for_test(replace(op_row, status=OP_STATUS_APPLIED))

        # 2. guild_trial_reward_owed 阻断
        h.store.state.trial_reward_owed[(pid, 888)] = TrialOwedRewardRecord(
            player_id=pid,
            battle_id=888,
            guild_id=100,
            activity_id=3,
            period_key=20261123,
            payload=b"\x01",
            attempts=0,
            created_ms=h.clock(),
        )
        r2 = await _prepare(3)
        assert r2.clean is False and r2.blocker == ROLE_BLOCKER_TRIAL_REWARD_OWED
        del h.store.state.trial_reward_owed[(pid, 888)]

        # 3. 活跃 guild_trial_launch_role 阻断
        h.store.state.trial_launches[999] = TrialLaunchRecord(
            request_id=999,
            battle_id=0,
            state=TRIAL_LAUNCH_STATE_PREPARED,
            original_proto=b"",
            request_digest=b"",
            created_ms=h.clock(),
            updated_ms=h.clock(),
            cancel_receipt=b"",
            next_check_ms=h.clock(),
            check_attempts=0,
            realtime_end_tip=b"",
        )
        h.store.state.trial_launch_roles[(pid, 999)] = TrialLaunchRoleRecord(
            player_id=pid,
            request_id=999,
            account=f"acc_{pid}",
            owner_revision=1,
            state=TRIAL_LAUNCH_ROLE_ACTIVE,
        )
        r3 = await _prepare(4)
        assert r3.clean is False and r3.blocker == ROLE_BLOCKER_TRIAL_LAUNCH_ROLE
        h.store.state.trial_launches[999] = replace(
            h.store.state.trial_launches[999], state=TRIAL_LAUNCH_STATE_TERMINAL
        )
        h.store.state.trial_launch_roles[(pid, 999)] = replace(
            h.store.state.trial_launch_roles[(pid, 999)], state=TRIAL_LAUNCH_ROLE_RELEASED
        )

        # 4. 活跃 guild_trial_lobby_slot 阻断
        h.store.state.trial_lobbies[777] = TrialLobbyRecord(
            lobby_id=777,
            guild_id=100,
            activity_id=3,
            initiator_player_id=pid,
            state=TRIAL_LOBBY_STATE_PENDING,
            view_proto=b"",
            original_proto=b"",
            consent_proto=b"",
            expires_ms=h.clock() + 30_000,
            launch_deadline_ms=0,
            battle_id=0,
            created_ms=h.clock(),
            updated_ms=h.clock(),
        )
        h.store.state.trial_lobby_slots[pid] = TrialLobbySlotRecord(
            player_id=pid, lobby_id=777, cooldown_until_ms=0
        )
        r4 = await _prepare(5)
        assert r4.clean is False and r4.blocker == ROLE_BLOCKER_TRIAL_LOBBY_SLOT

        # 5. 全部终结/清理后放行 clean=True
        h.store.state.trial_lobbies[777] = replace(h.store.state.trial_lobbies[777], state=TRIAL_LOBBY_STATE_ENDED)
        r5 = await _prepare(6)
        assert r5.clean is True and r5.blocker == ""


def test_guild_config_requires_match_secret_when_trials_enabled():
    cfg = GuildConfig(
        advertise_host="127.0.0.1",
        data_service="127.0.0.1:19001",
        mysql_host="127.0.0.1",
        mysql_user="root",
        mysql_database="mmorpg_guild",
        coordinator_secret=SECRET_COORDINATOR,
        role_fence_secret=SECRET_COORDINATOR,
        asset_op_enabled=False,
        trials_enabled=True,
        match_service="127.0.0.1:19999",
        match_internal_secret=b"short",
    )
    try:
        cfg.validate()
        raise AssertionError("expected ValueError for short match_internal_secret")
    except ValueError as exc:
        assert MATCH_INTERNAL_SECRET_ENV in str(exc)


@_async_test
async def test_guild_trial_loss_settlement_and_daily_funded_win_cap():
    from xuanming.guild import REQUIRED_TABLES
    from xuanming.guild.model import TRIAL_SETTLE_RESULT_LOSS

    for tbl in (
        "guild_trial_lobby",
        "guild_trial_lobby_slot",
        "guild_trial_launch",
        "guild_trial_launch_role",
    ):
        assert tbl in REQUIRED_TABLES

    async with EconomyTrialHarness(asset_op_enabled=True, trials_enabled=True) as h:
        gid = await h.create_guild_with_members("封顶历练宗", 6001, [6002])
        h.clock.advance(48 * MS_PER_HOUR)
        trial_row = next(r for r in h.store.catalog.activities if r.activity_type == ACTIVITY_TYPE_TRIAL)
        # 实表默认关闭日常帮会历练（反上班化）；本用例显式启用内存行以继续覆盖历练状态机与对账。
        trial_row = replace(trial_row, enabled=True)
        h.store.catalog = replace(
            h.store.catalog,
            activities=tuple(trial_row if r.activity_id == trial_row.activity_id else r for r in h.store.catalog.activities),
        )
        assert trial_row.guild_threshold >= 1

        # 1. LOSS 结算：不发放资金与帮贡，但标记 SETTLED/LOSS
        loss_bid = 91001
        ev_loss = PROTOCOL.message(
            "contracts.kafka.BattleResultEvent",
            battle_id=loss_bid,
            outcome=2,  # BATTLE_OUTCOME_SIDE_B_WIN
            winner_team_index=1,
            finished_at_ms=h.clock(),
        )
        ev_loss.teams.add(team_index=0, player_ids=[6001, 6002])
        ev_loss.activity_context.kind = BATTLE_ACTIVITY_KIND_GUILD_TRIAL
        ev_loss.activity_context.guild_id = gid
        ev_loss.activity_context.activity_id = trial_row.activity_id
        ev_loss.activity_context.period_key = day_key(h.clock())
        ev_loss.activity_context.guild_period_key = guild_period_key(trial_row, h.clock())
        ev_loss.activity_context.initiator_player_id = 6001

        res_loss = await h.service.handle_battle_result_event(ev_loss)
        assert res_loss is not None
        assert res_loss.status == "settled"
        assert res_loss.result == TRIAL_SETTLE_RESULT_LOSS
        assert res_loss.funds_granted is False
        assert (await h.store.get_guild(gid)).funds == 0

        # 2. 连续获胜超过 guild_threshold 次：超出每日资助上限后不再增加帮会资金
        # 为测试多场获胜，准备多个帮会成员轮流领奖（避免单人 daily_limit 拦截）
        extra_members = list(range(6003, 6003 + trial_row.guild_threshold + 2))
        for pid in extra_members:
            h.add_player(pid)
            h.store.state.members_by_player[pid] = (
                gid,
                replace(h.store.state.members_by_player[6002][1], player_id=pid),
            )

        granted_count = 0
        for idx in range(trial_row.guild_threshold + 1):
            bid = 92000 + idx
            winner_pid = extra_members[idx]
            ev_win = PROTOCOL.message(
                "contracts.kafka.BattleResultEvent",
                battle_id=bid,
                outcome=BATTLE_OUTCOME_SIDE_A_WIN,
                winner_team_index=0,
                finished_at_ms=h.clock(),
            )
            ev_win.teams.add(team_index=0, player_ids=[winner_pid])
            ev_win.activity_context.kind = BATTLE_ACTIVITY_KIND_GUILD_TRIAL
            ev_win.activity_context.guild_id = gid
            ev_win.activity_context.activity_id = trial_row.activity_id
            ev_win.activity_context.period_key = day_key(h.clock())
            ev_win.activity_context.guild_period_key = guild_period_key(trial_row, h.clock())
            ev_win.activity_context.initiator_player_id = winner_pid
            res_win = await h.service.handle_battle_result_event(ev_win)
            assert res_win is not None and res_win.status == "settled"
            if res_win.funds_granted:
                granted_count += 1

        assert granted_count == trial_row.guild_threshold
        assert (await h.store.get_guild(gid)).funds == trial_row.guild_funds * trial_row.guild_threshold
