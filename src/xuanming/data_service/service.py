"""DataService gRPC 处理器：完整对齐 Go ``data_service`` 全部 49 个 RPC（含严格 fail-closed 调用方鉴权）。"""

import asyncio
import hmac
import logging
import time

import grpc

from ..grpc_runtime import service_digest, verify_service_call
from . import names
from .errors import (
    Conflict,
    Full,
    Invalid,
    Missing,
    NameConflict,
    NameInUse,
    NameOutsideWindow,
    NotCommitted,
    RecallBatchExists,
    RecallBatchNotFound,
    RecallOpNotFound,
    RecallSourceConsumed,
    RetryLater,
    ZoneConflict,
)
from .model import (
    DEFAULT_DELETION_COOLING_OFF_MS,
    DEFAULT_ID_STEP,
    DEFAULT_ID_TAGS,
    DELETION_OUTCOME_RETRY,
    ERR_ADMIN_AUTH,
    ERR_ID_EXHAUSTED,
    ERR_ID_SOURCE,
    ERR_ID_UNKNOWN_TAG,
    ERR_INVALID_REQUEST,
    ERR_NAME_CONFLICT,
    ERR_NAME_DB,
    ERR_NAME_IN_USE,
    ERR_RECALL_DISABLED,
    ERR_RECALL_EVIDENCE_REFUSED,
    ERR_RECALL_INCOMPLETE,
    ERR_RECALL_NOT_FOUND,
    ERR_RECALL_NOTHING_TO_RECALL,
    ERR_RECALL_OP_CHANGED,
    ERR_RECALL_PENDING,
    ERR_RECALL_PLAN_CHANGED,
    ERR_RECALL_REQUEST_CONFLICT,
    ERR_RESULT_TRUNCATED,
    ERR_ROUTE_STORE,
    ERR_SNAPSHOT_DB,
    ERR_ZONE_CONFLICT,
    FIRST_STORAGE_ID,
    MAX_APPEARANCE_ID_BYTES,
    MAX_DELETION_CLAIM,
    MAX_ID_STEP,
    NAME_BATCH_LIMIT,
    RECALL_KIND_CURRENCY,
    RECALL_OP_APPLIED,
    RECALL_OP_PARTIAL,
    RECALL_OP_PENDING,
    RECALL_OP_PLANNED,
    ROUTE_HOME_ZONE_MAX,
    STAGE1_TRADABLE_MATERIAL_LIFETIME_LIMIT,
    MergedZones,
    valid_admission,
    valid_instance,
    valid_save_floor,
    valid_tag,
    valid_trade_seal,
)
from .recall import (
    BATCH_RECALL_QUERY_LIMIT,
    MANUAL_FINAL_STATUSES,
    MAX_RECALL_PLAYERS,
    MAX_RECALL_SOURCE_ROWS,
    RECALL_OPERATOR_MAX_RUNES,
    RECALL_REASON_MAX_RUNES,
    VALID_REVIEW_REASONS,
    RecallFilter,
    RecallPlanTooLarge,
    build_recall_plan,
    distinct_gains,
    format_recall_evidence,
    judge_manual_resolve,
    manual_resolve_allows,
    recall_instruction_ids,
    recall_source_tx_ids,
    status_label,
    valid_recall_asset,
    valid_recall_text,
)

logger = logging.getLogger(__name__)

SERVICE = "data_service.DataService"
RELEASE_WINDOW_MS = 600_000  # config.PlayerName.ReleaseWindow 默认 10 分钟
ADMIN_TOKEN_KEY = "x-admin-token"
CODE = grpc.StatusCode


def _directory_status(error: Exception) -> tuple:
    """对齐 Go ``directoryError`` 状态映射。"""
    match error:
        case NotCommitted():
            return CODE.ABORTED, "账号角色未提交"
        case RetryLater():
            return CODE.ABORTED, "account directory operation unfinished, replay the same request"
        case Invalid():
            return CODE.INVALID_ARGUMENT, "账号目录参数非法"
        case Missing():
            return CODE.NOT_FOUND, "账号角色目录不存在"
        case Full():
            return CODE.RESOURCE_EXHAUSTED, "账号角色数量已达全局上限"
        case Conflict():
            return CODE.FAILED_PRECONDITION, "账号角色归属或身份已变化"
    logger.error("account directory SQL dependency failed", exc_info=error)
    return CODE.UNAVAILABLE, "全局账号目录暂不可用"


def _name_status(error: Exception) -> tuple:
    """对齐 Go ``playerNameStatus`` 状态映射。"""
    match error:
        case Invalid():
            return CODE.INVALID_ARGUMENT, f"error_code={ERR_INVALID_REQUEST}: {error}"
        case NameConflict():
            logger.error("player name conflict (id reuse?): %s", error)
            return CODE.FAILED_PRECONDITION, f"error_code={ERR_NAME_CONFLICT}: {error}"
        case NameOutsideWindow():
            return CODE.FAILED_PRECONDITION, f"error_code={ERR_ADMIN_AUTH}: {error}"
        case NameInUse():
            logger.error("player name release refused, it belongs to a committed role: %s", error)
            return CODE.FAILED_PRECONDITION, f"error_code={ERR_NAME_IN_USE}: {error}"
    logger.error("player name store failed", exc_info=error)
    return CODE.UNAVAILABLE, f"error_code={ERR_NAME_DB}: {error}"


def _known_currencies(protocol) -> set[int]:
    """从协议枚举读取已知币种集合，避免手写上限漂移。"""
    try:
        enum_desc = protocol.pool.FindEnumTypeByName("common.base.CurrencyType")
        return set(enum_desc.values_by_number.keys())
    except KeyError:
        return {0, 1, 2}


class DataService:
    def __init__(
        self,
        protocol,
        store,
        secrets: dict[str, bytes],
        *,
        id_tags=DEFAULT_ID_TAGS,
        zone_refresh: float = 5.0,
        clock_ms=lambda: time.time_ns() // 1_000_000,
        admin_token: str = "",
        deletion_cooling_off_ms: int = DEFAULT_DELETION_COOLING_OFF_MS,
        recall_enabled: bool = True,
    ) -> None:
        self.protocol, self.store, self.secrets = protocol, store, secrets
        self.id_tags, self.zone_refresh, self.clock_ms = frozenset(id_tags), zone_refresh, clock_ms
        self.admin_token = admin_token
        self.deletion_cooling_off_ms = deletion_cooling_off_ms
        self.recall_enabled = recall_enabled
        self._known_currencies = _known_currencies(protocol)
        self.zones: MergedZones | None = None
        self._refresher: asyncio.Task | None = None

    # -- lifecycle -----------------------------------------------------------------------------------
    async def start(self) -> None:
        """首次合服映射读取失败直接拒启；后续刷新失败保留旧快照。"""
        self.zones = MergedZones(await self.store.merged_zones())
        self._refresher = asyncio.create_task(self._refresh_zones(), name="merged-zones")

    async def close(self) -> None:
        if self._refresher is not None:
            self._refresher.cancel()
            await asyncio.gather(self._refresher, return_exceptions=True)

    async def _refresh_zones(self) -> None:
        while True:
            await asyncio.sleep(self.zone_refresh)
            try:
                self.zones = MergedZones(await self.store.merged_zones())
            except Exception:
                logger.warning("merged zone refresh failed, keeping previous snapshot", exc_info=True)

    def services(self) -> dict:
        handlers = {
            # 账号目录与角色生命周期
            "ReadAccountDirectory": self.read_account_directory,
            "CreateAccountRole": self.create_account_role,
            "LookupAccountRole": self.lookup_account_role,
            "RepairAccountRole": self.repair_account_role,
            "RemoveAccountRoles": self.remove_account_roles,
            # 实名与渠道身份
            "SetAccountRealName": self.set_account_real_name,
            "GetAccountRealName": self.get_account_real_name,
            "ResetAccountRealName": self.reset_account_real_name,
            "UpsertAccountChannelIdentity": self.upsert_account_channel_identity,
            "GetPlayerChannelIdentity": self.get_player_channel_identity,
            # 角色交易冻结与过户
            "BeginAccountRoleTrade": self.begin_account_role_trade,
            "ConfirmAccountRoleTradeFreeze": self.confirm_account_role_trade_freeze,
            "CancelAccountRoleTrade": self.cancel_account_role_trade,
            "TransferAccountRole": self.transfer_account_role,
            "CheckAccountRoleSlot": self.check_account_role_slot,
            # 角色护栏与入场登记
            "GetRoleFenceState": self.get_role_fence_state,
            "VerifyRoleAdmission": self.verify_role_admission,
            "RegisterRoleAdmission": self.register_role_admission,
            "CloseRoleAdmissions": self.close_role_admissions,
            "AckRoleTradeDrain": self.ack_role_trade_drain,
            # 账号注销
            "RequestAccountDeletion": self.request_account_deletion,
            "CancelAccountDeletion": self.cancel_account_deletion,
            "ClaimAccountDeletions": self.claim_account_deletions,
            "FinishAccountDeletion": self.finish_account_deletion,
            # 存盘写权判死读取与释放
            "ReleaseDeadPlayerStoreHolder": self.release_dead_player_store_holder,
            "ReadPlayerStoreHolder": self.read_player_store_holder,
            # 玩家区服路由
            "RegisterPlayerZone": self.register_player_zone,
            "GetPlayerHomeZone": self.get_player_home_zone,
            "BatchGetPlayerHomeZone": self.batch_get_player_home_zone,
            "GetMergedZones": self.get_merged_zones,
            "CountZoneRoles": self.count_zone_roles,
            "GetPlayerRoute": self.get_player_route,
            # 快照与流水/批量回收
            "ListPlayerSnapshots": self.list_player_snapshots,
            "BatchRecallItems": self.batch_recall_items,
            "GetRecallBatch": self.get_recall_batch,
            "CancelRecallBatch": self.cancel_recall_batch,
            "ResolveRecallOp": self.resolve_recall_op,
            "RegisterRecallReviewHandled": self.register_recall_review_handled,
            "QueryTransactionLog": self.query_transaction_log,
            # 号段发号
            "AllocateIdSegment": self.allocate_id_segment,
            # 角色名、改名、展示资料与外观
            "ReservePlayerName": self.reserve_player_name,
            "ReleasePlayerName": self.release_player_name,
            "BatchGetPlayerName": self.batch_get_player_name,
            "RenameRole": self.rename_role,
            "BatchGetPlayerDisplay": self.batch_get_player_display,
            "UpdateRoleAppearance": self.update_role_appearance,
            "SyncAccountStage1Tradable": self.sync_account_stage1_tradable,
            # 已落盘账本与物品封禁只读
            "GetPlayerAssetOpLedger": self.get_player_asset_op_ledger,
            "GetPlayerInboxLedger": self.get_player_inbox_ledger,
            "GetPlayerItemGainBlock": self.get_player_item_gain_block,
        }
        return {SERVICE: handlers}

    # -- plumbing ------------------------------------------------------------------------------------
    async def _admit(self, context, request, path: str, callers: tuple[str, ...]) -> None:
        caller = request.auth.caller
        secret = self.secrets.get(caller)
        if (caller not in callers or secret is None
                or not verify_service_call(path, request, caller, self.clock_ms(), secret)):
            await context.abort(CODE.PERMISSION_DENIED, "账号目录调用证明无效")

    async def _authorize_admin(self, context, rpc_name: str) -> None:
        """运维接口 fail-closed 鉴权：未配置 admin_token 或常数时间比对失败一律拒绝。"""
        if not self.admin_token:
            logger.error("admin RPC %s refused: admin_token is not configured", rpc_name)
            await context.abort(
                CODE.PERMISSION_DENIED,
                f"error_code={ERR_ADMIN_AUTH}: {rpc_name} is disabled because admin_token is not configured",
            )
        got = ""
        for key, value in context.invocation_metadata():
            if key == ADMIN_TOKEN_KEY:
                got = value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value)
                break
        if not hmac.compare_digest(got.encode("utf-8"), self.admin_token.encode("utf-8")):
            logger.error("admin RPC %s refused: bad or missing %s (present=%s)", rpc_name, ADMIN_TOKEN_KEY, bool(got))
            await context.abort(
                CODE.PERMISSION_DENIED,
                f"error_code={ERR_ADMIN_AUTH}: {rpc_name} requires a valid {ADMIN_TOKEN_KEY}",
            )

    @staticmethod
    def _peer_addr(context) -> str:
        peer_fn = getattr(context, "peer", None)
        return str(peer_fn()) if callable(peer_fn) else "unknown"

    async def _run(self, context, work, mapper=_directory_status):
        try:
            return await work
        except Exception as error:  # AbortError 与 CancelledError 继承 BaseException，直接透传
            status = mapper(error)
        await context.abort(*status)

    def _empty(self):
        return self.protocol.message("google.protobuf.Empty")

    def _players(self, roles: list[bytes]):
        response = self.protocol.message("data_service.AccountDirectoryResponse")
        response.players.SetInParent()
        for raw in roles:
            response.players.players.add().ParseFromString(raw)
        return response

    def _trade_receipt_msg(self, receipt: dict):
        msg = self.protocol.message(
            "data_service.AccountRoleTradeReceipt",
            listing_id=receipt["listing_id"],
            player_id=receipt["player_id"],
            seller_account=receipt["seller_account"],
            seller_owner_revision=receipt["seller_owner_revision"],
            state=receipt["state"],
        )
        if receipt.get("freeze_evidence") is not None:
            msg.freeze_evidence.CopyFrom(receipt["freeze_evidence"])
        for h in receipt.get("holders", ()):
            entry = msg.holders.add()
            entry.seal.CopyFrom(h["seal"])
            entry.drained = bool(h["drained"])
        return msg

    def _deletion_resp(self, result: dict):
        resp = self.protocol.message("data_service.AccountDeletionResponse", outcome=result["outcome"])
        lifecycle = result.get("lifecycle")
        if lifecycle is not None:
            resp.lifecycle.deletion_state = lifecycle.get("deletion_state", 0)
            resp.lifecycle.deletion_execute_after_ms = lifecycle.get("deletion_execute_after_ms", 0)
        for pid, reason in result.get("blockers", ()):
            blocker = resp.blockers.add()
            blocker.player_id = pid
            blocker.reason = reason
        return resp

    # -- account directory ---------------------------------------------------------------------------
    async def read_account_directory(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        if hasattr(self.store, "read_account_with_lifecycle"):
            read = await self._run(
                context, self.store.read_account_with_lifecycle(request.account, request.create_if_missing),
            )
            resp = self._players(read.roles)
            resp.real_name_pi = read.real_name_pi
            resp.lifecycle.deletion_state = read.deletion_state
            resp.lifecycle.deletion_execute_after_ms = read.deletion_execute_after_ms
            resp.lifecycle.previous_deleted_ms = read.previous_deleted_ms
            return resp
        roles = await self._run(context, self.store.read_account(request.account, request.create_if_missing))
        return self._players(roles)

    async def create_account_role(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        roles = await self._run(context, self.store.create_role(request.account, request.role))
        return self._players(roles)

    async def lookup_account_role(self, request, context, path):
        # 仅此只读接口允许 scene_manager 与 trade 独立密钥
        await self._admit(context, request, path, ("login", "scene_manager", "trade"))
        owned = await self._run(context, self.store.lookup_role(request.player_id))
        deletion_state = 0
        if request.include_account_state and hasattr(self.store, "account_deletion_state"):
            deletion_state = await self._run(context, self.store.account_deletion_state(owned.account))
        response = self.protocol.message(
            "data_service.AccountRoleOwnership",
            account=owned.account,
            owner_revision=owned.owner_revision,
            trade_listing_id=owned.trade_listing_id,
            initial_player_commit=owned.initial_player_commit,
            account_deletion_state=deletion_state,
        )
        response.role.ParseFromString(owned.role)
        return response

    async def repair_account_role(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        roles = await self._run(
            context, self.store.repair_role(request.account, request.role, request.expected_owner_revision),
        )
        return self._players(roles)

    async def remove_account_roles(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        pairs = [(int(r.player_id), int(r.owner_revision)) for r in request.roles]
        await self._run(context, self.store.remove_roles(request.account, pairs))
        return self._empty()

    # -- real name -----------------------------------------------------------------------------------
    async def set_account_real_name(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        stored = await self._run(context, self.store.set_real_name(request.account, request.real_name_pi))
        return self.protocol.message("data_service.SetAccountRealNameResponse", real_name_pi=stored)

    async def get_account_real_name(self, request, context, path):
        await self._admit(context, request, path, ("payment",))
        if not request.player_id:
            await context.abort(CODE.INVALID_ARGUMENT, "player_id required")
        found, pi = await self._run(context, self.store.get_real_name_of_player(request.player_id))
        return self.protocol.message("data_service.GetAccountRealNameResponse", found=found, real_name_pi=pi)

    async def reset_account_real_name(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        had = await self._run(context, self.store.reset_real_name(request.account))
        return self.protocol.message("data_service.ResetAccountRealNameResponse", had_real_name=had)

    # -- channel identity ----------------------------------------------------------------------------
    async def upsert_account_channel_identity(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        await self._run(
            context, self.store.upsert_channel_identity(request.account, request.channel, request.open_id),
        )
        return self._empty()

    async def get_player_channel_identity(self, request, context, path):
        if not request.player_id:
            await context.abort(CODE.INVALID_ARGUMENT, "player_id=0")
        try:
            info = await self.store.player_channel_identity(request.player_id)
        except Exception as error:
            logger.error("GetPlayerChannelIdentity failed player=%d", request.player_id, exc_info=error)
            return self.protocol.message("data_service.GetPlayerChannelIdentityResponse", retry=True)
        if info is None:
            return self.protocol.message("data_service.GetPlayerChannelIdentityResponse", found=False)
        return self.protocol.message("data_service.GetPlayerChannelIdentityResponse", found=True, **info)

    # -- role trade & transfer -----------------------------------------------------------------------
    async def begin_account_role_trade(self, request, context, path):
        await self._admit(context, request, path, ("trade",))
        digest = bytes.fromhex(service_digest(request))
        receipt = await self._run(context, self.store.begin_role_trade(request, digest))
        return self._trade_receipt_msg(receipt)

    async def confirm_account_role_trade_freeze(self, request, context, path):
        await self._admit(context, request, path, ("trade",))
        receipt = await self._run(context, self.store.confirm_role_trade_freeze(request))
        return self._trade_receipt_msg(receipt)

    async def cancel_account_role_trade(self, request, context, path):
        await self._admit(context, request, path, ("trade",))
        begin_req = self.protocol.message(
            "data_service.BeginAccountRoleTradeRequest",
            listing_id=request.listing_id,
            player_id=request.player_id,
            seller_account=request.seller_account,
            expected_owner_revision=request.expected_owner_revision,
        )
        begin_digest = bytes.fromhex(service_digest(begin_req))
        receipt = await self._run(context, self.store.cancel_role_trade(request, begin_digest))
        return self._trade_receipt_msg(receipt)

    async def transfer_account_role(self, request, context, path):
        await self._admit(context, request, path, ("trade",))
        digest = bytes.fromhex(service_digest(request))
        receipt = await self._run(context, self.store.transfer_role(request, digest))
        msg = self.protocol.message(
            "data_service.AccountRoleTransferReceipt",
            tx_id=receipt["tx_id"],
            listing_id=receipt["listing_id"],
            player_id=receipt["player_id"],
            seller_account=receipt["seller_account"],
            buyer_account=receipt["buyer_account"],
            seller_owner_revision=receipt["seller_owner_revision"],
            buyer_owner_revision=receipt["buyer_owner_revision"],
            created_ms=receipt["created_ms"],
        )
        msg.role.CopyFrom(receipt["role"])
        return msg

    async def check_account_role_slot(self, request, context, path):
        await self._admit(context, request, path, ("trade",))
        try:
            full = await self.store.check_role_slot(request.account)
        except Invalid as error:
            await context.abort(*_directory_status(error))
        except Exception as error:
            logger.error("CheckAccountRoleSlot failed account=%s", request.account, exc_info=error)
            return self.protocol.message("data_service.CheckAccountRoleSlotResponse", retry=True)
        return self.protocol.message("data_service.CheckAccountRoleSlotResponse", full=full)

    # -- role admissions & drain ack -----------------------------------------------------------------
    async def register_role_admission(self, request, context, path):
        await self._admit(context, request, path, ("scene",))
        identity = request.identity
        if not valid_admission(identity) or identity.ByteSize() > 1024:
            await context.abort(*_directory_status(Invalid()))
        digest = bytes.fromhex(service_digest(request))
        closed = await self._run(context, self.store.register_admission(identity, digest))
        return self.protocol.message(
            "data_service.RegisterRoleAdmissionResponse", identity=identity, closed_through=closed,
        )

    async def close_role_admissions(self, request, context, path):
        await self._admit(context, request, path, ("scene",))
        if (not request.player_id or not valid_instance(request.scene_instance_id) or not request.owner_revision
                or not request.through_generation
                or not valid_save_floor(request.required_owner_epoch, request.required_save_seq)):
            await context.abort(*_directory_status(Invalid()))
        digest = bytes.fromhex(service_digest(request))
        closed = await self._run(context, self.store.close_admissions(request, digest))
        return self.protocol.message("data_service.CloseRoleAdmissionsResponse", closed_through=closed)

    async def ack_role_trade_drain(self, request, context, path):
        await self._admit(context, request, path, ("scene",))
        if (not valid_trade_seal(request.seal)
                or not valid_save_floor(request.required_owner_epoch, request.required_save_seq)):
            await context.abort(*_directory_status(Invalid()))
        digest = bytes.fromhex(service_digest(request))
        await self._run(context, self.store.ack_role_trade_drain(request, digest))
        return self._empty()

    # -- account deletion ----------------------------------------------------------------------------
    async def request_account_deletion(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        try:
            result = await self.store.request_deletion(request.account, request.source, self.deletion_cooling_off_ms)
        except Invalid as error:
            await context.abort(*_directory_status(error))
        except Exception as error:
            logger.error("RequestAccountDeletion failed account=%s", request.account, exc_info=error)
            return self.protocol.message("data_service.AccountDeletionResponse", outcome=DELETION_OUTCOME_RETRY)
        return self._deletion_resp(result)

    async def cancel_account_deletion(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        result = await self._run(context, self.store.cancel_deletion(request.account))
        return self._deletion_resp(result)

    async def claim_account_deletions(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        if not 1 <= request.limit <= MAX_DELETION_CLAIM:
            await context.abort(CODE.INVALID_ARGUMENT, f"limit must be within [1, {MAX_DELETION_CLAIM}]")
        accounts = await self._run(context, self.store.claim_deletions(int(request.limit)))
        return self.protocol.message("data_service.ClaimAccountDeletionsResponse", accounts=accounts)

    async def finish_account_deletion(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        done, remaining = await self._run(context, self.store.finish_deletion(request.account))
        return self.protocol.message(
            "data_service.FinishAccountDeletionResponse", done=done, remaining_refs=remaining,
        )

    # -- dead player_store holder read / release -----------------------------------------------------
    async def read_player_store_holder(self, request, context, path):
        await self._admit(context, request, path, ("scene_manager",))
        if not request.player_id:
            await context.abort(*_directory_status(Invalid("player_id")))
        holder = await self._run(context, self.store.read_player_store_holder(request.player_id))
        if holder is None:
            return self.protocol.message("data_service.ReadPlayerStoreHolderResponse", found=False)
        resp = self.protocol.message("data_service.ReadPlayerStoreHolderResponse", found=True)
        resp.holder.owner_epoch = holder[0]
        resp.holder.holder_instance = holder[1]
        resp.holder.claim_seq = holder[2]
        return resp

    async def release_dead_player_store_holder(self, request, context, path):
        await self._admit(context, request, path, ("scene_manager",))
        seen = None
        if request.HasField("seen"):
            seen = (int(request.seen.owner_epoch), bytes(request.seen.holder_instance), int(request.seen.claim_seq))
        await self._run(
            context, self.store.release_dead_player_store_holder(request.player_id, request.owner_epoch, seen),
        )
        return self._empty()

    # -- routing -------------------------------------------------------------------------------------
    async def register_player_zone(self, request, context, path):
        pid, home, storage = request.player_id, request.home_zone_id, request.storage_id
        if not pid or not 1 <= home <= ROUTE_HOME_ZONE_MAX or storage < FIRST_STORAGE_ID:
            logger.error("RegisterPlayerZone refused invalid registration: player=%d zone=%d storage=%d",
                         pid, home, storage)
            await context.abort(CODE.UNAVAILABLE, f"error_code={ERR_INVALID_REQUEST}: invalid registration")

        def mapper(error):
            if isinstance(error, ZoneConflict):
                logger.error("RegisterPlayerZone refused overwrite: player=%d existing=%d requested=%d",
                             pid, error.existing, home)
                return CODE.ALREADY_EXISTS, f"error_code={ERR_ZONE_CONFLICT}: {error}"
            logger.error("RegisterPlayerZone route store failure: player=%d", pid, exc_info=error)
            return CODE.UNAVAILABLE, f"error_code={ERR_ROUTE_STORE}: {error}"

        await self._run(context, self.store.register_zone(pid, home, storage), mapper)
        return self._empty()

    async def batch_get_player_home_zone(self, request, context, path):
        def mapper(error):
            logger.error("BatchGetPlayerHomeZone ids=%d failed", len(request.player_ids), exc_info=error)
            return CODE.UNAVAILABLE, f"error_code={ERR_ROUTE_STORE}: {error}"

        homes = await self._run(context, self.store.routes(list(request.player_ids)), mapper)
        if self.zones is None:
            await context.abort(CODE.UNAVAILABLE, f"error_code={ERR_ROUTE_STORE}: merged zone cache not assembled")
        return self.protocol.message("data_service.BatchGetPlayerHomeZoneResponse", player_zone_map={
            pid: self.zones.final(home) for pid, home in homes.items()})

    async def get_player_home_zone(self, request, context, path):
        if not request.player_id:
            await context.abort(CODE.INVALID_ARGUMENT, f"error_code={ERR_INVALID_REQUEST}: player_id=0")
        if self.zones is None:
            await context.abort(CODE.UNAVAILABLE, f"error_code={ERR_ROUTE_STORE}: merged zone cache not assembled")
        try:
            detail = await self.store.route_detail(request.player_id)
        except Exception as error:
            logger.error("GetPlayerHomeZone failed player=%d", request.player_id, exc_info=error)
            await context.abort(CODE.UNAVAILABLE, f"error_code={ERR_ROUTE_STORE}: {error}")
        if detail is None:
            await context.abort(CODE.NOT_FOUND, "no home zone mapping")
        return self.protocol.message("data_service.GetPlayerHomeZoneResponse", home_zone_id=self.zones.final(detail[0]))

    async def get_merged_zones(self, request, context, path):
        if self.zones is None:
            await context.abort(CODE.UNAVAILABLE, f"error_code={ERR_ROUTE_STORE}: merged zone cache not assembled")
        return self.protocol.message("data_service.GetMergedZonesResponse", merged_into=dict(self.zones.mapping))

    async def count_zone_roles(self, request, context, path):
        zone_id = int(request.zone_id)
        if not 1 <= zone_id <= ROUTE_HOME_ZONE_MAX:
            await context.abort(CODE.INVALID_ARGUMENT, f"error_code={ERR_INVALID_REQUEST}: invalid zone_id")
        if self.zones is None:
            await context.abort(CODE.UNAVAILABLE, f"error_code={ERR_ROUTE_STORE}: merged zone cache not assembled")
        target = self.zones.final(zone_id)
        sources = [target, *(src for src in self.zones.mapping if self.zones.final(src) == target)]
        try:
            total = await self.store.count_zone_roles(sorted(set(sources)))
        except Exception as error:
            logger.error("CountZoneRoles failed zone=%d", zone_id, exc_info=error)
            await context.abort(CODE.UNAVAILABLE, f"error_code={ERR_ROUTE_STORE}: {error}")
        return self.protocol.message("data_service.CountZoneRolesResponse", roles=total)

    async def get_player_route(self, request, context, path):
        await self._admit(context, request, path, ("db", "scene_manager", "player_inbox", "login"))
        if not request.player_id:
            await context.abort(CODE.INVALID_ARGUMENT, "player_id=0")
        try:
            detail = await self.store.route_detail(request.player_id)
        except Exception as error:
            logger.error("GetPlayerRoute failed player=%d", request.player_id, exc_info=error)
            return self.protocol.message("data_service.GetPlayerRouteResponse", retry=True)
        if detail is None:
            return self.protocol.message("data_service.GetPlayerRouteResponse", found=False)
        return self.protocol.message(
            "data_service.GetPlayerRouteResponse", home_zone=detail[0], storage_id=detail[1], found=True,
        )

    async def get_role_fence_state(self, request, context, path):
        await self._admit(context, request, path, ("player_inbox", "match", "mail", "guild", "trade", "friend", "chat", "db"))
        if not request.player_id:
            await context.abort(CODE.INVALID_ARGUMENT, "player_id=0")
        try:
            state = await self.store.role_fence_state(request.player_id)
        except Exception as error:
            logger.error("GetRoleFenceState failed player=%d", request.player_id, exc_info=error)
            return self.protocol.message("data_service.GetRoleFenceStateResponse", retry=True)
        if state is None:
            return self.protocol.message("data_service.GetRoleFenceStateResponse", found=False)
        return self.protocol.message("data_service.GetRoleFenceStateResponse", found=True, state=state)

    async def verify_role_admission(self, request, context, path):
        await self._admit(context, request, path, ("player_inbox", "match", "mail", "guild", "trade", "friend"))
        identity = request.admission
        if not valid_admission(identity):
            return self.protocol.message("data_service.VerifyRoleAdmissionResponse", valid=False)
        try:
            ok = await self.store.verify_admission(identity)
        except Exception as error:
            logger.error("VerifyRoleAdmission failed player=%d", identity.player_id, exc_info=error)
            return self.protocol.message("data_service.VerifyRoleAdmissionResponse", retry=True)
        return self.protocol.message("data_service.VerifyRoleAdmissionResponse", valid=ok)

    # -- snapshots & transaction log -----------------------------------------------------------------
    async def list_player_snapshots(self, request, context, path):
        if not request.player_id:
            return self.protocol.message("data_service.ListPlayerSnapshotsResponse", error_code=ERR_INVALID_REQUEST)
        try:
            items = await self.store.list_player_snapshots(request.player_id, request.before_time, request.limit)
        except Exception as error:
            logger.error("ListPlayerSnapshots failed player=%d", request.player_id, exc_info=error)
            return self.protocol.message("data_service.ListPlayerSnapshotsResponse", error_code=ERR_SNAPSHOT_DB)
        resp = self.protocol.message("data_service.ListPlayerSnapshotsResponse", error_code=0)
        for item in items:
            info = resp.snapshots.add()
            info.snapshot_id = item["snapshot_id"]
            info.player_id = item["player_id"]
            info.zone_id = item["zone_id"]
            info.type = item["type"]
            info.created_at = item["created_at"]
            info.reason = item["reason"]
            info.operator = item["operator"]
            info.data_size_bytes = item["data_size_bytes"]
        return resp

    async def query_transaction_log(self, request, context, path):
        try:
            rows, total = await self.store.query_transaction_log(
                player_id=request.player_id,
                time_start=request.time_start,
                time_end=request.time_end,
                tx_types=list(request.tx_types),
                item_config_id=request.item_config_id,
                currency_type=request.currency_type,
                limit=request.limit,
                offset=request.offset,
            )
        except Exception as error:
            logger.error("QueryTransactionLog failed player=%d", request.player_id, exc_info=error)
            return self.protocol.message("data_service.QueryTransactionLogResponse", error_code=ERR_SNAPSHOT_DB)
        resp = self.protocol.message("data_service.QueryTransactionLogResponse", error_code=0, total_count=total)
        for r in rows:
            entry = resp.rows.add()
            entry.tx_id = r["tx_id"]
            entry.timestamp = r["timestamp"]
            entry.tx_type = r["tx_type"]
            entry.from_player = r["from_player"]
            entry.to_player = r["to_player"]
            entry.item_uuid = r["item_uuid"]
            entry.item_config_id = r["item_config_id"]
            entry.item_quantity = r["item_quantity"]
            entry.currency_type = r["currency_type"]
            entry.currency_delta = r["currency_delta"]
            entry.balance_before = r["balance_before"]
            entry.balance_after = r["balance_after"]
            entry.correlation_id = r["correlation_id"]
            entry.extra = r["extra"]
        return resp

    # -- batch recall (admin) ------------------------------------------------------------------------
    async def _read_committed_ledger(self, player_id: int) -> tuple[bool, bool, object]:
        found, released, raw = await self.store.read_store_row_with_release(player_id, 7)
        if not found:
            return False, False, None
        ledger = self.protocol.message("PlayerAssetOpLedgerComp")
        if raw:
            db_msg = self.protocol.parse("player_database", raw)
            if db_msg.HasField("asset_op_ledger"):
                ledger.CopyFrom(db_msg.asset_op_ledger)
        return True, released, ledger

    async def _format_recall_batch_resp(self, batch: dict):
        ops = await self.store.list_recall_ops(batch["request_id"])
        total_failed = batch["ops_shortfall"] + batch["ops_failed"]
        if batch["open_ops"] > 0:
            err_code = ERR_RECALL_PENDING
        elif total_failed > 0:
            err_code = ERR_RECALL_INCOMPLETE
        else:
            err_code = 0
        resp = self.protocol.message(
            "data_service.BatchRecallItemsResponse",
            error_code=err_code,
            total_matched=batch["source_rows"],
            total_recalled=batch["ops_complete"],
            total_failed=total_failed,
            total_pending=batch["open_ops"],
            plan_digest=batch["plan_digest"].hex(),
            request_id=batch["request_id"],
            batch_state=batch["state"],
            amount_requested=batch["amount_requested"],
            amount_taken=batch["amount_taken"],
            amount_debt=batch["amount_debt"],
            amount_shortfall=batch["amount_shortfall"],
            cancelled_ms=batch["cancelled_ms"],
        )
        for op in ops:
            item = resp.results.add()
            item.player_id = op["player_id"]
            item.item_config_id = op["item_config_id"]
            item.currency_type = op["currency_type"]
            item.amount = op["requested"]
            item.op_id = op["op_id"]
            item.status = op["status"]
            item.taken = op["taken"]
            item.debt = op["debt"]
            item.shortfall = op["shortfall"]
            label = status_label(op["status"])
            if op["status"] in (RECALL_OP_PLANNED, RECALL_OP_PENDING):
                item.error_detail = f"{label}: not settled yet, attempts={op['attempts']} last_reason={op['last_reason']}"
            elif op["status"] == RECALL_OP_APPLIED and op["shortfall"] == 0:
                item.success = True
            elif op["status"] == RECALL_OP_APPLIED:
                item.error_detail = f"{label}: shortfall={op['shortfall']}"
            elif op["status"] == RECALL_OP_PARTIAL:
                item.error_detail = f"{label}: taken={op['taken']} is only a lower bound; verify GetPlayerAssetOpLedger"
            else:
                item.error_detail = f"{label}: nothing recalled, reason={op['last_reason']}"
        return resp

    async def batch_recall_items(self, request, context, path):
        await self._authorize_admin(context, "BatchRecallItems")
        player_ids = tuple(int(p) for p in request.player_ids)
        tx_types = tuple(int(t) for t in request.tx_types)
        if (not request.time_start or not request.time_end or request.time_start > request.time_end
                or len(player_ids) > MAX_RECALL_PLAYERS
                or not valid_recall_asset(request.recall_kind, request.item_config_id,
                                          request.currency_type, self._known_currencies)):
            return self.protocol.message("data_service.BatchRecallItemsResponse", error_code=ERR_INVALID_REQUEST)
        if not request.dry_run and (
            not request.request_id or not request.expected_plan_digest
            or not valid_recall_text(request.operator, RECALL_OPERATOR_MAX_RUNES)
            or not valid_recall_text(request.reason, RECALL_REASON_MAX_RUNES)
        ):
            return self.protocol.message("data_service.BatchRecallItemsResponse", error_code=ERR_INVALID_REQUEST)

        flt = RecallFilter(
            kind=int(request.recall_kind),
            player_ids=player_ids,
            item_config_id=int(request.item_config_id),
            currency_type=int(request.currency_type),
            time_start=int(request.time_start),
            time_end=int(request.time_end),
            tx_types=tx_types,
        )
        now_ms = self.clock_ms()
        # 非预览重放先看既有批次
        if not request.dry_run:
            try:
                existing = await self.store.read_recall_batch(request.request_id)
            except RecallBatchNotFound:
                existing = None
            except Exception as error:
                logger.error("BatchRecallItems read batch failed request_id=%d", request.request_id, exc_info=error)
                return self.protocol.message("data_service.BatchRecallItemsResponse", error_code=ERR_SNAPSHOT_DB)
            if existing is not None:
                if (existing["request_digest"] != flt.digest()
                        or existing["plan_digest"].hex() != request.expected_plan_digest):
                    return self.protocol.message(
                        "data_service.BatchRecallItemsResponse",
                        error_code=ERR_RECALL_REQUEST_CONFLICT, request_id=request.request_id,
                    )
                return await self._format_recall_batch_resp(existing)
            if not self.recall_enabled:
                await self.store.insert_recall_audit(
                    request.time_start, 0, 0, f"batch_recall REJECTED (DISABLED): {request.reason}",
                    request.operator, now_ms,
                )
                return self.protocol.message(
                    "data_service.BatchRecallItemsResponse",
                    error_code=ERR_RECALL_DISABLED, request_id=request.request_id,
                )

        # 查候选流水
        try:
            all_rows: list[dict] = []
            candidate_total = 0
            truncated = False
            targets = list(player_ids) if player_ids else [0]
            for pid in targets:
                rows, total = await self.store.query_transaction_log(
                    player_id=pid,
                    time_start=request.time_start,
                    time_end=request.time_end,
                    tx_types=list(tx_types),
                    item_config_id=request.item_config_id,
                    currency_type=request.currency_type,
                    exact_currency=(request.recall_kind == RECALL_KIND_CURRENCY),
                    limit=BATCH_RECALL_QUERY_LIMIT,
                )
                candidate_total += total
                if total != len(rows):
                    truncated = True
                all_rows.extend(rows)
            if candidate_total > BATCH_RECALL_QUERY_LIMIT:
                truncated = True
            if truncated:
                return self.protocol.message(
                    "data_service.BatchRecallItemsResponse",
                    error_code=ERR_RESULT_TRUNCATED, total_matched=candidate_total, total_failed=candidate_total,
                    request_id=request.request_id,
                )

            by_player = recall_instruction_ids(flt, all_rows)
            if len(by_player) > MAX_RECALL_PLAYERS:
                return self.protocol.message(
                    "data_service.BatchRecallItemsResponse",
                    error_code=ERR_RESULT_TRUNCATED, total_matched=candidate_total, request_id=request.request_id,
                )
            ledgers: dict[int, tuple[bool, bool, object]] = {}
            siblings: list[dict] = []
            for pid in sorted(by_player):
                ledgers[pid] = await self._read_committed_ledger(pid)
                sib_rows, sib_total = await self.store.query_transaction_log(
                    to_player_id=pid,
                    correlation_ids=by_player[pid],
                    item_config_id=request.item_config_id,
                    currency_type=request.currency_type,
                    exact_currency=(request.recall_kind == RECALL_KIND_CURRENCY),
                    limit=BATCH_RECALL_QUERY_LIMIT,
                )
                if sib_total != len(sib_rows) or len(siblings) + len(sib_rows) > MAX_RECALL_SOURCE_ROWS:
                    return self.protocol.message(
                        "data_service.BatchRecallItemsResponse",
                        error_code=ERR_RESULT_TRUNCATED, total_matched=candidate_total, request_id=request.request_id,
                    )
                siblings.extend(sib_rows)
            tx_ids = recall_source_tx_ids(flt, all_rows) + recall_source_tx_ids(flt, siblings)
            consumed = await self.store.consumed_recall_sources(tx_ids)
            plan = build_recall_plan(flt, all_rows, siblings, ledgers, consumed)
        except RecallPlanTooLarge:
            return self.protocol.message(
                "data_service.BatchRecallItemsResponse",
                error_code=ERR_RESULT_TRUNCATED, request_id=request.request_id,
            )
        except Exception as error:
            logger.error("BatchRecallItems plan failed", exc_info=error)
            return self.protocol.message("data_service.BatchRecallItemsResponse", error_code=ERR_SNAPSHOT_DB)

        if request.dry_run:
            resp = self.protocol.message(
                "data_service.BatchRecallItemsResponse",
                error_code=0,
                total_matched=candidate_total,
                plan_digest=plan.digest_hex,
                amount_requested=plan.amount_requested,
            )
            for op in plan.ops:
                r = resp.results.add()
                r.player_id = op.player_id
                r.item_config_id = op.item_config_id
                r.currency_type = op.currency_type
                r.amount = op.requested
            for rev in plan.reviews:
                row = rev.row
                rr = resp.needs_review.add()
                rr.player_id = row["to_player"]
                rr.tx_id = row["tx_id"]
                rr.correlation_id = row["correlation_id"]
                rr.tx_type = row["tx_type"]
                rr.amount = row["item_quantity"] if row["item_config_id"] else row["currency_delta"]
                rr.item_uuid = row["item_uuid"]
                rr.timestamp = row["timestamp"]
                rr.reason = rev.reason
            return resp

        if plan.digest_hex != request.expected_plan_digest:
            return self.protocol.message(
                "data_service.BatchRecallItemsResponse",
                error_code=ERR_RECALL_PLAN_CHANGED, total_matched=candidate_total, request_id=request.request_id,
            )
        if not plan.ops:
            return self.protocol.message(
                "data_service.BatchRecallItemsResponse",
                error_code=ERR_RECALL_NOTHING_TO_RECALL, total_matched=candidate_total,
                plan_digest=plan.digest_hex, request_id=request.request_id,
            )

        # 冻结计划
        op_plans: list[dict] = []
        lo, _ = await self.store.allocate(len(plan.ops))
        for idx, op in enumerate(plan.ops):
            bundle = self.protocol.message("AssetBundle")
            if request.recall_kind == RECALL_KIND_CURRENCY:
                c = bundle.currencies.add()
                c.currency_type = op.currency_type
                c.amount = op.requested
            else:
                it = bundle.items.add()
                it.config_id = op.item_config_id
                it.count = op.requested
                bundle.item_uuids.extend(op.hints)
            op_plans.append({
                "op_id": lo + idx,
                "player_id": op.player_id,
                "kind": int(request.recall_kind),
                "currency_type": op.currency_type,
                "item_config_id": op.item_config_id,
                "requested": op.requested,
                "payload": bundle.SerializeToString(deterministic=True),
                "source_tx_ids": list(op.source_tx_ids),
            })
        batch_row = {
            "request_id": int(request.request_id),
            "request_digest": flt.digest(),
            "plan_digest": plan.digest,
            "kind": int(request.recall_kind),
            "item_config_id": int(request.item_config_id),
            "currency_type": int(request.currency_type),
            "time_start": int(request.time_start),
            "time_end": int(request.time_end),
            "operator": request.operator,
            "reason": request.reason,
            "peer_addr": self._peer_addr(context),
            "player_count": plan.player_count,
            "op_count": len(op_plans),
            "source_rows": plan.source_rows,
            "amount_requested": plan.amount_requested,
        }
        try:
            await self.store.create_recall_batch(batch_row, op_plans, now_ms)
        except RecallBatchExists:
            existing = await self.store.read_recall_batch(request.request_id)
            if (existing["request_digest"] != flt.digest()
                    or existing["plan_digest"].hex() != request.expected_plan_digest):
                return self.protocol.message(
                    "data_service.BatchRecallItemsResponse",
                    error_code=ERR_RECALL_REQUEST_CONFLICT, request_id=request.request_id,
                )
            return await self._format_recall_batch_resp(existing)
        except RecallSourceConsumed:
            return self.protocol.message(
                "data_service.BatchRecallItemsResponse",
                error_code=ERR_RECALL_PLAN_CHANGED, request_id=request.request_id,
            )
        except Exception as error:
            logger.error("BatchRecallItems freeze failed request_id=%d", request.request_id, exc_info=error)
            return self.protocol.message("data_service.BatchRecallItemsResponse", error_code=ERR_SNAPSHOT_DB)
        stored = await self.store.read_recall_batch(request.request_id)
        return await self._format_recall_batch_resp(stored)

    async def get_recall_batch(self, request, context, path):
        await self._authorize_admin(context, "GetRecallBatch")
        if not request.request_id:
            return self.protocol.message("data_service.BatchRecallItemsResponse", error_code=ERR_INVALID_REQUEST)
        try:
            batch = await self.store.read_recall_batch(request.request_id)
            return await self._format_recall_batch_resp(batch)
        except RecallBatchNotFound:
            return self.protocol.message(
                "data_service.BatchRecallItemsResponse",
                error_code=ERR_RECALL_NOT_FOUND, request_id=request.request_id,
            )
        except Exception as error:
            logger.error("GetRecallBatch failed request_id=%d", request.request_id, exc_info=error)
            return self.protocol.message("data_service.BatchRecallItemsResponse", error_code=ERR_SNAPSHOT_DB)

    async def cancel_recall_batch(self, request, context, path):
        await self._authorize_admin(context, "CancelRecallBatch")
        if (not request.request_id or not valid_recall_text(request.operator, RECALL_OPERATOR_MAX_RUNES)
                or not valid_recall_text(request.reason, RECALL_REASON_MAX_RUNES)):
            return self.protocol.message("data_service.BatchRecallItemsResponse", error_code=ERR_INVALID_REQUEST)
        try:
            await self.store.cancel_recall_batch(request.request_id, request.operator, request.reason, self.clock_ms())
            batch = await self.store.read_recall_batch(request.request_id)
            return await self._format_recall_batch_resp(batch)
        except RecallBatchNotFound:
            return self.protocol.message(
                "data_service.BatchRecallItemsResponse",
                error_code=ERR_RECALL_NOT_FOUND, request_id=request.request_id,
            )
        except Exception as error:
            logger.error("CancelRecallBatch failed request_id=%d", request.request_id, exc_info=error)
            return self.protocol.message("data_service.BatchRecallItemsResponse", error_code=ERR_SNAPSHOT_DB)

    async def resolve_recall_op(self, request, context, path):
        await self._authorize_admin(context, "ResolveRecallOp")
        if (not request.op_id or request.final_status not in MANUAL_FINAL_STATUSES
                or not valid_recall_text(request.operator, RECALL_OPERATOR_MAX_RUNES)
                or not valid_recall_text(request.reason, RECALL_REASON_MAX_RUNES)):
            return self.protocol.message("data_service.ResolveRecallOpResponse", error_code=ERR_INVALID_REQUEST)
        try:
            op_state = await self.store.read_recall_op_state(request.op_id)
        except RecallOpNotFound:
            return self.protocol.message("data_service.ResolveRecallOpResponse", error_code=ERR_RECALL_NOT_FOUND)
        except Exception as error:
            logger.error("ResolveRecallOp read failed op_id=%d", request.op_id, exc_info=error)
            return self.protocol.message("data_service.ResolveRecallOpResponse", error_code=ERR_SNAPSHOT_DB)
        if op_state["status"] != RECALL_OP_PENDING:
            return self.protocol.message(
                "data_service.ResolveRecallOpResponse", error_code=0, resolved=False, status=op_state["status"],
            )
        now_ms = self.clock_ms()
        try:
            ledger_snapshot = await self._read_committed_ledger(op_state["player_id"])
        except Exception as error:
            logger.error("ResolveRecallOp ledger read failed op_id=%d", request.op_id, exc_info=error)
            return self.protocol.message(
                "data_service.ResolveRecallOpResponse", error_code=ERR_SNAPSHOT_DB, status=op_state["status"],
            )
        verdict, detail = judge_manual_resolve(ledger_snapshot, op_state, now_ms)
        evidence = format_recall_evidence(verdict, detail)
        if not manual_resolve_allows(verdict, int(request.final_status)):
            return self.protocol.message(
                "data_service.ResolveRecallOpResponse",
                error_code=ERR_RECALL_EVIDENCE_REFUSED, resolved=False, status=op_state["status"], evidence=evidence,
            )
        try:
            resolved = await self.store.resolve_recall_op_manually(
                request.op_id, int(request.final_status), request.operator, request.reason,
                op_state["lease_until_ms"], op_state["last_send_ms"], now_ms,
            )
        except Exception as error:
            logger.error("ResolveRecallOp commit failed op_id=%d", request.op_id, exc_info=error)
            return self.protocol.message(
                "data_service.ResolveRecallOpResponse",
                error_code=ERR_SNAPSHOT_DB, status=op_state["status"], evidence=evidence,
            )
        if not resolved:
            return self.protocol.message(
                "data_service.ResolveRecallOpResponse",
                error_code=ERR_RECALL_OP_CHANGED, resolved=False, status=op_state["status"], evidence=evidence,
            )
        return self.protocol.message(
            "data_service.ResolveRecallOpResponse",
            error_code=0, resolved=True, status=int(request.final_status), evidence=evidence,
        )

    async def register_recall_review_handled(self, request, context, path):
        await self._authorize_admin(context, "RegisterRecallReviewHandled")
        tx_ids = [int(t) for t in request.tx_ids]
        if (not request.player_id or not request.correlation_id or not tx_ids or len(tx_ids) > MAX_RECALL_SOURCE_ROWS
                or any(t == 0 for t in tx_ids) or len(set(tx_ids)) != len(tx_ids)
                or int(request.review_reason) not in VALID_REVIEW_REASONS
                or not valid_recall_asset(request.recall_kind, request.item_config_id,
                                          request.currency_type, self._known_currencies)
                or not valid_recall_text(request.operator, RECALL_OPERATOR_MAX_RUNES)
                or not valid_recall_text(request.reason, RECALL_REASON_MAX_RUNES)):
            return self.protocol.message(
                "data_service.RegisterRecallReviewHandledResponse", error_code=ERR_INVALID_REQUEST,
            )
        flt = RecallFilter(
            kind=int(request.recall_kind),
            player_ids=(int(request.player_id),),
            item_config_id=int(request.item_config_id),
            currency_type=int(request.currency_type),
        )
        try:
            rows, total = await self.store.query_transaction_log(
                to_player_id=request.player_id,
                correlation_ids=[request.correlation_id],
                item_config_id=request.item_config_id,
                currency_type=request.currency_type,
                exact_currency=(request.recall_kind == RECALL_KIND_CURRENCY),
                limit=BATCH_RECALL_QUERY_LIMIT,
            )
            if total != len(rows):
                return self.protocol.message(
                    "data_service.RegisterRecallReviewHandledResponse", error_code=ERR_RESULT_TRUNCATED,
                )
            gains = [r for r in distinct_gains(flt, rows) if int(r["correlation_id"]) == int(request.correlation_id)]
            if not gains:
                return self.protocol.message(
                    "data_service.RegisterRecallReviewHandledResponse", error_code=ERR_RECALL_NOT_FOUND,
                )
            gain_tx_ids = [int(r["tx_id"]) for r in gains]
            manual, batch = await self.store.recall_source_state(gain_tx_ids)
            if batch:
                return self.protocol.message(
                    "data_service.RegisterRecallReviewHandledResponse", error_code=ERR_RECALL_PLAN_CHANGED,
                )
            if manual:
                return self.protocol.message(
                    "data_service.RegisterRecallReviewHandledResponse", error_code=0, registered=False,
                )
            ledger_snapshot = await self._read_committed_ledger(request.player_id)
            plan = build_recall_plan(flt, gains, [], {int(request.player_id): ledger_snapshot}, set())
            if plan.ops or not plan.reviews:
                return self.protocol.message(
                    "data_service.RegisterRecallReviewHandledResponse", error_code=ERR_RECALL_PLAN_CHANGED,
                )
            recomputed_reason = plan.reviews[0].reason
            reviewed_ids = [int(rev.row["tx_id"]) for rev in plan.reviews]
            if recomputed_reason != int(request.review_reason) or sorted(tx_ids) != sorted(reviewed_ids):
                return self.protocol.message(
                    "data_service.RegisterRecallReviewHandledResponse",
                    error_code=ERR_RECALL_PLAN_CHANGED, review_reason=recomputed_reason,
                )
            detail = (
                f"kind={request.recall_kind} item={request.item_config_id} currency={request.currency_type} "
                f"review_reason={recomputed_reason} peer={self._peer_addr(context)}"
            )
            try:
                registered = await self.store.register_recall_review_handled(
                    request.player_id, request.correlation_id, reviewed_ids,
                    request.operator, request.reason, detail, self.clock_ms(),
                )
            except RecallSourceConsumed:
                manual2, batch2 = await self.store.recall_source_state(reviewed_ids)
                code = 0 if (manual2 and not batch2) else ERR_RECALL_PLAN_CHANGED
                return self.protocol.message(
                    "data_service.RegisterRecallReviewHandledResponse",
                    error_code=code, registered=False, review_reason=recomputed_reason,
                )
            return self.protocol.message(
                "data_service.RegisterRecallReviewHandledResponse",
                error_code=0, registered=registered,
                source_rows=len(reviewed_ids) if registered else 0,
                review_reason=recomputed_reason,
            )
        except Exception as error:
            logger.error("RegisterRecallReviewHandled failed player=%d corr=%d",
                         request.player_id, request.correlation_id, exc_info=error)
            return self.protocol.message(
                "data_service.RegisterRecallReviewHandledResponse", error_code=ERR_SNAPSHOT_DB,
            )

    # -- ledgers & displays --------------------------------------------------------------------------
    async def get_player_asset_op_ledger(self, request, context, path):
        if not request.player_id:
            await context.abort(CODE.INVALID_ARGUMENT, "player_id=0")
        try:
            found, released, raw = await self.store.read_store_row_with_release(request.player_id, 7)
        except Exception as error:
            logger.error("GetPlayerAssetOpLedger failed player=%d", request.player_id, exc_info=error)
            await context.abort(CODE.INTERNAL, str(error))
        if not found:
            return self.protocol.message("data_service.GetPlayerAssetOpLedgerResponse", found=False)
        resp = self.protocol.message(
            "data_service.GetPlayerAssetOpLedgerResponse", found=True, player_released=released,
        )
        if raw:
            db_msg = self.protocol.parse("player_database", raw)
            if db_msg.HasField("asset_op_ledger"):
                resp.ledger.CopyFrom(db_msg.asset_op_ledger)
        return resp

    async def batch_get_player_display(self, request, context, path):
        ids = list(dict.fromkeys(pid for pid in request.player_ids if pid))
        if len(ids) > 1024:
            await context.abort(CODE.INVALID_ARGUMENT, f"too many player_ids: {len(ids)}")
        try:
            found = await self.store.player_displays(ids)
        except Exception as error:
            logger.error("BatchGetPlayerDisplay failed count=%d", len(ids), exc_info=error)
            await context.abort(CODE.UNAVAILABLE, str(error))
        resp = self.protocol.message("data_service.BatchGetPlayerDisplayResponse")
        for pid, item in found.items():
            entry = resp.players[pid]
            entry.name = item["name"]
            entry.level = item["level"]
            entry.class_id = item["class_id"]
            entry.gender = item["gender"]
            entry.appearance_id = item["appearance_id"]
        return resp

    async def get_player_inbox_ledger(self, request, context, path):
        if not request.player_id:
            await context.abort(CODE.INVALID_ARGUMENT, "player_id=0")
        try:
            found, raw = await self.store.read_store_row(request.player_id, 7)  # PLAYER_STORE_KIND_WALLET
        except Exception as error:
            logger.error("GetPlayerInboxLedger failed player=%d", request.player_id, exc_info=error)
            await context.abort(CODE.UNAVAILABLE, str(error))
        if not found:
            return self.protocol.message("data_service.GetPlayerInboxLedgerResponse", found=False)
        resp = self.protocol.message("data_service.GetPlayerInboxLedgerResponse", found=True)
        if raw:
            db_msg = self.protocol.parse("player_database", raw)
            if db_msg.HasField("inbox_ledger"):
                resp.ledger.CopyFrom(db_msg.inbox_ledger)
        return resp

    async def get_player_item_gain_block(self, request, context, path):
        if not request.player_id:
            await context.abort(CODE.INVALID_ARGUMENT, "player_id=0")
        try:
            found, raw = await self.store.read_store_row(request.player_id, 4)  # PLAYER_STORE_KIND_PROFILE
        except Exception as error:
            logger.error("GetPlayerItemGainBlock failed player=%d", request.player_id, exc_info=error)
            await context.abort(CODE.UNAVAILABLE, str(error))
        if not found:
            return self.protocol.message("data_service.GetPlayerItemGainBlockResponse", found=False)
        resp = self.protocol.message("data_service.GetPlayerItemGainBlockResponse", found=True)
        if raw:
            db_msg = self.protocol.parse("player_database", raw)
            if db_msg.HasField("item_gain_block"):
                resp.block.CopyFrom(db_msg.item_gain_block)
        return resp

    # -- names, rename & appearance ------------------------------------------------------------------
    async def reserve_player_name(self, request, context, path):
        if not request.player_id:
            await context.abort(*_name_status(Invalid("reserve player_id=0")))
        display, norm, verdict = names.normalize(request.name)
        if verdict != names.OK:
            return self.protocol.message("data_service.ReservePlayerNameResponse", result=names.RESERVE_INVALID)
        result, owner = await self._run(
            context, self.store.reserve_name(request.player_id, display, norm, self.clock_ms()), _name_status)
        return self.protocol.message("data_service.ReservePlayerNameResponse", result=result, owner_player_id=owner)

    async def release_player_name(self, request, context, path):
        admin = False
        if any(key == ADMIN_TOKEN_KEY for key, _ in context.invocation_metadata()):
            await self._authorize_admin(context, "ReleasePlayerName")
            admin = True
        else:
            await self._admit(context, request, path, ("login",))
        if not request.player_id:
            await context.abort(*_name_status(Invalid("release player_id=0")))
        _, norm, verdict = names.normalize(request.name)
        if verdict not in (names.OK, names.SENSITIVE):
            await context.abort(*_name_status(Invalid(f"release verdict={verdict}")))
        floor = 0 if admin else max(1, self.clock_ms() - RELEASE_WINDOW_MS)
        await self._run(context, self.store.release_name(request.player_id, norm, floor), _name_status)
        return self._empty()

    async def batch_get_player_name(self, request, context, path):
        ids = list(dict.fromkeys(pid for pid in request.player_ids if pid))
        if len(ids) > NAME_BATCH_LIMIT:
            await context.abort(*_name_status(Invalid(f"{len(ids)} distinct ids, limit {NAME_BATCH_LIMIT}")))
        found = await self._run(context, self.store.player_names(ids), _name_status)
        return self.protocol.message("data_service.BatchGetPlayerNameResponse", names=found)

    async def rename_role(self, request, context, path):
        await self._admit(context, request, path, ("scene",))
        if not request.player_id or not request.seq:
            await context.abort(CODE.INVALID_ARGUMENT, "player_id and seq must be non-zero")
        min_runes, max_runes = 1, names.MAX_RUNES
        if request.min_chars or request.max_chars:
            min_runes, max_runes = int(request.min_chars), int(request.max_chars)
            if min_runes < 1 or max_runes < min_runes or max_runes > names.MAX_RUNES:
                await context.abort(CODE.INVALID_ARGUMENT, "invalid name length rules")
        display, norm, verdict = names.normalize(request.name, min_runes=min_runes, max_runes=max_runes)
        invalid = verdict != names.OK
        if invalid:
            display, norm = "", ""
        out = await self._run(
            context,
            self.store.rename_role(
                request.player_id,
                request.seq,
                display,
                norm,
                invalid=invalid,
                cooldown_ms=int(request.cooldown_seconds) * 1000,
                hold_ms=int(request.hold_seconds) * 1000,
                now_ms=self.clock_ms(),
            ),
        )
        return self.protocol.message(
            "data_service.RenameRoleResponse",
            result=out["result"],
            current_seq=out["current_seq"],
            name=out["name"],
            renamed_ms=out["renamed_ms"],
            next_allowed_ms=out["next_allowed_ms"],
        )

    async def update_role_appearance(self, request, context, path):
        await self._admit(context, request, path, ("scene",))
        appearance_id = request.appearance_id.strip()
        if not request.player_id or not appearance_id or len(appearance_id.encode("utf-8")) > MAX_APPEARANCE_ID_BYTES:
            await context.abort(CODE.INVALID_ARGUMENT, "player_id and appearance_id must be valid")
        updated = await self._run(context, self.store.update_role_appearance(request.player_id, appearance_id))
        return self.protocol.message("data_service.UpdateRoleAppearanceResponse", updated=updated)

    async def sync_account_stage1_tradable(self, request, context, path):
        """同步账号级阶段一可交易装备材料终身 60 份配额（仅认 scene 调用证明）。"""
        if self.store is None or not hasattr(self.store, "sync_account_stage1_tradable") or not self.secrets:
            await context.abort(CODE.UNAVAILABLE, "account tradable material quota service is unavailable")
        await self._admit(context, request, path, ("scene",))
        player_id = int(request.player_id)
        desired = int(request.desired_char_total)
        if not player_id or desired > STAGE1_TRADABLE_MATERIAL_LIFETIME_LIMIT:
            await context.abort(CODE.INVALID_ARGUMENT, "player_id and desired_char_total must be valid")
        found, confirmed, used = await self._run(
            context, self.store.sync_account_stage1_tradable(player_id, desired),
        )
        return self.protocol.message(
            "data_service.SyncAccountStage1TradableResponse",
            found=found,
            confirmed_char_total=confirmed,
            account_used_total=used,
        )

    # -- id segments ---------------------------------------------------------------------------------
    async def allocate_id_segment(self, request, context, path):
        """恒返回 OK 状态；失败通过 ``error_code`` 携带（[lo, hi) 半开区间）。"""
        def reply(code=0, lo=0, hi=0):
            return self.protocol.message("data_service.AllocateIdSegmentResponse", error_code=code, lo=lo, hi=hi)

        tag, step = request.biz_tag, request.step
        if not tag or not valid_tag(tag) or step > MAX_ID_STEP:
            return reply(ERR_INVALID_REQUEST)
        if tag not in self.id_tags:
            logger.error("id segment UNKNOWN TAG biz_tag=%s: add it to the data_service tag list", tag)
            return reply(ERR_ID_UNKNOWN_TAG)
        try:
            lo, hi = await self.store.allocate(step or DEFAULT_ID_STEP)
        except OverflowError:
            logger.error("id segment EXHAUSTED biz_tag=%s", tag)
            return reply(ERR_ID_EXHAUSTED)
        except Exception:
            logger.exception("id segment allocate failed biz_tag=%s", tag)
            return reply(ERR_ID_SOURCE)
        return reply(lo=lo, hi=hi)
