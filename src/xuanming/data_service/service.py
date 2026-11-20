"""DataService gRPC handlers: the 11 main-path RPCs of Go ``data_service``; everything else is UNIMPLEMENTED
(``serve_grpc`` only registers the methods listed in ``services()``)."""

import asyncio
import logging
import time

import grpc

from ..grpc_runtime import service_digest, verify_service_call
from . import names
from .errors import (Conflict, DirectoryError, Full, Invalid, Missing, NameConflict, NameInUse,
                     NameOutsideWindow, NotCommitted, RetryLater, ZoneConflict)
from .model import (DEFAULT_ID_STEP, DEFAULT_ID_TAGS, ERR_ID_EXHAUSTED, ERR_ID_SOURCE, ERR_ID_UNKNOWN_TAG,
                    ERR_INVALID_REQUEST, ERR_NAME_CONFLICT, ERR_NAME_DB, ERR_NAME_IN_USE, ERR_ADMIN_AUTH,
                    ERR_ROUTE_STORE, ERR_ZONE_CONFLICT, FIRST_STORAGE_ID, MAX_ID_STEP, NAME_BATCH_LIMIT,
                    ROUTE_HOME_ZONE_MAX, MergedZones, valid_admission, valid_instance, valid_save_floor, valid_tag)

logger = logging.getLogger(__name__)

SERVICE = "data_service.DataService"
RELEASE_WINDOW_MS = 600_000  # config.PlayerName.ReleaseWindow default (10 min)
ADMIN_TOKEN_KEY = "x-admin-token"
CODE = grpc.StatusCode


def _directory_status(error: Exception) -> tuple:
    """Go ``directoryError``."""
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
    """Go ``playerNameStatus``."""
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


class DataService:
    def __init__(self, protocol, store, secrets: dict[str, bytes], *, id_tags=DEFAULT_ID_TAGS,
                 zone_refresh: float = 5.0, clock_ms=lambda: time.time_ns() // 1_000_000) -> None:
        self.protocol, self.store, self.secrets = protocol, store, secrets
        self.id_tags, self.zone_refresh, self.clock_ms = frozenset(id_tags), zone_refresh, clock_ms
        self.zones: MergedZones | None = None
        self._refresher: asyncio.Task | None = None

    # -- lifecycle -----------------------------------------------------------------------------------
    async def start(self) -> None:
        """First merged-zone read is fatal (Go refuses to start); later failures keep the old snapshot."""
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
                logger.warning("合服映射刷新失败，继续使用旧快照", exc_info=True)

    def services(self) -> dict:
        handlers = {
            "ReadAccountDirectory": self.read_account_directory, "CreateAccountRole": self.create_account_role,
            "LookupAccountRole": self.lookup_account_role, "RegisterRoleAdmission": self.register_role_admission,
            "CloseRoleAdmissions": self.close_role_admissions, "RegisterPlayerZone": self.register_player_zone,
            "BatchGetPlayerHomeZone": self.batch_get_player_home_zone, "ReservePlayerName": self.reserve_player_name,
            "ReleasePlayerName": self.release_player_name, "BatchGetPlayerName": self.batch_get_player_name,
            "AllocateIdSegment": self.allocate_id_segment,
        }
        return {SERVICE: handlers}

    # -- plumbing ------------------------------------------------------------------------------------
    async def _admit(self, context, request, path: str, callers: tuple[str, ...]) -> None:
        caller = request.auth.caller
        secret = self.secrets.get(caller)
        if (caller not in callers or secret is None
                or not verify_service_call(path, request, caller, self.clock_ms(), secret)):
            await context.abort(CODE.PERMISSION_DENIED, "账号目录调用证明无效")

    async def _run(self, context, work, mapper=_directory_status):
        try:
            return await work
        except Exception as error:  # AbortError and CancelledError are BaseException and pass through
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

    # -- account directory ---------------------------------------------------------------------------
    async def read_account_directory(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        roles = await self._run(context, self.store.read_account(request.account, request.create_if_missing))
        return self._players(roles)

    async def create_account_role(self, request, context, path):
        await self._admit(context, request, path, ("login",))
        roles = await self._run(context, self.store.create_role(request.account, request.role))
        return self._players(roles)

    async def lookup_account_role(self, request, context, path):
        # Go: login first, else scene_manager, else trade; only this read-only method accepts the extra callers.
        await self._admit(context, request, path, ("login", "scene_manager", "trade"))
        owned = await self._run(context, self.store.lookup_role(request.player_id))
        response = self.protocol.message(
            "data_service.AccountRoleOwnership", account=owned.account, owner_revision=owned.owner_revision,
            trade_listing_id=owned.trade_listing_id, initial_player_commit=owned.initial_player_commit)
        response.role.ParseFromString(owned.role)
        return response

    async def register_role_admission(self, request, context, path):
        await self._admit(context, request, path, ("scene",))
        identity = request.identity
        if not valid_admission(identity) or identity.ByteSize() > 1024:
            await context.abort(*_directory_status(Invalid()))
        digest = bytes.fromhex(service_digest(request))
        closed = await self._run(context, self.store.register_admission(identity, digest))
        return self.protocol.message("data_service.RegisterRoleAdmissionResponse", identity=identity,
                                     closed_through=closed)

    async def close_role_admissions(self, request, context, path):
        await self._admit(context, request, path, ("scene",))
        if (not request.player_id or not valid_instance(request.scene_instance_id) or not request.owner_revision
                or not request.through_generation
                or not valid_save_floor(request.required_owner_epoch, request.required_save_seq)):
            await context.abort(*_directory_status(Invalid()))
        digest = bytes.fromhex(service_digest(request))
        closed = await self._run(context, self.store.close_admissions(request, digest))
        return self.protocol.message("data_service.CloseRoleAdmissionsResponse", closed_through=closed)

    # -- routing -------------------------------------------------------------------------------------
    async def register_player_zone(self, request, context, path):
        pid, home, storage = request.player_id, request.home_zone_id, request.storage_id
        if not pid or not 1 <= home <= ROUTE_HOME_ZONE_MAX or storage < FIRST_STORAGE_ID:
            logger.error("RegisterPlayerZone refused invalid registration: player=%d zone=%d storage=%d",
                         pid, home, storage)
            # Deliberately not INVALID_ARGUMENT: login only checks for an error and tells them apart by code 14.
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

    # -- names ---------------------------------------------------------------------------------------
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
        if any(key == ADMIN_TOKEN_KEY for key, _ in context.invocation_metadata()):
            # Go checks the token against config AdminToken; empty config disables the admin path.
            await context.abort(CODE.PERMISSION_DENIED, "admin path is not implemented")
        await self._admit(context, request, path, ("login",))
        if not request.player_id:
            await context.abort(*_name_status(Invalid("release player_id=0")))
        _, norm, verdict = names.normalize(request.name)
        if verdict not in (names.OK, names.SENSITIVE):  # sensitive names may still be released (word list changes)
            await context.abort(*_name_status(Invalid(f"release verdict={verdict}")))
        floor = max(1, self.clock_ms() - RELEASE_WINDOW_MS)  # never 0: 0 is the admin "no limit" sentinel
        await self._run(context, self.store.release_name(request.player_id, norm, floor), _name_status)
        return self._empty()

    async def batch_get_player_name(self, request, context, path):
        ids = list(dict.fromkeys(pid for pid in request.player_ids if pid))
        if len(ids) > NAME_BATCH_LIMIT:
            await context.abort(*_name_status(Invalid(f"{len(ids)} distinct ids, limit {NAME_BATCH_LIMIT}")))
        found = await self._run(context, self.store.player_names(ids), _name_status)
        return self.protocol.message("data_service.BatchGetPlayerNameResponse", names=found)

    # -- id segments ---------------------------------------------------------------------------------
    async def allocate_id_segment(self, request, context, path):
        """Always OK status; failures travel in ``error_code`` (Lo inclusive, Hi exclusive)."""
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
