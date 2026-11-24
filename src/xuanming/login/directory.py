"""Login's data_service client: account directory, names, home zones, player ids (Go ``shared/accountdirectory``)."""

import asyncio
import logging
import time

from ..data.id_segments import IdSegments
from ..grpc_runtime import sign_service_call

logger = logging.getLogger(__name__)

_PATH = "/data_service.DataService/"
_DIRECTORY_TIMEOUT = 2.5
RESERVE_OK, RESERVE_TAKEN, RESERVE_INVALID = 0, 1, 2


class DirectoryError(Exception):
    """Base: the directory outcome is unknown or the service is unavailable."""


class Missing(DirectoryError):
    pass


class Conflict(DirectoryError):
    pass


class Full(DirectoryError):
    pass


class Invalid(DirectoryError):
    pass


class NotCommitted(DirectoryError):
    pass


def status_name(error) -> str:
    code = getattr(error, "code", None)
    return code().name if callable(code) else ""


def _classify(error, *, create=False) -> DirectoryError:
    name = status_name(error)
    if create and name == "ABORTED":
        return NotCommitted(name)
    kind = {"NOT_FOUND": Missing, "ALREADY_EXISTS": Conflict, "FAILED_PRECONDITION": Conflict,
            "RESOURCE_EXHAUSTED": Full, "INVALID_ARGUMENT": Invalid}.get(name, DirectoryError)
    return kind(name or str(error))


class DataService:
    def __init__(self, protocol, call, secret: bytes, *, id_step: int = 100) -> None:
        self.protocol = protocol
        self.call = call
        self.secret = secret
        self.mint_player_id = IdSegments.remote(protocol, call, "player")
        self.mint_player_id.size = id_step

    async def _signed(self, method: str, request, response_type: str, *, create=False):
        path = _PATH + method
        try:
            sign_service_call(path, request, "login", time.time_ns() // 1_000_000, self.secret)
            raw = await self.call(path, request.SerializeToString(deterministic=True),
                                  timeout=_DIRECTORY_TIMEOUT)
        except Exception as error:
            if not status_name(error):
                raise
            raise _classify(error, create=create) from error
        return self.protocol.parse(f"data_service.{response_type}", raw)

    async def read_account_with_lifecycle(self, account: str, create: bool):
        """读取账号目录，同时返回角色列表、实名 pi 与注销生命周期（Go ``ReadWithLifecycle``）。"""
        request = self.protocol.message("data_service.ReadAccountDirectoryRequest", account=account,
                                        create_if_missing=create)
        response = await self._signed("ReadAccountDirectory", request, "AccountDirectoryResponse")
        if not response.HasField("players"):
            raise DirectoryError("directory reply has no players")
        return list(response.players.players), response.real_name_pi, response.lifecycle

    async def read_account(self, account: str, create: bool) -> list:
        players, _, _ = await self.read_account_with_lifecycle(account, create)
        return players

    async def create_role(self, account: str, role) -> list:
        request = self.protocol.message("data_service.CreateAccountRoleRequest", account=account, role=role)
        response = await self._signed("CreateAccountRole", request, "AccountDirectoryResponse", create=True)
        if not response.HasField("players"):
            raise DirectoryError("directory reply has no players")
        return list(response.players.players)

    async def lookup_role(self, player_id: int):
        request = self.protocol.message("data_service.LookupAccountRoleRequest", player_id=player_id)
        owned = await self._signed("LookupAccountRole", request, "AccountRoleOwnership")
        if (owned.role.player_id != player_id or not owned.owner_revision
                or not 0 < len(owned.account.encode()) <= 191):
            raise DirectoryError("directory ownership reply invalid")
        return owned

    async def remove_account_roles(self, account: str, roles: list[tuple[int, int]]) -> None:
        """按 (player_id, owner_revision) 列表摘除账号目录中的角色（Go ``AccountDirectory.Remove``）。"""
        role_versions = [
            self.protocol.message("data_service.AccountRoleVersion", player_id=pid, owner_revision=rev)
            for pid, rev in roles
        ]
        request = self.protocol.message("data_service.RemoveAccountRolesRequest", account=account, roles=role_versions)
        path = _PATH + "RemoveAccountRoles"
        try:
            sign_service_call(path, request, "login", time.time_ns() // 1_000_000, self.secret)
            await self.call(path, request.SerializeToString(deterministic=True), timeout=_DIRECTORY_TIMEOUT)
        except Exception as error:
            if not status_name(error):
                raise
            raise _classify(error) from error

    async def set_real_name(self, account: str, real_name_pi: str) -> str:
        """写入账号实名 pi（只写一次，返回库中实际存留的 pi，Go ``SetAccountRealName``）。"""
        request = self.protocol.message("data_service.SetAccountRealNameRequest", account=account,
                                        real_name_pi=real_name_pi)
        response = await self._signed("SetAccountRealName", request, "SetAccountRealNameResponse")
        return response.real_name_pi

    async def reset_real_name(self, account: str) -> bool:
        """客服重置账号实名，返回重置前是否存在实名（Go ``ResetAccountRealName``）。"""
        request = self.protocol.message("data_service.ResetAccountRealNameRequest", account=account)
        response = await self._signed("ResetAccountRealName", request, "ResetAccountRealNameResponse")
        return bool(response.had_real_name)

    async def request_account_deletion(self, account: str, source: int):
        """发起账号注销（source: 1=用户本人，2=客服代办，Go ``RequestAccountDeletion``）。"""
        request = self.protocol.message("data_service.RequestAccountDeletionRequest", account=account, source=source)
        return await self._signed("RequestAccountDeletion", request, "AccountDeletionResponse")

    async def cancel_account_deletion(self, account: str):
        """撤销冷静期内的账号注销（Go ``CancelAccountDeletion``）。"""
        request = self.protocol.message("data_service.CancelAccountDeletionRequest", account=account)
        return await self._signed("CancelAccountDeletion", request, "AccountDeletionResponse")

    async def count_zone_roles(self, zone_id: int) -> int:
        """查询本区（含合服最终归属）累计创建角色数（Go ``CountZoneRoles``）。"""
        request = self.protocol.message("data_service.CountZoneRolesRequest", zone_id=zone_id)
        raw = await self.call(_PATH + "CountZoneRoles", request.SerializeToString(), timeout=3.0)
        return int(self.protocol.parse("data_service.CountZoneRolesResponse", raw).roles)

    async def get_player_asset_op_ledger(self, player_id: int) -> bool:
        """查询 player_store 中是否存在该角色的资产账本行（用于孤儿角色核验）。"""
        request = self.protocol.message("data_service.GetPlayerAssetOpLedgerRequest", player_id=player_id)
        raw = await self.call(_PATH + "GetPlayerAssetOpLedger", request.SerializeToString(), timeout=2.5)
        return bool(self.protocol.parse("data_service.GetPlayerAssetOpLedgerResponse", raw).found)

    async def get_player_home_zone(self, player_id: int) -> int:
        """查询单个玩家当前归属区；未映射（NOT_FOUND 或老版文案）统一返回 0。"""
        request = self.protocol.message("data_service.GetPlayerHomeZoneRequest", player_id=player_id)
        try:
            raw = await self.call(_PATH + "GetPlayerHomeZone", request.SerializeToString(), timeout=1.5)
        except Exception as error:
            if status_name(error) == "NOT_FOUND" or "no home zone mapping" in str(error):
                return 0
            raise
        return int(self.protocol.parse("data_service.GetPlayerHomeZoneResponse", raw).home_zone_id)

    async def get_player_home_zone_strict(self, player_id: int) -> int:
        """查询玩家归属区；路由缺失时抛出 ``Missing``（供孤儿角色核验区分路由缺失与存储故障）。"""
        request = self.protocol.message("data_service.GetPlayerHomeZoneRequest", player_id=player_id)
        try:
            raw = await self.call(_PATH + "GetPlayerHomeZone", request.SerializeToString(), timeout=2.5)
        except Exception as error:
            if status_name(error) == "NOT_FOUND":
                raise Missing("player route missing") from error
            raise
        return int(self.protocol.parse("data_service.GetPlayerHomeZoneResponse", raw).home_zone_id)

    async def get_merged_zones(self) -> dict[int, int]:
        """读取合服映射（源区 -> 目标区，Go ``GetMergedZones``）。"""
        request = self.protocol.message("google.protobuf.Empty")
        raw = await self.call(_PATH + "GetMergedZones", request.SerializeToString(), timeout=1.5)
        return dict(self.protocol.parse("data_service.GetMergedZonesResponse", raw).merged_into)

    async def reserve_name(self, player_id: int, name: str) -> tuple[int, int]:
        request = self.protocol.message("data_service.ReservePlayerNameRequest", player_id=player_id, name=name)
        raw = await self.call(_PATH + "ReservePlayerName", request.SerializeToString(), timeout=0.8)
        response = self.protocol.parse("data_service.ReservePlayerNameResponse", raw)
        return response.result, response.owner_player_id

    async def release_name(self, player_id: int, name: str) -> None:
        path = _PATH + "ReleasePlayerName"
        request = self.protocol.message("data_service.ReleasePlayerNameRequest", player_id=player_id, name=name)
        sign_service_call(path, request, "login", time.time_ns() // 1_000_000, self.secret)
        await self.call(path, request.SerializeToString(deterministic=True), timeout=1.0)

    async def player_names(self, player_ids) -> dict[int, str]:
        request = self.protocol.message("data_service.BatchGetPlayerNameRequest", player_ids=player_ids)
        raw = await self.call(_PATH + "BatchGetPlayerName", request.SerializeToString(), timeout=0.5)
        return dict(self.protocol.parse("data_service.BatchGetPlayerNameResponse", raw).names)

    async def register_player_zone(self, player_id: int, zone: int, storage_id: int) -> None:
        request = self.protocol.message("data_service.RegisterPlayerZoneRequest", player_id=player_id,
                                        home_zone_id=zone, storage_id=storage_id)
        await self.call(_PATH + "RegisterPlayerZone", request.SerializeToString(), timeout=0.8)

    async def home_zones(self, player_ids) -> dict[int, int]:
        request = self.protocol.message("data_service.BatchGetPlayerHomeZoneRequest", player_ids=player_ids)
        raw = await self.call(_PATH + "BatchGetPlayerHomeZone", request.SerializeToString(), timeout=0.5)
        return dict(self.protocol.parse("data_service.BatchGetPlayerHomeZoneResponse", raw).player_zone_map)

    async def role_list(self, players, *, refresh_zones: bool = True) -> list:
        """Client role list: current home zone (fail-open), removed appearances dropped, missing names filled."""
        players = [player for player in players if player.appearance_id != REMOVED_APPEARANCE]
        if players and refresh_zones:
            try:
                zones = await self.home_zones([player.player_id for player in players])
                for player in players:
                    if zones.get(player.player_id):
                        player.zone_id = zones[player.player_id]
            except Exception:
                logger.info("角色列表归属区查询失败，按建角区返回", exc_info=True)
        unnamed = [player.player_id for player in players if not player.name]
        if unnamed:
            try:
                names = await self.player_names(unnamed)
                for player in players:
                    if not player.name and names.get(player.player_id):
                        player.name = names[player.player_id]
            except Exception:
                logger.info("角色列表名字回源失败，本次不补名", exc_info=True)
        return [self.protocol.message("loginpb.AccountSimplePlayerWrapper", player=player) for player in players]


REMOVED_APPEARANCE = "06_thunder_caster_boy"


async def delayed(seconds: float, coroutine_factory) -> None:
    await asyncio.sleep(seconds)
    await coroutine_factory()
