"""Login's data_service client: account directory, names, home zones, player ids (Go ``shared/accountdirectory``)."""

import asyncio
import logging

from ..data.id_segments import IdSegments
from ..data.player_store import _sign_request

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
            raw = await self.call(path, _sign_request(request, path, self.secret, caller="login"),
                                  timeout=_DIRECTORY_TIMEOUT)
        except Exception as error:
            if not status_name(error):
                raise
            raise _classify(error, create=create) from error
        return self.protocol.parse(f"data_service.{response_type}", raw)

    async def read_account(self, account: str, create: bool) -> list:
        request = self.protocol.message("data_service.ReadAccountDirectoryRequest", account=account,
                                        create_if_missing=create)
        response = await self._signed("ReadAccountDirectory", request, "AccountDirectoryResponse")
        if not response.HasField("players"):
            raise DirectoryError("directory reply has no players")
        return list(response.players.players)

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

    async def reserve_name(self, player_id: int, name: str) -> tuple[int, int]:
        request = self.protocol.message("data_service.ReservePlayerNameRequest", player_id=player_id, name=name)
        raw = await self.call(_PATH + "ReservePlayerName", request.SerializeToString(), timeout=0.8)
        response = self.protocol.parse("data_service.ReservePlayerNameResponse", raw)
        return response.result, response.owner_player_id

    async def release_name(self, player_id: int, name: str) -> None:
        path = _PATH + "ReleasePlayerName"
        request = self.protocol.message("data_service.ReleasePlayerNameRequest", player_id=player_id, name=name)
        await self.call(path, _sign_request(request, path, self.secret, caller="login"), timeout=1.0)

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
