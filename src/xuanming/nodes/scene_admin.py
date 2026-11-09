"""Scene 管理 RPC。鉴权、签名目标和输出上限沿用原版规则。"""

from __future__ import annotations

import logging
from dataclasses import replace

from google.protobuf import text_format

from ..node_link import _parse
from ..transport import RpcError, RpcErrorCode, RpcReply
from .gate_rpc import GmRequestVerifier

logger = logging.getLogger(__name__)

MAX_VALUE_BYTES = 4 * 1024
MAX_DESCRIPTION_BYTES = 64 * 1024
_VALUE_TYPES = frozenset({
    "Transform", "PlayerUint64Comp", "PlayerSkillListComp", "PlayerUint32Comp", "BaseAttributesComp",
    "LevelComp", "PlayerAttributeComp", "PlayerPetComp", "PlayerProfileComp", "PlayerAssetOpLedgerComp",
    "PlayerInboxLedgerComp", "CurrencyComp",
})


def _truncate(text: str, size: int) -> str:
    return text.encode("utf-8")[:size].decode("utf-8", errors="ignore")


class SceneAdmin:
    def __init__(self, scene, on_shutdown, *, verifier: GmRequestVerifier | None = None) -> None:
        self.scene, self.protocol, self.on_shutdown = scene, scene.protocol, on_shutdown
        self.verifier = verifier if verifier is not None else GmRequestVerifier()
        self.shutdown_requested = False

    def _authorize(self, method: str, request) -> str:
        operator, result = self.verifier.verify(
            "Scene." + method, self.scene.info.node_id, getattr(request, "operator"), request.reason,
        )
        if operator is None:
            logger.warning("Scene GM request rejected method=%s reason=%s", method, result)
            raise RpcError(RpcErrorCode.HANDLER_FAILED)
        return operator

    async def shutdown(self, body: bytes, *, on_authorized=None) -> RpcReply:
        request = _parse(self.protocol, "GmGracefulShutdownRequest", body)
        operator = self._authorize("GmGracefulShutdown", request)
        if self.shutdown_requested or self.scene._closing:
            raise RpcError(RpcErrorCode.NO_SERVICE)
        self.shutdown_requested = True
        if on_authorized is not None:
            on_authorized()
        count = len(self.scene.locations)
        logger.info("Scene GM shutdown requested operator=%r reason=%r players=%d", operator, request.reason[:128], count)
        response = self.protocol.message("GmGracefulShutdownResponse", affected_count=count)
        return RpcReply(response.SerializeToString(), self.on_shutdown)

    async def describe(self, body: bytes, *, on_authorized=None) -> bytes:
        request = _parse(self.protocol, "GmDescribeEntityRequest", body)
        operator = self._authorize("GmDescribeEntity", request)
        if request.reason != str(request.player_id):
            logger.warning("Scene GM describe rejected operator=%r player_id=%d reason=target_mismatch",
                           operator, request.player_id)
            raise RpcError(RpcErrorCode.HANDLER_FAILED)
        if self.shutdown_requested or self.scene._closing:
            raise RpcError(RpcErrorCode.NO_SERVICE)
        if on_authorized is not None:
            on_authorized()
        async with self.scene._lock(request.player_id):
            world = self.scene.locations.get(request.player_id)
            player = world.players.get(request.player_id) if world is not None else None
            if player is None:
                description, count = "player not on this node", 0
            else:
                description, count = self._description(player, request.include_values)
        logger.info("Scene GM describe operator=%r player_id=%d components=%d", operator, request.player_id, count)
        return self.protocol.message("GmDescribeEntityResponse", description=description).SerializeToString()

    def _description(self, player, include_values: bool) -> tuple[str, int]:
        snapshot = player.snapshot
        if player.pending_save is not None:
            snapshot = replace(snapshot, rows={**snapshot.rows, **player.pending_save.rows})
        data = self.scene.rows.assemble(snapshot)
        components = {value.DESCRIPTOR.name: value for field, value in data.ListFields() if field.message_type}
        # Transform 在待保存的场景位置中实时推进，读取不引发存盘。
        transform = self.protocol.message("Transform", **player.transform.wire())
        components[transform.DESCRIPTOR.name] = transform
        names = [type(player).__name__, type(player.motion).__name__, *sorted(components)]
        if player.pending_save is not None:
            names.append(type(player.pending_save).__name__)
        description = f"entity={player.entity} valid=1 components=[{','.join(names)}]"
        remaining = MAX_DESCRIPTION_BYTES - len(description.encode("utf-8"))
        if include_values:
            for name in sorted(components.keys() & _VALUE_TYPES):
                value = text_format.MessageToString(components[name], as_one_line=True, as_utf8=True).strip()
                line = "\n" + name + "=" + _truncate(value, MAX_VALUE_BYTES)
                rendered = _truncate(line, max(0, remaining))
                description += rendered
                remaining -= len(rendered.encode("utf-8"))
                if remaining <= 0:
                    break
        return _truncate(description, MAX_DESCRIPTION_BYTES), len(names)
