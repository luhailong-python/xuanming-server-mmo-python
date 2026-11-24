"""GetZoneRoleCount（对应 Go ``getzonerolecountlogic.go``）。"""

import logging

import grpc

from . import RpcAbort

logger = logging.getLogger(__name__)


async def run(ctx, request, details):
    zone = ctx.config.zone_id
    if request.zone_id != zone:
        raise RpcAbort(
            grpc.StatusCode.INVALID_ARGUMENT,
            f"zone role count asked for zone {request.zone_id} at login of zone {zone}",
        )
    if ctx.data is None:
        raise RpcAbort(grpc.StatusCode.UNAVAILABLE, "zone role count unavailable: data_service not configured")
    try:
        roles = await ctx.data.count_zone_roles(zone)
    except Exception as error:
        logger.error("GetZoneRoleCount zone=%d failed: %s", zone, error)
        raise RpcAbort(grpc.StatusCode.UNAVAILABLE, f"zone role count unavailable: {error}") from error
    return ctx.protocol.message("loginpb.ZoneRoleCountResponse", roles=roles)
