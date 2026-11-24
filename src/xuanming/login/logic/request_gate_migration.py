"""RequestGateMigration（对应 Go ``requestgatemigrationlogic.go``）。"""

from .. import gate_drain


async def run(ctx, request, details):
    """仅由 gate 合成；缺失已验签会话头时返回 UNSPECIFIED 让 gate 稍后重试。"""
    if details is None or not details.session_id:
        return ctx.protocol.message("loginpb.GateMigrationResponse", result=gate_drain.MIGRATION_UNSPECIFIED)
    return await gate_drain.sign_gate_migration(ctx, details)
