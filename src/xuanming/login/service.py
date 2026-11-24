"""Login 节点服务上下文与 gRPC 接口装配（对应 Go ``svc`` + ``internal/server``）。"""

from __future__ import annotations

import asyncio
import base64
import logging
from dataclasses import dataclass, field

import grpc

from . import gate_commands
from .account_deletion import AccountDeletionService
from .caller_auth import SESSION_DETAIL, AuthError, CallerVerifier, parse_details
from .config import LoginConfig
from .directory import DataService
from .logic import (
    RpcAbort,
    account_deletion,
    assign_gate,
    create_player,
    disconnect,
    enter_game,
    get_zone_role_count,
    leave_game,
    login,
    login_admin,
    refresh_token,
    renew_session,
    request_gate_migration,
    submit_real_name,
)
from .sessions import PlayerLocator
from .tokens import TokenManager

logger = logging.getLogger(__name__)
_SHUTDOWN_DRAIN = 15


@dataclass(eq=False)
class ServiceContext:
    protocol: object
    config: LoginConfig
    login_redis: object
    player_redis: object
    tokens: TokenManager
    locator: PlayerLocator
    data: DataService
    scene_manager_call: object
    publish: object  # async (gate_node_id, command, key) -> None
    directory: object  # 节点发现目录（查找本区 gate）
    tasks: set = field(default_factory=set)
    # 生产完整功能的可选注入组件（默认 None，保持旧调用方与基础单测零侵入兼容）
    control_redis: object = None
    login_queue: object = None
    password_provider: object = None
    anti_addiction: object = None
    account_deletion: object = None
    content_guard_call: object = None
    merged_zones: dict[int, int] = field(default_factory=dict)
    zone_capacity_fn: object = None

    async def send_gate(self, built, player_id: int) -> None:
        node_id, command = built
        await self.publish(node_id, command, str(player_id).encode())

    def submit(self, coroutine) -> bool:
        """在受限并发池中异步执行 EnterGame 链；池满时关闭协程并返回 False。"""
        if len(self.tasks) >= self.config.preload_pool_size:
            coroutine.close()
            return False
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return True

    async def close(self) -> None:
        if self.tasks:
            _, pending = await asyncio.wait(set(self.tasks), timeout=_SHUTDOWN_DRAIN)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    def push_tip(self, *args, **kwargs):
        return gate_commands.push_tip(self.protocol, *args, **kwargs)

    def account_deletion_service(self) -> AccountDeletionService | None:
        """返回已注入的注销服务；未显式注入但具备 ``data`` 与 ``tokens`` 时按需构造默认实例。"""
        if self.account_deletion is not None:
            return self.account_deletion
        if self.data is None:
            return None
        return AccountDeletionService(self.protocol, self.data, self.tokens, self.locator, self.publish)

    def resolve_target_zone(self, zone_id: int) -> int:
        """解析合服后的最终归属区（沿 ``merged_zones`` 链路追踪并防环）。"""
        current = int(zone_id)
        if not self.merged_zones:
            return current
        visited: set[int] = set()
        while current in self.merged_zones and current not in visited:
            visited.add(current)
            nxt = int(self.merged_zones[current])
            if not nxt:
                break
            current = nxt
        return current


class LoginService:
    def __init__(self, ctx: ServiceContext, verifier: CallerVerifier, *, kill_switch=None) -> None:
        self.ctx = ctx
        self.verifier = verifier
        self.kill_switch = kill_switch

    def services(self) -> dict:
        wrap = self._wrap
        admin = self._wrap_admin
        return {
            "loginpb.ClientPlayerLogin": {
                "Login": wrap(login.run),
                "CreatePlayer": wrap(create_player.run),
                "EnterGame": wrap(enter_game.run),
                "LeaveGame": wrap(leave_game.run),
                "Disconnect": wrap(disconnect.run),
                "RefreshToken": wrap(refresh_token.run),
                "RenewSession": wrap(renew_session.run),
                "RequestGateMigration": wrap(request_gate_migration.run),
            },
            "loginpb.LoginPreGate": {
                "AssignGate": wrap(assign_gate.run),
                "QueryQueueStatus": wrap(assign_gate.query_queue_status),
                "GetZoneRoleCount": wrap(get_zone_role_count.run),
                "SubmitRealName": wrap(submit_real_name.run),
                "RequestAccountDeletion": wrap(account_deletion.request_deletion),
                "CancelAccountDeletion": wrap(account_deletion.cancel_deletion),
            },
            "loginpb.LoginAdmin": {
                "RemovePlayersFromAccounts": admin(login_admin.remove_players_from_accounts),
                "ScheduleAccountDeletion": admin(login_admin.schedule_account_deletion),
                "ResetAccountRealName": admin(login_admin.reset_account_real_name),
            },
        }

    async def _check_kill_switch(self, context, path: str) -> None:
        if self.kill_switch is None:
            return
        parts = path.lstrip("/").split("/", 1)
        if len(parts) != 2:
            return
        service_name, method_name = parts
        rule = self.kill_switch.check(service_name, method_name)
        if rule is not None:
            code = next((item for item in grpc.StatusCode if item.value[0] == rule.grpc_code),
                        grpc.StatusCode.UNAVAILABLE)
            reason = f", reason: {rule.reason}" if rule.reason else ""
            await context.abort(code, f"killswitch: method {path} is disabled{reason}")

    def _wrap(self, logic):
        async def handler(request, context, path):
            await self._check_kill_switch(context, path)
            try:
                raw = self.verifier.authenticate(context.invocation_metadata(), path)
                details = parse_details(self.ctx.protocol, raw) if raw is not None else None
            except AuthError as error:
                logger.error("Internal caller identity verification failed method=%s reason=%s", path, error)
                await context.abort(grpc.StatusCode.UNAUTHENTICATED,
                                    f"Internal caller identity signature verification failed ({error})")
            if raw is not None:
                # gate 校验该回显头后才会信任应答
                await context.send_initial_metadata(((SESSION_DETAIL, base64.b64encode(raw)),))
            try:
                return await logic(self.ctx, request, details)
            except RpcAbort as error:
                await context.abort(error.code, error.detail)

        return handler

    def _wrap_admin(self, logic):
        async def handler(request, context, path):
            await self._check_kill_switch(context, path)
            try:
                login_admin.authorize_admin(context.invocation_metadata(), self.ctx.config.admin_token, path)
                return await logic(self.ctx, request, None)
            except RpcAbort as error:
                await context.abort(error.code, error.detail)

        return handler
