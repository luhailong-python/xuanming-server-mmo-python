"""Login node: service context and gRPC surface (Go ``svc`` + ``internal/server``)."""

import asyncio
import base64
import logging
from dataclasses import dataclass, field

import grpc

from . import gate_commands
from .caller_auth import SESSION_DETAIL, AuthError, CallerVerifier, parse_details
from .config import LoginConfig
from .directory import DataService
from .logic import assign_gate, create_player, disconnect, enter_game, leave_game, login, refresh_token, \
    renew_session
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
    directory: object  # node directory used to find gates
    tasks: set = field(default_factory=set)

    async def send_gate(self, built, player_id: int) -> None:
        node_id, command = built
        await self.publish(node_id, command, str(player_id).encode())

    def submit(self, coroutine) -> bool:
        """Run an EnterGame chain in the background; False when the pool is saturated."""
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


class LoginService:
    def __init__(self, ctx: ServiceContext, verifier: CallerVerifier) -> None:
        self.ctx = ctx
        self.verifier = verifier

    def services(self) -> dict:
        wrap = self._wrap
        return {
            "loginpb.ClientPlayerLogin": {
                "Login": wrap(login.run), "CreatePlayer": wrap(create_player.run),
                "EnterGame": wrap(enter_game.run), "LeaveGame": wrap(leave_game.run),
                "Disconnect": wrap(disconnect.run), "RefreshToken": wrap(refresh_token.run),
                "RenewSession": wrap(renew_session.run), "RequestGateMigration": wrap(_migration_disabled),
            },
            "loginpb.LoginPreGate": {
                "AssignGate": wrap(assign_gate.run), "QueryQueueStatus": wrap(assign_gate.query_queue_status),
            },
        }

    def _wrap(self, logic):
        async def handler(request, context, path):
            try:
                raw = self.verifier.authenticate(context.invocation_metadata(), path)
                details = parse_details(self.ctx.protocol, raw) if raw is not None else None
            except AuthError as error:
                logger.error("内部调用方身份验签失败 method=%s reason=%s", path, error)
                await context.abort(grpc.StatusCode.UNAUTHENTICATED,
                                    f"Internal caller identity signature verification failed ({error})")
            if raw is not None:
                # The gate checks this echo before trusting the reply.
                await context.send_initial_metadata(((SESSION_DETAIL, base64.b64encode(raw)),))
            return await logic(self.ctx, request, details)

        return handler


async def _migration_disabled(ctx, request, details):
    # shortcut: gate drain migration is not ported; gates keep their sessions until they close.
    return ctx.protocol.message("loginpb.GateMigrationResponse", result=4)
