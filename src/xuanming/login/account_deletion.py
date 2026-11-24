"""账号注销编排服务（对应 Go ``pkg/accountdeletion/service.go``）。

状态与互斥全部落在 data_service 账号行上，本模块只负责：
- 申请/撤销注销调用与响应码映射；
- 申请进入冷静期后尽力吊销令牌家族并踢掉在线设备。
"""

from __future__ import annotations

import logging

from . import gate_commands, tips

logger = logging.getLogger(__name__)

# 申请来源（与 data_service DeletionSourceUser / DeletionSourceAdmin 同值）
SOURCE_USER = 1
SOURCE_ADMIN = 2

# data_service.AccountDeletionOutcome 枚举值
OUTCOME_OK = 1
OUTCOME_NOT_FOUND = 2
OUTCOME_BLOCKED = 3
OUTCOME_RETRY = 4
OUTCOME_TOO_LATE = 5

# data_service.AccountDeletionState 枚举值
STATE_NONE = 0
STATE_PENDING = 1
STATE_DELETING = 2


def mask_account(account: str) -> str:
    """日志脱敏：仅保留账号前 4 位与后 2 位。"""
    if len(account) <= 8:
        return "***"
    return f"{account[:4]}***{account[-2:]}"


def _tip_response(protocol, tip_id: int):
    return protocol.message("loginpb.AccountDeletionResponse", error_message={"id": tip_id})


def build_reply(protocol, ds_response):
    """将 ``data_service.AccountDeletionResponse`` 转换为 ``loginpb.AccountDeletionResponse``。"""
    lifecycle = ds_response.lifecycle
    response = protocol.message(
        "loginpb.AccountDeletionResponse",
        state=int(lifecycle.deletion_state),
        execute_after=int(lifecycle.deletion_execute_after_ms // 1000),
    )
    outcome = int(ds_response.outcome)
    if outcome == OUTCOME_OK:
        return response
    if outcome == OUTCOME_NOT_FOUND:
        response.error_message.id = tips.ACCOUNT_NOT_FOUND
        return response
    if outcome == OUTCOME_BLOCKED:
        response.error_message.id = tips.ACCOUNT_DELETION_BLOCKED
        for blocker in ds_response.blockers:
            response.blockers.add(player_id=blocker.player_id, reason=blocker.reason)
        return response
    if outcome == OUTCOME_TOO_LATE:
        response.error_message.id = tips.ACCOUNT_DELETION_IN_PROGRESS
        return response
    response.error_message.id = tips.DATA_SERIALIZE_FAILED
    return response


class AccountDeletionService:
    """账号注销申请与撤销编排器。"""

    def __init__(self, protocol, data, tokens, locator=None, publish=None, apple_revoker=None) -> None:
        self.protocol = protocol
        self.data = data
        self.tokens = tokens
        self.locator = locator
        self.publish = publish
        self.apple_revoker = apple_revoker

    async def request(self, account: str, source: int, *, is_apple: bool = False, apple_code: str = ""):
        """提交账号注销（用户本人或客服代办），进入冷静期后吊销全部令牌并踢线。"""
        grant = None
        if is_apple and self.apple_revoker is not None:
            try:
                grant = await self.apple_revoker.exchange_code(apple_code)
            except PermissionError:
                return _tip_response(self.protocol, tips.ACCOUNT_DELETION_REAUTH_REQUIRED)
            except Exception as error:
                logger.error("Apple authorization code exchange failed: %s", error)
                return _tip_response(self.protocol, tips.AUTH_PROVIDER_UNAVAILABLE)

        try:
            ds_reply = await self.data.request_account_deletion(account, source)
        except Exception as error:
            logger.error("RequestAccountDeletion failed account=%s err=%s", mask_account(account), error)
            return _tip_response(self.protocol, tips.DATA_SERIALIZE_FAILED)

        reply = build_reply(self.protocol, ds_reply)
        if int(ds_reply.outcome) != OUTCOME_OK or int(ds_reply.lifecycle.deletion_state) == STATE_NONE:
            return reply

        if grant is not None:
            try:
                await self.apple_revoker.revoke(grant)
            except Exception as error:
                logger.error("Apple token revoke failed after deletion scheduled account=%s err=%s",
                             mask_account(account), error)

        await self._sign_out(account)
        logger.info("Account deletion scheduled account=%s source=%d execute_after=%d",
                    mask_account(account), source, reply.execute_after)
        return reply

    async def cancel(self, account: str):
        """撤销冷静期内的账号注销。"""
        try:
            ds_reply = await self.data.cancel_account_deletion(account)
        except Exception as error:
            logger.error("CancelAccountDeletion failed account=%s err=%s", mask_account(account), error)
            return _tip_response(self.protocol, tips.DATA_SERIALIZE_FAILED)
        if int(ds_reply.outcome) == OUTCOME_OK:
            logger.info("Account deletion cancelled account=%s", mask_account(account))
        return build_reply(self.protocol, ds_reply)

    async def _sign_out(self, account: str) -> None:
        """尽力吊销令牌并踢出在线角色；失败仅记日志，冷静期拦截仍由目录状态兜底。"""
        if self.tokens is not None:
            try:
                await self.tokens.revoke_all(account)
            except Exception as error:
                logger.error("Revoke tokens failed during account deletion account=%s err=%s",
                             mask_account(account), error)
        if self.locator is None or self.publish is None:
            return
        try:
            players = await self.data.read_account(account, False)
        except Exception as error:
            logger.info("Read roles for sign-out kick failed account=%s err=%s", mask_account(account), error)
            return
        for role in players:
            try:
                session = await self.locator.get(role.player_id)
                if session is not None and session.gate_id and session.gate_instance_id:
                    built = gate_commands.kick_player(
                        self.protocol, session.gate_id, session.gate_instance_id,
                        session.session_id, role.player_id, session.login_generation + 1,
                    )
                    node_id, command = built
                    await self.publish(node_id, command, str(role.player_id).encode())
                    await self.locator.delete(role.player_id, session.session_id, session.gate_instance_id)
            except Exception as error:
                logger.info("Kick player failed during account deletion player_id=%d err=%s", role.player_id, error)
