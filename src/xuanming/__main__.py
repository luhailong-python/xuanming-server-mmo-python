import argparse
import asyncio
import base64
import json
import logging
import signal
import sys
from pathlib import Path

from .nodes.gate import issue_gate_ticket
from .protocol import Protocol
from .runtime import Runtime
from .settings import Settings, secret

logger = logging.getLogger(__name__)


async def run(args) -> None:
    settings = Settings.load(args.config)
    runtime = Runtime(settings, args.role)
    loop = asyncio.get_running_loop()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(runtime.request_shutdown))
    try:
        async with runtime:
            infos = [node.info for node in (runtime.gate, runtime.scene) if node]
            infos.extend(info for info in (runtime.battle_info, runtime.login_info) if info)
            for info in infos:
                logger.info(
                    "节点就绪 type=%d zone=%d node_id=%d tcp=%s:%d grpc_port=%d",
                    info.node_type, info.zone_id, info.node_id, info.endpoint.ip,
                    info.endpoint.port, info.grpc_endpoint.port,
                )
            await runtime.wait()
    finally:
        signal.signal(signal.SIGTERM, previous)


def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="玄冥 Python 服务端")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    commands = parser.add_subparsers(dest="command", required=True)
    server = commands.add_parser("run", help="启动服务端节点")
    server.add_argument("role", choices=("local", "gate", "scene", "battle", "login"))
    server.add_argument("--config", type=Path)
    commands.add_parser("smoke", help="用真实 TCP 与 gRPC 运行本地端到端验证")
    ticket = commands.add_parser("gate-ticket", help="使用本机设置的密钥签发开发 Gate 票据")
    ticket.add_argument("--config", type=Path)
    portal = commands.add_parser("portal", help="启动 HTTP 门户")
    portal.add_argument("--config", type=Path, required=True)
    data_service = commands.add_parser("data-service", help="启动 data_service（账号目录/路由/名字/号段）")
    data_service.add_argument("--config", type=Path, required=True)
    router = commands.add_parser("router", help="启动 client_rpc_router（Gate 客户端 RPC 路由服务）")
    router.add_argument("--config", type=Path, required=True)
    db_cmd = commands.add_parser("db", help="启动 db（PlayerStore 玩家存档服务）")
    db_cmd.add_argument("--config", type=Path, required=True)
    locator_cmd = commands.add_parser("player-locator", help="启动 player_locator（玩家在线态与租约定位服务）")
    locator_cmd.add_argument("--config", type=Path, required=True)
    inbox_cmd = commands.add_parser("player-inbox", help="启动 player_inbox（玩家可靠收件箱服务）")
    inbox_cmd.add_argument("--config", type=Path, required=True)
    sm_cmd = commands.add_parser("scene-manager", help="启动 scene_manager（场景生命周期与路由调度服务）")
    sm_cmd.add_argument("--config", type=Path, required=True)
    match_cmd = commands.add_parser("match", help="启动 match（组队、匹配、战斗房间分配与结算协调服务）")
    match_cmd.add_argument("--config", type=Path, required=True)
    cg_cmd = commands.add_parser("content-guard", help="启动 content_guard（文本敏感词与广告审核服务）")
    cg_cmd.add_argument("--config", type=Path, required=True)
    chat_cmd = commands.add_parser("chat", help="启动 chat（世界与私聊权威服务）")
    chat_cmd.add_argument("--config", type=Path, required=True)
    mail_cmd = commands.add_parser("mail", help="启动 mail（邮件与附件发放服务）")
    mail_cmd.add_argument("--config", type=Path, required=True)
    rank_cmd = commands.add_parser("rank", help="启动 rank（排行榜与结算发奖服务）")
    rank_cmd.add_argument("--config", type=Path, required=True)
    friend_cmd = commands.add_parser("friend", help="启动 friend（好友关系、黑名单与在线推荐服务）")
    friend_cmd.add_argument("--config", type=Path, required=True)
    payment_cmd = commands.add_parser("payment", help="启动 payment（充值意图、渠道回调与退款服务）")
    payment_cmd.add_argument("--config", type=Path, required=True)
    trade_cmd = commands.add_parser("trade", help="启动 trade（聚宝斋目录、寄售/市集/拍卖与资产交割服务）")
    trade_cmd.add_argument("--config", type=Path, required=True)
    guild_cmd = commands.add_parser("guild", help="启动 guild（公会、成员权限、排行与活动服务）")
    guild_cmd.add_argument("--config", type=Path, required=True)
    migrate_cmd = commands.add_parser("schemamigrate", help="校验并执行全库 SQL Schema 幂等迁移", add_help=False)
    sync_cmd = commands.add_parser("table-sync", help="打包、校验或解包配置表与指针文件", add_help=False)
    args, extra = parser.parse_known_args()
    if args.command not in ("schemamigrate", "table-sync") and extra:
        parser.error(f"unrecognized arguments: {' '.join(extra)}")
    logging.basicConfig(
        level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        if args.command == "smoke":
            from .smoke import smoke

            for result in asyncio.run(smoke()):
                print(result)
        elif args.command == "gate-ticket":
            settings = Settings.load(args.config)
            settings.validate("local")
            request = issue_gate_ticket(
                Protocol(), settings.gate.node_id, settings.zone_id, secret("MMORPG_GATE_TOKEN_SECRET"),
            )
            print(json.dumps({
                "payload_b64": base64.b64encode(request.payload).decode(),
                "signature_hex": request.signature.decode(),
            }))
        elif args.command == "portal":
            from .portal import load_settings, run_portal

            asyncio.run(run_portal(load_settings(args.config)))
        elif args.command == "data-service":
            from .data_service.node import run as run_data_service

            asyncio.run(run_data_service(Settings.load(args.config)))
        elif args.command == "router":
            from .client_rpc_router.node import run as run_router

            asyncio.run(run_router(Settings.load(args.config)))
        elif args.command == "db":
            from .db.node import run as run_db

            asyncio.run(run_db(Settings.load(args.config)))
        elif args.command == "player-locator":
            from .player_locator.node import run as run_locator

            asyncio.run(run_locator(Settings.load(args.config)))
        elif args.command == "player-inbox":
            from .player_inbox.node import run as run_inbox

            asyncio.run(run_inbox(Settings.load(args.config)))
        elif args.command == "scene-manager":
            from .scene_manager.node import run as run_scene_manager

            asyncio.run(run_scene_manager(Settings.load(args.config)))
        elif args.command == "match":
            from .match.node import run as run_match

            asyncio.run(run_match(Settings.load(args.config)))
        elif args.command == "content-guard":
            from .content_guard.node import run as run_content_guard

            asyncio.run(run_content_guard(Settings.load(args.config)))
        elif args.command == "chat":
            from .chat.node import run as run_chat

            asyncio.run(run_chat(Settings.load(args.config)))
        elif args.command == "mail":
            from .mail.node import run as run_mail

            asyncio.run(run_mail(Settings.load(args.config)))
        elif args.command == "rank":
            from .rank.node import run as run_rank

            asyncio.run(run_rank(Settings.load(args.config)))
        elif args.command == "friend":
            from .friend.node import run as run_friend

            asyncio.run(run_friend(Settings.load(args.config)))
        elif args.command == "payment":
            from .payment.node import run as run_payment

            asyncio.run(run_payment(Settings.load(args.config)))
        elif args.command == "trade":
            from .trade.node import run as run_trade

            asyncio.run(run_trade(Settings.load(args.config)))
        elif args.command == "guild":
            from .guild.node import run as run_guild

            asyncio.run(run_guild(Settings.load(args.config)))
        elif args.command == "schemamigrate":
            from .ops.schemamigrate import main as run_schemamigrate

            raise SystemExit(run_schemamigrate(extra))
        elif args.command == "table-sync":
            from .ops.table_sync import main as run_table_sync

            raise SystemExit(run_table_sync(extra))
        else:
            asyncio.run(run(args))
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
