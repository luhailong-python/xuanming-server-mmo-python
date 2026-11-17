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
            if runtime.battle:
                infos.append(runtime.battle_info)
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
    server.add_argument("role", choices=("local", "gate", "scene", "battle"))
    server.add_argument("--config", type=Path)
    commands.add_parser("smoke", help="用真实 TCP 与 gRPC 运行本地端到端验证")
    ticket = commands.add_parser("gate-ticket", help="使用本机设置的密钥签发开发 Gate 票据")
    ticket.add_argument("--config", type=Path)
    portal = commands.add_parser("portal", help="启动 HTTP 门户")
    portal.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
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
        else:
            asyncio.run(run(args))
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
