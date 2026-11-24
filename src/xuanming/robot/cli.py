"""机器人命令行入口：支持 ``local-smoke``、``smoke`` 与 ``stress`` 子命令。"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
import json
import os
import sys
from typing import Any

from ..nodes.gate import issue_gate_ticket
from ..protocol import Protocol
from .client import GateRobotClient, PortalHttpClient, RobotError, decode_gate_ticket
from .scenarios import (
    SMOKE_SCENARIOS,
    ScenarioResult,
    StressReport,
    run_local_smoke,
    run_local_stress,
    run_smoke_suite,
    run_stress,
)


def _parse_scenarios(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    items = [part.strip() for part in raw.split(",") if part.strip()]
    return items or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="xuanming-robot",
        description="玄冥服务端纯 Python 机器人冒烟与压测工具",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    local_smoke = sub.add_parser("local-smoke", help="启动进程内 local 运行时并执行全套冒烟")
    local_smoke.add_argument("--websocket", action="store_true", help="通过 WebSocket 二进制帧接入 Gate")
    local_smoke.add_argument("--scenarios", default=None, help="逗号分隔的冒烟场景名列表")
    local_smoke.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON 格式结果")

    smoke = sub.add_parser("smoke", help="连接指定 Gate/Portal 执行冒烟套件")
    smoke.add_argument("--gate-host", default="", help="Gate 主机地址")
    smoke.add_argument("--gate-port", type=int, default=0, help="Gate 端口")
    smoke.add_argument("--portal-url", default="", help="可选 Portal HTTP 地址（用于登录与分配 Gate 票据）")
    smoke.add_argument("--zone-id", type=int, default=1, help="区服编号")
    smoke.add_argument("--gate-node-id", type=int, default=1, help="直连签票使用的 Gate 节点编号")
    smoke.add_argument("--account", default="robot-smoke", help="测试账号")
    smoke.add_argument("--password", default="", help="登录密码（或开发令牌）")
    smoke.add_argument("--access-token", default="", help="登录访问令牌")
    smoke.add_argument("--ticket-secret", default="", help="直连签票密钥（默认读 MMORPG_GATE_TOKEN_SECRET）")
    smoke.add_argument("--websocket", action="store_true", help="通过 WebSocket 接入 Gate")
    smoke.add_argument("--websocket-url", default="", help="显式指定 Gate WebSocket URL")
    smoke.add_argument("--scenarios", default=None, help="逗号分隔的冒烟场景名列表")
    smoke.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON 格式结果")

    stress = sub.add_parser("stress", help="执行多并发机器人压测")
    stress.add_argument("--local", action="store_true", help="启动进程内 local 运行时执行压测")
    stress.add_argument("--gate-host", default="", help="Gate 主机地址")
    stress.add_argument("--gate-port", type=int, default=0, help="Gate 端口")
    stress.add_argument("--portal-url", default="", help="可选 Portal HTTP 地址")
    stress.add_argument("--zone-id", type=int, default=1, help="区服编号")
    stress.add_argument("--gate-node-id", type=int, default=1, help="直连签票使用的 Gate 节点编号")
    stress.add_argument("--access-token", default="", help="登录访问令牌")
    stress.add_argument("--ticket-secret", default="", help="直连签票密钥（默认读 MMORPG_GATE_TOKEN_SECRET）")
    stress.add_argument("--concurrency", type=int, default=4, help="并发机器人数")
    stress.add_argument("--rounds", type=int, default=3, help="单机器人压测轮次")
    stress.add_argument("--websocket", action="store_true", help="通过 WebSocket 接入 Gate")
    stress.add_argument("--websocket-url", default="", help="显式指定 Gate WebSocket URL")
    stress.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON 格式结果")

    return parser


def _print_smoke(results: list[ScenarioResult], *, as_json: bool) -> int:
    passed = all(item.passed for item in results)
    if as_json:
        payload: dict[str, Any] = {
            "ok": passed,
            "total": len(results),
            "passed": sum(1 for item in results if item.passed),
            "failed": sum(1 for item in results if not item.passed),
            "scenarios": [item.to_dict() for item in results],
        }
        print(json.dumps(payload, ensure_ascii=False))
    else:
        for item in results:
            status = "PASS" if item.passed else "FAIL"
            extra = item.detail if item.passed else item.error
            print(f"[{status}] {item.name} ({item.duration_ms:.2f}ms) {extra}".rstrip())
    return 0 if passed else 1


def _print_stress(report: StressReport, *, as_json: bool) -> int:
    if as_json:
        print(json.dumps(report.to_dict(), ensure_ascii=False))
    else:
        print(
            f"stress concurrency={report.concurrency} rounds={report.rounds} "
            f"total_ops={report.total_ops} errors={report.errors} "
            f"throughput={report.throughput_ops_sec:.2f} ops/s "
            f"p50={report.p50_ms:.2f}ms p95={report.p95_ms:.2f}ms p99={report.p99_ms:.2f}ms"
        )
    return 0 if report.errors == 0 else 1


async def _run_remote_smoke(args: argparse.Namespace) -> list[ScenarioResult]:
    protocol = Protocol()
    scenarios = _parse_scenarios(args.scenarios)
    host = args.gate_host
    port = args.gate_port
    ws_url = args.websocket_url
    access_token = args.access_token or os.getenv("XUANMING_DEV_LOGIN_TOKEN", "")
    password = args.password
    payload = b""
    signature = b""

    if args.portal_url:
        async with PortalHttpClient(args.portal_url) as portal:
            if not access_token:
                login_resp = await portal.login(
                    zone_id=args.zone_id,
                    account=args.account,
                    password=password,
                )
                if login_resp.get("code") != 0:
                    raise RobotError(f"Portal login failed: code={login_resp.get('code')}")
                access_token = login_resp.get("access_token", "")
            assign = await portal.assign_gate(zone_id=args.zone_id, access_token=access_token)
            if assign.get("code") != 0:
                raise RobotError(f"Portal assign-gate failed: code={assign.get('code')} error={assign.get('error')}")
            payload, signature = decode_gate_ticket(assign)
            host = host or assign.get("gate_ip", "")
            port = port or int(assign.get("gate_port", 0))
            if args.websocket and not ws_url:
                ws_url = assign.get("gate_ws_url", "")
    else:
        secret_text = args.ticket_secret or os.getenv("MMORPG_GATE_TOKEN_SECRET", "")
        if len(secret_text.encode("utf-8")) < 32:
            raise RobotError("Remote smoke without --portal-url requires --ticket-secret (>= 32 bytes)")
        ticket = issue_gate_ticket(protocol, args.gate_node_id, args.zone_id, secret_text.encode("utf-8"))
        payload, signature = ticket.payload, ticket.signature

    if not args.websocket and (not host or port <= 0):
        raise RobotError("Remote smoke requires valid --gate-host and --gate-port (or --portal-url)")
    if args.websocket and not ws_url:
        if not host or port <= 0:
            raise RobotError("Remote WebSocket smoke requires --websocket-url or --gate-host/--gate-port")
        ws_url = f"ws://{host}:{port}"

    async with GateRobotClient(protocol) as client:
        if args.websocket:
            await client.connect_websocket(ws_url)
        else:
            await client.connect_tcp(host, port)
        await client.verify_ticket(payload, signature)
        await client.login_and_enter(args.account, access_token, password=password)
        results = await run_smoke_suite(client, scenarios=scenarios)
        await client.leave_game()
    return results


async def _run_remote_stress(args: argparse.Namespace) -> StressReport:
    protocol = Protocol()
    host = args.gate_host
    port = args.gate_port
    ws_url = args.websocket_url
    access_token = args.access_token or os.getenv("XUANMING_DEV_LOGIN_TOKEN", "")

    if args.portal_url:
        async with PortalHttpClient(args.portal_url) as portal:
            first_assign = await portal.assign_gate(zone_id=args.zone_id, access_token=access_token)
            if first_assign.get("code") != 0:
                raise RobotError(f"Portal assign-gate failed: code={first_assign.get('code')}")
            host = host or first_assign.get("gate_ip", "")
            port = port or int(first_assign.get("gate_port", 0))
            if args.websocket and not ws_url:
                ws_url = first_assign.get("gate_ws_url", "")

            async def portal_issuer():
                resp = await portal.assign_gate(zone_id=args.zone_id, access_token=access_token)
                if resp.get("code") != 0:
                    raise RobotError(f"Portal assign-gate failed: code={resp.get('code')}")
                return decode_gate_ticket(resp)

            return await run_stress(
                host=host,
                port=port,
                ticket_issuer=portal_issuer,
                access_token=access_token,
                concurrency=args.concurrency,
                rounds=args.rounds,
                use_websocket=args.websocket,
                websocket_url=ws_url,
                protocol=protocol,
            )

    secret_text = args.ticket_secret or os.getenv("MMORPG_GATE_TOKEN_SECRET", "")
    if len(secret_text.encode("utf-8")) < 32:
        raise RobotError("Remote stress without --portal-url requires --ticket-secret (>= 32 bytes)")
    if not host or port <= 0:
        raise RobotError("Remote stress requires --gate-host and --gate-port (or --local)")
    secret_bytes = secret_text.encode("utf-8")

    return await run_stress(
        host=host,
        port=port,
        ticket_issuer=lambda: issue_gate_ticket(protocol, args.gate_node_id, args.zone_id, secret_bytes),
        access_token=access_token,
        concurrency=args.concurrency,
        rounds=args.rounds,
        use_websocket=args.websocket,
        websocket_url=ws_url,
        protocol=protocol,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """解析命令行参数并同步运行所选机器人子命令，返回进程退出码。"""
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "local-smoke":
            scenarios = _parse_scenarios(args.scenarios)
            results = asyncio.run(run_local_smoke(use_websocket=args.websocket, scenarios=scenarios))
            return _print_smoke(results, as_json=args.as_json)
        if args.command == "smoke":
            results = asyncio.run(_run_remote_smoke(args))
            return _print_smoke(results, as_json=args.as_json)
        if args.command == "stress":
            if args.local:
                report = asyncio.run(
                    run_local_stress(
                        concurrency=args.concurrency,
                        rounds=args.rounds,
                        use_websocket=args.websocket,
                    )
                )
            else:
                report = asyncio.run(_run_remote_stress(args))
            return _print_stress(report, as_json=args.as_json)
    except Exception as exc:
        if getattr(args, "as_json", False):
            print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        else:
            print(f"robot error: {exc}", file=sys.stderr)
        return 1
    return 1


__all__ = ["SMOKE_SCENARIOS", "main"]
