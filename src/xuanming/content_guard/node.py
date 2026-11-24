"""content_guard 节点生命周期：配置校验 -> 词库监听（含 require_lexicon 门禁）-> 启动 gRPC -> 注册 etcd(ttl=60) -> 优雅下线。"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
import logging
import signal
import time
import uuid

from ..cluster import EtcdDirectory
from ..grpc_runtime import serve_grpc
from ..kill_switch import EtcdKillSwitch, KillSwitch
from ..protocol import Protocol
from ..settings import Settings
from .config import CONTENT_GUARD_NODE_TYPE, ContentGuardConfig
from .engine import Engine
from .lexstore import LexiconWatcher, Pointer, wait_for_lexicon
from .service import ContentGuardService, PlatformReviewer
from .wordlist import parse_lexicon

logger = logging.getLogger(__name__)


def build_lexicon_watcher(
    protocol: Protocol,
    endpoint: str,
    config: ContentGuardConfig,
    engine: Engine,
) -> LexiconWatcher:
    """构造绑定到 ``Engine`` 的 etcd 词库热更新监听器；日志只输出元数据，严禁记录词条或玩家原文。"""

    def _apply(pointer: Pointer, text: bytes) -> None:
        lexicon = parse_lexicon(text)
        report = engine.swap(pointer.version, lexicon)
        logger.info(
            "content_guard lexicon loaded version=%d deny_words=%d allow_words=%d "
            "dropped=%d duplicates=%d published_by=%r published_ms=%d",
            pointer.version,
            report.deny_words,
            report.allow_words,
            report.dropped,
            report.duplicates,
            pointer.updated_by,
            pointer.updated_ms,
        )

    def _on_failure(reason: str, exc: Exception) -> None:
        logger.error(
            "content_guard lexicon load failed reason=%s keeping_version=%d err=%s",
            reason,
            engine.current().version,
            exc,
        )

    return LexiconWatcher(
        protocol,
        endpoint,
        prefix=config.effective_lexicon_prefix,
        apply_fn=_apply,
        on_failure=_on_failure,
        resync_sec=config.resync_sec,
        retry_backoff_sec=config.retry_backoff_sec,
    )


async def run(
    settings: Settings,
    *,
    reviewer: PlatformReviewer | None = None,
) -> None:
    """运行 content_guard 节点直至收到 SIGTERM 或 etcd 租约失效。"""
    protocol = Protocol()
    config = ContentGuardConfig.from_settings(settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with AsyncExitStack() as stack:
            engine = Engine()
            watcher = build_lexicon_watcher(protocol, settings.etcd, config, engine)
            watcher.start()
            stack.push_async_callback(watcher.close)

            # 生产环境要求词库：未拿到第一份合法词库前不监听也不注册，避免未就绪实例被发现。
            if config.require_lexicon:
                loaded = await wait_for_lexicon(
                    lambda: engine.current().version != 0,
                    watcher,
                    stop=stop,
                )
                if not loaded:
                    return
                logger.info(
                    "content_guard initial lexicon ready version=%d",
                    engine.current().version,
                )

            service = ContentGuardService(
                protocol,
                engine,
                require_lexicon=config.require_lexicon,
                max_text_bytes=config.max_text_bytes,
                reviewer=reviewer,
            )

            info = protocol.message(
                "NodeInfo",
                node_id=config.node_id,
                node_type=CONTENT_GUARD_NODE_TYPE,
                zone_id=config.zone_id,
                launch_time=int(time.time()),
                node_uuid=str(uuid.uuid4()),
                physical_region=config.physical_region,
                release_track=config.release_track,
                protocol_type=1,
            )

            kill_switch = KillSwitch(protocol, info)
            etcd_ks = EtcdKillSwitch(protocol, settings.etcd, kill_switch)
            etcd_ks.start()
            stack.push_async_callback(etcd_ks.close)

            server, port = await serve_grpc(
                protocol,
                f"{config.host}:{config.grpc_port}",
                service.services(),
                kill_switch=kill_switch,
                pass_context=True,
            )
            stack.push_async_callback(server.stop, 3.0)

            info.grpc_endpoint.ip = info.endpoint.ip = config.advertise_host
            info.grpc_endpoint.port = info.endpoint.port = port
            directory = EtcdDirectory(protocol, settings.etcd, ttl=config.lease_ttl_sec)
            # AsyncExitStack 按 LIFO 逆序执行：先注销 etcd，再排空 gRPC，最后关闭热关停与词库监听。
            stack.push_async_callback(directory.close)
            await directory.register(info)
            keepalive = asyncio.create_task(
                directory.maintain(info),
                name="content-guard-etcd-lease",
            )
            stack.callback(keepalive.cancel)

            logger.info(
                "content_guard ready node_id=%d zone=%d grpc=%s:%d ttl=%d "
                "lexicon_prefix=%s require_lexicon=%s lexicon_version=%d max_text_bytes=%d",
                config.node_id,
                config.zone_id,
                config.advertise_host,
                port,
                config.lease_ttl_sec,
                config.effective_lexicon_prefix,
                config.require_lexicon,
                engine.current().version,
                config.max_text_bytes,
            )
            stop_wait = asyncio.create_task(stop.wait())
            try:
                done, _ = await asyncio.wait(
                    {keepalive, stop_wait},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                if not stop_wait.done():
                    stop_wait.cancel()
                    await asyncio.gather(stop_wait, return_exceptions=True)
            if keepalive in done:
                keepalive.result()
    finally:
        signal.signal(signal.SIGTERM, previous)
