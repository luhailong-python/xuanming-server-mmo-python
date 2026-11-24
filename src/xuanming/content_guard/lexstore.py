"""etcd 词库分块编解码、线性读取与指针监听（热更新 + 损坏保旧快照）。

键布局与 Go 版 ``content_guard/internal/lexstore`` 完全对齐：
- ``<prefix>current``：JSON 指针（version / chunks / sha256 / bytes / entries / updated_by / updated_ms）
- ``<prefix>v/<version>/<0000..>``：gzip 压缩后按 512 KiB 切分的词库分块

任一阶段出现 etcd 故障、分块缺失、解压炸弹、校验不符或词库解析失败，均保持当前生效快照不变。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass
import gzip
import hashlib
import io
import json
import logging

import grpc

from ..grpc_runtime import GrpcClient
from ..protocol import Protocol

logger = logging.getLogger(__name__)

DEFAULT_PREFIX = "/mmorpg/content_guard/lexicon/"
CHUNK_BYTES = 512 * 1024
MAX_CHUNKS = 256
MAX_TEXT_BYTES = 64 * 1024 * 1024

DEFAULT_RETRY_BACKOFF_SEC = 3.0
DEFAULT_RESYNC_SEC = 300.0

FAILURE_ETCD = "etcd"
FAILURE_CORRUPT = "corrupt"
FAILURE_APPLY = "apply"
FAILURE_WATCH = "watch"


class LexiconCorruptError(ValueError):
    """etcd 中的词库指针或分块损坏、缺失、校验失败或超出大小上限。"""


def pointer_key(prefix: str = DEFAULT_PREFIX) -> str:
    """返回指针键路径。"""
    return f"{prefix}current"


def versions_prefix(prefix: str = DEFAULT_PREFIX) -> str:
    """返回全部版本分块的公共前缀。"""
    return f"{prefix}v/"


def chunk_prefix(prefix: str, version: int) -> str:
    """返回指定版本全部分块的前缀。"""
    return f"{versions_prefix(prefix)}{version}/"


def chunk_key(prefix: str, version: int, index: int) -> str:
    """返回指定版本第 ``index`` 个分块的四位定宽键。"""
    return f"{chunk_prefix(prefix, version)}{index:04d}"


@dataclass(frozen=True, slots=True)
class Pointer:
    """``<prefix>current`` 中存储的词库版本指针。"""

    version: int
    chunks: int
    sha256: str
    bytes: int
    entries: int = 0
    updated_by: str = ""
    updated_ms: int = 0

    def validate(self) -> None:
        """核对指针字段合法性，不通过抛出 ``LexiconCorruptError``。"""
        if self.version <= 0:
            raise LexiconCorruptError("lexstore: pointer version is 0")
        if not 1 <= self.chunks <= MAX_CHUNKS:
            raise LexiconCorruptError(
                f"lexstore: pointer chunks={self.chunks} out of [1, {MAX_CHUNKS}]"
            )
        if not 1 <= self.bytes <= MAX_TEXT_BYTES:
            raise LexiconCorruptError(
                f"lexstore: pointer bytes={self.bytes} out of [1, {MAX_TEXT_BYTES}]"
            )
        if len(self.sha256) != 64 or self.sha256.lower() != self.sha256:
            raise LexiconCorruptError(
                "lexstore: pointer sha256 is not 64 lowercase hex characters"
            )
        try:
            bytes.fromhex(self.sha256)
        except ValueError as exc:
            raise LexiconCorruptError("lexstore: pointer sha256 is not hex") from exc

    def to_json(self) -> bytes:
        """序列化为与 Go 侧字段名一致的 JSON 字节串。"""
        return json.dumps(
            {
                "version": self.version,
                "chunks": self.chunks,
                "sha256": self.sha256,
                "bytes": self.bytes,
                "entries": self.entries,
                "updated_by": self.updated_by,
                "updated_ms": self.updated_ms,
            },
            separators=(",", ":"),
        ).encode("utf-8")


def _require_int(data: dict, key: str, default: int = 0) -> int:
    value = data.get(key, default)
    if type(value) is not int:
        raise LexiconCorruptError(f"lexstore: pointer field {key!r} is not an integer")
    return value


def _require_str(data: dict, key: str, default: str = "") -> str:
    value = data.get(key, default)
    if not isinstance(value, str):
        raise LexiconCorruptError(f"lexstore: pointer field {key!r} is not a string")
    return value


def parse_pointer(raw: bytes | str) -> Pointer:
    """解析并校验 etcd 指针 JSON。"""
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        parsed = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LexiconCorruptError(f"lexstore: pointer is not JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise LexiconCorruptError("lexstore: pointer JSON must be an object")

    pointer = Pointer(
        version=_require_int(parsed, "version", 0),
        chunks=_require_int(parsed, "chunks", 0),
        sha256=_require_str(parsed, "sha256", ""),
        bytes=_require_int(parsed, "bytes", 0),
        entries=_require_int(parsed, "entries", 0),
        updated_by=_require_str(parsed, "updated_by", ""),
        updated_ms=_require_int(parsed, "updated_ms", 0),
    )
    pointer.validate()
    return pointer


def encode_lexicon(text: bytes) -> tuple[list[bytes], str]:
    """将词库正文以最佳压缩比 gzip 压缩并切块，返回 ``(分块列表, 原文小写 SHA-256)``。"""
    if not 1 <= len(text) <= MAX_TEXT_BYTES:
        raise ValueError(
            f"lexstore: lexicon text is {len(text)} bytes, must be in [1, {MAX_TEXT_BYTES}]"
        )
    packed = gzip.compress(text, compresslevel=9)
    chunks = [packed[offset : offset + CHUNK_BYTES] for offset in range(0, len(packed), CHUNK_BYTES)]
    if len(chunks) > MAX_CHUNKS:
        raise ValueError(f"lexstore: lexicon needs {len(chunks)} chunks, the limit is {MAX_CHUNKS}")
    return chunks, hashlib.sha256(text).hexdigest()


def _inflate(chunks: Sequence[bytes], limit: int) -> bytes:
    """合并分块并受限解压，最多读取 ``limit + 1`` 字节以防压缩炸弹撑爆内存。"""
    joined = b"".join(chunks)
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(joined), mode="rb") as reader:
            return reader.read(limit + 1)
    except Exception as exc:
        raise LexiconCorruptError(f"lexstore: failed to decompress gzip payload: {exc}") from exc


def decode_lexicon(pointer: Pointer, chunks: Sequence[bytes]) -> bytes:
    """按指针校验块数、解压后字节数与 SHA-256 摘要，返回词库明文字节。"""
    if len(chunks) != pointer.chunks:
        raise LexiconCorruptError(
            f"lexstore: version {pointer.version} has {len(chunks)} chunks, pointer says {pointer.chunks}"
        )
    if not 1 <= pointer.bytes <= MAX_TEXT_BYTES:
        raise LexiconCorruptError(
            f"lexstore: pointer bytes={pointer.bytes} out of [1, {MAX_TEXT_BYTES}]"
        )
    text = _inflate(chunks, pointer.bytes)
    if len(text) != pointer.bytes:
        raise LexiconCorruptError(
            f"lexstore: version {pointer.version} decompressed to a different size than declared ({pointer.bytes})"
        )
    digest = hashlib.sha256(text).hexdigest()
    if digest != pointer.sha256:
        raise LexiconCorruptError(f"lexstore: version {pointer.version} checksum mismatch")
    return text


@dataclass(frozen=True, slots=True)
class LoadedLexicon:
    """一次从 etcd 读取词库的结果；``text`` 为 ``None`` 表示 etcd 尚未发布词库。"""

    pointer: Pointer | None
    text: bytes | None
    revision: int


async def _etcd_range_key(protocol: Protocol, client: GrpcClient, key: str):
    request = protocol.message("etcdserverpb.RangeRequest", key=key.encode("utf-8"))
    raw = await client.call("/etcdserverpb.KV/Range", request.SerializeToString())
    return protocol.parse("etcdserverpb.RangeResponse", raw)


async def load_lexicon(
    protocol: Protocol,
    client: GrpcClient,
    prefix: str = DEFAULT_PREFIX,
) -> LoadedLexicon:
    """通过 ``/etcdserverpb.KV/Range`` 读取指针与全部分块并校验完整性。"""
    resp = await _etcd_range_key(protocol, client, pointer_key(prefix))
    revision = int(resp.header.revision)
    if not resp.kvs:
        return LoadedLexicon(pointer=None, text=None, revision=revision)

    pointer = parse_pointer(resp.kvs[0].value)
    chunks: list[bytes] = []
    for idx in range(pointer.chunks):
        chunk_resp = await _etcd_range_key(protocol, client, chunk_key(prefix, pointer.version, idx))
        if not chunk_resp.kvs:
            raise LexiconCorruptError(
                f"lexstore: version {pointer.version} is missing chunk {idx}"
            )
        chunks.append(bytes(chunk_resp.kvs[0].value))

    text = decode_lexicon(pointer, chunks)
    return LoadedLexicon(pointer=pointer, text=text, revision=revision)


class LexiconWatcher:
    """持续监听 etcd ``<prefix>current`` 并将合法新词库热应用到回调。

    任何读取、校验、解析或监听异常都只回调 ``on_failure`` 并保留旧快照。
    """

    def __init__(
        self,
        protocol: Protocol,
        endpoint: str,
        *,
        prefix: str = DEFAULT_PREFIX,
        apply_fn: Callable[[Pointer, bytes], None],
        on_failure: Callable[[str, Exception], None] | None = None,
        resync_sec: float = DEFAULT_RESYNC_SEC,
        retry_backoff_sec: float = DEFAULT_RETRY_BACKOFF_SEC,
        rpc_timeout_sec: float = 5.0,
        client: GrpcClient | None = None,
    ) -> None:
        self._protocol = protocol
        self._prefix = prefix or DEFAULT_PREFIX
        self._apply = apply_fn
        self._on_failure = on_failure
        self._resync_sec = resync_sec if resync_sec > 0 else DEFAULT_RESYNC_SEC
        self._retry_backoff_sec = (
            retry_backoff_sec if retry_backoff_sec > 0 else DEFAULT_RETRY_BACKOFF_SEC
        )
        self._owns_client = client is None
        self._client = client if client is not None else GrpcClient(endpoint, timeout=rpc_timeout_sec)
        self._task: asyncio.Task | None = None
        self._closed = False
        self._applied: Pointer | None = None
        self.loaded_event = asyncio.Event()

    def start(self) -> None:
        """启动后台词库同步与监听协程（幂等）。"""
        if self._task is None and not self._closed:
            self._task = asyncio.create_task(self._run(), name="content-guard-lexicon-watch")

    async def close(self) -> None:
        """停止后台监听并关闭自建的 etcd gRPC 通道。"""
        self._closed = True
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self._owns_client:
            await self._client.close()

    def _fail(self, reason: str, exc: Exception) -> None:
        if self._on_failure is not None:
            self._on_failure(reason, exc)

    async def _run(self) -> None:
        while not self._closed:
            try:
                loaded = await load_lexicon(self._protocol, self._client, self._prefix)
            except asyncio.CancelledError:
                raise
            except LexiconCorruptError as exc:
                self._fail(FAILURE_CORRUPT, exc)
                await asyncio.sleep(self._retry_backoff_sec)
                continue
            except Exception as exc:
                self._fail(FAILURE_ETCD, exc)
                await asyncio.sleep(self._retry_backoff_sec)
                continue

            if loaded.text is not None and loaded.pointer is not None:
                same_as_applied = (
                    self._applied is not None
                    and loaded.pointer.version == self._applied.version
                    and loaded.pointer.sha256 == self._applied.sha256
                )
                if not same_as_applied:
                    try:
                        self._apply(loaded.pointer, loaded.text)
                    except Exception as exc:
                        self._fail(FAILURE_APPLY, exc)
                    else:
                        self._applied = loaded.pointer
                        self.loaded_event.set()

            ok = await self._watch_once(loaded.revision + 1)
            if not ok and not self._closed:
                await asyncio.sleep(self._retry_backoff_sec)

    async def _watch_once(self, start_revision: int) -> bool:
        """在 ``<prefix>current`` 上监听单轮变更；返回 True 表示收到变更或触发定期重同步。"""
        call = None
        try:
            call = self._client.channel.stream_stream("/etcdserverpb.Watch/Watch")()
            request = self._protocol.message(
                "etcdserverpb.WatchRequest",
                create_request={
                    "key": pointer_key(self._prefix).encode("utf-8"),
                    "start_revision": start_revision,
                },
            )
            await call.write(request.SerializeToString())
            async with asyncio.timeout(self._resync_sec):
                while not self._closed:
                    raw = await call.read()
                    if raw is grpc.aio.EOF:
                        if not self._closed:
                            self._fail(FAILURE_WATCH, ConnectionError("lexstore: watch stream closed"))
                        return False
                    response = self._protocol.parse("etcdserverpb.WatchResponse", raw)
                    if response.canceled:
                        if not self._closed:
                            reason = response.cancel_reason or "watch canceled"
                            self._fail(FAILURE_WATCH, RuntimeError(f"lexstore: {reason}"))
                        return False
                    if response.events:
                        return True
            return True
        except TimeoutError:
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self._closed:
                self._fail(FAILURE_WATCH, exc)
            return False
        finally:
            if call is not None:
                call.cancel()


async def wait_for_lexicon(
    is_loaded: Callable[[], bool],
    watcher: LexiconWatcher,
    *,
    stop: asyncio.Event | None = None,
    log_interval_sec: float = 30.0,
) -> bool:
    """``require_lexicon=True`` 时的启动门禁：阻塞至词库加载完成或收到停机信号。"""
    waited = 0.0
    while not is_loaded():
        if stop is not None and stop.is_set():
            return False
        waiters: set[asyncio.Task] = {
            asyncio.create_task(watcher.loaded_event.wait()),
        }
        if stop is not None:
            waiters.add(asyncio.create_task(stop.wait()))
        try:
            done, pending = await asyncio.wait(
                waiters,
                timeout=log_interval_sec,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in waiters:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)

        if is_loaded():
            return True
        if stop is not None and stop.is_set():
            return False
        if not done:
            waited += log_interval_sec
            logger.error(
                "content_guard waiting for initial lexicon elapsed_sec=%.1f prefix=%s",
                waited,
                watcher._prefix,
            )
    return True
