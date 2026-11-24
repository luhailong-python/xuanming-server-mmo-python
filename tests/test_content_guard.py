"""content_guard 全链路单元测试：文本折叠、词库匹配、外链联系检测、etcd 分块热更与 gRPC 回环。"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
from pathlib import Path
import random
from types import SimpleNamespace

import aiohttp
from aiohttp import web
import grpc
import pytest

from xuanming.content_guard import (
    BUILTIN_DENY,
    CHANNEL_CUES,
    CHUNK_BYTES,
    CONTENT_GUARD_NODE_TYPE,
    DEFAULT_GRPC_PORT,
    DEFAULT_PREFIX,
    FAILURE_APPLY,
    FAILURE_CORRUPT,
    MAX_CHUNKS,
    MAX_TEXT_BYTES,
    MAX_WORD_RUNES,
    PROVIDER_DOUYIN,
    PROVIDER_WECHAT,
    TOKEN_REFRESH_MARGIN_SEC,
    Category,
    CompileReport,
    ContentGuardConfig,
    ContentGuardService,
    DouyinAdapter,
    Elem,
    Engine,
    Entry,
    GrpcIdentityLookup,
    Hit,
    Identity,
    Lexicon,
    LexiconCorruptError,
    LexiconFormatError,
    LexiconWatcher,
    LexiconWordError,
    Mode,
    PlatformRouter,
    Pointer,
    ReviewOutcome,
    ReviewResult,
    SceneClass,
    Verdict,
    WeChatAdapter,
    build_lexicon_watcher,
    build_platform_router,
    category_name,
    chunk_key,
    chunk_prefix,
    class_of,
    compile_matcher,
    contains_ad,
    decode_lexicon,
    default_mode,
    encode_lexicon,
    fold,
    fold_word,
    is_mark,
    is_platform_skipped_scene,
    marshal_lexicon,
    mode_name,
    parse_category,
    parse_channel_account,
    parse_lexicon,
    parse_mode,
    parse_pointer,
    pointer_key,
    provider_from_identity,
    skippable,
    wait_for_lexicon,
    wechat_label_category,
    wechat_scene_code,
)
from xuanming.content_guard.textnorm import _T2S
from xuanming.grpc_runtime import GrpcClient, serve_grpc
from xuanming.protocol import Protocol
from xuanming.settings import Settings

_VECTORS_PATH = Path(__file__).parent / "fixtures" / "content_guard_vectors.json"


# -- textnorm ----------------------------------------------------------------------------------------


def test_textnorm_fold_and_t2s_table():
    assert len(_T2S) >= 2000
    assert _T2S["體"] == "体"
    assert _T2S["後"] == "后"
    for src, dst in _T2S.items():
        assert ord(src) >= 0x3400 and ord(dst) >= 0x3400

    cases = [
        ("HeLLo", "hello"),
        ("ＡＢｃ１２", "abc12"),
        ("①②", "12"),
        ("\u00e9", "e"),
        ("體國發", "体国发"),
        ("以後", "以后"),
        ("官 方", "官方"),
        ("官\u200b方", "官方"),
        ("官😀方", "官方"),
        ("加-微.信~", "加微信"),
        ("㍿", "株式会社"),
        ("!!!…", ""),
    ]
    for raw, want in cases:
        assert fold_word(raw) == want

    elems, orig = fold("a㍿ b")
    assert "".join(orig) == "a㍿ b"
    assert elems == [
        Elem("a", 0, False),
        Elem("株", 1, False),
        Elem("式", 1, False),
        Elem("会", 1, False),
        Elem("社", 1, False),
        Elem(" ", 2, True),
        Elem("b", 3, False),
    ]

    accent_elems, _ = fold("\u00e9")
    assert len(accent_elems) == 2
    assert accent_elems[0] == Elem("e", 0, False)
    assert accent_elems[1].orig == 0 and accent_elems[1].skip is True
    assert is_mark("\u0301") is True
    assert is_mark("e") is False

    for ch in (" ", "-", "_", "*", "\t", "\u3000", "\u200b", "，", "。", "★", "😀"):
        assert skippable(ch) is True
    for ch in ("a", "Z", "0", "9", "官", "あ", "я", "٣"):
        assert skippable(ch) is False


# -- wordlist ----------------------------------------------------------------------------------------


def _find(lex: Lexicon | None, text: str) -> list[Hit]:
    matcher, _ = compile_matcher(lex)
    elems, _ = fold(text)
    return matcher.find(elems)


def test_wordlist_marshal_parse_and_validation():
    lex = Lexicon(
        deny=(
            Entry("加微信", Category.AD, Mode.SUB),
            Entry("av", Category.PORN, Mode.TOKEN),
            Entry("gm", Category.IMPERSONATION, Mode.PREFIX),
        ),
        allow=("臭豆腐",),
    )
    raw = marshal_lexicon(lex)
    want = (
        "#content_guard_lexicon 1\n"
        "D\tad\tsub\t加微信\n"
        "D\tporn\ttoken\tav\n"
        "D\timpersonation\tprefix\tgm\n"
        "A\t臭豆腐\n"
    ).encode("utf-8")
    assert raw == want
    assert parse_lexicon(raw) == lex
    assert parse_lexicon(raw.replace(b"\n", b"\r\n")) == lex

    header = "#content_guard_lexicon 1\n"
    bad_payloads = [
        b"D\tad\tsub\tfoo\n",
        b"#content_guard_lexicon 2\n",
        (header + "X\tfoo\n").encode(),
        (header + "D\tspam\tsub\tfoo\n").encode(),
        (header + "D\tad\tregex\tfoo\n").encode(),
        (header + "D\tad\tfoo\n").encode(),
        (header + "A\tfoo\tbar\n").encode(),
        (header + "D\tad\tsub\t\n").encode(),
        (header + "D\tad\tsub\t" + "字" * (MAX_WORD_RUNES + 1) + "\n").encode(),
        header.encode() + b"A\t\xff\n",
        (header + "D\tnone\tsub\tfoo\n").encode(),
    ]
    for payload in bad_payloads:
        with pytest.raises((LexiconFormatError, LexiconWordError)):
            parse_lexicon(payload)

    for bad_lex in (
        Lexicon(deny=(Entry("a\tb", Category.AD, Mode.SUB),)),
        Lexicon(deny=(Entry("ok", Category.UNSPECIFIED, Mode.SUB),)),
        Lexicon(allow=("",)),
    ):
        with pytest.raises(LexiconWordError):
            marshal_lexicon(bad_lex)

    assert default_mode("gm") == Mode.TOKEN
    assert default_mode("a1") == Mode.TOKEN
    assert default_mode("abcd") == Mode.SUB
    assert default_mode("微信") == Mode.SUB
    assert category_name(Category.UNSPECIFIED) == "none"
    assert category_name(99) == "none"
    assert parse_category("none") is None
    assert mode_name(99) == "invalid"


def test_wordlist_modes_allowlist_multi_hits_and_report():
    assert len(BUILTIN_DENY) == 6
    assert _find(None, "官方客服") == [
        Hit(0, 1, Category.IMPERSONATION),
        Hit(2, 3, Category.IMPERSONATION),
    ]
    for text, matched in (
        ("GM01", True),
        ("gm小明", True),
        (" gm", True),
        ("g-m01", True),
        ("sigma", False),
        ("大gm", False),
    ):
        assert bool(_find(None, text)) is matched

    token_lex = Lexicon(deny=(Entry("av", Category.PORN, Mode.TOKEN),))
    for text, matched in (
        ("看av吗", True),
        ("AV", True),
        ("av 资源", True),
        ("have fun", False),
        ("av1", False),
        ("1av", False),
        ("a v", False),
    ):
        assert bool(_find(token_lex, text)) is matched

    # 同一匹配起点报告短词与长词，不在首个命中处早停
    multi_lex = Lexicon(
        deny=(
            Entry("笨", Category.ABUSE, Mode.SUB),
            Entry("笨蛋大王", Category.POLITICS, Mode.SUB),
        )
    )
    assert _find(multi_lex, "笨蛋大王") == [
        Hit(0, 0, Category.ABUSE),
        Hit(0, 3, Category.POLITICS),
    ]

    # 兼容展开字回映到单一原文码点下标
    assert _find(Lexicon(deny=(Entry("株式会社", Category.OTHER, Mode.SUB),)), "a㍿") == [
        Hit(1, 1, Category.OTHER),
    ]

    report_lex = Lexicon(
        deny=(
            Entry("笨蛋", Category.ABUSE, Mode.SUB),
            Entry("笨 蛋", Category.ABUSE, Mode.SUB),
            Entry("!!!", Category.ABUSE, Mode.SUB),
            Entry("官方", Category.POLITICS, Mode.SUB),
            Entry("笨蛋", Category.ABUSE, Mode.TOKEN),
        ),
        allow=("臭豆腐", "臭豆腐"),
    )
    _, report = compile_matcher(report_lex)
    assert report == CompileReport(
        deny_words=len(BUILTIN_DENY) + 2,
        allow_words=1,
        dropped=1,
        duplicates=3,
    )
    assert _find(report_lex, "官方") == [Hit(0, 1, Category.IMPERSONATION)]


# -- contact -----------------------------------------------------------------------------------------


def test_contact_ad_detection_and_allowlist():
    assert len(CHANNEL_CUES) == 24
    ads = [
        "加我微信号 wx123456 领折扣",
        "加v：①②③④⑤⑥⑦",
        "企鹅群 987654",
        "加微 一三八零零一",
        "VX：abc_998877",
        "详情看 https://example.xyz/pay",
        "来 www.test-shop.top 买金",
        "联系 13812345678 低价代练",
        "联系 138-1234-5678",
        "咸鱼搜 123456 出号",
    ]
    for sample in ads:
        elems, _ = fold(sample)
        assert contains_ad(sample, elems) is True, f"expected ad for {sample!r}"

    clean = [
        "晚上八点青云试炼缺个奶妈",
        "刚刚打出 12345 暴击伤害太爽了",
        "今天买丹药花了 50000 灵石",
        "队伍集合,加我一起刷本",
        "欢迎各位道友加入青云门!",
        "编号 138123456789 超过11位不是手机号",
        "mywx123456 前缀粘连不算线索词",
    ]
    for sample in clean:
        elems, _ = fold(sample)
        assert contains_ad(sample, elems) is False, f"unexpected ad for {sample!r}"

    msg = "官网 https://safe.example.com"
    elems, _ = fold(msg)
    assert contains_ad(msg, elems, lambda _s, _e: True) is False


# -- engine & shared vectors -------------------------------------------------------------------------


def test_shared_behavior_vectors():
    data = json.loads(_VECTORS_PATH.read_text(encoding="utf-8"))
    assert data["cases"], "vectors.json must not be empty"

    deny_entries = []
    for item in data["deny"]:
        cat = parse_category(item["category"])
        mode = parse_mode(item["mode"])
        assert cat is not None and mode is not None
        deny_entries.append(Entry(item["word"], cat, mode))

    engine = Engine()
    engine.swap(1, Lexicon(deny=tuple(deny_entries), allow=tuple(data["allow"])))

    scene_map = {
        "name": SceneClass.NAME,
        "notice": SceneClass.NOTICE,
        "chat": SceneClass.CHAT,
    }
    verdict_names = {
        Verdict.PASS: "pass",
        Verdict.MASKED: "masked",
        Verdict.REJECT: "reject",
    }

    for case in data["cases"]:
        res = engine.check(scene_map[case["scene"]], case["text"])
        assert verdict_names[res.verdict] == case["verdict"], case["name"]
        assert category_name(res.category) == case["category"], case["name"]
        assert res.masked == case.get("masked", ""), case["name"]
        assert len(res.masked.encode("utf-8")) <= len(case["text"].encode("utf-8")), case["name"]


def test_engine_combining_mark_masking_private_label_and_swap():
    engine = Engine()
    assert engine.current().version == 0
    assert engine.check(SceneClass.NAME, "官方小助手").verdict == Verdict.REJECT
    assert engine.check(SceneClass.CHAT, "官方活动开始了").verdict == Verdict.PASS

    # 未知场景大类按 fail-closed 拒绝
    assert engine.check(99, "官方").verdict == Verdict.REJECT

    # 紧跟在命中段后的组合记号一并打码为 *，避免组合记号落在 * 上
    engine.swap(
        1,
        Lexicon(
            deny=(Entry("be", Category.ABUSE, Mode.SUB),),
            allow=("13800138000",),
        ),
    )
    masked = engine.check(SceneClass.CHAT, "be\u0301x")
    assert masked.verdict == Verdict.MASKED
    assert masked.masked == "***x"
    assert masked.version == 1

    # 辱骂与外链引流并存时，拒绝优先于打码
    combined = engine.check(SceneClass.CHAT, "be 加vx123456")
    assert combined.verdict == Verdict.REJECT
    assert combined.category == Category.AD

    # 白名单放行联系号码，私有标签跳过联系方式启发式检测
    assert engine.check(SceneClass.CHAT, "官方示例 13800138000").verdict == Verdict.PASS
    assert engine.check(SceneClass.PRIVATE_LABEL, "加v123456方案").verdict == Verdict.PASS

    # 整体替换而非合并
    engine.swap(2, Lexicon())
    assert engine.check(SceneClass.CHAT, "be").verdict == Verdict.PASS
    assert engine.current().version == 2


# -- lexstore codec & etcd watcher -------------------------------------------------------------------


def test_lexstore_chunked_gzip_sha256_and_zip_bomb_guard():
    # 不可压缩数据，切分为至少 3 块
    incompressible = random.Random(20261119).randbytes(CHUNK_BYTES * 2 + 1000)
    chunks, digest = encode_lexicon(incompressible)
    assert len(chunks) >= 3
    assert all(0 < len(chunk) <= CHUNK_BYTES for chunk in chunks)

    pointer = Pointer(
        version=3,
        chunks=len(chunks),
        sha256=digest,
        bytes=len(incompressible),
        entries=42,
        updated_by="tester",
        updated_ms=1700000000000,
    )
    assert parse_pointer(pointer.to_json()) == pointer
    assert decode_lexicon(pointer, chunks) == incompressible

    # 压缩炸弹防线：实际解压远大于 pointer.bytes（仅 64 字节声明却塞入 1 MiB 零字节）
    bomb_plain = b"A" * (1024 * 1024)
    bomb_gz = gzip.compress(bomb_plain, compresslevel=9)
    bomb_ptr = Pointer(
        version=4,
        chunks=1,
        sha256=hashlib.sha256(b"A" * 64).hexdigest(),
        bytes=64,
    )
    with pytest.raises(LexiconCorruptError):
        decode_lexicon(bomb_ptr, [bomb_gz])

    # 块数不符、摘要不符、声明字节偏大、非 gzip 均拒绝
    small = b"#content_guard_lexicon 1\nD\tad\tsub\tfoo\n"
    small_chunks, small_sum = encode_lexicon(small)
    good_ptr = Pointer(version=5, chunks=len(small_chunks), sha256=small_sum, bytes=len(small))
    bad_cases = [
        (Pointer(5, len(small_chunks) + 1, small_sum, len(small)), small_chunks),
        (Pointer(5, len(small_chunks), "0" * 64, len(small)), small_chunks),
        (Pointer(5, len(small_chunks), small_sum, len(small) + 1), small_chunks),
        (Pointer(5, len(small_chunks), small_sum, len(small) - 1), small_chunks),
        (good_ptr, [b"not gzip at all"]),
    ]
    for bad_ptr, bad_chunks in bad_cases:
        with pytest.raises(LexiconCorruptError):
            decode_lexicon(bad_ptr, bad_chunks)

    # 指针键布局与版本前缀隔离
    assert pointer_key(DEFAULT_PREFIX) == "/mmorpg/content_guard/lexicon/current"
    assert chunk_prefix(DEFAULT_PREFIX, 12) == "/mmorpg/content_guard/lexicon/v/12/"
    assert chunk_key(DEFAULT_PREFIX, 12, 3) == "/mmorpg/content_guard/lexicon/v/12/0003"
    assert not chunk_key(DEFAULT_PREFIX, 12, 0).startswith(chunk_prefix(DEFAULT_PREFIX, 1))

    # 非法指针 JSON
    for bad_raw in (
        b"{",
        b"{}",
        json.dumps({"version": 0, "chunks": 1, "sha256": "a" * 64, "bytes": 10}).encode(),
        json.dumps({"version": 1, "chunks": MAX_CHUNKS + 1, "sha256": "a" * 64, "bytes": 10}).encode(),
        json.dumps({"version": 1, "chunks": 1, "sha256": "A" * 64, "bytes": 10}).encode(),
        json.dumps({"version": 1, "chunks": 1, "sha256": "a" * 64, "bytes": MAX_TEXT_BYTES + 1}).encode(),
    ):
        with pytest.raises(LexiconCorruptError):
            parse_pointer(bad_raw)


def test_lexstore_etcd_watch_hot_reload_and_corrupt_keeps_old_snapshot():
    async def exercise():
        protocol = Protocol()
        prefix = DEFAULT_PREFIX
        kv: dict[bytes, bytes] = {}
        revision = [10]
        watch_queue: asyncio.Queue[bytes] = asyncio.Queue()
        watch_ready = asyncio.Event()

        def put_lexicon(version: int, text: bytes, *, corrupt_sha: bool = False, drop_last_chunk: bool = False) -> None:
            chunks, digest = encode_lexicon(text)
            ptr = Pointer(
                version=version,
                chunks=len(chunks),
                sha256=("0" * 64 if corrupt_sha else digest),
                bytes=len(text),
                entries=1,
                updated_by="ops",
                updated_ms=1700000000000 + version,
            )
            limit = len(chunks) - 1 if drop_last_chunk else len(chunks)
            for idx in range(limit):
                kv[chunk_key(prefix, version, idx).encode("utf-8")] = chunks[idx]
            ptr_key = pointer_key(prefix).encode("utf-8")
            kv[ptr_key] = ptr.to_json()
            revision[0] += 1
            watch_queue.put_nowait(
                protocol.message(
                    "etcdserverpb.WatchResponse",
                    header={"revision": revision[0]},
                    events=[{"kv": {"key": ptr_key, "value": kv[ptr_key], "mod_revision": revision[0]}}],
                ).SerializeToString()
            )

        async def handle_range(data: bytes, _context) -> bytes:
            req = protocol.parse("etcdserverpb.RangeRequest", data)
            value = kv.get(req.key)
            kvs = [{"key": req.key, "value": value, "mod_revision": revision[0]}] if value is not None else []
            return protocol.message(
                "etcdserverpb.RangeResponse",
                header={"revision": revision[0]},
                kvs=kvs,
            ).SerializeToString()

        async def handle_watch(requests, _context):
            req = protocol.parse("etcdserverpb.WatchRequest", await anext(requests))
            assert req.create_request.key == pointer_key(prefix).encode("utf-8")
            watch_ready.set()
            yield protocol.message(
                "etcdserverpb.WatchResponse",
                header={"revision": revision[0]},
                created=True,
            ).SerializeToString()
            while True:
                msg = await watch_queue.get()
                yield msg

        etcd_server = grpc.aio.server()
        etcd_server.add_generic_rpc_handlers((
            grpc.method_handlers_generic_handler("etcdserverpb.KV", {
                "Range": grpc.unary_unary_rpc_method_handler(handle_range),
            }),
            grpc.method_handlers_generic_handler("etcdserverpb.Watch", {
                "Watch": grpc.stream_stream_rpc_method_handler(handle_watch),
            }),
        ))
        port = etcd_server.add_insecure_port("127.0.0.1:0")
        await etcd_server.start()

        engine = Engine()
        failures: list[str] = []

        def _apply(ptr: Pointer, text: bytes) -> None:
            lex = parse_lexicon(text)
            engine.swap(ptr.version, lex)

        watcher = LexiconWatcher(
            protocol,
            f"127.0.0.1:{port}",
            prefix=prefix,
            apply_fn=_apply,
            on_failure=lambda reason, _exc: failures.append(reason),
            resync_sec=60.0,
            retry_backoff_sec=0.05,
        )
        watcher.start()
        try:
            # 初始 etcd 为空，wait_for_lexicon 阻塞直到发布 v1
            await asyncio.wait_for(watch_ready.wait(), 5.0)
            assert engine.current().version == 0
            gate_task = asyncio.create_task(
                wait_for_lexicon(lambda: engine.current().version != 0, watcher, log_interval_sec=5.0)
            )
            await asyncio.sleep(0.05)
            assert not gate_task.done()

            # 发布合法 v1，验证启动门禁立刻放行且命中生效
            v1_text = marshal_lexicon(Lexicon(deny=(Entry("笨蛋", Category.ABUSE, Mode.SUB),)))
            put_lexicon(1, v1_text)
            assert await asyncio.wait_for(gate_task, 5.0) is True
            assert engine.current().version == 1
            assert engine.check(SceneClass.CHAT, "你个笨蛋").verdict == Verdict.MASKED

            # 发布损坏 SHA256 的 v2 -> 记录 corrupt，但仍保留 v1 快照
            v2_text = marshal_lexicon(Lexicon(deny=(Entry("新词", Category.AD, Mode.SUB),)))
            put_lexicon(2, v2_text, corrupt_sha=True)
            async def wait_failure(target: str) -> None:
                while target not in failures:
                    await asyncio.sleep(0.02)
            await asyncio.wait_for(wait_failure(FAILURE_CORRUPT), 5.0)
            assert engine.current().version == 1
            assert engine.check(SceneClass.CHAT, "你个笨蛋").verdict == Verdict.MASKED

            # 发布缺块的 v3_missing -> 记录 corrupt，仍保留 v1 快照
            failures.clear()
            put_lexicon(3, v2_text, drop_last_chunk=True)
            await asyncio.wait_for(wait_failure(FAILURE_CORRUPT), 5.0)
            assert engine.current().version == 1
            assert engine.check(SceneClass.CHAT, "你个笨蛋").verdict == Verdict.MASKED

            # 发布格式损坏的 v4（合法 gzip + 合法 sha256，但词库头错误）-> 记录 apply，仍保留 v1 快照
            put_lexicon(4, b"bad header\nD\tad\tsub\tfoo\n")
            await asyncio.wait_for(wait_failure(FAILURE_APPLY), 5.0)
            assert engine.current().version == 1
            assert engine.check(SceneClass.CHAT, "你个笨蛋").verdict == Verdict.MASKED

            # 发布合法 v5 -> 恢复正常热更新
            v5_text = marshal_lexicon(Lexicon(deny=(Entry("新违规词", Category.POLITICS, Mode.SUB),)))
            put_lexicon(5, v5_text)
            async def wait_v5() -> None:
                while engine.current().version != 5:
                    await asyncio.sleep(0.02)
            await asyncio.wait_for(wait_v5(), 5.0)
            assert engine.check(SceneClass.CHAT, "你个笨蛋").verdict == Verdict.PASS
            assert engine.check(SceneClass.CHAT, "测试新违规词").verdict == Verdict.REJECT

            # wait_for_lexicon 在收到 stop 信号时立即返回 False
            stop_ev = asyncio.Event()
            stop_ev.set()
            assert await wait_for_lexicon(lambda: False, watcher, stop=stop_ev) is False
        finally:
            await watcher.close()
            await etcd_server.stop(0)

    asyncio.run(exercise())


# -- config & proto parity ---------------------------------------------------------------------------


def test_config_defaults_validation_and_proto_parity():
    assert CONTENT_GUARD_NODE_TYPE == 32
    assert DEFAULT_GRPC_PORT == 50250

    protocol = Protocol()
    node_enum = protocol.pool.FindEnumTypeByName("common.base.eNodeType")
    assert node_enum.values_by_number[CONTENT_GUARD_NODE_TYPE].name == "ContentGuardNodeService"

    # 核对 proto 每个 TextScene 均在 SCENE_CLASS_MAP 中，且 UNSPECIFIED 被拒绝
    scene_enum = protocol.pool.FindEnumTypeByName("content_guard.TextScene")
    seen_names: set[str] = set()
    for val in scene_enum.values:
        mapped = class_of(val.number)
        if val.number == 0:
            assert mapped is None
        else:
            assert mapped is not None, f"unmapped TextScene {val.name}"
            assert mapped[1] not in seen_names
            seen_names.add(mapped[1])

    # 核对 proto 每个 TextCategory 与 wordlist.Category 数值和名称逐一对应
    cat_enum = protocol.pool.FindEnumTypeByName("content_guard.TextCategory")
    for val in cat_enum.values:
        want = "none" if val.number == 0 else val.name.removeprefix("TEXT_CATEGORY_").lower()
        assert category_name(val.number) == want

    settings = SimpleNamespace(
        zone_id=2,
        physical_region=1,
        release_track="stable",
        etcd="127.0.0.1:2379",
        content_guard_node={"require_lexicon": True, "max_text_bytes": 1024},
    )
    cfg = ContentGuardConfig.from_settings(settings, environ={"POD_IP": "10.0.0.8"})
    assert cfg.advertise_host == "10.0.0.8"
    assert cfg.grpc_port == DEFAULT_GRPC_PORT
    assert cfg.lease_ttl_sec == 60
    assert cfg.require_lexicon is True
    assert cfg.max_text_bytes == 1024
    assert cfg.effective_lexicon_prefix == DEFAULT_PREFIX

    with pytest.raises(ValueError):
        ContentGuardConfig(lexicon_prefix="/no/trailing/slash").validate()
    with pytest.raises(ValueError):
        ContentGuardConfig(max_text_bytes=16).validate()
    with pytest.raises(ValueError):
        ContentGuardConfig.from_settings(
            SimpleNamespace(
                zone_id=1,
                physical_region=1,
                release_track="stable",
                etcd="127.0.0.1:2379",
                content_guard_node={"unknown_field": 1},
            )
        )


# -- gRPC loopback service tests ---------------------------------------------------------------------


class _StubReviewer:
    def __init__(self, review_res: ReviewResult, sync_res: ReviewResult | None = None) -> None:
        self._review_res = review_res
        self._sync_res = sync_res
        self.calls: list[tuple[str, int, int, str, bool]] = []

    async def review(
        self,
        scene: int,
        player_id: int,
        account: str,
        text: str,
        async_platform: bool,
    ) -> ReviewResult:
        self.calls.append(("review", scene, player_id, account, async_platform))
        return self._review_res

    async def review_sync(
        self,
        scene: int,
        player_id: int,
        account: str,
        text: str,
    ) -> ReviewResult:
        self.calls.append(("review_sync", scene, player_id, account, False))
        return self._sync_res if self._sync_res is not None else self._review_res


def test_grpc_loopback_check_text_boundaries_require_lexicon_and_platform(caplog):
    async def exercise():
        protocol = Protocol()
        engine = Engine()
        reviewer = _StubReviewer(
            review_res=ReviewResult(ReviewOutcome.PENDING),
            sync_res=ReviewResult(ReviewOutcome.REJECT, Category.POLITICS, "wechat"),
        )
        service = ContentGuardService(
            protocol,
            engine,
            require_lexicon=True,
            max_text_bytes=64,
            reviewer=reviewer,
        )
        server, port = await serve_grpc(
            protocol,
            "127.0.0.1:0",
            service.services(),
            pass_context=True,
        )
        client = GrpcClient(f"127.0.0.1:{port}", timeout=3.0)
        try:
            async def call(method: str, **kwargs):
                req = protocol.message("content_guard.CheckTextRequest", **kwargs)
                raw = await client.call(f"/content_guard.ContentGuard/{method}", req.SerializeToString())
                return protocol.parse("content_guard.CheckTextResponse", raw)

            # 1. require_lexicon=True 未加载词库前返回 UNAVAILABLE
            assert service.ready() is False
            with pytest.raises(grpc.aio.AioRpcError) as exc_unavail:
                await call("CheckText", scene=1, text="逍遥子")
            assert exc_unavail.value.code() == grpc.StatusCode.UNAVAILABLE

            # 2. 加载词库后就绪
            engine.swap(
                7,
                Lexicon(
                    deny=(
                        Entry("笨蛋", Category.ABUSE, Mode.SUB),
                        Entry("加微信", Category.AD, Mode.SUB),
                    )
                ),
            )
            assert service.ready() is True

            # 3. 非法场景 / 未知场景 -> INVALID_ARGUMENT
            for bad_scene in (0, 999):
                with pytest.raises(grpc.aio.AioRpcError) as exc_scene:
                    await call("CheckText", scene=bad_scene, text="hello")
                assert exc_scene.value.code() == grpc.StatusCode.INVALID_ARGUMENT

            # 4. MaxTextBytes 按 UTF-8 字节数校验（21 个汉字 = 63 字节放行，22 个汉字 = 66 字节拒绝；64 ASCII 字节放行）
            secret_overflow = "秘" * 22  # 66 UTF-8 bytes > 64, 码点数 22 < 64
            with pytest.raises(grpc.aio.AioRpcError) as exc_len:
                await call("CheckText", scene=5, text=secret_overflow)
            assert exc_len.value.code() == grpc.StatusCode.INVALID_ARGUMENT
            assert secret_overflow not in (exc_len.value.details() or "")

            exact_ascii = await call("CheckText", scene=5, text="a" * 64)
            assert exact_ascii.verdict == int(Verdict.PASS)
            assert exact_ascii.lexicon_version == 7

            exact_utf8 = await call("CheckText", scene=5, text="剑" * 21 + "!")  # 64 UTF-8 bytes
            assert exact_utf8.verdict == int(Verdict.PASS)

            # 5. 世界频道本地通过 + 平台返回 PENDING -> CheckText 放行并标 pending_platform_review=True
            world_resp = await call(
                "CheckText",
                scene=5,
                text="正常聊天原文绝不进日志",
                player_id=42,
                account="wx_openid_1",
                async_platform=True,
            )
            assert world_resp.verdict == int(Verdict.PASS)
            assert world_resp.pending_platform_review is True
            assert world_resp.lexicon_version == 7

            # 6. CheckPlatformText 调 review_sync 返回 REJECT
            recheck_resp = await call(
                "CheckPlatformText",
                scene=5,
                text="正常聊天原文绝不进日志",
                player_id=42,
                account="wx_openid_1",
            )
            assert recheck_resp.verdict == int(Verdict.REJECT)
            assert recheck_resp.category == int(Category.POLITICS)
            assert recheck_resp.masked_text == ""

            # 7. 本地辱骂打码 + 平台 review_sync 拒绝 -> 清空 masked_text 并转 REJECT
            masked_then_reject = await call("CheckPlatformText", scene=7, text="你个笨蛋")
            assert masked_then_reject.verdict == int(Verdict.REJECT)
            assert masked_then_reject.masked_text == ""
            assert masked_then_reject.category == int(Category.POLITICS)

            # 8. 本地直接拒绝（冒充官方名字 / 广告）不额外调用平台审核
            before_calls = len(reviewer.calls)
            imp_resp = await call("CheckText", scene=1, text="官方客服")
            assert imp_resp.verdict == int(Verdict.REJECT)
            assert imp_resp.category == int(Category.IMPERSONATION)
            assert len(reviewer.calls) == before_calls

            # 9. 平台返回 UNAVAILABLE -> gRPC UNAVAILABLE
            reviewer._review_res = ReviewResult(ReviewOutcome.UNAVAILABLE)
            with pytest.raises(grpc.aio.AioRpcError) as exc_ext:
                await call("CheckText", scene=1, text="逍遥剑仙", account="wx_openid_1")
            assert exc_ext.value.code() == grpc.StatusCode.UNAVAILABLE

            # 10. 无平台审核器时的打码回归
            service._reviewer = None
            masked_resp = await call("CheckText", scene=7, text="你个笨蛋")
            assert masked_resp.verdict == int(Verdict.MASKED)
            assert masked_resp.masked_text == "你个**"
            assert masked_resp.category == int(Category.ABUSE)
        finally:
            await client.close()
            await server.stop(0)

    with caplog.at_level(logging.DEBUG):
        asyncio.run(exercise())
    assert "正常聊天原文绝不进日志" not in caplog.text
    assert "你个笨蛋" not in caplog.text
    assert "秘" * 22 not in caplog.text


def test_build_lexicon_watcher_logs_metadata_only(caplog):
    async def exercise():
        protocol = Protocol()
        engine = Engine()
        cfg = ContentGuardConfig()
        watcher = build_lexicon_watcher(protocol, "127.0.0.1:2379", cfg, engine)
        try:
            secret_word = "极密词条样例"
            lex_bytes = marshal_lexicon(Lexicon(deny=(Entry(secret_word, Category.AD, Mode.SUB),)))
            ptr = Pointer(
                version=9,
                chunks=1,
                sha256=hashlib.sha256(lex_bytes).hexdigest(),
                bytes=len(lex_bytes),
                entries=1,
                updated_by="ci",
                updated_ms=1700000000009,
            )
            watcher._apply(ptr, lex_bytes)
            assert engine.current().version == 9
        finally:
            await watcher.close()

    with caplog.at_level(logging.INFO):
        asyncio.run(exercise())
    assert "version=9" in caplog.text
    assert "极密词条样例" not in caplog.text


# -- platform adapters (WeChat / Douyin) & PlatformRouter --------------------------------------------


async def _start_mock_http_server(
    routes: list[tuple[str, str, object]],
) -> tuple[web.AppRunner, str]:
    app = web.Application()
    for method, path, handler in routes:
        app.router.add_route(method, path, handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{port}"


def test_wechat_adapter_pass_reject_labels_token_cache_and_retry(caplog):
    # 校验场景码与标签映射全表
    assert wechat_scene_code(1) == 1
    assert wechat_scene_code(2) == 1
    assert wechat_scene_code(3) == 1
    assert wechat_scene_code(9) == 1
    assert wechat_scene_code(4) == 2
    assert wechat_scene_code(5) == 3
    assert wechat_scene_code(6) == 4
    assert wechat_label_category(20001) == Category.POLITICS
    assert wechat_label_category(20002) == Category.PORN
    assert wechat_label_category(20003) == Category.PORN
    assert wechat_label_category(20012) == Category.ABUSE
    assert wechat_label_category(20013) == Category.ABUSE
    assert wechat_label_category(10001) == Category.AD
    assert wechat_label_category(21000) == Category.OTHER
    assert wechat_label_category(99999) == Category.OTHER

    async def exercise():
        state = {
            "token_calls": 0,
            "check_calls": 0,
            "suggest": "pass",
            "label": 100,
            "expire_errcode": 0,
            "last_body": None,
            "last_token_query": None,
            "http_status": 200,
        }
        now = [1000.0]

        async def handle_token(request: web.Request) -> web.Response:
            assert request.query.get("grant_type") == "client_credential"
            assert request.query.get("appid") == "wx-app-1"
            assert request.query.get("secret") == "wx-super-secret-xyz"
            state["token_calls"] += 1
            return web.json_response({
                "access_token": f"wx-tok-{state['token_calls']}",
                "expires_in": 7200,
            })

        async def handle_check(request: web.Request) -> web.Response:
            state["check_calls"] += 1
            state["last_token_query"] = request.query.get("access_token")
            state["last_body"] = await request.json()
            if state["http_status"] != 200:
                return web.Response(status=state["http_status"], text="upstream error")
            if state["expire_errcode"]:
                code = state["expire_errcode"]
                state["expire_errcode"] = 0
                return web.json_response({"errcode": code, "errmsg": "invalid credential"})
            return web.json_response({
                "errcode": 0,
                "result": {"suggest": state["suggest"], "label": state["label"]},
            })

        runner, base_url = await _start_mock_http_server([
            ("GET", "/cgi-bin/token", handle_token),
            ("POST", "/wxa/msg_sec_check", handle_check),
        ])
        adapter = WeChatAdapter(
            "wx-app-1",
            "wx-super-secret-xyz",
            base_url=base_url,
            monotonic_clock=lambda: now[0],
        )
        try:
            # 1. 缺 openid 直接跳过外部审核
            skip = await adapter.check(1, "", "绝密玩家文本A")
            assert skip == ReviewResult(ReviewOutcome.SKIP, provider=PROVIDER_WECHAT)
            assert state["token_calls"] == 0

            # 2. 正常放行并校验请求载荷
            res = await adapter.check(1, "oid-1", "青云剑客")
            assert res == ReviewResult(ReviewOutcome.PASS, provider=PROVIDER_WECHAT)
            assert state["last_body"] == {
                "content": "青云剑客",
                "version": 2,
                "scene": 1,
                "openid": "oid-1",
            }
            assert state["last_token_query"] == "wx-tok-1"
            assert state["token_calls"] == 1

            # 3. 命中各标签映射且复用缓存 token
            for label, want_cat, scene, want_scene in (
                (10001, Category.AD, 9, 1),
                (20001, Category.POLITICS, 4, 2),
                (20002, Category.PORN, 5, 3),
                (20013, Category.ABUSE, 6, 4),
                (20006, Category.OTHER, 1, 1),
            ):
                state["suggest"] = "risky"
                state["label"] = label
                rej = await adapter.check(scene, "oid-1", "绝密玩家文本B")
                assert rej == ReviewResult(ReviewOutcome.REJECT, want_cat, PROVIDER_WECHAT)
                assert state["last_body"]["scene"] == want_scene
            assert state["token_calls"] == 1

            # 4. 提前 5 分钟刷新窗口生效（expires_in=7200，推进到 7200 - 300 触发刷新）
            now[0] += 7200.0 - TOKEN_REFRESH_MARGIN_SEC
            state["suggest"] = "pass"
            res = await adapter.check(1, "oid-1", "正常昵称")
            assert res == ReviewResult(ReviewOutcome.PASS, provider=PROVIDER_WECHAT)
            assert state["token_calls"] == 2
            assert state["last_token_query"] == "wx-tok-2"

            # 5. errcode 40001 / 40014 / 42001 触发清缓存并重试一次
            for errcode in (40001, 40014, 42001):
                before_tok = state["token_calls"]
                before_chk = state["check_calls"]
                state["expire_errcode"] = errcode
                retried = await adapter.check(5, "oid-1", "绝密玩家文本C")
                assert retried == ReviewResult(ReviewOutcome.PASS, provider=PROVIDER_WECHAT)
                assert state["token_calls"] == before_tok + 1
                assert state["check_calls"] == before_chk + 2

            # 6. HTTP 500 -> UNAVAILABLE，日志不泄漏玩家文本与密钥
            state["http_status"] = 500
            fail = await adapter.check(1, "oid-1", "绝密玩家文本D")
            assert fail == ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_WECHAT)
        finally:
            await adapter.close()
            await runner.cleanup()

    with caplog.at_level(logging.DEBUG):
        asyncio.run(exercise())
    for forbidden in ("绝密玩家文本A", "绝密玩家文本B", "绝密玩家文本C", "绝密玩家文本D", "wx-super-secret-xyz", "wx-tok-"):
        assert forbidden not in caplog.text


def test_douyin_adapter_pass_reject_prob_and_401_retry(caplog):
    async def exercise():
        state = {
            "token_calls": 0,
            "check_calls": 0,
            "hit": False,
            "prob": 0.0,
            "retry_401": True,
            "retry_code": 0,
            "last_header_token": None,
            "last_body": None,
        }

        async def handle_token(request: web.Request) -> web.Response:
            body = await request.json()
            assert body == {
                "appid": "dy-app-1",
                "secret": "dy-super-secret-xyz",
                "grant_type": "client_credential",
            }
            state["token_calls"] += 1
            return web.json_response({
                "err_no": 0,
                "data": {
                    "access_token": f"dy-tok-{state['token_calls']}",
                    "expires_in": 7200,
                },
            })

        async def handle_antidirt(request: web.Request) -> web.Response:
            state["check_calls"] += 1
            state["last_header_token"] = request.headers.get("X-Token")
            state["last_body"] = await request.json()
            if state["retry_401"] and state["last_header_token"] == "dy-tok-1":
                state["retry_401"] = False
                return web.Response(status=401)
            if state["retry_code"]:
                code = state["retry_code"]
                state["retry_code"] = 0
                return web.json_response({"code": code, "message": "token expired"})
            return web.json_response({
                "data": [{
                    "code": 0,
                    "predicts": [{"hit": state["hit"], "prob": state["prob"], "model_name": "short_content_antidirt"}],
                }],
            })

        runner, base_url = await _start_mock_http_server([
            ("POST", "/api/apps/v2/token", handle_token),
            ("POST", "/api/v2/tags/text/antidirt", handle_antidirt),
        ])
        adapter = DouyinAdapter("dy-app-1", "dy-super-secret-xyz", base_url=base_url)
        try:
            # 1. HTTP 401 刷新 token 并重试成功
            res = await adapter.check(2, "dy-open", "逍遥帮")
            assert res == ReviewResult(ReviewOutcome.PASS, provider=PROVIDER_DOUYIN)
            assert state["token_calls"] == 2
            assert state["check_calls"] == 2
            assert state["last_header_token"] == "dy-tok-2"
            assert state["last_body"] == {"tasks": [{"content": "逍遥帮"}]}

            # 2. 业务 code 40002 / 40020 同样刷新重试一次
            for code in (40002, 40020):
                before = state["token_calls"]
                state["retry_code"] = code
                res = await adapter.check(5, "", "绝密抖音聊天1")
                assert res == ReviewResult(ReviewOutcome.PASS, provider=PROVIDER_DOUYIN)
                assert state["token_calls"] == before + 1

            # 3. predict.hit=True -> REJECT(OTHER)
            state["hit"] = True
            state["prob"] = 0.0
            rej_hit = await adapter.check(2, "", "绝密抖音聊天2")
            assert rej_hit == ReviewResult(ReviewOutcome.REJECT, Category.OTHER, PROVIDER_DOUYIN)

            # 4. predict.hit=False 且 prob >= 1 -> REJECT(OTHER)
            state["hit"] = False
            state["prob"] = 1.0
            rej_prob = await adapter.check(2, "", "绝密抖音聊天3")
            assert rej_prob == ReviewResult(ReviewOutcome.REJECT, Category.OTHER, PROVIDER_DOUYIN)
        finally:
            await adapter.close()
            await runner.cleanup()

    with caplog.at_level(logging.DEBUG):
        asyncio.run(exercise())
    for forbidden in ("绝密抖音聊天1", "绝密抖音聊天2", "绝密抖音聊天3", "dy-super-secret-xyz", "dy-tok-"):
        assert forbidden not in caplog.text


def test_platform_router_identity_cache_and_end_to_end_grpc(caplog):
    assert parse_channel_account("wx_open1") == (PROVIDER_WECHAT, "open1")
    assert parse_channel_account("dy_open2") == (PROVIDER_DOUYIN, "open2")
    assert parse_channel_account("wx_") == ("", "")
    assert parse_channel_account("guest_1") == ("", "")
    assert provider_from_identity(Identity("wechat_minigame", "oid-w", "acc")) == (PROVIDER_WECHAT, "oid-w")
    assert provider_from_identity(Identity("douyin_minigame", "oid-d", "acc")) == (PROVIDER_DOUYIN, "oid-d")
    assert provider_from_identity(Identity("", "", "wx_fallback_oid")) == (PROVIDER_WECHAT, "fallback_oid")
    assert is_platform_skipped_scene(6) is True
    assert is_platform_skipped_scene(7) is True
    assert is_platform_skipped_scene(8) is True
    assert is_platform_skipped_scene(5) is False

    async def exercise():
        protocol = Protocol()
        ds_calls: list[int] = []
        now = [100.0]

        async def handle_get_identity(request, context, path):
            pid = int(request.player_id)
            ds_calls.append(pid)
            if pid == 99:
                return protocol.message("data_service.GetPlayerChannelIdentityResponse", retry=True)
            if pid == 88:
                await asyncio.sleep(0.3)
                return protocol.message("data_service.GetPlayerChannelIdentityResponse", found=False)
            if pid == 77:
                return protocol.message(
                    "data_service.GetPlayerChannelIdentityResponse",
                    found=True,
                    channel="douyin_minigame",
                    open_id="dy-oid-77",
                    account="custom_77",
                )
            return protocol.message(
                "data_service.GetPlayerChannelIdentityResponse",
                found=True,
                channel="wechat_minigame",
                open_id=f"wx-oid-{pid}",
                account=f"acc_{pid}",
            )

        ds_server, ds_port = await serve_grpc(
            protocol,
            "127.0.0.1:0",
            {"data_service.DataService": {"GetPlayerChannelIdentity": handle_get_identity}},
            pass_context=True,
        )

        wx_state = {"delay": 0.0, "suggest": "pass", "label": 100}
        dy_state = {"hit": False}

        async def wx_token(_req: web.Request) -> web.Response:
            return web.json_response({"access_token": "wx-t", "expires_in": 7200})

        async def wx_check(_req: web.Request) -> web.Response:
            if wx_state["delay"] > 0:
                await asyncio.sleep(wx_state["delay"])
            return web.json_response({
                "errcode": 0,
                "result": {"suggest": wx_state["suggest"], "label": wx_state["label"]},
            })

        async def dy_token(_req: web.Request) -> web.Response:
            return web.json_response({"err_no": 0, "data": {"access_token": "dy-t", "expires_in": 7200}})

        async def dy_check(_req: web.Request) -> web.Response:
            return web.json_response({
                "data": [{"code": 0, "predicts": [{"hit": dy_state["hit"], "prob": 0.0}]}],
            })

        http_runner, base_url = await _start_mock_http_server([
            ("GET", "/cgi-bin/token", wx_token),
            ("POST", "/wxa/msg_sec_check", wx_check),
            ("POST", "/api/apps/v2/token", dy_token),
            ("POST", "/api/v2/tags/text/antidirt", dy_check),
        ])

        lookup = GrpcIdentityLookup(protocol, f"127.0.0.1:{ds_port}", timeout_sec=0.15)
        wx_adapter = WeChatAdapter("wx-app", "wx-sec", base_url=base_url)
        dy_adapter = DouyinAdapter("dy-app", "dy-sec", base_url=base_url)
        router = PlatformRouter(
            [wx_adapter, dy_adapter],
            lookup=lookup,
            sync_timeout_ms=300,
            world_timeout_ms=40,
            async_timeout_ms=300,
            lookup_timeout_ms=100,
            identity_ttl_sec=300.0,
            monotonic_clock=lambda: now[0],
        )

        engine = Engine()
        service = ContentGuardService(protocol, engine, require_lexicon=False, reviewer=router)
        cg_server, cg_port = await serve_grpc(
            protocol,
            "127.0.0.1:0",
            service.services(),
            pass_context=True,
        )
        cg_client = GrpcClient(f"127.0.0.1:{cg_port}", timeout=3.0)

        try:
            async def call(method: str, **kwargs):
                req = protocol.message("content_guard.CheckTextRequest", **kwargs)
                raw = await cg_client.call(f"/content_guard.ContentGuard/{method}", req.SerializeToString())
                return protocol.parse("content_guard.CheckTextResponse", raw)

            # 1. 私有标签 / 队伍 / 私聊跳过外部审核（不查 DataService、不触发微信拒绝）
            wx_state["suggest"], wx_state["label"] = "risky", 10001
            for skipped_scene in (6, 7, 8):
                resp = await call("CheckText", scene=skipped_scene, player_id=10, text="正常文本")
                assert resp.verdict == int(Verdict.PASS)
                assert resp.pending_platform_review is False
            assert ds_calls == []

            # 2. 直传 wx_ 前缀账号（建角无 player_id）：直接命中微信，不查 DataService
            resp = await call("CheckText", scene=1, player_id=0, account="wx_direct_1", text="绝密建角名")
            assert resp.verdict == int(Verdict.REJECT)
            assert resp.category == int(Category.AD)
            assert ds_calls == []

            # 3. 仅传 player_id 走 DataService.GetPlayerChannelIdentity 且命中 5 分钟 TTL 缓存
            resp1 = await call("CheckText", scene=1, player_id=12, text="角色名一")
            resp2 = await call("CheckText", scene=1, player_id=12, text="角色名二")
            assert resp1.verdict == int(Verdict.REJECT)
            assert resp2.verdict == int(Verdict.REJECT)
            assert ds_calls == [12]

            # 超过 5 分钟 TTL 后重新查询一次
            now[0] += 301.0
            resp3 = await call("CheckText", scene=1, player_id=12, text="角色名三")
            assert resp3.verdict == int(Verdict.REJECT)
            assert ds_calls == [12, 12]

            # 4. DataService 映射到抖音渠道
            dy_state["hit"] = True
            dy_resp = await call("CheckText", scene=2, player_id=77, text="抖音帮派名")
            assert dy_resp.verdict == int(Verdict.REJECT)
            assert dy_resp.category == int(Category.OTHER)

            # 5. player_id==0 且 account=="" 回退到抖音适配器
            fallback_resp = await call("CheckText", scene=2, player_id=0, account="", text="无身份回退抖音")
            assert fallback_resp.verdict == int(Verdict.REJECT)
            assert fallback_resp.category == int(Category.OTHER)

            # 6. 名字类查 DataService 返回 retry=True 或超时 -> fail-closed 返回 gRPC UNAVAILABLE
            for failing_pid in (99, 88):
                with pytest.raises(grpc.aio.AioRpcError) as exc_lookup:
                    await call("CheckText", scene=1, player_id=failing_pid, text="绝密起名失败")
                assert exc_lookup.value.code() == grpc.StatusCode.UNAVAILABLE

            # 7. 世界频道短超时（40ms < 120ms）且 async_platform=True -> 放行并标 pending_platform_review=True
            wx_state["delay"] = 0.12
            wx_state["suggest"], wx_state["label"] = "risky", 20001
            world_pending = await call(
                "CheckText",
                scene=5,
                player_id=12,
                text="绝密世界聊天原文",
                async_platform=True,
            )
            assert world_pending.verdict == int(Verdict.PASS)
            assert world_pending.pending_platform_review is True

            # 8. 世界频道 async_platform=False 超时 -> 降级跳过（非 pending、非 unavailable）
            world_skip = await call(
                "CheckText",
                scene=5,
                player_id=12,
                text="绝密世界聊天原文2",
                async_platform=False,
            )
            assert world_skip.verdict == int(Verdict.PASS)
            assert world_skip.pending_platform_review is False

            # 9. CheckPlatformText 使用完整 async_timeout_ms(300ms > 120ms) 跑完并返回 REJECT
            recheck = await call("CheckPlatformText", scene=5, player_id=12, text="绝密世界聊天原文")
            assert recheck.verdict == int(Verdict.REJECT)
            assert recheck.category == int(Category.POLITICS)
            assert recheck.pending_platform_review is False

            # 10. 名字类超过 sync_timeout_ms(300ms < 450ms) -> fail-closed gRPC UNAVAILABLE
            wx_state["delay"] = 0.45
            with pytest.raises(grpc.aio.AioRpcError) as exc_timeout:
                await call("CheckText", scene=1, player_id=0, account="wx_direct_1", text="绝密起名超时")
            assert exc_timeout.value.code() == grpc.StatusCode.UNAVAILABLE
        finally:
            await cg_client.close()
            await cg_server.stop(0)
            await router.close()
            await http_runner.cleanup()
            await ds_server.stop(0)

    with caplog.at_level(logging.DEBUG):
        asyncio.run(exercise())
    for secret_text in ("绝密建角名", "绝密起名失败", "绝密世界聊天原文", "绝密起名超时"):
        assert secret_text not in caplog.text


def test_platform_config_env_only_secrets_and_build_platform_router():
    protocol = Protocol()
    base_settings = SimpleNamespace(
        zone_id=1,
        physical_region=1,
        release_track="stable",
        etcd="127.0.0.1:2379",
        data_service="127.0.0.1:50100",
        content_guard_node={},
    )

    # 未配置外部平台时 build_platform_router 返回 None
    plain_cfg = ContentGuardConfig.from_settings(base_settings, environ={})
    assert plain_cfg.has_platform is False
    assert plain_cfg.data_service == "127.0.0.1:50100"
    assert build_platform_router(protocol, plain_cfg) is None

    # 禁止在 TOML 中明文写平台密钥
    for forbidden_key in ("platform_wechat_app_secret", "platform_douyin_app_secret"):
        bad_settings = SimpleNamespace(
            zone_id=1,
            physical_region=1,
            release_track="stable",
            etcd="127.0.0.1:2379",
            data_service="",
            content_guard_node={forbidden_key: "plain-secret"},
        )
        with pytest.raises(ValueError, match="environment variables"):
            ContentGuardConfig.from_settings(bad_settings, environ={})

    # 缺环境变量或含未展开占位符时启动期 fail-closed
    for bad_node, env in (
        ({"platform_wechat_app_id": "wx1", "platform_wechat_app_secret_env": "CG_WX_SEC"}, {}),
        ({"platform_wechat_app_id": "${WX_APP}", "platform_wechat_app_secret_env": "CG_WX_SEC"}, {"CG_WX_SEC": "s"}),
        ({"platform_douyin_app_id": "dy1"}, {}),
        ({"platform_sync_timeout_ms": 6000}, {}),
        ({"platform_world_timeout_ms": 2500}, {}),
        ({"platform_async_timeout_ms": 12000}, {}),
        ({"platform_lookup_timeout_ms": 2500}, {}),
    ):
        bad_settings = SimpleNamespace(
            zone_id=1,
            physical_region=1,
            release_track="stable",
            etcd="127.0.0.1:2379",
            data_service="",
            content_guard_node=bad_node,
        )
        with pytest.raises(ValueError):
            ContentGuardConfig.from_settings(bad_settings, environ=env)

    # 合法环境密钥加载：repr 不暴露密钥，build_platform_router 装配双平台与 DataService 查询器
    full_settings = SimpleNamespace(
        zone_id=1,
        physical_region=1,
        release_track="stable",
        etcd="127.0.0.1:2379",
        data_service="127.0.0.1:50100",
        content_guard_node={
            "platform_wechat_app_id": "wx-id",
            "platform_wechat_app_secret_env": "CG_WX_SECRET",
            "platform_wechat_base_url": "http://127.0.0.1:18080",
            "platform_douyin_app_id": "dy-id",
            "platform_douyin_app_secret_env": "CG_DY_SECRET",
            "platform_douyin_base_url": "http://127.0.0.1:18081",
            "platform_sync_timeout_ms": 1200,
            "platform_world_timeout_ms": 200,
            "platform_async_timeout_ms": 2000,
            "platform_lookup_timeout_ms": 120,
        },
    )
    cfg = ContentGuardConfig.from_settings(
        full_settings,
        environ={"CG_WX_SECRET": "secret-wechat-val", "CG_DY_SECRET": "secret-douyin-val"},
    )
    assert cfg.has_platform is True
    assert "secret-wechat-val" not in repr(cfg)
    assert "secret-douyin-val" not in repr(cfg)

    async def exercise_router():
        router = build_platform_router(protocol, cfg)
        assert router is not None
        assert router.providers == (PROVIDER_DOUYIN, PROVIDER_WECHAT)
        await router.close()

    asyncio.run(exercise_router())
