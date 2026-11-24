"""配置表确定性打包、安全解包与指针读写工具（对齐 Go 侧 ``table_sync`` 格式）。

职责（严格遵守单一职责）：
1. 打包（``pack_bundle``）：将配置表目录内的 ``*.json``、``*.pb`` 与 ``_table_descriptors.pb``
   打成字节确定性的 ``.tar.gz`` 归档，生成含 ``version``、``files``（path/size/sha256）与
   ``bundle_sha256`` 的清单 JSON 与 ``.sha256`` 校验文件；
2. 校验与原子解包（``verify_bundle`` / ``verify_and_unpack``）：fail-closed 拒绝路径穿越（``..``）、
   绝对路径、符号链接/硬链接/非普通文件、重复条目、超限归档与 SHA-256 篡改，全部通过后再原子替换目标目录；
3. 指针文件（``read_pointer`` / ``write_pointer``）：严格校验并原子落盘 ``POINTER.json``
   （``current_version``、``bundle_key``、``bundle_sha256``、``updated_ms``）。
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import logging
import os
import secrets
import shutil
import sys
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# 与 Go 侧 table_sync/internal/bundle 保持一致的安全上限。
MAX_BUNDLE_BYTES: int = 256 * 1024 * 1024
MAX_BUNDLE_FILES: int = 1024
BUNDLE_MANIFEST_FILENAME: str = "bundle_manifest.json"
POINTER_FILENAME: str = "POINTER.json"
DESCRIPTOR_FILENAME: str = "_table_descriptors.pb"

_COPY_CHUNK_BYTES = 64 * 1024
_PACKABLE_SUFFIXES = frozenset({".json", ".pb"})


class TableSyncError(ValueError):
    """配置表打包、解包校验或指针读写失败。"""


@dataclass(frozen=True)
class BundleFileEntry:
    """表包清单中的单个文件记录。"""

    path: str
    size: int
    sha256: str

    def to_dict(self) -> dict[str, object]:
        return {"path": self.path, "size": self.size, "sha256": self.sha256}

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> BundleFileEntry:
        if not isinstance(raw, dict):
            raise TableSyncError("manifest file entry must be a JSON object")
        path = raw.get("path")
        size = raw.get("size")
        sha256_hex = raw.get("sha256")
        if not isinstance(path, str) or not is_safe_archive_path(path):
            raise TableSyncError(f"invalid manifest file path: {path!r}")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise TableSyncError(f"invalid manifest file size for {path}: {size!r}")
        if not isinstance(sha256_hex, str) or not is_lower_hex(sha256_hex, 64):
            raise TableSyncError(f"invalid manifest file sha256 for {path}: {sha256_hex!r}")
        return cls(path=path, size=size, sha256=sha256_hex)


@dataclass(frozen=True)
class BundleManifest:
    """表包元数据清单（含 ``version``、``files`` 与 ``bundle_sha256``）。"""

    version: str
    files: tuple[BundleFileEntry, ...]
    bundle_sha256: str
    content_digest: str

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "content_digest": self.content_digest,
            "bundle_sha256": self.bundle_sha256,
            "files": [entry.to_dict() for entry in self.files],
        }

    @classmethod
    def from_dict(cls, raw: dict[str, object], *, bundle_sha256: str | None = None) -> BundleManifest:
        if not isinstance(raw, dict):
            raise TableSyncError("bundle manifest must be a JSON object")
        raw_version = raw.get("version")
        if raw_version is None or isinstance(raw_version, bool):
            raise TableSyncError("bundle manifest missing version")
        version = str(raw_version).strip()
        if not version:
            raise TableSyncError("bundle manifest version must not be empty")

        raw_files = raw.get("files")
        if not isinstance(raw_files, list) or not raw_files:
            raise TableSyncError("bundle manifest files must be a non-empty list")
        entries = tuple(BundleFileEntry.from_dict(item) for item in raw_files)
        seen: set[str] = set()
        for entry in entries:
            if entry.path in seen:
                raise TableSyncError(f"duplicate file entry in bundle manifest: {entry.path}")
            seen.add(entry.path)

        content_digest = raw.get("content_digest")
        expected_digest = _compute_content_digest(entries)
        if content_digest is not None:
            if not isinstance(content_digest, str) or not is_lower_hex(content_digest, 64):
                raise TableSyncError(f"invalid content_digest in bundle manifest: {content_digest!r}")
            if content_digest != expected_digest:
                raise TableSyncError(
                    f"bundle manifest content_digest mismatch: got={content_digest} want={expected_digest}"
                )
        else:
            content_digest = expected_digest

        resolved_sha = bundle_sha256 if bundle_sha256 is not None else raw.get("bundle_sha256", "")
        if not isinstance(resolved_sha, str) or not is_lower_hex(resolved_sha, 64):
            raise TableSyncError(f"invalid bundle_sha256: {resolved_sha!r}")

        return cls(
            version=version,
            files=entries,
            bundle_sha256=resolved_sha,
            content_digest=content_digest,
        )


@dataclass(frozen=True)
class PackResult:
    """``pack_bundle`` 产出的归档、校验文件与清单对象。"""

    bundle_path: Path
    sha256_path: Path
    manifest_path: Path
    manifest: BundleManifest
    size_bytes: int


@dataclass(frozen=True)
class TablePointer:
    """配置表发布轨道指针（``POINTER.json``）。"""

    current_version: str
    bundle_key: str
    bundle_sha256: str
    updated_ms: int

    def __post_init__(self) -> None:
        if not isinstance(self.current_version, str) or not self.current_version.strip():
            raise TableSyncError("pointer current_version must be a non-empty string")
        if not isinstance(self.bundle_key, str) or not self.bundle_key.strip():
            raise TableSyncError("pointer bundle_key must be a non-empty string")
        if not isinstance(self.bundle_sha256, str) or not is_lower_hex(self.bundle_sha256, 64):
            raise TableSyncError(f"pointer bundle_sha256 must be 64 lower-hex chars: {self.bundle_sha256!r}")
        if isinstance(self.updated_ms, bool) or not isinstance(self.updated_ms, int) or self.updated_ms < 0:
            raise TableSyncError(f"pointer updated_ms must be a non-negative integer: {self.updated_ms!r}")

    def to_dict(self) -> dict[str, object]:
        return {
            "current_version": self.current_version,
            "bundle_key": self.bundle_key,
            "bundle_sha256": self.bundle_sha256,
            "updated_ms": self.updated_ms,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> TablePointer:
        if not isinstance(raw, dict):
            raise TableSyncError("pointer JSON must be an object")
        return cls(
            current_version=raw.get("current_version"),  # type: ignore[arg-type]
            bundle_key=raw.get("bundle_key"),  # type: ignore[arg-type]
            bundle_sha256=raw.get("bundle_sha256"),  # type: ignore[arg-type]
            updated_ms=raw.get("updated_ms"),  # type: ignore[arg-type]
        )


def is_lower_hex(value: str, length: int) -> bool:
    """检查字符串是否恰好为 ``length`` 位小写十六进制。"""
    return len(value) == length and all(c in "0123456789abcdef" for c in value)


def object_key(schema_digest: str, content_digest: str) -> str:
    """返回与 Go ``table_sync`` 对齐的对象存储键：``tables/<schema_digest>/<content_digest>.tar.gz``。"""
    return f"tables/{schema_digest}/{content_digest}.tar.gz"


def object_sha256_key(schema_digest: str, content_digest: str) -> str:
    """返回与表包对象同目录的 ``.sha256`` 校验文件键。"""
    return f"{object_key(schema_digest, content_digest)}.sha256"


def is_safe_archive_path(name: str) -> bool:
    """严格校验归档内相对路径，拒绝绝对路径、反斜杠、盘符、空字节与 ``..`` 穿越。"""
    if not name or "\x00" in name:
        return False
    if name.startswith(("/", "\\")) or "\\" in name:
        return False
    segments = name.split("/")
    if ":" in segments[0]:
        return False
    return all(seg not in ("", ".", "..") for seg in segments)


def _hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _hash_file(path: Path) -> tuple[int, str]:
    if path.is_symlink() or not path.is_file():
        raise TableSyncError(f"not a regular file: {path}")
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(_COPY_CHUNK_BYTES):
            size += len(chunk)
            hasher.update(chunk)
    return size, hasher.hexdigest()


def _compute_content_digest(entries: tuple[BundleFileEntry, ...]) -> str:
    hasher = hashlib.sha256()
    for entry in sorted(entries, key=lambda e: e.path):
        hasher.update(f"{entry.path}\0{entry.size}\0{entry.sha256}\n".encode("utf-8"))
    return hasher.hexdigest()


def _write_atomic_bytes(target_path: Path, payload: bytes) -> None:
    """写同目录临时文件、``fsync`` 落盘后原子替换，避免读到半写文件。"""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.parent / f".tmp-{target_path.name}-{secrets.token_hex(8)}"
    try:
        with tmp_path.open("wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, target_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def _collect_source_files(
    source_dir: Path,
    descriptor_path: Path | None,
) -> list[tuple[str, Path]]:
    if not source_dir.is_dir():
        raise TableSyncError(f"source tables directory not found: {source_dir}")

    collected: dict[str, Path] = {}
    for entry in sorted(source_dir.rglob("*"), key=lambda p: p.as_posix()):
        if entry.is_symlink():
            raise TableSyncError(f"symlinks are forbidden in source tables directory: {entry}")
        if not entry.is_file():
            continue
        rel = entry.relative_to(source_dir).as_posix()
        if rel == BUNDLE_MANIFEST_FILENAME:
            continue
        if entry.suffix.lower() not in _PACKABLE_SUFFIXES:
            continue
        if not is_safe_archive_path(rel):
            raise TableSyncError(f"unsafe relative path in source directory: {rel!r}")
        collected[rel] = entry

    if descriptor_path is not None:
        if descriptor_path.is_symlink() or not descriptor_path.is_file():
            raise TableSyncError(f"descriptor file not found or not regular: {descriptor_path}")
        collected[DESCRIPTOR_FILENAME] = descriptor_path

    if not collected:
        raise TableSyncError(f"no packable *.json/*.pb files found in {source_dir}")
    # 额外为内置 bundle_manifest.json 留出 1 个文件配额。
    if len(collected) + 1 > MAX_BUNDLE_FILES:
        raise TableSyncError(f"bundle file count {len(collected) + 1} exceeds limit {MAX_BUNDLE_FILES}")
    return sorted(collected.items(), key=lambda pair: pair[0])


def _infer_version(source_dir: Path, explicit_version: str | int | None, content_digest: str) -> str:
    if explicit_version is not None:
        if isinstance(explicit_version, bool):
            raise TableSyncError("version must not be a boolean")
        ver = str(explicit_version).strip()
        if not ver:
            raise TableSyncError("version must not be empty")
        return ver
    source_manifest = source_dir / "manifest.json"
    if source_manifest.is_file() and not source_manifest.is_symlink():
        try:
            data = json.loads(source_manifest.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data.get("version") is not None and not isinstance(data["version"], bool):
                ver = str(data["version"]).strip()
                if ver:
                    return ver
        except (OSError, ValueError):
            pass
    return content_digest[:12]


def _add_deterministic_tar_member(tar: tarfile.TarFile, arcname: str, payload: bytes) -> None:
    info = tarfile.TarInfo(name=arcname)
    info.size = len(payload)
    info.mode = 0o644
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.type = tarfile.REGTYPE
    tar.addfile(info, io.BytesIO(payload))


def pack_bundle(
    source_dir: Path | str,
    output_path: Path | str,
    *,
    version: str | int | None = None,
    descriptor_path: Path | str | None = None,
) -> PackResult:
    """将配置表目录确定性打包为 ``.tar.gz``，并原子写出 ``.sha256`` 与清单 JSON。"""
    src_root = Path(source_dir)
    out_path = Path(output_path)
    desc_path = Path(descriptor_path) if descriptor_path is not None else None

    files = _collect_source_files(src_root, desc_path)

    payloads: list[tuple[str, bytes]] = []
    entries: list[BundleFileEntry] = []
    total_raw = 0
    for rel_path, disk_path in files:
        data = disk_path.read_bytes()
        total_raw += len(data)
        if total_raw > MAX_BUNDLE_BYTES:
            raise TableSyncError(f"uncompressed bundle size {total_raw} exceeds limit {MAX_BUNDLE_BYTES}")
        payloads.append((rel_path, data))
        entries.append(BundleFileEntry(path=rel_path, size=len(data), sha256=_hash_bytes(data)))

    entries_tuple = tuple(entries)
    content_digest = _compute_content_digest(entries_tuple)
    resolved_version = _infer_version(src_root, version, content_digest)

    # 归档内置清单记录 version、content_digest 与逐文件校验和；bundle_sha256 在压缩完成后写入侧车清单。
    inner_manifest_bytes = (
        json.dumps(
            {
                "version": resolved_version,
                "content_digest": content_digest,
                "files": [entry.to_dict() for entry in entries_tuple],
            },
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")
    if total_raw + len(inner_manifest_bytes) > MAX_BUNDLE_BYTES:
        raise TableSyncError("uncompressed bundle size exceeds limit after adding manifest")

    all_members = sorted(
        [*payloads, (BUNDLE_MANIFEST_FILENAME, inner_manifest_bytes)],
        key=lambda item: item[0],
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_archive = out_path.parent / f".pack-{out_path.name}-{secrets.token_hex(8)}"
    try:
        with tmp_archive.open("wb") as raw_fh:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=raw_fh,
                compresslevel=9,
                mtime=0,
            ) as gz_fh:
                with tarfile.open(fileobj=gz_fh, mode="w", format=tarfile.GNU_FORMAT) as tar:
                    for arcname, data in all_members:
                        _add_deterministic_tar_member(tar, arcname, data)
            raw_fh.flush()
            os.fsync(raw_fh.fileno())

        compressed_size, bundle_sha = _hash_file(tmp_archive)
        if compressed_size > MAX_BUNDLE_BYTES:
            raise TableSyncError(
                f"compressed bundle size {compressed_size} exceeds limit {MAX_BUNDLE_BYTES}"
            )
        os.replace(tmp_archive, out_path)
    except BaseException:
        tmp_archive.unlink(missing_ok=True)
        raise

    manifest = BundleManifest(
        version=resolved_version,
        files=entries_tuple,
        bundle_sha256=bundle_sha,
        content_digest=content_digest,
    )

    sha256_path = Path(f"{out_path}.sha256")
    manifest_path = Path(f"{out_path}.manifest.json")
    _write_atomic_bytes(sha256_path, f"{bundle_sha}  {out_path.name}\n".encode("utf-8"))
    _write_atomic_bytes(
        manifest_path,
        (json.dumps(manifest.to_dict(), sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )

    logger.info(
        "table_sync packed bundle: path=%s version=%s files=%d bytes=%d bundle_sha256=%s",
        out_path,
        resolved_version,
        len(entries_tuple),
        compressed_size,
        bundle_sha[:12],
    )
    return PackResult(
        bundle_path=out_path,
        sha256_path=sha256_path,
        manifest_path=manifest_path,
        manifest=manifest,
        size_bytes=compressed_size,
    )


def _validate_archive_and_read(
    bundle_path: Path,
    *,
    expected_sha256: str | None,
    stage_dir: Path | None,
) -> BundleManifest:
    if expected_sha256 is not None and not is_lower_hex(expected_sha256, 64):
        raise TableSyncError(f"expected_sha256 must be 64 lower-hex chars: {expected_sha256!r}")

    archive_size, actual_sha = _hash_file(bundle_path)
    if archive_size > MAX_BUNDLE_BYTES:
        raise TableSyncError(f"archive size {archive_size} exceeds limit {MAX_BUNDLE_BYTES}")
    if expected_sha256 is not None and actual_sha != expected_sha256:
        raise TableSyncError(
            f"archive sha256 mismatch: got={actual_sha} want={expected_sha256}"
        )

    stage_resolved = stage_dir.resolve() if stage_dir is not None else None
    seen_names: set[str] = set()
    extracted_stats: dict[str, tuple[int, str]] = {}
    manifest_raw: bytes | None = None
    file_count = 0
    total_uncompressed = 0

    try:
        with tarfile.open(bundle_path, mode="r:gz") as tar:
            for member in tar:
                file_count += 1
                if file_count > MAX_BUNDLE_FILES:
                    raise TableSyncError(f"archive file count exceeds limit {MAX_BUNDLE_FILES}")
                if member.issym() or member.islnk() or not member.isfile():
                    raise TableSyncError(
                        f"archive entry {member.name!r} has forbidden type {member.type!r} "
                        "(only regular files allowed)"
                    )
                if not is_safe_archive_path(member.name):
                    raise TableSyncError(
                        f"archive entry {member.name!r} has forbidden path or traversal"
                    )
                if member.name in seen_names:
                    raise TableSyncError(f"archive contains duplicate entry {member.name!r}")
                seen_names.add(member.name)

                if member.size < 0 or total_uncompressed + member.size > MAX_BUNDLE_BYTES:
                    raise TableSyncError(
                        f"archive entry {member.name!r} exceeds max bundle bytes {MAX_BUNDLE_BYTES}"
                    )

                stream = tar.extractfile(member)
                if stream is None:
                    raise TableSyncError(f"unable to read archive entry {member.name!r}")

                hasher = hashlib.sha256()
                written = 0
                manifest_chunks: list[bytes] | None = (
                    [] if member.name == BUNDLE_MANIFEST_FILENAME else None
                )

                dst_file = None
                if stage_resolved is not None:
                    dst_path = (stage_resolved / member.name).resolve()
                    try:
                        dst_path.relative_to(stage_resolved)
                    except ValueError as exc:
                        raise TableSyncError(
                            f"archive entry {member.name!r} escapes destination directory"
                        ) from exc
                    dst_path.parent.mkdir(parents=True, exist_ok=True)
                    dst_file = dst_path.open("wb")

                try:
                    with stream:
                        while chunk := stream.read(_COPY_CHUNK_BYTES):
                            written += len(chunk)
                            total_uncompressed += len(chunk)
                            if total_uncompressed > MAX_BUNDLE_BYTES:
                                raise TableSyncError(
                                    f"archive uncompressed bytes exceed limit {MAX_BUNDLE_BYTES}"
                                )
                            hasher.update(chunk)
                            if dst_file is not None:
                                dst_file.write(chunk)
                            if manifest_chunks is not None:
                                manifest_chunks.append(chunk)
                    if dst_file is not None:
                        dst_file.flush()
                        os.fsync(dst_file.fileno())
                finally:
                    if dst_file is not None:
                        dst_file.close()

                if member.name == BUNDLE_MANIFEST_FILENAME:
                    manifest_raw = b"".join(manifest_chunks or ())
                else:
                    extracted_stats[member.name] = (written, hasher.hexdigest())
    except TableSyncError:
        raise
    except (tarfile.TarError, gzip.BadGzipFile, EOFError, OSError) as exc:
        raise TableSyncError(f"failed to read tar.gz archive {bundle_path}: {exc}") from exc

    if manifest_raw is None:
        raise TableSyncError(f"archive missing required {BUNDLE_MANIFEST_FILENAME}")
    try:
        parsed_manifest = json.loads(manifest_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TableSyncError(f"invalid {BUNDLE_MANIFEST_FILENAME} JSON: {exc}") from exc

    manifest = BundleManifest.from_dict(parsed_manifest, bundle_sha256=actual_sha)

    expected_paths = {entry.path for entry in manifest.files}
    actual_paths = set(extracted_stats.keys())
    if expected_paths != actual_paths:
        missing = sorted(expected_paths - actual_paths)
        extra = sorted(actual_paths - expected_paths)
        raise TableSyncError(
            f"archive file list does not match manifest: missing={missing} extra={extra}"
        )

    for entry in manifest.files:
        actual_size, actual_file_sha = extracted_stats[entry.path]
        if actual_size != entry.size:
            raise TableSyncError(
                f"file {entry.path} size mismatch: got={actual_size} want={entry.size}"
            )
        if actual_file_sha != entry.sha256:
            raise TableSyncError(
                f"file {entry.path} sha256 mismatch: got={actual_file_sha} want={entry.sha256}"
            )

    return manifest


def verify_bundle(
    bundle_path: Path | str,
    *,
    expected_sha256: str | None = None,
) -> BundleManifest:
    """只读校验归档的完整性、路径安全与清单摘要，不在磁盘落盘解包产物。"""
    path = Path(bundle_path)
    manifest = _validate_archive_and_read(path, expected_sha256=expected_sha256, stage_dir=None)
    logger.info(
        "table_sync verified bundle: path=%s version=%s files=%d bundle_sha256=%s",
        path,
        manifest.version,
        len(manifest.files),
        manifest.bundle_sha256[:12],
    )
    return manifest


def _promote_directory_atomic(stage_dir: Path, dest_dir: Path) -> None:
    """将已校验完成的暂存目录原子切换到目标目录（兼容 Windows 与 POSIX）。"""
    if not dest_dir.exists():
        os.replace(stage_dir, dest_dir)
        return

    backup_dir = dest_dir.parent / f".backup-{dest_dir.name}-{secrets.token_hex(8)}"
    os.replace(dest_dir, backup_dir)
    try:
        os.replace(stage_dir, dest_dir)
    except BaseException:
        # 切换失败时尽力恢复旧目录，保证目标目录不丢失。
        if not dest_dir.exists() and backup_dir.exists():
            os.replace(backup_dir, dest_dir)
        raise
    shutil.rmtree(backup_dir, ignore_errors=True)


def verify_and_unpack(
    bundle_path: Path | str,
    dest_dir: Path | str,
    *,
    expected_sha256: str | None = None,
) -> BundleManifest:
    """严格校验归档后先解包到同级暂存目录，全量通过后再原子切换至 ``dest_dir``。"""
    src_archive = Path(bundle_path)
    target_dir = Path(dest_dir)
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    stage_dir = target_dir.parent / f".extract-{target_dir.name}-{secrets.token_hex(8)}"
    stage_dir.mkdir(mode=0o755)
    promoted = False
    try:
        manifest = _validate_archive_and_read(
            src_archive,
            expected_sha256=expected_sha256,
            stage_dir=stage_dir,
        )
        _promote_directory_atomic(stage_dir, target_dir)
        promoted = True
    finally:
        if not promoted:
            shutil.rmtree(stage_dir, ignore_errors=True)

    logger.info(
        "table_sync unpacked bundle: path=%s dest=%s version=%s files=%d bundle_sha256=%s",
        src_archive,
        target_dir,
        manifest.version,
        len(manifest.files),
        manifest.bundle_sha256[:12],
    )
    return manifest


def write_pointer(path: Path | str, pointer: TablePointer) -> None:
    """严格校验并原子写入指针 JSON（``POINTER.json``）。"""
    target = Path(path)
    payload = (json.dumps(pointer.to_dict(), sort_keys=True, indent=2) + "\n").encode("utf-8")
    _write_atomic_bytes(target, payload)
    logger.info(
        "table_sync wrote pointer: path=%s version=%s bundle_key=%s bundle_sha256=%s",
        target,
        pointer.current_version,
        pointer.bundle_key,
        pointer.bundle_sha256[:12],
    )


def read_pointer(path: Path | str) -> TablePointer:
    """读取并严格校验指针 JSON（``POINTER.json``）。"""
    target = Path(path)
    if not target.is_file():
        raise TableSyncError(f"pointer file not found: {target}")
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TableSyncError(f"failed to parse pointer file {target}: {exc}") from exc
    return TablePointer.from_dict(raw)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="table_sync",
        description="Pack, verify, atomically unpack config table bundles, and manage pointer JSON.",
    )
    sub = parser.add_subparsers(dest="subcommand", required=True)

    pack_cmd = sub.add_parser("pack", help="Pack a config table directory into a deterministic .tar.gz bundle.")
    pack_cmd.add_argument("--source-dir", type=Path, required=True)
    pack_cmd.add_argument("--output", type=Path, required=True)
    pack_cmd.add_argument("--version", default=None)
    pack_cmd.add_argument("--descriptor", type=Path, default=None)

    verify_cmd = sub.add_parser("verify", help="Verify a bundle's SHA-256, safety, and inner manifest.")
    verify_cmd.add_argument("--bundle", type=Path, required=True)
    verify_cmd.add_argument("--expected-sha256", default=None)

    unpack_cmd = sub.add_parser("unpack", help="Verify and atomically unpack a bundle into a target directory.")
    unpack_cmd.add_argument("--bundle", type=Path, required=True)
    unpack_cmd.add_argument("--dest-dir", type=Path, required=True)
    unpack_cmd.add_argument("--expected-sha256", default=None)

    ptr_write = sub.add_parser("pointer-write", help="Atomically write a POINTER.json file.")
    ptr_write.add_argument("--path", type=Path, required=True)
    ptr_write.add_argument("--version", required=True)
    ptr_write.add_argument("--bundle-key", required=True)
    ptr_write.add_argument("--bundle-sha256", required=True)
    ptr_write.add_argument("--updated-ms", type=int, default=None)

    ptr_read = sub.add_parser("pointer-read", help="Read and validate a POINTER.json file.")
    ptr_read.add_argument("--path", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        if args.subcommand == "pack":
            res = pack_bundle(
                args.source_dir,
                args.output,
                version=args.version,
                descriptor_path=args.descriptor,
            )
            print(json.dumps(res.manifest.to_dict(), indent=2, ensure_ascii=False))
            return 0
        if args.subcommand == "verify":
            manifest = verify_bundle(args.bundle, expected_sha256=args.expected_sha256)
            print(json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False))
            return 0
        if args.subcommand == "unpack":
            manifest = verify_and_unpack(
                args.bundle, args.dest_dir, expected_sha256=args.expected_sha256
            )
            print(json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False))
            return 0
        if args.subcommand == "pointer-write":
            updated_ms = args.updated_ms if args.updated_ms is not None else int(time.time() * 1000)
            ptr = TablePointer(
                current_version=args.version,
                bundle_key=args.bundle_key,
                bundle_sha256=args.bundle_sha256,
                updated_ms=updated_ms,
            )
            write_pointer(args.path, ptr)
            print(json.dumps(ptr.to_dict(), indent=2, ensure_ascii=False))
            return 0
        if args.subcommand == "pointer-read":
            ptr = read_pointer(args.path)
            print(json.dumps(ptr.to_dict(), indent=2, ensure_ascii=False))
            return 0
    except (TableSyncError, ValueError, OSError) as exc:
        logger.error("table_sync failed: %s", exc)
        return 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
