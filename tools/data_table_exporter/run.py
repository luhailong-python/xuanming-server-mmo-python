"""纯 Python 配置表导出工具独立 CLI 入口。"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    """独立运行时将仓库 ``src/`` 目录加入导入路径。"""
    repo_root = Path(__file__).resolve().parents[2]
    src_dir = str(repo_root / "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def main() -> int:
    _ensure_src_on_path()
    from xuanming.ops.table_exporter import main as exporter_main

    return exporter_main()


if __name__ == "__main__":
    raise SystemExit(main())
