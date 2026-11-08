"""用本目录的协议快照生成 Python 运行时描述符。"""

from pathlib import Path
from shutil import copyfile

import grpc_tools
from grpc_tools import protoc


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    assets = root / "src" / "xuanming" / "contracts"
    well_known = Path(grpc_tools.__file__).parent / "_proto"
    sources = sorted((root / "proto").rglob("*.proto"))
    result = protoc.main([
        "protoc", f"-I{root}", f"-I{well_known}", "--include_imports",
        f"--descriptor_set_out={assets / 'schema.pb'}",
        *(str(path) for path in sources),
    ])
    if result == 0:
        for name in ("message_id.txt", "event_id.txt"):
            copyfile(root / "proto" / name, assets / name)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
