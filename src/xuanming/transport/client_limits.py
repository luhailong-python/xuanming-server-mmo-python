"""客户端连接共用的非法包阈值。"""

import os


def illegal_packet_threshold() -> int:
    try:
        threshold = int(os.getenv("GATE_ILLEGAL_PACKET_THRESHOLD", "50"))
    except ValueError:
        return 50
    return threshold if 0 <= threshold < 1 << 32 else 50
