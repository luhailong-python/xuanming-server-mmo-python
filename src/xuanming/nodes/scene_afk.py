"""原版 AFK 状态，按模拟帧计数且只影响普通移动积分。"""

from dataclasses import dataclass

TARGET_FPS = 20
INACTIVITY_FRAMES = 30 * TARGET_FPS
FRAME_MASK = (1 << 32) - 1


@dataclass(slots=True)
class FrameActivity:
    last_active_frame: int = 0
    afk_start_frame: int | None = None

    @property
    def afk(self) -> bool:
        return self.afk_start_frame is not None

    def record(self, current_frame: int) -> None:
        self.last_active_frame = current_frame & FRAME_MASK

    def update(self, current_frame: int, *, frozen: bool = False) -> None:
        if frozen:
            return
        current_frame &= FRAME_MASK
        idle_frames = (current_frame - self.last_active_frame) & FRAME_MASK
        if self.afk_start_frame is None and idle_frames >= INACTIVITY_FRAMES:
            self.afk_start_frame = current_frame
        if self.afk_start_frame is not None and self.last_active_frame > self.afk_start_frame:
            self.afk_start_frame = None
