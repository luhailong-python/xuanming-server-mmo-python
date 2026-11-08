"""角色和宠物共用的属性点分配规则。"""

from dataclasses import dataclass
from enum import Enum, auto

_MAX_UINT32 = (1 << 32) - 1


class AllocationError(Enum):
    OK = auto()
    POOL_LOCKED = auto()
    DIMENSION_NOT_IN_POOL = auto()
    CANNOT_DECREASE = auto()
    CAP_EXCEEDED = auto()
    NOT_ENOUGH_POINTS = auto()
    NOTHING_TO_CHANGE = auto()


@dataclass(frozen=True, slots=True)
class PoolAllocation:
    total: int
    known_used: int
    effective: bool
    remaining: int


def is_unlocked(pool, level: int) -> bool:
    return 0 < pool.unlock_level <= level


def total_points(pool, level: int, bonus: int = 0) -> int:
    if not is_unlocked(pool, level):
        return 0
    return min(_MAX_UINT32, pool.base_points + pool.points_per_level * (level - pool.unlock_level + 1) + bonus)


def evaluate_pool(pool, level, current, *, bonus=0, foreign=0) -> PoolAllocation:
    total = total_points(pool, level, bonus)
    used = sum(current.values())
    effective = used <= total and (not pool.dimension_cap or max(current.values(), default=0) <= pool.dimension_cap)
    return PoolAllocation(total, used, effective, max(0, total - used - foreign) if effective else 0)


def validate_allocation(pool, level, current, target, remaining) -> AllocationError:
    if not is_unlocked(pool, level):
        return AllocationError.POOL_LOCKED
    delta = 0
    for dimension, want in sorted(target.items()):
        if dimension not in current:
            return AllocationError.DIMENSION_NOT_IN_POOL
        prior = current[dimension]
        if want < prior:
            return AllocationError.CANNOT_DECREASE
        if pool.dimension_cap and want > pool.dimension_cap:
            return AllocationError.CAP_EXCEEDED
        delta += want - prior
    if not delta:
        return AllocationError.NOTHING_TO_CHANGE
    if delta > remaining:
        return AllocationError.NOT_ENOUGH_POINTS
    return AllocationError.OK


def distribute_points(pool, order, weights, remaining, allocated) -> None:
    if not remaining or not order:
        return
    if pool.dimension_cap:
        for dimension in order:
            current = allocated.get(dimension, 0)
            points = min(remaining, max(0, pool.dimension_cap - current))
            allocated[dimension] = current + points
            remaining -= points
            if not remaining:
                break
        return
    weights = [weights[index] if index < len(weights) else 1 for index in range(len(order))]
    total_weight = sum(weights)
    if not total_weight:
        return
    left = remaining
    for dimension, weight in zip(order, weights):
        points = remaining * weight // total_weight
        allocated[dimension] = allocated.get(dimension, 0) + points
        left -= points
    for dimension, weight in zip(order, weights):
        if not left:
            break
        if weight:
            allocated[dimension] += 1
            left -= 1
