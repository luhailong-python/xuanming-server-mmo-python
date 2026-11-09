"""任务状态和正式事件进度，跟背包奖励共用一次玩家存盘。"""

import time
from collections import Counter

KILL, LEVEL, COMPLETE = 1, 6, 8
MAX_COUNT = (1 << 32) - 1


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def effective_level(data) -> int:
    return min(85, max(1, data.level_component.level))


def reward_items(tables, reward_id: int) -> dict[int, int] | None:
    if not reward_id:
        return {}
    row = tables["Reward"].get(reward_id)
    if row is None or not row.reward:
        return None
    items = Counter()
    for entry in row.reward:
        config = entry.reward_item
        items[config] += entry.reward_count
        if not entry.reward_count or not tables["Item"].get(config) or items[config] > MAX_COUNT:
            return None
    return dict(items)


class Missions:
    def __init__(self, scene, tables, assets, *, clock=now_ms):
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets
        self.clock = clock
        self.on_completed = None

    def register(self) -> None:
        name = "SceneMissionClientPlayer"
        self.scene.register(name, "GetMissionList", self.get)
        self.scene.register(name, "AcceptMission", self.accept, mutating=True)
        self.scene.register(name, "ClaimMissionReward", self.claim, mutating=True)

    def scopes(self, data):
        """合并重复 scope，并按 C++ 兼容旧任务存档。"""
        state = data.mission_component
        if not state.scoped_state_present and not state.scopes:
            scope = state.scopes.add(scope=0)
            finished = {entry.config_id for entry in state.active if entry.state == 3}
            done, claimable = set(state.completed) | finished, set()
            for entry in state.active:
                key = entry.config_id
                if key in finished:
                    continue
                if entry.state == 2:
                    done.add(key)
                    claimable.add(key)
                    scope.mission_list.missions.pop(key, None)
                    scope.mission_list.mission_begin_time[key] = entry.accepted_at_ms
                elif key not in claimable and key not in scope.mission_list.missions:
                    row = self.tables["Mission"].get(key)
                    progress = [entry.progress]
                    if row:
                        progress.extend([0] * (len(row.condition_id) - 1))
                    scope.mission_list.missions[key].CopyFrom(
                        self.protocol.message("MissionComp", id=key, progress=progress),
                    )
                    scope.mission_list.mission_begin_time[key] = entry.accepted_at_ms
            scope.completed_mission_ids.extend(sorted(done))
            scope.claimable_mission_ids.extend(sorted(claimable))
        canonical = {}
        for original in state.scopes:
            if original.scope not in canonical:
                copy = self.protocol.message("QuestScopeData")
                copy.CopyFrom(original)
                canonical[original.scope] = copy
                continue
            first = canonical[original.scope]
            for key, mission in original.mission_list.missions.items():
                if key not in first.mission_list.missions:
                    first.mission_list.missions[key].CopyFrom(mission)
            for key, stamp in original.mission_list.mission_begin_time.items():
                if key not in first.mission_list.mission_begin_time:
                    first.mission_list.mission_begin_time[key] = stamp
            first.completed_mission_ids.extend(original.completed_mission_ids)
            first.claimable_mission_ids.extend(original.claimable_mission_ids)
        canonical.setdefault(0, self.protocol.message("QuestScopeData"))
        state.ClearField("active")
        state.ClearField("completed")
        state.ClearField("scopes")
        state.scoped_state_present = True
        for key, value in sorted(canonical.items()):
            value.mission_list.type = key
            value.mission_list.ClearField("complete_missions")
            for field in (value.completed_mission_ids, value.claimable_mission_ids):
                ordered = sorted(set(field))
                del field[:]
                field.extend(ordered)
            state.scopes.add().CopyFrom(value)
        return {scope.scope: scope for scope in state.scopes}

    def _requirements(self, row):
        for index, condition_id in enumerate(row.condition_id):
            condition = self.tables["Condition"].get(condition_id)
            target = row.target_count[index] if index < len(row.target_count) else 0
            yield condition, target or (condition.target_count if condition else 0)

    def _accept_error(self, scope, row, stamp: int) -> int:
        if row is None:
            return 1001
        if not row.condition_id or row.condition_order > 1:
            return 1002
        if row.id in scope.mission_list.missions:
            return 5004
        if row.id in scope.completed_mission_ids or row.id in scope.claimable_mission_ids:
            return 5001
        for key in scope.mission_list.missions:
            # Native RebuildIndexes preserves completed progress without reserving its type.
            if key in scope.completed_mission_ids:
                continue
            existing = self.tables["Mission"].get(key)
            if existing and (existing.mission_type, existing.mission_sub_type) == (row.mission_type, row.mission_sub_type):
                return 5000
        for condition, target in self._requirements(row):
            if (not condition or not target or condition.comparison_op not in (0, 1, 4)
                    or condition.comparison_op == 1 and target == MAX_COUNT):
                return 1002
            category = condition.condition_category
            if (
                category not in (KILL, LEVEL, COMPLETE) or condition.valid_duration
                or condition.condition2 or condition.condition3 or condition.condition4
                or condition.quantity_type > 1 or condition.quantity_type == 1 and category != LEVEL
            ):
                return 1003
            if category == KILL and not any(
                monster and self.tables["Monster"].get(monster)
                and (not condition.condition1 or monster in condition.condition1)
                for dungeon in self.tables["Dungeon"].rows() for monster in dungeon.monster
            ):
                return 1003
        if reward_items(self.tables, row.reward_id) is None:
            return 1002
        if row.mission_type == 2:
            from .scene_activity import schedule

            error, activity = schedule(self.protocol, self.tables, row, stamp)
            return error or (0 if activity.status == 2 else 1006)
        return 0

    def _claim_error(self, scope, row, mission_id) -> int:
        done = mission_id in scope.completed_mission_ids
        if mission_id not in scope.claimable_mission_ids:
            return 12000 if done else 5002
        return 0 if done and row and row.reward_id and reward_items(self.tables, row.reward_id) else 1002

    async def _grant(self, data, scope, mission_id, *, gains=None) -> int:
        row = self.tables["Mission"].get(mission_id)
        error = self._claim_error(scope, row, mission_id)
        if error:
            return error
        error = await self.assets.add_items(data, reward_items(self.tables, row.reward_id), gains=gains)
        if not error:
            scope.claimable_mission_ids.remove(mission_id)
        return error

    async def _accept(self, data, scope, row, stamp: int, *, gains=None) -> int:
        error = self._accept_error(scope, row, stamp)
        if error:
            return error
        mission = scope.mission_list.missions[row.id]
        mission.id = row.id
        mission.progress.extend([0] * len(row.condition_id))
        scope.mission_list.mission_begin_time[row.id] = stamp
        level = effective_level(data)
        await self._event(data, scope, LEVEL, level, level, stamp, only=row.id, gains=gains)
        for completed in sorted(set(scope.completed_mission_ids)):
            await self._event(data, scope, COMPLETE, completed, 1, stamp, only=row.id, gains=gains)
        return 0

    async def battle_defeats(self, data, defeated, stamp: int, *, gains=None) -> None:
        scope = self.scopes(data)[0]
        for monster in defeated:
            if monster.monster_config_id:
                for _ in range(monster.count):
                    await self._event(data, scope, KILL, monster.monster_config_id, 1, stamp, gains=gains)

    async def _event(self, data, scope, category, value, amount, stamp, *, only=0, gains=None) -> None:
        active = scope.mission_list.missions
        completed = []
        for mission_id in sorted(active) if not only else (only,):
            row = self.tables["Mission"].get(mission_id)
            mission = active.get(mission_id)
            if (row is None or mission is None or mission.id != mission_id or mission.status in (3, 4)
                    or mission_id in scope.completed_mission_ids or len(mission.progress) != len(row.condition_id)):
                continue
            requirements = list(self._requirements(row))
            changed = False
            for index, (condition, target) in enumerate(requirements):
                if condition is None or fulfilled(condition.comparison_op, mission.progress[index], target):
                    continue
                if condition.condition_category == category and _matches(condition, category, value):
                    candidate = amount if category == LEVEL or condition.quantity_type == 1 else min(
                        mission.progress[index] + amount, target + int(condition.comparison_op == 1),
                    )
                    if candidate != mission.progress[index]:
                        mission.progress[index] = candidate
                        changed = True
                if row.condition_order:
                    break
            if changed and all(condition and fulfilled(condition.comparison_op, value, target)
                               for (condition, target), value in zip(requirements, mission.progress)):
                completed.append(row)
                del active[mission_id]
                scope.completed_mission_ids.append(mission_id)
                if row.reward_id:
                    scope.claimable_mission_ids.append(mission_id)
        for row in completed:
            if row.auto_reward and row.reward_id:
                await self._grant(data, scope, row.id, gains=gains)
        for row in completed:
            for next_id in row.next_mission_id:
                await self._accept(data, scope, self.tables["Mission"].get(next_id), stamp, gains=gains)
        for row in completed:
            if self.on_completed:
                self.on_completed(scope, row, stamp)
            await self._event(data, scope, COMPLETE, row.id, 1, stamp, gains=gains)

    def get(self, world, player, route, request):
        data = self.scene.rows.assemble(player.snapshot)
        return self._list(player, self.scopes(data), self.clock())

    async def accept(self, world, player, route, request):
        return await self._action(player, request, True)

    async def claim(self, world, player, route, request):
        return await self._action(player, request, False)

    async def _action(self, player, request, accept: bool):
        if request.scope or not self.scene.mutable(player):
            return self.protocol.message("GetMissionListResponse", error_message={"id": 1005})
        data = self.scene.rows.assemble(player.snapshot)
        before = type(data).FromString(data.SerializeToString()) if self.assets.audit_enabled else None
        gains = []
        scopes = self.scopes(data)
        stamp = self.clock()
        if accept:
            error = await self._accept(data, scopes[0], self.tables["Mission"].get(request.mission_id), stamp, gains=gains)
        else:
            error = await self._grant(data, scopes[0], request.mission_id, gains=gains)
        if error:
            return self.protocol.message("GetMissionListResponse", error_message={"id": error})
        self.scopes(data)
        audit = self.assets.make_audit_callback(player, before, data, 7, gains=gains)
        await self.scene.commit_player_data(player, data, on_committed=audit)
        return self.get(None, player, None, None)

    def _list(self, player, scopes, stamp: int):
        response = self.protocol.message("GetMissionListResponse", state_persistent=True)
        keys = {(0, row.id) for row in self.tables["Mission"].rows()}
        for key, scope in scopes.items():
            keys.update((key, mission) for mission in (
                set(scope.mission_list.missions) | set(scope.completed_mission_ids) | set(scope.claimable_mission_ids)
            ))
        for key, mission_id in sorted(keys):
            scope = scopes[key]
            active = scope.mission_list.missions.get(mission_id)
            status = (3 if mission_id in scope.claimable_mission_ids else 2 if mission_id in scope.completed_mission_ids
                      else 0 if active is None else 4 if active.status in (3, 4) else 1)
            info = response.missions.add(mission_id=mission_id, scope=key, status=status)
            row = self.tables["Mission"].get(mission_id)
            if row is None:
                info.unavailable_reason = "任务配置暂不可用"
                continue
            info.configured = True
            info.mission_type, info.mission_sub_type = row.mission_type, row.mission_sub_type
            info.reward_id, info.auto_reward = row.reward_id, bool(row.auto_reward)
            accept = self._accept_error(scope, row, stamp)
            info.can_accept = not status and not key and not accept and self.scene.mutable(player)
            info.can_claim = status == 3 and not key and not self._claim_error(scope, row, mission_id) and self.scene.mutable(player)
            if not info.can_accept and not info.can_claim:
                info.unavailable_reason = {
                    1: "完成任务目标后领取奖励", 2: "任务已完成", 3: "当前状态暂不可领奖，请稍后重试", 4: "任务已失效",
                }.get(status, "请先完成同类型任务" if accept == 5000 else "当前条件下暂不可接取")
                if not status and row.mission_type == 2 and accept != 5000:
                    from .scene_activity import schedule

                    info.unavailable_reason = schedule(self.protocol, self.tables, row, stamp)[1].unavailable_reason
            for index, (condition, target) in enumerate(self._requirements(row)):
                objective = info.objectives.add(objective_index=index, condition_id=row.condition_id[index])
                if condition is None:
                    continue
                progress = active.progress[index] if active and index < len(active.progress) else 0
                objective.category, objective.target, objective.progress = condition.condition_category, target, progress
                objective.completed = status in (2, 3) or active is not None and fulfilled(
                    condition.comparison_op, progress, target,
                )
        return response


def fulfilled(operation: int, progress: int, target: int) -> bool:
    return (
        operation == 0 and progress >= target or operation == 1 and progress > target
        or operation == 2 and progress <= target or operation == 3 and progress < target
        or operation == 4 and progress == target
    )


def _matches(condition, category, value) -> bool:
    if category == LEVEL:
        return not condition.condition1 or any(value >= level for level in condition.condition1)
    return not condition.condition1 or value in condition.condition1
