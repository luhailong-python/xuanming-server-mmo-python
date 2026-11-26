"""从任务完成事实计算周期活跃度和幂等阈值奖励。"""

from .scene_mission import now_ms, reward_items

MAX_LEDGER = 4096
MAX_UINT64 = (1 << 64) - 1


def schedule(protocol, tables, mission, stamp: int):
    info = protocol.message("PlayerActivityInfo")
    row = tables["ActivitySchedule"].get(mission.id)
    if mission.mission_type != 2 or row and (
        row.category > 4 or row.vitality_points and (not row.definition_key or not row.configuration_revision)
        or (row.baseline_start_at_ms or row.baseline_end_at_ms)
        and (not row.baseline_start_at_ms or row.baseline_end_at_ms <= row.baseline_start_at_ms)
        or row.enabled and not row.baseline_start_at_ms
    ):
        info.unavailable_reason = "活动配置异常，暂不可参与"
        return 1002, info
    info.activity_id = info.mission_id = mission.id
    info.reward_id = mission.reward_id
    info.entry_kind = 1
    if row:
        for key in ("definition_key", "configuration_revision", "category", "name", "description",
                    "icon_key", "vitality_points"):
            setattr(info, key, getattr(row, key))
    if not row or not row.enabled:
        info.unavailable_reason = "活动尚未排期，敬请期待"
        return 0, info
    info.starts_at_ms, info.ends_at_ms = row.baseline_start_at_ms, row.baseline_end_at_ms
    if stamp < info.starts_at_ms:
        info.status, info.unavailable_reason = 1, "活动尚未开始"
    elif stamp >= info.ends_at_ms:
        info.status, info.unavailable_reason = 3, "活动已结束"
    else:
        info.status = 2
    return 0, info


def _period(rule, stamp: int):
    first, duration = rule.first_period_starts_at_ms, rule.period_duration_ms
    if not all((rule.id, rule.configuration_revision, rule.points_limit, first, duration)) or stamp < first:
        return None
    start = stamp - (stamp - first) % duration
    return (start, start + duration) if start + duration <= MAX_UINT64 else None


def _valid_ledger(ledger) -> bool:
    if len(ledger.sources) > MAX_LEDGER or len(ledger.claims) > MAX_LEDGER:
        return False
    sources, claims, request_ids = set(), set(), set()
    for source in ledger.sources:
        if (
            source.mission_scope or source.mission_id in sources
            or not all((source.mission_id, source.rule_id, source.configuration_revision, source.period_id, source.points))
        ):
            return False
        sources.add(source.mission_id)
    for claim in ledger.claims:
        key = claim.rule_id, claim.period_id, claim.threshold_id
        if (
            key in claims or claim.request_id in request_ids or len(claim.request_id.encode()) > 64
            or not all((*key, claim.configuration_revision, claim.reward_id, claim.claimed_at_ms, claim.request_id))
        ):
            return False
        claims.add(key)
        request_ids.add(claim.request_id)
    return True


def _points(ledger, rule, period) -> int:
    return min(rule.points_limit, sum(
        source.points for source in ledger.sources if source.rule_id == rule.id and source.period_id == period
    ))


class Activities:
    def __init__(self, missions, *, clock=now_ms) -> None:
        self.missions = missions
        self.scene = missions.scene
        self.protocol = missions.protocol
        self.tables = missions.tables
        self.assets = missions.assets
        self.clock = clock
        missions.on_completed = self.completed

    def register(self) -> None:
        name = "SceneActivityClientPlayer"
        self.scene.register(name, "GetActivityList", self.get)
        self.scene.register(name, "ClaimActivityVitalityReward", self.claim, mutating=True)

    def _rule(self):
        rows = [row for row in self.tables["ActivityVitalityRule"].rows() if row.enabled]
        return rows[0] if len(rows) == 1 else None

    def _thresholds(self, rule):
        rows = [row for row in self.tables["ActivityVitalityReward"].rows() if row.rule_id == rule.id]
        if not rows or len(rows) > 64 or len({row.id for row in rows}) != len(rows):
            return None
        if not all(0 < row.required_points <= rule.points_limit and row.id
                   and reward_items(self.tables, row.reward_id) for row in rows):
            return None
        return sorted(rows, key=lambda row: (row.required_points, row.id))

    def _can_claim(self, player):
        return self.scene.mutable(player) and not (self.scene.battle and self.scene.battle.frozen(player.player_id))

    def completed(self, scope, mission, stamp: int) -> None:
        if mission.mission_type != 2:
            return
        row = self.tables["ActivitySchedule"].get(mission.id)
        rule = self._rule()
        period = _period(rule, stamp) if rule else None
        ledger = scope.mission_list.activity_ledger
        if (
            not row or not row.definition_key or not row.configuration_revision or not row.vitality_points
            or not period or len(ledger.sources) >= MAX_LEDGER or not _valid_ledger(ledger)
            or any(source.mission_id == mission.id for source in ledger.sources)
        ):
            return
        ledger.sources.add(mission_id=mission.id, rule_id=rule.id, configuration_revision=rule.configuration_revision,
                           period_id=period[0], points=row.vitality_points)

    def _display(self, counts, destination) -> set[int]:
        types = set()
        for key, amount in sorted(counts.items()):
            item = self.tables["Item"].get(key)
            destination.add(item_config_id=key, count=amount, name=item.name, icon_key=item.icon_key)
            types.add(4 if item.equip_kind else 3)
        return types

    def _vitality(self, player, ledger, stamp, out) -> int:
        if not _valid_ledger(ledger):
            return 1002
        rule = self._rule()
        if rule is None:
            out.unavailable_reason = "活跃规则与阈值奖励尚未配置"
            return 0
        period, thresholds = _period(rule, stamp), self._thresholds(rule)
        if not period or thresholds is None:
            out.unavailable_reason = "活跃配置异常或尚未开放"
            return 0
        out.available = True
        out.rule_id, out.configuration_revision = rule.id, rule.configuration_revision
        out.period_id = out.period_starts_at_ms = period[0]
        out.period_ends_at_ms, out.points_limit = period[1], rule.points_limit
        out.points = _points(ledger, rule, period[0])
        claimed = {claim.threshold_id for claim in ledger.claims
                   if claim.rule_id == rule.id and claim.period_id == period[0]}
        for row in thresholds:
            shown = out.rewards.add(threshold_id=row.id, required_points=row.required_points, reward_id=row.reward_id)
            shown.claimed = shown.durable = row.id in claimed
            shown.can_claim = (not shown.claimed and out.points >= row.required_points
                               and len(ledger.claims) < MAX_LEDGER and self._can_claim(player))
            self._display(reward_items(self.tables, row.reward_id), shown.items)
            if shown.claimed:
                shown.unavailable_reason = "已领取"
            elif out.points < row.required_points:
                shown.unavailable_reason = "活跃积分尚未达到门槛"
            elif not shown.can_claim:
                shown.unavailable_reason = "当前状态暂不可领取"
        return 0

    def get(self, world, player, route, request):
        data = self.scene.rows.assemble(player.snapshot)
        scope = self.missions.scopes(data)[0]
        stamp = self.clock()
        response = self.protocol.message("GetActivityListResponse", server_time_ms=stamp)
        error = self._vitality(player, scope.mission_list.activity_ledger, stamp, response.vitality)
        if error:
            response.error_message.id = error
            return response
        for mission in sorted(self.tables["Mission"].rows(), key=lambda row: row.id):
            if mission.mission_type != 2:
                continue
            error, info = schedule(self.protocol, self.tables, mission, stamp)
            if error:
                info.activity_id = info.mission_id = mission.id
                response.activities.append(info)
                continue
            info.completion_count_known = True
            info.completed_count = int(mission.id in scope.completed_mission_ids)
            info.completion_limit, info.completion_period = 1, "每角色一次"
            counts = reward_items(self.tables, mission.reward_id)
            if counts is None:
                info.unavailable_reason = "活动奖励配置异常，暂不可参与"
            else:
                info.reward_types.extend(sorted(self._display(counts, info.rewards)))
                if info.status == 2:
                    error = self.missions._accept_error(scope, mission, stamp)
                    info.can_participate = not error and self.scene.mutable(player)
                    if not info.can_participate:
                        info.unavailable_reason = {
                            5004: "请在任务页查看活动进度", 5001: "请在任务页查看活动进度",
                            5000: "请先完成同类型活动任务",
                        }.get(error, "活动所需玩法或当前状态暂不满足参与条件")
            response.activities.append(info)
        festival_beast = getattr(self.scene, "festival_beast", None)
        if festival_beast is not None and hasattr(festival_beast, "append_activity_catalog_entry"):
            festival_beast.append_activity_catalog_entry(player, stamp, response)
        travel_pass = getattr(self.scene, "travel_pass", None)
        if travel_pass is not None and hasattr(travel_pass, "append_activity_catalog_entries"):
            travel_pass.append_activity_catalog_entries(player, data, stamp, response)
        return response

    async def claim(self, world, player, route, request):
        response = self.protocol.message("ClaimActivityVitalityRewardResponse", request_id=request.request_id)
        data = self.scene.rows.assemble(player.snapshot)
        ledger = self.missions.scopes(data)[0].mission_list.activity_ledger
        stamp = self.clock()
        error = await self._claim(player, data, ledger, stamp, request, response)
        if error:
            response.error_message.id = error
        return response

    async def _claim(self, player, data, ledger, stamp, request, out) -> int:
        key = request.rule_id, request.period_id, request.threshold_id
        if not _valid_ledger(ledger):
            return 1002
        if not all((*key, request.configuration_revision, request.request_id)) or len(request.request_id.encode()) > 64:
            return 1005
        for claim in ledger.claims:
            if claim.request_id == request.request_id:
                if (claim.rule_id, claim.period_id, claim.threshold_id, claim.configuration_revision) != (*key, request.configuration_revision):
                    return 1005
                out.replayed = out.claimed = out.durable = True
                return self._vitality(player, ledger, stamp, out.snapshot)
        if any((claim.rule_id, claim.period_id, claim.threshold_id) == key for claim in ledger.claims):
            return 12000
        rule = self._rule()
        if not self._can_claim(player) or rule is None:
            return 1006
        period, thresholds = _period(rule, stamp), self._thresholds(rule)
        if not period or thresholds is None:
            return 1002
        row = next((row for row in thresholds if row.id == request.threshold_id), None)
        if (
            not row or (request.rule_id, request.configuration_revision, request.period_id)
            != (rule.id, rule.configuration_revision, period[0])
            or _points(ledger, rule, period[0]) < row.required_points
        ):
            return 1005
        if len(ledger.claims) >= MAX_LEDGER:
            return 1003
        before = type(data).FromString(data.SerializeToString()) if self.assets.audit_enabled else None
        gains = []
        error = await self.assets.add_items(data, reward_items(self.tables, row.reward_id), gains=gains)
        if error:
            return error
        ledger.claims.add(rule_id=rule.id, configuration_revision=rule.configuration_revision, period_id=period[0],
                          threshold_id=row.id, request_id=request.request_id, reward_id=row.reward_id, claimed_at_ms=stamp)
        audit = self.assets.make_audit_callback(player, before, data, 7, gains=gains)
        await self.scene.commit_player_data(player, data, on_committed=audit)
        out.claimed = out.durable = True
        return self._vitality(player, ledger, stamp, out.snapshot)
