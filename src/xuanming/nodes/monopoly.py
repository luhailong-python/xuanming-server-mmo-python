"""服务端棋盘移动、奖励与地标建造。"""

from inspect import isawaitable
from dataclasses import dataclass
from secrets import randbelow
from time import time_ns

from ..data.asset_inventory import FEATURE_UNAVAILABLE, INVALID, INVALID_DATA, balance, clone
from ..data.asset_ledger import UINT64_MAX

START, PROPERTY, BONUS, EMPTY = 1, 2, 3, 4
INITIAL_DICE = 20
DICE_REFILL_INTERVAL_MS = 5 * 60 * 1000
PASS_START_REWARD = 100


@dataclass(frozen=True, slots=True)
class Board:
    name: str
    tiles: tuple[tuple[int, int], ...]
    landmarks: dict[int, tuple[str, tuple[int, ...]]]
    completion_reward: int
    next_board_id: int = 0


BOARDS = {
    1: Board("起始小镇", (
        (START, 0), (PROPERTY, 20), (PROPERTY, 35), (EMPTY, 0),
        (PROPERTY, 40), (BONUS, 60), (PROPERTY, 45), (EMPTY, 0),
        (PROPERTY, 65), (BONUS, 80), (PROPERTY, 85), (BONUS, 100),
    ), {
        1: ("钟楼", (30, 60, 120)),
        2: ("市集", (40, 80, 160)),
        3: ("花园", (50, 100, 200)),
    }, 300, next_board_id=2),
    2: Board("海港小镇", (
        (START, 0), (PROPERTY, 30), (BONUS, 50), (EMPTY, 0),
        (PROPERTY, 55), (PROPERTY, 65), (BONUS, 75), (EMPTY, 0),
        (PROPERTY, 80), (BONUS, 95), (PROPERTY, 100), (BONUS, 120),
    ), {
        1: ("灯塔", (30, 60, 120)),
        2: ("码头", (40, 80, 160)),
        3: ("水族馆", (50, 100, 200)),
    }, 450),
}


def _now_ms() -> int:
    return time_ns() // 1_000_000


class Monopoly:
    def __init__(self, scene) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.assets = scene.assets
        currency = self.protocol.pool.FindEnumTypeByName("common.base.CurrencyType")
        self.coin = currency.values_by_name["CURRENCY_TYPE_GOLD"].number
        tx_types = self.protocol.pool.FindEnumTypeByName("TransactionType")
        self.reward_tx = tx_types.values_by_name["TX_SYSTEM_GRANT"].number
        self.upgrade_tx = tx_types.values_by_name["TX_CURRENCY_DEDUCT"].number

    def register(self) -> None:
        service = "monopoly.MonopolyClientPlayer"
        self.scene.register(service, "GetBoard", self.get_board, mutating=True)
        self.scene.register(service, "RollDice", self.roll_dice, mutating=True)
        self.scene.register(service, "UpgradeLandmark", self.upgrade_landmark, mutating=True)

    def _can_mutate(self, player) -> bool:
        return self.scene.mutable(player) and not (self.scene.battle and self.scene.battle.frozen(player.player_id))

    def _state(self, data, board_id):
        ledger = data.client_request_ledger
        last = ledger.monopoly_last_roll
        state = clone(last.state) if last.roll_seq and (last.board_id or 1) == board_id else self.protocol.message(
            "monopoly.BoardState", dice_remaining=INITIAL_DICE, next_roll_seq=last.roll_seq + 1,
        )
        state.coins = balance(data, self.coin)
        if ledger.HasField("monopoly_dice"):
            self._set_dice_view(state, ledger.monopoly_dice)
        return state

    @staticmethod
    def _set_dice_view(state, dice):
        state.dice_remaining = dice.dice_remaining
        state.next_dice_at_ms = (
            dice.anchor_ms + DICE_REFILL_INTERVAL_MS if dice.anchor_ms and dice.dice_remaining < INITIAL_DICE else 0
        )

    def _refresh_dice(self, state, dice, now):
        if state.dice_remaining >= INITIAL_DICE:
            return False
        if not dice.anchor_ms:
            # 旧存档只有余额，首次读取或使用时开始计时。
            dice.dice_remaining, dice.anchor_ms = state.dice_remaining, now
        else:
            ticks = (now - dice.anchor_ms) // DICE_REFILL_INTERVAL_MS
            if ticks <= 0:
                return False
            dice.dice_remaining = min(INITIAL_DICE, dice.dice_remaining + ticks)
            # 满额以后不累计超出的时间。保留这个时刻防止时钟回拨提前起算。
            if dice.dice_remaining == INITIAL_DICE:
                dice.anchor_ms = now
            else:
                dice.anchor_ms += ticks * DICE_REFILL_INTERVAL_MS
        self._set_dice_view(state, dice)
        return True

    @staticmethod
    def _at_cap(board, levels):
        return all(levels.get(landmark_id, 0) == len(costs) for landmark_id, (_, costs) in board.landmarks.items())

    def _complete_board(self, player, data, board_id, board, correlation, now):
        before = clone(data) if self.assets.audit_enabled else None
        previous_coins = balance(data, self.coin)
        gains = []
        error = self.assets.add_currency(data, self.coin, board.completion_reward, gains=gains)
        if error:
            return error, None
        progress = data.client_request_ledger.monopoly_landmarks
        progress.last_completion.CopyFrom(self.protocol.message(
            "monopoly.BoardCompletion", board_id=board_id, next_board_id=board.next_board_id,
            coin_reward=board.completion_reward, coins_credited=balance(data, self.coin) - previous_coins,
        ))
        progress.board_id = board.next_board_id or board_id
        if board.next_board_id:
            progress.levels.clear()
            dice = data.client_request_ledger.monopoly_dice
            dice.dice_remaining = INITIAL_DICE
            dice.anchor_ms = max(now, dice.anchor_ms)
        audit = self.assets.make_audit_callback(
            player, before, data, self.reward_tx, gains=gains, correlation=correlation, credit=True,
        )
        return 0, audit

    async def get_board(self, _world, player, _route, _request):
        data = self.scene.rows.assemble(player.snapshot)
        progress = data.client_request_ledger.monopoly_landmarks
        board_id = progress.board_id or 1
        board = BOARDS.get(board_id)
        if board is None:
            return self.protocol.message("monopoly.GetBoardResponse", error_message={"id": INVALID_DATA})

        # 旧版本允许满级但没有通关记录，首次读取时用同一存档补结算。
        now = _now_ms()
        writable = self._can_mutate(player)
        error = 0
        changed = False
        audit = None
        if not progress.board_id and self._at_cap(board, progress.levels):
            if writable:
                error, audit = self._complete_board(player, data, board_id, board, progress.last_upgrade.upgrade_seq, now)
                if not error:
                    changed = True
                    board_id = progress.board_id
                    board = BOARDS[board_id]
            else:
                error = FEATURE_UNAVAILABLE

        state = self._state(data, board_id)
        if writable and not error and self._refresh_dice(state, data.client_request_ledger.monopoly_dice, now):
            changed = True
        if changed:
            await self.scene.commit_player_data(player, data, on_committed=audit)
        response = self.protocol.message(
            "monopoly.GetBoardResponse", state=state, pass_start_reward=PASS_START_REWARD,
            tiles=[{"position": index, "kind": kind, "coin_reward": reward}
                   for index, (kind, reward) in enumerate(board.tiles)],
            next_upgrade_seq=progress.last_upgrade.next_upgrade_seq or 1,
            board_id=board_id, board_name=board.name, completion_reward=board.completion_reward,
            dice_cap=INITIAL_DICE, dice_refill_interval_ms=DICE_REFILL_INTERVAL_MS,
        )
        if progress.HasField("last_completion"):
            response.last_completion.CopyFrom(progress.last_completion)
        if error:
            response.error_message.id = error
        for landmark_id, (name, costs) in board.landmarks.items():
            level = progress.levels.get(landmark_id, 0)
            response.landmarks.add(landmark_id=landmark_id, name=name, level=level, max_level=len(costs),
                                   next_cost=costs[level] if level < len(costs) else 0)
        return response

    async def roll_dice(self, _world, player, _route, request):
        response = self.protocol.message("monopoly.RollDiceResponse")
        data = self.scene.rows.assemble(player.snapshot)
        last = data.client_request_ledger.monopoly_last_roll
        board_id = data.client_request_ledger.monopoly_landmarks.board_id or 1
        requested_board = request.board_id or 1
        seq = request.roll_seq
        if seq and seq == last.roll_seq:
            if requested_board != (last.board_id or 1):
                response.error_message.id = INVALID
            else:
                response.result.CopyFrom(last)
                response.durable = response.replayed = True
            return response

        state = self._state(data, board_id)
        board = BOARDS.get(board_id)
        if requested_board != board_id or not 0 < seq < UINT64_MAX or seq != state.next_roll_seq:
            response.error_message.id = INVALID
        elif (board is None or state.position >= len(board.tiles) or seq != last.roll_seq + 1
              or state.dice_remaining > INITIAL_DICE):
            response.error_message.id = INVALID_DATA
        elif not self._can_mutate(player):
            response.error_message.id = FEATURE_UNAVAILABLE
        if response.error_message.id:
            return response

        now = _now_ms()
        dice = data.client_request_ledger.monopoly_dice
        refreshed = self._refresh_dice(state, dice, now)
        if not state.dice_remaining:
            if refreshed:
                await self.scene.commit_player_data(player, data)
            response.error_message.id = FEATURE_UNAVAILABLE
            return response

        die_one, die_two = randbelow(6) + 1, randbelow(6) + 1
        origin = state.position
        laps, destination = divmod(origin + die_one + die_two, len(board.tiles))
        landing_reward = board.tiles[destination][1]
        pass_reward = laps * PASS_START_REWARD
        reward = landing_reward + pass_reward
        previous_coins = state.coins
        before = clone(data) if reward and self.assets.audit_enabled else None
        gains = []
        if reward:
            error = self.assets.add_currency(data, self.coin, reward, gains=gains)
            if error:
                response.error_message.id = error
                return response

        state.position = destination
        if state.dice_remaining == INITIAL_DICE:
            dice.anchor_ms = max(now, dice.anchor_ms)
        dice.dice_remaining = state.dice_remaining - 1
        self._set_dice_view(state, dice)
        state.next_roll_seq = seq + 1
        state.coins = balance(data, self.coin)
        result = self.protocol.message(
            "monopoly.RollResult", roll_seq=seq, die_one=die_one, die_two=die_two,
            from_position=origin, to_position=destination, passed_start=bool(laps), landing_reward=landing_reward,
            pass_start_reward=pass_reward, coins_credited=state.coins - previous_coins, state=state, board_id=board_id,
        )
        last.CopyFrom(result)
        audit = self.assets.make_audit_callback(
            player, before, data, self.reward_tx, gains=gains, correlation=seq, credit=True,
        )
        await self.scene.commit_player_data(player, data, on_committed=audit)
        response.result.CopyFrom(result)
        response.durable = True
        return response

    async def upgrade_landmark(self, _world, player, _route, request):
        response = self.protocol.message("monopoly.UpgradeLandmarkResponse")
        data = self.scene.rows.assemble(player.snapshot)
        progress = data.client_request_ledger.monopoly_landmarks
        last = progress.last_upgrade
        board_id = progress.board_id or 1
        requested_board = request.board_id or 1
        landmark_id, seq = request.landmark_id, request.upgrade_seq
        if seq and seq == last.upgrade_seq:
            if (requested_board, landmark_id, request.expected_level) != (
                last.board_id or 1, last.landmark_id, last.previous_level,
            ):
                response.error_message.id = INVALID
            else:
                response.result.CopyFrom(last)
                response.durable = response.replayed = True
            return response

        board = BOARDS.get(board_id)
        if board is None:
            response.error_message.id = INVALID_DATA
            return response
        landmark = board.landmarks.get(landmark_id)
        level = progress.levels.get(landmark_id, 0)
        if (requested_board != board_id or landmark is None or request.expected_level != level
                or not 0 < seq < UINT64_MAX or seq != last.upgrade_seq + 1):
            response.error_message.id = INVALID
            return response
        _, costs = landmark
        if level > len(costs):
            response.error_message.id = INVALID_DATA
            return response
        if level == len(costs) or not self._can_mutate(player):
            response.error_message.id = FEATURE_UNAVAILABLE
            return response

        cost = costs[level]
        before = clone(data) if self.assets.audit_enabled else None
        error = self.assets.deduct_currency(data, self.coin, cost)
        if error:
            response.error_message.id = error
            return response
        debit_audit = self.assets.make_audit_callback(player, before, data, self.upgrade_tx, correlation=seq)
        progress.levels[landmark_id] = level + 1
        completed = self._at_cap(board, progress.levels)
        reward_audit = None
        if completed:
            error, reward_audit = self._complete_board(player, data, board_id, board, seq, _now_ms())
            if error:
                response.error_message.id = error
                return response
        result = self.protocol.message(
            "monopoly.LandmarkUpgradeResult", upgrade_seq=seq, landmark_id=landmark_id, previous_level=level,
            level=level + 1, coins_spent=cost, coins_after=balance(data, self.coin), next_upgrade_seq=seq + 1,
            board_id=board_id,
        )
        if completed:
            result.completion.CopyFrom(progress.last_completion)
        last.CopyFrom(result)

        async def audit(snapshot):
            if debit_audit:
                result = debit_audit(snapshot)
                if isawaitable(result):
                    await result
            if reward_audit:
                result = reward_audit(snapshot)
                if isawaitable(result):
                    await result

        await self.scene.commit_player_data(player, data, on_committed=audit)
        response.result.CopyFrom(result)
        response.durable = True
        return response
