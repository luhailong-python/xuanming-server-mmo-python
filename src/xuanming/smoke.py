import asyncio
import secrets
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from .grpc_runtime import GrpcClient
from .runtime import Runtime, grpc_address
from .settings import Settings
from .transport import Packet, read_packet, write_packet


def require(condition, reason: str) -> None:
    if not condition:
        raise RuntimeError(reason)


class Client:
    def __init__(self, protocol, reader, writer):
        self.protocol = protocol
        self.reader = reader
        self.writer = writer
        self.pending = []

    async def send(self, message):
        await write_packet(self.writer, Packet.from_message(message))

    async def receive(self, name):
        async with asyncio.timeout(5):
            packet = await read_packet(self.reader)
        require(packet is not None and packet.type_name == name, f"未收到 {name}")
        return self.protocol.parse(name, packet.payload)

    async def content(self, message_id, call_id=0):
        for index, message in enumerate(self.pending):
            if message.message_id == message_id and message.id == call_id:
                return self.pending.pop(index)
        async with asyncio.timeout(5):
            while len(self.pending) < 128:
                message = await self.receive("MessageContent")
                if message.message_id == message_id and message.id == call_id:
                    return message
                self.pending.append(message)
        raise RuntimeError(f"未收到消息号 {message_id}")

    async def rpc(self, message_id, call_id, message):
        await self.send(self.protocol.message(
            "ClientRequest", id=call_id, message_id=message_id, body=message.SerializeToString(),
        ))
        if not call_id:
            return None
        content = await self.content(message_id, call_id)
        require(not content.error_message.id, f"RPC 错误 {content.error_message.id}")
        route = self.protocol.routes[message_id]
        return self.protocol.parse(route.response_type, content.serialized_message)


@asynccontextmanager
async def connect(protocol, endpoint):
    reader, writer = await asyncio.open_connection(endpoint.ip, endpoint.port)
    try:
        yield Client(protocol, reader, writer)
    finally:
        writer.close()
        await writer.wait_closed()


async def smoke() -> list[str]:
    with TemporaryDirectory(prefix="xuanming-smoke-") as folder:
        settings = Settings()
        settings = replace(
            settings, database=Path(folder) / "players.sqlite3",
            gate=replace(settings.gate, port=0), scene=replace(settings.scene, port=0, grpc_port=0),
            battle=replace(settings.battle, port=0, grpc_port=0),
        )
        token = secrets.token_hex(16)
        async with Runtime(settings, "local", dev_token=token) as runtime:
            async with asyncio.timeout(25):
                await _gate_scene(runtime, token)
                await _battle(runtime)
    return [
        "Gate → Scene：验签、登录入场、移动存档及大富翁掷骰、走格、发奖与存档回读通过",
        "Battle：gRPC 建房、签票、TCP 直连及回合结算通过",
    ]


async def _gate_scene(runtime, token):
    p = runtime.protocol
    gate = runtime.gate
    ids = p.message_ids
    async with connect(p, gate.info.endpoint) as client:
        await client.send(gate.issue_ticket())
        verify = await client.receive("ClientTokenVerifyResponse")
        require(verify.success and verify.heartbeat_interval_ms == 10_000, "Gate 票据校验失败")
        await client.send(p.message("ClientHeartbeatRequest", seq=42))
        require((await client.receive("ClientHeartbeatResponse")).seq == 42, "Gate 心跳回显失败")
        login = await client.rpc(ids["ClientPlayerLoginLogin"], 1, p.message(
            "loginpb.LoginRequest", account="local", password=token,
        ))
        require(not login.error_message.id and login.players[0].player.player_id == 1, "开发登录失败")
        entered = await client.rpc(ids["ClientPlayerLoginEnterGame"], 2, p.message(
            "loginpb.EnterGameRequest", player_id=1, request_id="smoke-enter",
        ))
        require(entered.player_id == 1, "Scene 入场失败")
        await client.content(ids["SceneSceneClientPlayerNotifyEnterScene"])
        await client.rpc(ids["SceneSceneClientPlayerSceneInfoC2S"], 0, p.message("SceneInfoRequest"))
        content = await client.content(ids["SceneSceneClientPlayerNotifySceneInfo"])
        scene = p.parse("SceneInfoS2C", content.serialized_message)
        require(scene.scene_info[0].scene_id == 1, "Scene 回包失效")
        wallet = await client.rpc(ids["SceneCurrencyClientPlayerGetCurrencyList"], 3,
                                  p.message("GetCurrencyListRequest"))
        require(wallet.DESCRIPTOR.name == "GetCurrencyListResponse", "钱包协议回包失效")
        await client.rpc(ids["SceneMovementClientPlayerMoveStart"], 0, p.message(
            "MoveStartC2S", input_seq=1, velocity={"x": 1000},
        ))
        ack = await client.content(ids["SceneMovementClientPlayerNotifyMoveAck"])
        motion = p.parse("MoveAckS2C", ack.serialized_message)
        require(motion.input_seq == 1 and motion.server_velocity.x == 10, "移动权威确认失效")
        await asyncio.sleep(runtime.settings.scene_tick_seconds * 2)
        await client.rpc(ids["SceneMovementClientPlayerMoveStop"], 0, p.message("MoveStopC2S", input_seq=2))
        ack = await client.content(ids["SceneMovementClientPlayerNotifyMoveAck"])
        require(p.parse("MoveAckS2C", ack.serialized_message).server_location.x > 0, "Scene 移动未推进")
        roll = await _monopoly(client)
        await client.rpc(ids["ClientPlayerLoginLeaveGame"], 4, p.message("loginpb.LeaveGameRequest"))
        async with asyncio.timeout(5):
            while runtime.scene.locations:
                await asyncio.sleep(0.01)
    snapshot = await runtime.scene.store.load_and_claim(1, b"smoke-verifier")
    saved = runtime.scene.rows.assemble(snapshot)
    require(saved.transform.location.x > 0, "登出位置未持久化")
    require(saved.client_request_ledger.monopoly_last_roll == roll and saved.currency.values[0] == roll.state.coins,
            "棋盘回执和奖励未一同存档")
    await runtime.scene.store.release(snapshot)


async def _monopoly(client):
    p = client.protocol
    board = await client.rpc(p.message_ids["MonopolyClientPlayerGetBoard"], 50, p.message("monopoly.GetBoardRequest"))
    roll_id = p.message_ids["MonopolyClientPlayerRollDice"]
    state = board.state
    for call_id in range(51, 51 + (len(board.tiles) + 1) // 2):
        request = p.message("monopoly.RollDiceRequest", roll_seq=state.next_roll_seq)
        reply = await client.rpc(roll_id, call_id, request)
        require(reply.durable and not reply.replayed and not reply.error_message.id, "掷骰未完成存档")
        result = reply.result
        require(result.to_position == (state.position + result.die_one + result.die_two) % len(board.tiles)
                and result.state.dice_remaining == state.dice_remaining - 1, "掷骰走格结果不符")
        require(result.state.coins == state.coins + result.coins_credited, "棋盘奖励余额不符")
        if result.coins_credited:
            replay = await client.rpc(roll_id, 60, request)
            require(replay.replayed and replay.durable and replay.result == result, "掷骰重试未返回原回执")
            return result
        state = result.state
    raise RuntimeError("绕行棋盘后未收到奖励")


async def _battle(runtime):
    p = runtime.protocol
    node = runtime.battle_info
    now = time.time_ns() // 1_000_000
    create = p.message(
        "CreateBattleRequest", battle_id=10, match_mode=7, seed=5489,
        deadline_ms=now + 60_000, admit_before_ms=now + 10_000,
        placement_operation_id="smoke-placement", target_instance_id=node.node_uuid,
        required_physical_region=node.physical_region, outcome_protocol_version=1,
    )
    for player_id, side, speed in ((101, 0, 10), (202, 1, 5)):
        player = create.players.add(player_id=player_id, team_index=side, max_health=10, physical_attack=80)
        player.base_attributes.health, player.base_attributes.speed = 10, speed
        player.routing.gate_instance_id = runtime.gate.info.node_uuid
        player.routing.scene_instance_id = runtime.scene.info.node_uuid
    grpc = GrpcClient(grpc_address(node.grpc_endpoint.ip, node.grpc_endpoint.port))
    try:
        data = await grpc.call("/BattleNode/CreateBattle", create.SerializeToString())
        response = p.parse("CreateBattleResponse", data)
        require(not response.error_message.id and response.room_count == 1, "Battle 建房失败")
        assignments = []
        for player_id in (101, 202):
            request = p.message(
                "IssueBattleTicketRequest", battle_id=10, player_id=player_id, target_instance_id=node.node_uuid,
            )
            data = await grpc.call("/BattleNode/IssueBattleTicket", request.SerializeToString())
            response = p.parse("IssueBattleTicketResponse", data)
            require(not response.error_message.id and response.assignment.token_payload, "Battle 签票失败")
            assignments.append(response.assignment)
    finally:
        await grpc.close()
    async with connect(p, node.endpoint) as first, connect(p, node.endpoint) as second:
        for client, ticket in zip((first, second), assignments):
            await client.send(p.message(
                "BattleTokenVerifyRequest", payload=ticket.token_payload, signature=ticket.token_signature,
            ))
            verify = await client.receive("BattleTokenVerifyResponse")
            require(verify.success and verify.battle_id == 10, "Battle 直连票据校验失败")
        state = await first.rpc(140, 1, p.message("GetBattleStateRequest", battle_id=10))
        require(len(state.actors) == 2 and state.round_index == 1, "Battle 房间状态不完整")
        action = p.message("SubmitBattleActionRequest", battle_id=10)
        action.action.action_type = 1
        for client, target in ((first, 202), (second, 101)):
            action.action.target_id = target
            response = await client.rpc(149, 2, action)
            require(not response.error_message.id, "Battle 行动被拒绝")
        push = await first.content(139)
        turn = p.parse("TurnResultS2C", push.serialized_message)
        require(turn.round_index == 1 and any(actor.is_dead for actor in turn.state.actors),
                "Battle 回合结算未完成")
