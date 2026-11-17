import asyncio
import hashlib
import re
import secrets
import time
import uuid
from itertools import count

from aiokafka import AIOKafkaConsumer

from smoke_original import ENV, GO, RUN, SCRATCH, SOURCE, endpoint, run_mysql, wait_etcd
from xuanming.cluster import EtcdDirectory
from xuanming.grpc_runtime import GrpcClient
from xuanming.nodes.battle_control import service_digest, sign_service_call, verify_service_call
from xuanming.protocol import Protocol
from xuanming.runtime import grpc_address
from xuanming.smoke import connect
from xuanming.transport import read_packet


MATCH_SCHEMA = "mmorpg_match"
INBOX_SCHEMA = "mmorpg_player_inbox"
TERMINAL = "/match.MatchInternal/GetBattleTerminalDecision"
CONTROL_TABLES = {
    "route_meta", "writer_registry", "writer_slot", "writer_lifecycle_receipt",
    "sql_writer_drain_run", "sql_writer_freeze_ticket", "sql_writer_maintenance_receipt", "db_move_sentinel",
}


def configure_battle_env():
    for name in (
        "MMORPG_MATCH_OUTCOME_SECRET_BATTLE", "MMORPG_PLAYER_INBOX_SECRET_MATCH",
        "MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE", "MMORPG_ROLE_FENCE_SECRET_MATCH",
        "MMORPG_ROLE_FENCE_SECRET_PLAYER_INBOX", "MMORPG_ROLE_FENCE_SECRET_COORDINATOR",
        "MMORPG_BATTLE_TOKEN_SECRET", "RANK_PRODUCER_SECRET_MATCH", "MATCH_PROBE_SECRET",
    ):
        ENV.setdefault(name, secrets.token_hex(32))


def _executable(service):
    return GO / f"{service}.exe"


def _config(service, user, password):
    filename = "match_service.yaml" if service == "match" else "player_inbox.yaml"
    original = (SOURCE / "go" / service / "etc" / filename).read_text(encoding="utf-8")
    for current, new in (
        ("127.0.0.1:2379", endpoint("etcd")), ("127.0.0.1:6379", endpoint("redis")),
        ("127.0.0.1:9092", endpoint("kafka")), ("127.0.0.1:4000", endpoint("mysql")),
        ("127.0.0.1:50500" if service == "match" else "127.0.0.1:50950",
         endpoint("match" if service == "match" else "inbox")),
    ):
        original = original.replace(current, new)
    original = re.sub(r"^MetricsListenAddr:.*$", 'MetricsListenAddr: ""', original, flags=re.M)
    original = re.sub(r"^  User:.*$", f"  User: {user}", original, flags=re.M)
    original = re.sub(r"^DataServiceRpc:\n  Etcd:\n    Hosts:\n      - [^\n]+\n    Key: [^\n]+",
                      f'DataServiceRpc:\n  Endpoints: ["{endpoint("data")}"]', original, flags=re.M)
    if service == "match":
        original = original.replace("Backend: tidb", "Backend: mysql")
        original = original.replace("127.0.0.1:51100", endpoint("rank"))
        original = original.replace("../../generated/tables", (SOURCE / 'generated/tables').as_posix())
        original = re.sub(r"^MatchRedis:\n  Host: [^\n]+\n  Type: cluster", f"MatchRedis:\n  Host: {endpoint('redis')}\n  Type: node", original, flags=re.M)
        original = original.replace("  RelayEnabled: true", "  RelayEnabled: false")
        original = original.replace("RatingEnabled: true", "RatingEnabled: false")
        original = original.replace("    IntervalMs: 2000", "    IntervalMs: 500")
    else:
        original = re.sub(r"^  Password: apppass123$", f'  Password: "{password}"', original, flags=re.M)
        original = original.replace("MySQL:\n", "MySQL:\n  Backend: mysql\n", 1)
        original = original.replace("  ReconcileIntervalMinutes: 60", "  ReconcileIntervalMinutes: 0")
    return original


async def start_battle_peers(processes):
    from data_full import register_writer

    configure_battle_env()
    service_names = (("match", "match", MATCH_SCHEMA), ("inbox", "player_inbox", INBOX_SCHEMA))
    for name, service, schema in service_names:
        migrator, password = f"{name}_interop_migration", secrets.token_hex(24)
        await asyncio.to_thread(run_mysql, f"CREATE DATABASE {schema} CHARACTER SET utf8mb4;"
                                f"CREATE USER '{migrator}'@'%' IDENTIFIED BY '{password}';"
                                f"GRANT ALL ON {schema}.* TO '{migrator}'@'%';")
        config = RUN / f"{name}-migrate.yaml"
        config.write_text(_config(service, migrator, password), encoding="utf-8")
        migration_env = ENV | {
            "MMORPG_SQL_SCHEMA_PRINCIPAL": f"{migrator}@%",
            "MATCH_MIGRATION_USER": migrator, "MATCH_MIGRATION_PASSWORD": password,
        }
        processes.start(f"{name}-migrate", [_executable(service), "-f", config, "-migrate"], env=migration_env)
        await processes.finish(f"{name}-migrate", timeout=60)
        rows = await asyncio.to_thread(run_mysql, f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}';")
        tables = set(rows.decode().splitlines()[1:]) - CONTROL_TABLES
        if not tables:
            raise RuntimeError(f"{schema} migration created no business tables")
        config = RUN / f"{name}.yaml"
        runtime_user, runtime_password = f"it_{name}_run", secrets.token_hex(24)
        config.write_text(_config(service, runtime_user, runtime_password), encoding="utf-8")
        writer_env, pinned = await register_writer(name, config, {
            schema: {table: "SELECT,INSERT,UPDATE,DELETE" for table in tables},
        }, runtime_credentials=(runtime_user, runtime_password))
        processes.start(name, [_executable(service), "-f", pinned],
                        env=ENV | writer_env | {"MATCH_MYSQL_PASSWORD": runtime_password})
        await processes.ready(name)
    protocol = Protocol()
    directory = EtcdDirectory(protocol, endpoint("etcd"))
    try:
        node_types = protocol.pool.FindEnumTypeByName("common.base.eNodeType").values_by_name
        for kind, name in (("PlayerInboxNodeService", "inbox"), ("MatchNodeService", "match")):
            info = await wait_etcd(directory, node_types[kind].number, 1)
            assert grpc_address(info.grpc_endpoint.ip, info.grpc_endpoint.port) == endpoint(name), info
    finally:
        await directory.close()


def _wire(message):
    return message.SerializeToString(deterministic=True)


def _rows(sql):
    return [line.split("\t") for line in run_mysql(sql).decode().splitlines()[1:]]


async def _call(client, protocol, path, response, request):
    return protocol.parse(response, await client.call(path, _wire(request)))


async def _turn_result(client):
    async with asyncio.timeout(20):
        while len(client.pending) < 128:
            for index, content in enumerate(client.pending):
                if content.message_id == 139 and not content.id:
                    return client.protocol.parse("TurnResultS2C", client.pending.pop(index).serialized_message)
            packet = await read_packet(client.reader)
            assert packet and packet.type_name == "MessageContent", packet
            client.pending.append(client.protocol.parse("MessageContent", packet.payload))
    raise RuntimeError("No terminal or turn result arrived on Battle TCP")


async def _match_published(battle_id, raw):
    consumer = AIOKafkaConsumer("match-results", bootstrap_servers=endpoint("kafka"), group_id=None,
                                enable_auto_commit=False, auto_offset_reset="earliest")
    try:
        async with asyncio.timeout(15):
            await consumer.start()
            async for record in consumer:
                if record.key == str(battle_id).encode("ascii"):
                    assert record.value == raw, "Kafka changed the native terminal payload"
                    return {"partition": record.partition, "offset": record.offset}
    finally:
        await consumer.stop()


async def _lobby_rpc(client, sequence, method, **fields):
    protocol = client.protocol
    message_id = protocol.message_ids[f"MatchService{method}"]
    request = protocol.message(protocol.routes[message_id].request_type, **fields)
    try:
        reply = await client.rpc(message_id, next(sequence), request)
    except (RuntimeError, TimeoutError) as exc:
        if method == "GetQueueStatus" and str(exc) == "RPC 错误 1003":
            return None
        raise RuntimeError(f"native MatchService.{method} did not complete through Gate: {exc}") from exc
    error = getattr(reply, "error_message", None)
    if error and error.id:
        raise RuntimeError(f"native MatchService.{method} rejected the request: {error.id} {list(error.parameters)}")
    return reply


async def _probe_match(lobby, sequence, node, player_id):
    p = lobby.protocol
    plan = await _lobby_rpc(lobby, sequence, "GetNetworkProbePlan")
    assert plan.required and plan.plan_id, plan
    address = node.client_endpoint
    candidates = [target for target in plan.targets
                  if (target.host, target.port, target.physical_region) == (address.ip, address.port, node.physical_region)]
    assert len(candidates) == 1, "native Match did not discover the Python Battle probe endpoint"
    grant = candidates[0].grant
    async with connect(p, address) as probe:
        await probe.send(p.message("match.NetworkProbeStart", grant=grant))
        for sample in range(3):
            challenge = await probe.receive("match.NetworkProbeChallenge")
            assert challenge.sample_index == sample and len(challenge.nonce) == 32, challenge
            await probe.send(p.message("match.NetworkProbeEcho", nonce=challenge.nonce, sample_index=sample))
        complete = await probe.receive("match.NetworkProbeComplete")
    receipt = p.parse("match.NetworkProbeReceipt", complete.receipt.payload)
    assert receipt.grant == p.parse("match.NetworkProbeGrant", grant.payload)
    assert receipt.grant.plan_id == plan.plan_id and receipt.grant.player_id == player_id
    assert receipt.sample_count == 3 and receipt.rtt_ms > 0 and len(complete.receipt.signature) == 32
    verified = await _lobby_rpc(lobby, sequence, "CommitNetworkProbe", plan_id=plan.plan_id, receipts=[complete.receipt])
    assert verified.expires_at_ms == receipt.expires_at_ms == plan.expires_at_ms
    print(f"Native Match committed the Python Battle network probe: region={node.physical_region}, rtt_ms={receipt.rtt_ms}", flush=True)
    return plan.plan_id, receipt.rtt_ms


async def _join_match(lobby, sequence, player_id, scene_zone, plan_id):
    request_id = str(uuid.uuid4())
    ticket_query = (f"SELECT ticket,request_id,state,formation_id,selected_region,owner_revision "
                    f"FROM {MATCH_SCHEMA}.match_authority_ticket WHERE player_id={player_id};")
    assert not await asyncio.to_thread(_rows, ticket_query), "this fixture requires a role with no prior Match ticket"
    joined = await _lobby_rpc(lobby, sequence, "JoinQueue", request_id=request_id,
                              mode=4, battle_config_id=1, zone_id=scene_zone, network_plan_id=plan_id)
    assert joined.queue_ticket and not joined.error_code, joined
    status = None
    try:
        async with asyncio.timeout(35):
            while True:
                status = await _lobby_rpc(lobby, sequence, "GetQueueStatus", request_id=request_id)
                if status is not None and status.state == 3 and status.battle_id:
                    assert status.queue_ticket == joined.queue_ticket, status
                    return request_id, status, ticket_query
                await asyncio.sleep(0.6)
    except TimeoutError as exc:
        rows = await asyncio.to_thread(_rows, ticket_query)
        raise RuntimeError(f"native PvE Match did not form: last status={status}, native ticket={rows}") from exc


async def _formation(battle_runtime, scene, player, request_id, status, ticket_query):
    node = battle_runtime.battle_info
    battle_id = status.battle_id
    ticket_rows, formation_rows = await asyncio.gather(
        asyncio.to_thread(_rows, ticket_query),
        asyncio.to_thread(_rows, f"SELECT state,operation_id,target_node_id,target_instance_id,HEX(create_request),"
                                 f"preflight_supported_version,preflight_instance_id,adopted_outcome_protocol_version,"
                                 f"adopted_outcome_observed FROM {MATCH_SCHEMA}.match_formation WHERE battle_id={battle_id};"),
    )
    assert ticket_rows == [[status.queue_ticket, request_id, "3", str(battle_id), str(node.physical_region),
                            str(player.role_identity.owner_revision)]], ticket_rows
    assert len(formation_rows) == 1, formation_rows
    state, operation, target_id, instance, wire, version, preflight, adopted, observed = formation_rows[0]
    assert (state, target_id, instance, version, preflight, adopted, observed) == (
        "4", str(node.node_id), node.node_uuid, "1", node.node_uuid, "1", "1",
    ), formation_rows
    create = battle_runtime.protocol.parse("CreateBattleRequest", bytes.fromhex(wire))
    assert create == battle_runtime.battle.rooms[battle_id].original
    assert create.placement_operation_id == operation and create.outcome_protocol_version == 1
    assert len(create.players) == 1 and create.players[0].player_id == player.player_id
    route = create.players[0].routing
    assert route.scene_node_id == scene.info.node_id and route.scene_instance_id == scene.info.node_uuid
    assert route.session_id == player.session_id and scene.battle.current(player.player_id) == battle_id
    print(f"Native Match formed battle {battle_id}, persisted the creation request, and created the Python Battle room", flush=True)


async def _battle_ticket(lobby, sequence, node, battle_id):
    protocol = lobby.protocol
    issued = await _lobby_rpc(lobby, sequence, "RequestBattleTicket", battle_id=battle_id)
    assert issued.assignment.battle_id == battle_id and issued.assignment.token_payload and issued.assignment.token_signature
    assert (issued.assignment.host, issued.assignment.port) == (node.client_endpoint.ip, node.client_endpoint.port)
    for name in ("BattleClientPlayerNotifyBattleAssigned", "BattleClientPlayerNotifyBattleStart"):
        message_id = protocol.message_ids[name]
        pushed = protocol.parse(protocol.routes[message_id].request_type,
                                (await lobby.content(message_id)).serialized_message)
        assert pushed.battle_id == battle_id, pushed
        if name == "BattleClientPlayerNotifyBattleAssigned":
            assert pushed == issued.assignment, "lobby assignment and native Match reissue differ"
        else:
            actor_types = [actor.actor_type for actor in pushed.state.actors]
            assert pushed.state.round_index == 1 and actor_types.count(1) == 1 and 2 in actor_types, pushed
    return issued.assignment


async def verify_native_battle(battle_runtime, scene_runtime, player_id, lobby_client):
    protocol = battle_runtime.protocol
    node = battle_runtime.battle_info
    scene = scene_runtime.scene
    player = scene.locations[player_id].players[player_id]
    identity = player.role_identity
    assert identity and identity.player_id == player_id and identity.owner_revision
    sequence = count(600)
    plan_id, rtt_ms = await _probe_match(lobby_client, sequence, node, player_id)
    request_id, status, ticket_query = await _join_match(lobby_client, sequence, player_id, scene.info.zone_id, plan_id)
    battle_id = status.battle_id
    await _formation(battle_runtime, scene, player, request_id, status, ticket_query)
    assignment = await _battle_ticket(lobby_client, sequence, node, battle_id)
    match_grpc = GrpcClient(endpoint("match"), timeout=10)
    try:
        async with connect(protocol, node.client_endpoint) as client:
            await client.send(protocol.message("BattleTokenVerifyRequest", payload=assignment.token_payload,
                                               signature=assignment.token_signature))
            auth = await client.receive("BattleTokenVerifyResponse")
            assert auth.success and auth.battle_id == battle_id, auth
            for turn_no in range(1, 13):
                action = protocol.message("SubmitBattleActionRequest", battle_id=battle_id, action={"action_type": 5})
                action_reply = await client.rpc(149, turn_no, action)
                assert not action_reply.error_message.id, action_reply
                turn = await _turn_result(client)
                if turn.state.outcome:
                    break
                await asyncio.sleep(1.05)
            assert turn.state.outcome and battle_id not in battle_runtime.battle.rooms, turn
        terminal = protocol.message("match.GetBattleTerminalDecisionRequest", battle_id=battle_id)
        terminal_key = ENV["MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE"].encode()
        sign_service_call(TERMINAL, terminal, "scene", time.time_ns() // 1_000_000, terminal_key)
        decided = await _call(match_grpc, protocol, TERMINAL, "match.GetBattleTerminalDecisionResponse", terminal)
        assert decided.decision == 1 and not decided.retry and decided.request_digest == service_digest(terminal), decided
        assert verify_service_call(TERMINAL + "/reply", decided, "match", time.time_ns() // 1_000_000, terminal_key)
        print(f"Native Match signed COMPLETE for battle {battle_id}; waiting on its Kafka result and Inbox", flush=True)
        report = await _await_persistence(protocol, scene, player, turn)
        return report | {"native_match_formation": True, "native_match_reissued_ticket": True,
                         "gate_battle_assignment_and_start": True, "probe_rtt_ms": rtt_ms,
                         "formation_seeded": False}
    finally:
        await match_grpc.close()


async def _await_persistence(protocol, scene, player, turn):
    from data_full import read_native_player

    player_id, battle_id = player.player_id, turn.battle_id
    match_sql = f"SELECT decision,result_published,HEX(raw_payload),HEX(payload_digest) FROM {MATCH_SCHEMA}.match_battle_completion WHERE battle_id={battle_id};"
    source_sql = (f"SELECT inbox_seq,inbox_state,dispatch_state,HEX(settlement_proto) FROM {MATCH_SCHEMA}.match_battle_settlement_source "
                  f"WHERE battle_id={battle_id} AND player_id={player_id};")
    inbox_sql = (f"SELECT seq,state,HEX(payload) FROM {INBOX_SCHEMA}.player_inbox_item "
                 f"WHERE player_id={player_id} AND domain=2 AND request_id={battle_id} AND msg_type=4;")
    match = source = inbox = []
    try:
        async with asyncio.timeout(95):
            while True:
                match, source, inbox = await asyncio.gather(*(asyncio.to_thread(_rows, sql) for sql in (match_sql, source_sql, inbox_sql)))
                if (match and match[0][:2] == ["1", "1"] and source and source[0][1:3] == ["2", "2"]
                        and inbox and inbox[0][1] == "2"):
                    break
                await asyncio.sleep(1)
    except TimeoutError as exc:
        raise RuntimeError(f"Settlement {battle_id} timed out: Match decision/published={match[0][:2] if match else []}, "
                           f"source seq/inbox/dispatch={source[0][:3] if source else []}, Inbox seq/state={inbox[0][:2] if inbox else []}") from exc
    raw = bytes.fromhex(match[0][2])
    assert hashlib.sha256(raw).hexdigest().upper() == match[0][3]
    published = await _match_published(battle_id, raw)
    event = protocol.parse("contracts.kafka.BattleResultEvent", raw)
    assert (event.battle_id, event.outcome, event.total_rounds) == (battle_id, turn.state.outcome, turn.round_index)
    assert (event.match_mode, event.battle_config_id) == (4, 1)
    assert len(event.settlements) == 1 and event.settlements[0].player_id == player_id
    assert source[0][0] == inbox[0][0] and bytes.fromhex(source[0][3]) == bytes.fromhex(inbox[0][2]) == _wire(event.settlements[0])
    saved = scene.rows.assemble(player.snapshot)
    assert player.snapshot.has_saved and not scene.battle.frozen(player_id)
    assert sum(entry.battle_id == battle_id for entry in saved.battle_results.entries) == 1
    owner_epoch, save_seq = player.snapshot.owner_epoch, player.snapshot.save_seq
    native = await read_native_player(player_id)
    assert native.owner_epoch == owner_epoch and native.save_seq >= save_seq and not native.released
    persisted = scene.rows.assemble(native)
    persisted_results = [entry for entry in persisted.battle_results.entries if entry.battle_id == battle_id]
    assert len(persisted_results) == 1 and persisted_results[0].outcome == event.outcome
    applied = [item for cursor in persisted.inbox_ledger.cursors if cursor.domain == 2 for item in cursor.recent
               if item.request_id == battle_id and item.msg_type == 4]
    assert len(applied) == 1 and applied[0].outcome == 1 and applied[0].seq == int(inbox[0][0]), applied
    print(f"PASS battle {battle_id}: Kafka {published['partition']}:{published['offset']}, native Inbox APPLIED seq "
          f"{inbox[0][0]}, original Match receipt and native Go DB save seq {native.save_seq}", flush=True)
    return {"battle_id": battle_id, "player_id": player_id, "native_match_complete": True,
            "native_match_published_kafka_result": published, "native_inbox_seq": int(inbox[0][0]),
            "native_inbox_applied": True, "native_match_received_applied": True,
            "scene_go_db_save_seq": native.save_seq, "native_go_db_battle_result": True,
            "battle_outcome": event.outcome, "rounds": event.total_rounds}
