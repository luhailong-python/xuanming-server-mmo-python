"""Isolated two-zone acceptance using Python portal and native Go services."""

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import secrets
import sys
from contextlib import AsyncExitStack, asynccontextmanager

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from xuanming.smoke import _monopoly

from scene_full import PARTITIONS, credit, entry, redis_epoch, start_control, start_python, travel, wallet
from smoke_original import ENV, ETCD, PORT, REDIS, RUN, TICKET_SECRET, Processes, addresses, endpoint, kafka, mysql

if not __debug__:
    raise RuntimeError("interop acceptance must not run with Python assertions disabled")


def configure():
    # Reserve the full set in one pass so that even temporary listeners cannot collide.
    extra = ("manager", "manager2", "locator", "db", "data", "match", "inbox", "rank", "login2", "metrics2",
             "data_metrics", "db_metrics", "match_metrics", "inbox_metrics", "rank_metrics", "locator_metrics")
    PORT.update(addresses(*PORT.keys(), *extra))
    ENV.update(KAFKA_COMMAND_TOPIC_PARTITIONS=str(PARTITIONS), KAFKA_COMMAND_TOPIC_GENERATION="1",
               MMORPG_GATE_TOKEN_SECRET=TICKET_SECRET)
    for name in ("BATTLE_TOKEN_SECRET", "PLAYER_STORE_SECRET_SCENE", "ACCOUNT_DIRECTORY_SECRET_SCENE",
                 "ACCOUNT_DIRECTORY_SECRET_SCENE_MANAGER", "ASSET_OP_SECRET_MAIL", "MATCH_OUTCOME_SECRET_BATTLE"):
        ENV.setdefault(f"MMORPG_{name}", secrets.token_hex(32))


async def topics(extra):
    counts = {f"{role}-cmd_g1": PARTITIONS for role in ("gate", "scene")}
    counts.update({"transaction_log_topic_g1": 3, "player_snapshot_topic_g1": 3, "anomaly_alert_topic": 1,
                   "match-results": 3, "chat-world-bcast_g1": 4})
    counts.update(extra)
    last_error = None
    for _ in range(20):
        admin = AIOKafkaAdminClient(bootstrap_servers=endpoint("kafka"), request_timeout_ms=3000)
        try:
            await admin.start()
            result = await admin.create_topics([NewTopic(name, count, 1) for name, count in counts.items()])
            errors = [(name, code) for name, code, *_ in result.topic_errors if code not in (0, 36)]
            if errors:
                raise RuntimeError(f"Kafka topic creation rejected: {errors}")
            return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.35)
        finally:
            await admin.close()
    raise RuntimeError("isolated Kafka topics were not ready") from last_error


async def await_worlds(scenes):
    async with asyncio.timeout(40):
        while not all(runtime.scene.worlds for runtime in scenes.values()):
            await asyncio.sleep(0.2)
    return {str(zone): len(runtime.scene.worlds) for zone, runtime in scenes.items()}


async def prime_kafka_groups():
    consumer = AIOKafkaConsumer("anomaly_alert_topic", bootstrap_servers=endpoint("kafka"),
                                group_id="isolated-interop-startup", request_timeout_ms=7000)
    try:
        async with asyncio.timeout(20):
            await consumer.start()
            assert consumer.assignment()
    finally:
        await consumer.stop()


@asynccontextmanager
async def cross_zone(session, source, gates, scenes, authenticate, enter_game, report):
    from data_full import read_native_player

    player_id, home = session.player_id, session.client
    original_epoch = source.snapshot.owner_epoch

    async def assert_sql(zone, epoch, amount):
        snapshot = await read_native_player(player_id)
        scene = scenes[zone].scene
        assert snapshot.owner == scene.info.node_uuid.encode() and snapshot.owner_epoch == epoch
        assert not snapshot.released
        data = scene.rows.assemble(snapshot)
        assert (data.currency.values[0] if data.currency.values else 0) == amount
        return snapshot.save_seq

    before = await wallet(home, 5)
    await credit(scenes[1], player_id, 1, 11)
    assert await wallet(home, 6) == before + 11
    first_save = await assert_sql(1, original_epoch, before + 11)
    async with travel(home, gates[2], scenes[2], player_id, session.access_token, authenticate, enter_game,
                      request_id=7) as (visitor, player):
        visit_epoch = player.snapshot.owner_epoch
        assert visit_epoch > original_epoch and await redis_epoch(player_id) == visit_epoch
        assert player.home_zone_id == 1 and await wallet(visitor, 3) == before + 11
        await credit(scenes[2], player_id, 2, 7)
        assert await wallet(visitor, 4) == before + 18
        visit_save = await assert_sql(2, visit_epoch, before + 18)
        print(f"PASS actual cross-zone admission into zone 2; player={player_id}, epoch={visit_epoch}", flush=True)
        async with travel(visitor, gates[1], scenes[1], player_id, session.access_token, authenticate, enter_game,
                          request_id=5) as (returned, player):
            home_epoch = player.snapshot.owner_epoch
            assert home_epoch > visit_epoch and await redis_epoch(player_id) == home_epoch
            assert await wallet(returned, 3) == before + 18
            return_save = await assert_sql(1, home_epoch, before + 18)
            async with asyncio.timeout(18):
                while player_id in scenes[2].scene.locations:
                    await asyncio.sleep(0.1)
            report.update(visit_epoch=visit_epoch, return_epoch=home_epoch, initial_wallet=before,
                          final_wallet=before + 18, round_trip=True, source_evicted=True,
                          mysql_save_seq=[first_save, visit_save, return_save])
            print(f"PASS cross-zone round trip; owner epoch {original_epoch} → {visit_epoch} → {home_epoch}", flush=True)
            yield returned


async def main(probe, ingress, skip_cross, monopoly, monopoly_landmarks=False, monopoly_board=False,
               monopoly_dice_recovery=False, scene_landing=False, scene_team=False, direct_gate=False):
    configure()
    if not probe:
        import battle_full
        import data_full
        import login_full

        data_full.configure_data_env()
        battle_full.configure_battle_env()
    processes = Processes()
    report = {"ok": False, "stage": "middleware", "login_ingress": ingress, "cross_zone_requested": not skip_cross,
              "gate_routing": "direct" if direct_gate else "router"}
    etcd, peer = f"http://{endpoint('etcd')}", f"http://{endpoint('peer')}"
    background = []
    try:
        processes.start("etcd", [ETCD, "--name=interop", f"--data-dir={RUN / 'etcd-data'}",
                                 f"--listen-client-urls={etcd}", f"--advertise-client-urls={etcd}",
                                 f"--listen-peer-urls={peer}", f"--initial-advertise-peer-urls={peer}",
                                 f"--initial-cluster=interop={peer}", "--log-level=warn"])
        processes.start("redis", [REDIS, "--bind", "127.0.0.1", "--port", PORT["redis"], "--save", "",
                                  "--appendonly", "yes", "--dir", RUN / "redis"])
        if not probe:
            background.append(asyncio.create_task(mysql(processes)))
        broker = asyncio.create_task(kafka(processes))
        background.append(broker)
        await asyncio.gather(processes.ready("redis"), processes.ready("etcd"), broker)
        extras = {} if probe else (getattr(data_full, "KAFKA_TOPICS", {}) | getattr(battle_full, "KAFKA_TOPICS", {}))
        await topics(extras)
        await prime_kafka_groups()
        print("READY isolated Redis AOF, Kafka topics and etcd", flush=True)

        report["stage"] = "native_coordinators"
        await start_control(processes)
        print("READY original Go SceneManager in both zones and PlayerLocator", flush=True)
        if not probe:
            report["stage"] = "native_database"
            await background[0]
            await data_full.start_data(processes)
            print("READY original Go DataService and DB", flush=True)

        async with AsyncExitStack() as stack:
            report["stage"] = "python_cluster_nodes"
            gates, scenes, battle = await start_python(stack, landing=scene_landing, direct_gate=direct_gate)
            report["worlds"] = await await_worlds(scenes)
            print("READY Python Battle, Gates, Scenes, and Go-managed worlds", flush=True)
            if not probe:
                if scene_landing:
                    from landing_full import subscribe, verify_channel_directory, verify_landing, verify_role_panels

                    landing_peers = await subscribe(stack)
                report["stage"] = "native_game_peers"
                match_peers = asyncio.create_task(battle_full.start_battle_peers(processes))
                background.append(match_peers)
                await login_full.start_login(processes, ingress=ingress)
                print(f"READY original Go Login and Router, ingress={ingress}", flush=True)
                if ingress == "python":
                    from portal_checks import verify
                    report["stage"] = "python_portal_apis"
                    report["python_portal_apis"] = await verify()
                report["stage"] = "native_match_and_inbox"
                await match_peers
                print("READY original Go Match and PlayerInbox", flush=True)
                report["stage"] = "login_and_entry"
                async with login_full.exercise_login(gates[1].protocol, gates[1].gate.info.client_endpoint,
                                                    ingress=ingress) as session:
                    await login_full.enter_game(session.client, session.player_id, call_id=3)
                    source = await entry(session.client, scenes[1], session.player_id)
                    assert await redis_epoch(session.player_id) == source.snapshot.owner_epoch and source.home_zone_id == 1
                    report.update(authenticated_player_id=session.player_id, home_epoch=source.snapshot.owner_epoch,
                                  account_role_owner_revision=session.owner_revision, native_password_verified=True)
                    print(f"PASS actual account logged in and entered zone 1; player={session.player_id}, "
                          f"epoch={source.snapshot.owner_epoch}", flush=True)
                    if ingress == "python":
                        from portal_checks import verify_observability
                        report["stage"] = "python_portal_observability"
                        report["python_portal_observability"] = await verify_observability(processes.portal_runner)
                    if ingress == "python":
                        from portal_checks import verify_capacity
                        report["stage"] = "python_role_capacity_guard"
                        report["python_role_capacity_guard"] = await verify_capacity(processes.portal_runner.deps, session)
                    if scene_landing:
                        from world_chat_full import verify_world_chat

                        report["stage"] = "scene_landing"
                        report["scene_landing"] = await verify_landing(session, landing_peers)
                        report["stage"] = "gate_world_chat"
                        report["gate_world_chat"] = await verify_world_chat(session, gates)
                        report["stage"] = "scene_role_panels"
                        report["scene_role_panels"] = await verify_role_panels(session, scenes[1])
                        report["stage"] = "scene_channel_directory"
                        report["scene_channel_directory"] = await verify_channel_directory(
                            session, scenes[1], landing_peers,
                        )
                    if scene_team:
                        from team_full import verify_team

                        report["stage"] = "scene_team"
                        report["scene_team"] = await verify_team(session, source, gates[1], scenes[1], ingress)
                    if monopoly_dice_recovery:
                        import dice_recovery_full

                        report["stage"] = "monopoly_dice_recovery"
                        session, source, dice_receipt = await dice_recovery_full.exercise_recovery(
                            stack, session, source, gates[1], scenes[1], ingress,
                            report.setdefault("monopoly_dice_recovery", {}),
                        )
                    elif monopoly_board:
                        import board_full

                        report["stage"] = "monopoly_board"
                        session, source, completion = await board_full.exercise_board(
                            stack, session, source, gates[1], scenes[1], ingress,
                            report.setdefault("monopoly_board", {}),
                        )
                    elif monopoly_landmarks:
                        import landmark_full

                        report["stage"] = "monopoly_landmarks"
                        session, source, landmark_receipt = await landmark_full.exercise_landmarks(
                            stack, session, source, gates[1], scenes[1], ingress,
                            report.setdefault("monopoly_landmarks", {}),
                        )
                    elif monopoly:
                        report["stage"] = "monopoly_roll"
                        roll = await _monopoly(session.client)

                        async def verify_board(lobby_client, extra_coins):
                            p = lobby_client.protocol
                            board = await lobby_client.rpc(p.message_ids["MonopolyClientPlayerGetBoard"], 61,
                                                           p.message("monopoly.GetBoardRequest"))
                            replay = await lobby_client.rpc(p.message_ids["MonopolyClientPlayerRollDice"], 62,
                                                            p.message("monopoly.RollDiceRequest", roll_seq=roll.roll_seq))
                            sql = await data_full.read_native_player(session.player_id)
                            saved = scenes[1].scene.rows.assemble(sql)
                            assert replay.durable and replay.replayed and replay.result == roll
                            assert (board.state.position, board.state.dice_remaining, board.state.next_roll_seq) == (
                                roll.to_position, roll.state.dice_remaining, roll.state.next_roll_seq,
                            )
                            assert saved.client_request_ledger.monopoly_last_roll.SerializeToString(deterministic=True) == (
                                roll.SerializeToString(deterministic=True)
                            )
                            assert board.state.coins == saved.currency.values[0] == roll.state.coins + extra_coins
                            report["monopoly"] = dict(
                                dice=[roll.die_one, roll.die_two], position=roll.to_position,
                                dice_remaining=roll.state.dice_remaining, coins_credited=roll.coins_credited,
                                mysql_save_seq=sql.save_seq, native_mysql_verified=True, replay_verified=True,
                                cross_zone_preserved=bool(extra_coins),
                            )
                            print(f"PASS Monopoly roll {roll.roll_seq}, tile {roll.to_position}, "
                                  f"credited {roll.coins_credited}, native MySQL save seq {sql.save_seq}", flush=True)

                    async def verify_battle(lobby_client):
                        report["stage"] = "battle_settlement"
                        return await battle_full.verify_native_battle(battle, scenes[1], session.player_id, lobby_client)

                    if skip_cross:
                        if monopoly and not (monopoly_landmarks or monopoly_board or monopoly_dice_recovery):
                            await verify_board(session.client, 0)
                        report["battle"] = await verify_battle(session.client)
                    else:
                        report["stage"] = "cross_zone"
                        async with cross_zone(session, source, gates, scenes, login_full.authenticate, login_full.enter_game,
                                              report.setdefault("cross_zone", {})) as returned:
                            if monopoly_dice_recovery:
                                report["stage"] = "monopoly_dice_recovery_readback"
                                native_save = await dice_recovery_full.verify_recovery(
                                    returned, scenes[1], session.player_id, dice_receipt, extra_coins=18,
                                )
                                report["monopoly_dice_recovery"].update(cross_zone_preserved=True,
                                                                        cross_zone_mysql_save_seq=native_save.save_seq)
                            elif monopoly_board:
                                report["stage"] = "monopoly_board_readback"
                                native_save = await board_full.verify_completion(
                                    returned, scenes[1], session.player_id, completion, extra_coins=18,
                                )
                                report["monopoly_board"].update(cross_zone_preserved=True,
                                                                cross_zone_mysql_save_seq=native_save.save_seq)
                            elif monopoly_landmarks:
                                report["stage"] = "monopoly_landmark_readback"
                                native_save = await landmark_full.verify_reentry(returned, scenes[1], session.player_id,
                                                                                landmark_receipt, extra_coins=18)
                                report["monopoly_landmarks"].update(cross_zone_preserved=True,
                                                                    cross_zone_mysql_save_seq=native_save.save_seq)
                            elif monopoly:
                                report["stage"] = "monopoly_readback"
                                await verify_board(returned, 18)
                            report["battle"] = await verify_battle(returned)
            report.update(ok=True, stage="complete")
    except BaseException as exc:
        report.update(error=type(exc).__name__, reason=str(exc))
        raise
    finally:
        for task in background:
            if not task.done():
                task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        try:
            try:
                if hasattr(processes, "portal_runner"):
                    await processes.portal_runner.aclose()
            finally:
                processes.close()
        except BaseException as exc:
            report.update(ok=False, stage="cleanup", error=type(exc).__name__, reason=str(exc))
            raise
        finally:
            (RUN / "acceptance.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"acceptance={RUN / 'acceptance.json'}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe-scenes", action="store_true")
    parser.add_argument("--ingress", choices=("python",), default="python")
    parser.add_argument("--skip-cross", action="store_true")
    parser.add_argument("--monopoly", action="store_true")
    parser.add_argument("--monopoly-landmarks", action="store_true")
    parser.add_argument("--monopoly-board", action="store_true")
    parser.add_argument("--monopoly-dice-recovery", action="store_true")
    parser.add_argument("--scene-landing", action="store_true")
    parser.add_argument("--scene-team", action="store_true")
    parser.add_argument("--direct-gate", action="store_true")
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.WARNING, stream=sys.stdout)
    if args.direct_gate:
        logging.getLogger("xuanming.nodes.gate_direct").setLevel(logging.DEBUG)
    asyncio.run(main(args.probe_scenes, args.ingress, args.skip_cross, args.monopoly, args.monopoly_landmarks,
                     args.monopoly_board, args.monopoly_dice_recovery, args.scene_landing, args.scene_team,
                     args.direct_gate))
