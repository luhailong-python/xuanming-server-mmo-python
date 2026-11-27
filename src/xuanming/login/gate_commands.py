"""Commands login sends to a gate over Kafka (Go ``svc/gate_command.go``)."""

_SEND_TIP_MESSAGE = "SceneClientPlayerCommonSendTipToClient"


def _command(protocol, gate_id: str, gate_instance_id: str, event_name: str, payload, **fields):
    if not gate_instance_id:
        raise ValueError("gate command rejected: empty gate_instance_id")
    if not gate_id.isdigit() or int(gate_id) == 0:
        raise ValueError(f"gate command rejected: invalid gate node id {gate_id!r}")
    node_id = int(gate_id)
    command = protocol.message(
        "contracts.kafka.GateCommand", event_id=protocol.event_ids[event_name],
        payload=payload.SerializeToString(), target_gate_id=node_id, target_instance_id=gate_instance_id, **fields,
    )
    return node_id, command


def bind_session(protocol, gate_id: str, gate_instance_id: str, event):
    return _command(protocol, gate_id, gate_instance_id, "ContractsKafkaBindSessionEvent", event,
                    session_id=event.session_id, player_id=event.player_id, enter_gs_type=event.enter_gs_type)


def kick_player(
    protocol,
    gate_id: str,
    gate_instance_id: str,
    session_id: int,
    player_id: int,
    generation: int,
    *,
    reason_tip: int = 0,
):
    event = protocol.message(
        "contracts.kafka.KickPlayerEvent",
        session_id=session_id,
        player_id=player_id,
        login_generation=generation,
        reason_tip=int(reason_tip),
    )
    return _command(protocol, gate_id, gate_instance_id, "ContractsKafkaKickPlayerEvent", event,
                    session_id=session_id, player_id=player_id)


def push_tip(protocol, gate_id: str, gate_instance_id: str, session_id: int, player_id: int, tip: int, *,
             bound: bool):
    """Unbound sessions get fence player 0, otherwise the gate's identity fence would drop the tip."""
    if not player_id:
        raise ValueError("push tip rejected: zero player_id")
    body = protocol.message("TipInfoMessage", id=tip).SerializeToString()
    event = protocol.message(
        "contracts.kafka.PushToPlayerEvent", session_id=session_id, player_id=player_id if bound else 0,
        message_content=protocol.content(protocol.message_ids[_SEND_TIP_MESSAGE], body=body),
    )
    return _command(protocol, gate_id, gate_instance_id, "ContractsKafkaPushToPlayerEvent", event,
                    session_id=session_id, player_id=player_id)


def drain_migration(protocol, gate_id: str, gate_instance_id: str, drain_marked_at: int,
                    rate_per_second: int, lease_ms: int):
    """向正在排空的 gate 发送迁移许可事件（Go ``buildGateDrainMigrationCommand``）。"""
    event = protocol.message(
        "contracts.kafka.GateDrainMigrationEvent",
        drain_marked_at=drain_marked_at,
        rate_per_second=rate_per_second,
        lease_ms=lease_ms,
    )
    return _command(protocol, gate_id, gate_instance_id, "ContractsKafkaGateDrainMigrationEvent", event)
