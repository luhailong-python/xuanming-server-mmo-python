"""Per-RPC login logic, one module per RPC like Go ``internal/logic``."""


def tip(protocol, tip_id: int, *parameters: str):
    return protocol.message("TipInfoMessage", id=tip_id, parameters=parameters)
