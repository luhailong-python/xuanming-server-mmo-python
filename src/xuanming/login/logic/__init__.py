"""按 RPC 拆分的 login 业务逻辑模块（对应 Go ``internal/logic``）。"""

import grpc


class RpcAbort(Exception):
    """需要以指定 gRPC 状态码直接中止调用的异常，由 ``LoginService`` 统一捕获并 abort。"""

    def __init__(self, code: grpc.StatusCode, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def tip(protocol, tip_id: int, *parameters: str):
    return protocol.message("TipInfoMessage", id=tip_id, parameters=parameters)
