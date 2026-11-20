import asyncio
import base64

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from login_fakes import FakeRedis
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata
from xuanming.login import keys, tips
from xuanming.login.caller_auth import CallerVerifier
from xuanming.login.config import LoginConfig
from xuanming.login.directory import DataService
from xuanming.login.logic import create_player, disconnect, enter_game, leave_game, login
from xuanming.login.service import LoginService, ServiceContext
from xuanming.login.sessions import PlayerLocator
from xuanming.login.tokens import TokenManager
from xuanming.protocol import Protocol

PASSWORD = "dev-shared-secret"


class Cluster:
    """Fake data_service / player_locator / scene_manager behind the login's outbound call functions."""

    def __init__(self, protocol):
        self.protocol = protocol
        self.roles = {}  # account -> [AccountSimplePlayer]
        self.sessions = {}
        self.calls = []
        self.enter_error = 0

    async def data_service(self, path, data, timeout=None):
        p, method = self.protocol, path.rsplit("/", 1)[1]
        self.calls.append(method)
        if method == "AllocateIdSegment":
            return p.message("data_service.AllocateIdSegmentResponse", lo=1, hi=101).SerializeToString()
        if method in ("ReadAccountDirectory", "CreateAccountRole", "LookupAccountRole"):
            request = p.parse(f"data_service.{method}Request", data)
            assert request.auth.caller == "login" and len(request.auth.signature_hex) == 64
            if method == "LookupAccountRole":
                account, role = next((a, r) for a, roles in self.roles.items() for r in roles
                                     if r.player_id == request.player_id)
                return p.message("data_service.AccountRoleOwnership", account=account, role=role,
                                 owner_revision=5).SerializeToString()
            roles = self.roles.setdefault(request.account, [])
            if method == "CreateAccountRole":
                roles.append(request.role)
            return p.message("data_service.AccountDirectoryResponse",
                             players={"players": roles}).SerializeToString()
        if method == "ReservePlayerName":
            return p.message("data_service.ReservePlayerNameResponse", result=0).SerializeToString()
        if method == "BatchGetPlayerHomeZone":
            return p.message("data_service.BatchGetPlayerHomeZoneResponse").SerializeToString()
        assert method in ("RegisterPlayerZone", "ReleasePlayerName"), method
        return b""

    async def player_locator(self, path, data, timeout=None):
        p, method = self.protocol, path.rsplit("/", 1)[1]
        self.calls.append(method)
        if method == "GetSession":
            request = p.parse("playerlocator.GetSessionRequest", data)
            session = self.sessions.get(request.player_id)
            return p.message("playerlocator.GetSessionResponse", session=session,
                             found=session is not None).SerializeToString()
        if method == "SetSession":
            session = p.parse("playerlocator.SetSessionRequest", data).session
            self.sessions[session.player_id] = session
        return b""

    async def scene_manager(self, path, data, timeout=None):
        request = self.protocol.parse("scene_manager.EnterSceneRequest", data)
        self.calls.append(("EnterScene", request.player_id, request.login_generation, request.zone_id))
        return self.protocol.message("scene_manager.EnterSceneResponse",
                                     error_code=self.enter_error).SerializeToString()


def make_context(protocol, cluster, published, directory=None):
    config = LoginConfig(zone_id=1, gate_token_secret=b"g" * 32, directory_secret=b"d" * 32,
                         dev_password_secret=PASSWORD)
    redis = FakeRedis()

    async def publish(node_id, command, key):
        published.append((node_id, command, key))

    return ServiceContext(
        protocol=protocol, config=config, login_redis=redis, player_redis=redis, tokens=TokenManager(redis),
        locator=PlayerLocator(protocol, cluster.player_locator),
        data=DataService(protocol, cluster.data_service, config.directory_secret),
        scene_manager_call=cluster.scene_manager, publish=publish, directory=directory,
    )


def details(protocol, **fields):
    values = dict(session_id=917505, gate_node_id=7, gate_instance_id="gate-uuid", session_incarnation=91)
    return protocol.message("SessionDetails", **(values | fields))


def test_login_create_enter_leave_flow():
    async def exercise():
        protocol = Protocol()
        cluster, published = Cluster(protocol), []
        ctx = make_context(protocol, cluster, published)
        http = await login.run(ctx, protocol.message("loginpb.LoginRequest", account="robot_1", password=PASSWORD),
                               None)
        assert not http.HasField("error_message") and http.access_token
        bad = await login.run(ctx, protocol.message("loginpb.LoginRequest", account="robot_1", password="x"), None)
        assert bad.error_message.id == tips.ACCOUNT_NOT_FOUND

        session = details(protocol)
        bound = await login.run(ctx, protocol.message("loginpb.LoginRequest", auth_type="access_token",
                                                      auth_token=http.access_token), session)
        assert not bound.HasField("error_message") and not bound.access_token  # access-token bind issues nothing

        created = await create_player.run(ctx, protocol.message("loginpb.CreatePlayerRequest", name="小明"), session)
        assert not created.HasField("error_message")
        player = created.players[0].player
        assert (player.name, player.class_id, player.gender, player.zone_id) == ("小明", 1, 1, 1)
        invalid = await create_player.run(ctx, protocol.message("loginpb.CreatePlayerRequest", name="x"), session)
        assert list(invalid.error_message.parameters) == ["2", "12"]

        entered = await enter_game.run(ctx, protocol.message("loginpb.EnterGameRequest", player_id=player.player_id,
                                                             request_id="r1"), session)
        assert not entered.HasField("error_message") and entered.owner_revision == 5
        await asyncio.gather(*ctx.tasks)
        node_id, command, key = published[-1]
        assert (node_id, command.target_instance_id, key) == (7, "gate-uuid", str(player.player_id).encode())
        bind = protocol.parse("contracts.kafka.BindSessionEvent", command.payload)
        assert (bind.enter_gs_type, bind.authenticated_account, bind.session_incarnation) == (1, "robot_1", 91)
        assert ("EnterScene", player.player_id, bind.login_generation, 1) in cluster.calls
        assert await ctx.player_redis.get(keys.login_idempotent_key(player.player_id, "r1")) == b"1"
        assert await ctx.player_redis.get(keys.player_locker_key(player.player_id)) is None  # released

        again = await enter_game.run(ctx, protocol.message("loginpb.EnterGameRequest", player_id=999), session)
        assert again.error_message.id == tips.IN_PROGRESS  # another role is bound to this connection

        await leave_game.run(ctx, protocol.message("loginpb.LeaveGameRequest"), session)
        assert cluster.calls[-1] == "MarkOffline"
        assert await ctx.login_redis.get(keys.login_session_key("gate-uuid:91")) is None

    asyncio.run(exercise())


def test_enter_scene_rejection_pushes_failure_tip_to_the_bound_session():
    async def exercise():
        protocol = Protocol()
        cluster, published = Cluster(protocol), []
        cluster.enter_error = 18
        ctx = make_context(protocol, cluster, published)
        role = protocol.message("AccountSimplePlayer", player_id=42, class_id=1, gender=1, zone_id=1, name="a")
        cluster.roles["robot_1"] = [role]
        session = details(protocol)
        await ctx.login_redis.set(keys.login_session_key("gate-uuid:91"), "robot_1")
        entered = await enter_game.run(ctx, protocol.message("loginpb.EnterGameRequest", player_id=42), session)
        assert not entered.HasField("error_message")
        await asyncio.gather(*ctx.tasks)
        command = published[-1][1]
        assert command.event_id == protocol.event_ids["ContractsKafkaPushToPlayerEvent"]
        push = protocol.parse("contracts.kafka.PushToPlayerEvent", command.payload)
        assert push.player_id == 42  # Bind already went out, so the identity fence applies
        assert protocol.parse("TipInfoMessage", push.message_content.serialized_message).id == tips.ENTER_SCENE_FAILED

        await disconnect.run(ctx, protocol.message("loginpb.LoginNodeDisconnectRequest"), session)
        assert cluster.calls[-1] == "SetDisconnecting"

    asyncio.run(exercise())


def test_gate_identity_is_verified_and_echoed():
    async def exercise():
        protocol = Protocol()
        key = Ed25519PrivateKey.generate()
        other = Ed25519PrivateKey.generate()
        ctx = make_context(protocol, Cluster(protocol), [])
        service = LoginService(ctx, CallerVerifier([key.public_key()]))
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}")
        path = "/loginpb.ClientPlayerLogin/RenewSession"
        subject = details(protocol).SerializeToString()
        try:
            metadata = session_metadata(subject, path, key)
            call = client.request(path, b"", metadata=metadata)
            reply = protocol.parse("loginpb.RenewSessionResponse", await call)
            assert not reply.renewed  # verified, but no login state to renew
            assert dict(await call.initial_metadata())["x-session-detail-bin"] == base64.b64encode(subject)

            for bad in (metadata,  # nonce replay
                        session_metadata(subject, path, other),  # unknown key
                        session_metadata(subject, "/loginpb.ClientPlayerLogin/Login", key),  # other method
                        session_metadata(details(protocol, session_incarnation=0).SerializeToString(), path, key)):
                with pytest.raises(grpc.aio.AioRpcError) as error:
                    await client.call(path, b"", metadata=bad)
                assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED

            # No identity claim at all is the HTTP gateway path: served without a session.
            anonymous = await client.call("/loginpb.LoginPreGate/QueryQueueStatus", b"")
            assert protocol.parse("loginpb.QueryQueueStatusResponse", anonymous).status == 3
        finally:
            await client.close()
            await server.stop(None)

    asyncio.run(exercise())
