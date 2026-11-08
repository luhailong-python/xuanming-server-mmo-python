"""单机开发模式的登录适配器，复用正式的 Gate 入场契约。"""

import hashlib
import hmac
from itertools import count

_LOGIN_SERVICE = "loginpb.ClientPlayerLogin"
_INVALID_PARAMETER = 1005
_UNAVAILABLE = 1003
_ACCOUNT_NOT_FOUND = 2000
_PLAYER_FULL = 2001
_NOT_LOGGED_IN = 2004
_INVALID_PLAYER = 2006
_ROLE_NAME_INVALID = 2032
_LOGIN_FIRST = 1
_LOGIN_REPLACE = 2
_EXIT_LOGOUT = 1
_MAX_LOCAL_ROLES = 2


class LocalLogin:
    """开发账号共用本地访问令牌，角色档案依旧由 Scene 持久化。"""

    def __init__(self, protocol, scene_info, *, access_token: str | bytes) -> None:
        self.protocol = protocol
        self.scene_info = scene_info
        self._token = access_token.encode("utf-8") if isinstance(access_token, str) else access_token
        if not self._token:
            raise ValueError("单机登录需要配置开发访问令牌")
        self._sessions: dict[tuple[int, int], str] = {}
        self._roles: dict[str, list] = {}
        self._profiles: dict[int, object] = {}
        self._active: dict[int, tuple[int, int, int]] = {}
        self._generation = count(1)

    def initialize_role(self, data) -> None:
        role = self._profiles.get(data.player_id)
        if role is None:
            return
        identity = data.uint32_pb_component
        if not getattr(identity, "class"):
            setattr(identity, "class", role.class_id)
        profile = data.profile_component
        for field in ("name", "gender", "appearance_id"):
            if not getattr(profile, field) and (value := getattr(role, field)):
                setattr(profile, field, value)

    async def __call__(self, gate, session, route, request, seq):
        if route.service != _LOGIN_SERVICE:
            return gate._error(request, _UNAVAILABLE)
        payload = self.protocol.parse(route.request_type, request.body)
        response = self.protocol.message(route.response_type)
        key = session.session_id, session.incarnation
        account = self._sessions.get(key)
        if route.method == "Login":
            self._login(gate, key, account, payload, response)
        elif route.method == "CreatePlayer":
            self._create(gate, account, payload, response)
        elif route.method == "EnterGame":
            await self._enter(gate, session, account, payload, response)
        elif route.method == "LeaveGame":
            await gate.exit_scene(session, _EXIT_LOGOUT)
            self._forget(session)
            session.player_id = session.scene_id = session.scene_node_id = session.owner_revision = 0
            session.account = ""
            session.pending_route = None
        elif route.method == "Disconnect":
            if seq != 0 or payload.session_id != session.session_id:
                return gate._error(request, _INVALID_PARAMETER)
            self._forget(session)
            return None
        else:
            return gate._error(request, _UNAVAILABLE)
        return self.protocol.content(
            request.message_id, request.id, body=response.SerializeToString()
        )

    def _login(self, gate, key, current, request, response) -> None:
        account = request.account.strip()
        if request.auth_type in ("", "password"):
            credential = request.password
        elif request.auth_type == "access_token":
            credential = request.auth_token
        else:
            credential = ""
        if (
            not account or len(account.encode("utf-8")) > 128
            or current not in (None, account)
            or not hmac.compare_digest(self._token, credential.encode("utf-8"))
        ):
            response.error_message.id = _ACCOUNT_NOT_FOUND
            return
        if account not in self._roles:
            self._roles[account] = [
                self._new_role(gate, account, 0, name=account[:12], class_id=1, gender=1)
            ]
        self._sessions[key] = account
        self._list_roles(account, response)

    def _create(self, gate, account, request, response) -> None:
        if account is None:
            response.error_message.id = _NOT_LOGGED_IN
            return
        roles = self._roles[account]
        if len(roles) >= _MAX_LOCAL_ROLES:
            response.error_message.id = _PLAYER_FULL
            return
        name = request.name.strip() or f"{account[:10]}-2"
        if len(name) > 12 or request.gender not in (0, 1, 2):
            response.error_message.id = _ROLE_NAME_INVALID
            return
        roles.append(self._new_role(
            gate, account, len(roles), name=name, class_id=request.class_id or 1,
            gender=request.gender or 1, appearance_id=request.appearance_id,
        ))
        self._list_roles(account, response)

    async def _enter(self, gate, session, account, request, response) -> None:
        if account is None:
            response.error_message.id = _NOT_LOGGED_IN
            return
        player_id = request.player_id
        if not any(role.player_id == player_id for role in self._roles[account]) or (
            session.ticket_player_id not in (0, player_id) or session.player_id not in (0, player_id)
        ):
            response.error_message.id = _INVALID_PLAYER
            return
        if session.player_id == player_id and session.scene_node_id == self.scene_info.node_id:
            response.player_id = player_id
            return
        previous = self._active.get(player_id)
        generation = next(self._generation)
        enter_type = _LOGIN_REPLACE if previous is not None else _LOGIN_FIRST
        await gate.bind(self.protocol.message(
            "contracts.kafka.BindSessionEvent",
            session_id=session.session_id, player_id=player_id, session_version=session.version + 1,
            login_generation=generation, authenticated_account=account, owner_revision=1,
            session_incarnation=session.incarnation, enter_gs_type=enter_type,
        ))
        if session.player_id != player_id:
            response.error_message.id = _INVALID_PLAYER
            return
        await gate.route(self.protocol.message(
            "contracts.kafka.RoutePlayerEvent",
            session_id=session.session_id, player_id=player_id, scene_id=1,
            target_node_id=self.scene_info.node_id, home_zone_id=self.scene_info.zone_id,
            login_generation=generation, owner_revision=1,
        ), enter_type)
        if session.scene_node_id != self.scene_info.node_id:
            response.error_message.id = _UNAVAILABLE
            return
        active = self._active.get(player_id)
        if active is not None and active[2] > generation:
            response.error_message.id = _INVALID_PLAYER
            session.close()
            return
        self._active[player_id] = (session.session_id, session.incarnation, generation)
        if active is not None:
            previous_session = gate.sessions.get(active[0])
            if previous_session is not None and previous_session is not session and (
                previous_session.incarnation == active[1]
            ):
                previous_session.close()
        response.player_id = player_id

    def _forget(self, session) -> None:
        key = session.session_id, session.incarnation
        self._sessions.pop(key, None)
        active = self._active.get(session.player_id)
        if active is not None and active[:2] == key:
            self._active.pop(session.player_id, None)

    def _new_role(self, gate, account: str, slot: int, **fields):
        if account == "local" and slot == 0:
            player_id = 1
        else:
            key = f"{account}\0{slot}".encode("utf-8")
            digest = hashlib.blake2b(key, digest_size=8, person=b"xuanming-local").digest()
            player_id = (int.from_bytes(digest, "big") & ((1 << 62) - 1)) | (1 << 62)
        role = self.protocol.message_class("AccountSimplePlayer")(
            player_id=player_id, zone_id=gate.info.zone_id, **fields
        )
        self._profiles[player_id] = role
        return role

    def _list_roles(self, account: str, response) -> None:
        for role in self._roles[account]:
            response.players.add().player.CopyFrom(role)
