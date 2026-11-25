"""单机开发模式的登录适配器，复用正式的 Gate 入场契约。"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
import time
from itertools import count
from pathlib import Path

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
_REPLACE_RETRY_COUNT = 12
_REPLACE_RETRY_BASE_SECONDS = 0.04


class LocalLogin:
    """开发账号共用本地访问令牌，角色档案由 Scene 持久化，角色列表支持可选 JSON 落盘。"""

    def __init__(
        self,
        protocol,
        scene_info,
        *,
        access_token: str | bytes,
        store_path: str | Path | None = None,
    ) -> None:
        self.protocol = protocol
        self.scene_info = scene_info
        self._token = access_token.encode("utf-8") if isinstance(access_token, str) else access_token
        if not self._token:
            raise ValueError("单机登录需要配置开发访问令牌")
        self._store_path = Path(store_path) if store_path is not None else None
        self._sessions: dict[tuple[int, int], str] = {}
        self._roles: dict[str, list] = {}
        self._profiles: dict[int, object] = {}
        self._player_accounts: dict[int, str] = {}
        self._active: dict[int, tuple[int, int, int]] = {}
        self._generation = count(1)
        self._load_accounts()

    def _load_accounts(self) -> None:
        if self._store_path is None or not self._store_path.exists():
            return
        try:
            doc = json.loads(self._store_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return
        accounts = doc.get("accounts") if isinstance(doc, dict) else None
        if not isinstance(accounts, dict):
            return
        role_cls = self.protocol.message_class("AccountSimplePlayer")
        for account, entries in accounts.items():
            if not isinstance(account, str) or not isinstance(entries, list):
                continue
            roles = []
            for entry in entries:
                if not isinstance(entry, dict) or not entry.get("player_id"):
                    continue
                role = role_cls(
                    player_id=int(entry["player_id"]),
                    zone_id=int(entry.get("zone_id") or self.scene_info.zone_id or 1),
                    name=str(entry.get("name") or ""),
                    class_id=int(entry.get("class_id") or 1),
                    gender=int(entry.get("gender") or 1),
                    appearance_id=str(entry.get("appearance_id") or ""),
                )
                roles.append(role)
                self._profiles[role.player_id] = role
                self._player_accounts[role.player_id] = account
            if roles:
                self._roles[account] = roles

    def _save_accounts(self) -> None:
        if self._store_path is None:
            return
        payload = {
            "format_version": 1,
            "accounts": {
                account: [
                    {
                        "player_id": int(role.player_id),
                        "zone_id": int(role.zone_id),
                        "name": str(role.name),
                        "class_id": int(role.class_id),
                        "gender": int(role.gender),
                        "appearance_id": str(role.appearance_id),
                    }
                    for role in roles
                ]
                for account, roles in sorted(self._roles.items())
            },
        }
        self._store_path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self._store_path.name}.", suffix=".tmp", dir=self._store_path.parent
        )
        try:
            with os.fdopen(fd, "wb") as output:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
            for attempt in range(_REPLACE_RETRY_COUNT):
                try:
                    os.replace(temporary, self._store_path)
                    break
                except PermissionError:
                    if attempt + 1 >= _REPLACE_RETRY_COUNT:
                        raise
                    time.sleep(_REPLACE_RETRY_BASE_SECONDS * (attempt + 1))
        finally:
            try:
                os.unlink(temporary)
            except OSError:
                pass

    def account_of(self, player_id: int) -> str:
        return self._player_accounts.get(int(player_id), "")

    def role_of(self, player_id: int):
        return self._profiles.get(int(player_id))

    def all_roles(self) -> list[tuple[str, object]]:
        result: list[tuple[str, object]] = []
        for account, roles in self._roles.items():
            for role in roles:
                result.append((account, role))
        return result

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
        elif route.method == "RefreshToken":
            self._refresh_token(account, payload, response)
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

    def _refresh_token(self, account: str | None, request, response) -> None:
        if account is None:
            response.error_message.id = _NOT_LOGGED_IN
            return
        token = self._token.decode("utf-8", errors="replace")
        now = int(time.time())
        response.access_token = token
        response.refresh_token = request.refresh_token or token
        response.access_token_expire = now + 3600
        response.refresh_token_expire = now + 86400

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
            self._save_accounts()
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
        self._save_accounts()
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
        self._player_accounts[player_id] = account
        return role

    def _list_roles(self, account: str, response) -> None:
        for role in self._roles[account]:
            response.players.add().player.CopyFrom(role)
