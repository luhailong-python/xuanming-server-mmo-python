"""In-memory Redis for login tests: commands plus Python twins of the login Lua scripts.

fakeredis/lupa are not installed, so each script is emulated by identity; the twins follow the Go Lua
line by line. Real-Redis byte compatibility is not exercised here.
"""

import json
import time

from xuanming.login import locker, session_fence, sessions, tokens


def _b(value) -> bytes:
    return value if isinstance(value, bytes) else str(value).encode()


class Pipeline:
    def __init__(self, redis):
        self.redis, self.calls = redis, []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def __getattr__(self, name):
        return lambda *args, **kwargs: self.calls.append((name, args, kwargs))

    async def execute(self):
        return [await getattr(self.redis, name)(*args, **kwargs) for name, args, kwargs in self.calls]


class FakeRedis:
    def __init__(self):
        self.data: dict[bytes, object] = {}
        self.expiry: dict[bytes, float] = {}
        self.fail = set()
        self.scripts = {
            session_fence.COMMIT_LOGIN: self._commit_login, session_fence.CLOSE: self._close,
            session_fence.CHECK_ENTER_GAME: self._check_enter, session_fence.BIND_PLAYER: self._bind,
            session_fence.LEAVE: self._leave, session_fence.RENEW: self._renew,
            tokens.ISSUE_FAMILY: self._issue, tokens.VALIDATE_FAMILY_ACCESS: self._validate,
            tokens.ROTATE_REFRESH: self._rotate, tokens.REPLAY_REFRESH: self._replay,
            tokens.RETIRE_REFRESH: lambda keys, args: 0,
            sessions.MINT_LOGIN_GENERATION: self._mint, locker.RELEASE: self._release, locker.RENEW: self._lock_renew,
        }

    # -- plumbing --------------------------------------------------------------------------------
    def _alive(self, key):
        key = _b(key)
        deadline = self.expiry.get(key)
        if deadline is not None and deadline <= time.monotonic() * 1000:
            self.data.pop(key, None)
            self.expiry.pop(key, None)
        return key if key in self.data else None

    def _pttl(self, key):
        key = self._alive(key)
        if key is None:
            return -2
        deadline = self.expiry.get(key)
        return -1 if deadline is None else max(1, int(deadline - time.monotonic() * 1000))

    def _px(self, key, ms):
        self.expiry[_b(key)] = time.monotonic() * 1000 + int(ms)

    def _hash(self, key):
        key = _b(key)
        if self._alive(key) is None:
            self.data[key] = {}
        return self.data[key]

    def _hget(self, key, field):
        key = self._alive(key)
        return None if key is None else self.data[key].get(_b(field))

    def _type(self, key):
        key = self._alive(key)
        if key is None:
            return "none"
        value = self.data[key]
        return {bytes: "string", dict: "hash", set: "set"}.get(type(value), "zset")

    def ttl_ms(self, key):
        return self._pttl(key)

    # -- commands --------------------------------------------------------------------------------
    async def ping(self):
        return True

    async def get(self, key):
        if "get" in self.fail:
            raise ConnectionError("redis down")
        key = self._alive(key)
        return None if key is None else self.data[key]

    async def set(self, key, value, *, nx=False, px=None, ex=None):
        key = _b(key)
        if nx and self._alive(key) is not None:
            return None
        self.data[key] = _b(value)
        self.expiry.pop(key, None)
        if px:
            self._px(key, px)
        if ex:
            self._px(key, ex * 1000)
        return True

    async def delete(self, *keys_):
        for key in keys_:
            self.data.pop(_b(key), None)
            self.expiry.pop(_b(key), None)

    async def sadd(self, key, *members):
        key = _b(key)
        if self._alive(key) is None:
            self.data[key] = set()
        self.data[key].update(_b(m) for m in members)

    async def srem(self, key, *members):
        key = self._alive(key)
        if key is not None:
            self.data[key].difference_update(_b(m) for m in members)

    async def smembers(self, key):
        key = self._alive(key)
        return set() if key is None else set(self.data[key])

    async def scard(self, key):
        return len(await self.smembers(key))

    async def pexpire(self, key, ms):
        if self._alive(key) is not None:
            self._px(key, ms)

    async def hmget(self, key, *fields):
        return [self._hget(key, field) for field in fields]

    def pipeline(self, transaction=True):
        return Pipeline(self)

    async def eval(self, script, numkeys, *args):
        if "eval" in self.fail:
            raise ConnectionError("redis down")
        keys_, argv = [_b(k) for k in args[:numkeys]], [_b(a) for a in args[numkeys:]]
        return self.scripts[script](keys_, argv)

    # -- session fence twins ---------------------------------------------------------------------
    def _commit_login(self, keys_, argv):
        if self._hget(keys_[0], "closed") == b"1":
            return 0
        self.data[keys_[1]] = argv[0]
        self._px(keys_[1], argv[1])
        if self._hget(keys_[0], "player") is not None:
            self._hash(keys_[0])[b"account"] = argv[0]
            self._px(keys_[0], argv[2])
        return 1

    def _close(self, keys_, argv):
        self._hash(keys_[0])[b"closed"] = b"1"
        self._px(keys_[0], argv[0])
        return self._hget(keys_[0], "player") or b"0"

    def _check_enter(self, keys_, argv):
        self._hash(keys_[0]).setdefault(b"gen", b"0")
        closed, gen, player = (self._hget(keys_[0], f) for f in ("closed", "gen", "player"))
        self._px(keys_[0], argv[1] if player and closed != b"1" else argv[0])
        return [closed or b"", gen or b"0", player or b""]

    def _bind(self, keys_, argv):
        closed, gen, player = (self._hget(keys_[0], f) for f in ("closed", "gen", "player"))
        if closed == b"1":
            return b"closed"
        if (gen or b"0") != argv[0]:
            return b"left"
        if player and player != argv[1]:
            return b"other_player"
        record = self._hash(keys_[0])
        record[b"player"] = argv[1]
        account = self._alive(keys_[1]) and self.data[keys_[1]]
        if account:
            record[b"account"] = account
        else:
            record.pop(b"account", None)
        self._px(keys_[0], argv[2])
        return b"ok"

    def _leave(self, keys_, argv):
        record = self._hash(keys_[0])
        record[b"gen"] = str(int(record.get(b"gen", b"0")) + 1).encode()
        player = record.get(b"player")
        if player:
            record[b"left_player"] = player
            record.pop(b"player")
            record.pop(b"account", None)
        else:
            player = record.get(b"left_player")
        self._px(keys_[0], argv[0])
        return player or b"0"

    def _renew(self, keys_, argv):
        closed, player, copy = (self._hget(keys_[0], f) for f in ("closed", "player", "account"))
        if closed == b"1":
            return [0, b"", 0]
        kept = restored = 0
        account = self._alive(keys_[1]) and self.data[keys_[1]]
        if account:
            self._px(keys_[1], argv[0])
            kept = 1
        if player:
            self._px(keys_[0], argv[1])
            kept = 1
            if not account and copy:
                self.data[keys_[1]] = copy
                self._px(keys_[1], argv[0])
                account, restored = copy, 1
        return [kept, account or b"", restored]

    # -- token twins -----------------------------------------------------------------------------
    def _issue(self, keys_, argv):
        if any(self._alive(k) for k in (keys_[0], keys_[1], keys_[3])):
            raise RuntimeError("token identity collision")
        for key, ttl in ((keys_[0], argv[3]), (keys_[1], argv[4])):
            self.data[key] = argv[0]
            self._px(key, ttl)
        self.data[keys_[3]] = {b"account": argv[1], b"head": argv[7], b"prev": b"", b"created_at": argv[2],
                               b"rotated_at": argv[2]}
        self._px(keys_[3], argv[4])
        if self._alive(keys_[2]) is None:
            self.data[keys_[2]] = _Zset()
        self.data[keys_[2]][argv[7]] = int(argv[6])
        self._px(keys_[2], argv[4])
        return 1

    def _validate(self, keys_, argv):
        if (self._alive(keys_[0]) is None or self.data[keys_[0]] != argv[0] or self._type(keys_[1]) != "hash"
                or self._hget(keys_[1], "account") != argv[1]):
            return 0
        head = self._hget(keys_[1], "head")
        if not head or self._type(keys_[2]) != "zset" or head not in self.data[keys_[2]]:
            return 0
        raw = self._alive(argv[3] + head) and self.data[argv[3] + head]
        if not raw:
            return 0
        data = json.loads(raw)
        return int(data["account"].encode() == argv[1] and data["family_id"].encode() == argv[2])

    def _rotate(self, keys_, argv):
        if self._alive(keys_[0]) is None or self.data[keys_[0]] != argv[0]:
            return 0
        if self._type(keys_[1]) != "zset" or argv[1] not in self.data[keys_[1]]:
            return 0
        if self._type(keys_[4]) == "none":
            return 2
        if self._hget(keys_[4], "head") != argv[1]:
            return 3
        self.data.pop(keys_[0])
        del self.data[keys_[1]][argv[1]]
        for key, ttl in ((keys_[2], argv[3]), (keys_[3], argv[4])):
            self.data[key] = argv[2]
            self._px(key, ttl)
        self.data[keys_[4]].update({b"head": argv[7], b"prev": argv[1], b"rotated_at": argv[9]})
        self.data[keys_[1]][argv[7]] = int(argv[6])
        if int(argv[10]) > 0:
            self.data[keys_[5]] = {b"access": argv[11], b"refresh": argv[7], b"access_expire": argv[12],
                                   b"refresh_expire": argv[6], b"rotated_at": argv[9]}
            self._px(keys_[5], argv[10])
        return 1

    def _replay(self, keys_, argv):
        record = self._type(keys_[1])
        if self._type(keys_[0]) == "none":
            return [2] if record == "hash" else [3]
        account, head = self._hget(keys_[0], "account"), self._hget(keys_[0], "head")
        if head == argv[0]:
            return [3]
        if record == "hash":
            if self._pttl(keys_[1]) > int(argv[1]):
                return [3]
            successor = self._hget(keys_[1], "refresh")
            if head == successor:
                if self._pttl(argv[3] + successor) == -2:
                    return [0]
                return [1, account, self._hget(keys_[1], "access"), successor,
                        self._hget(keys_[1], "access_expire"), self._hget(keys_[1], "refresh_expire"),
                        self._hget(keys_[1], "rotated_at")]
        replayed = b"predecessor" if self._hget(keys_[0], "prev") == argv[0] else b"older"
        result = [account, replayed, self._hget(keys_[0], "rotated_at"), self._hget(keys_[0], "created_at")]
        if argv[5] == b"1":
            return [5, *result]
        self.data.pop(argv[3] + head, None)
        self.data.pop(keys_[0], None)
        index = self._alive(argv[4] + account)
        if index is not None:
            self.data[index].pop(head, None)
        return [4, *result]

    # -- misc twins ------------------------------------------------------------------------------
    def _mint(self, keys_, argv):
        current = self._alive(keys_[0]) and int(self.data[keys_[0]])
        value = max((current or 0) + 1, int(argv[0]), int(argv[1]) + 1)
        self.data[keys_[0]] = str(value).encode()
        self._px(keys_[0], int(argv[2]) * 1000)
        return str(value).encode()

    def _release(self, keys_, argv):
        if self._alive(keys_[0]) is not None and self.data[keys_[0]] == argv[0]:
            self.data.pop(keys_[0])
            return 1
        return 0

    def _lock_renew(self, keys_, argv):
        if self._alive(keys_[0]) is not None and self.data[keys_[0]] == argv[0]:
            self._px(keys_[0], argv[1])
            return 1
        return 0


class _Zset(dict):
    """Marks a dict as a sorted set (member -> score)."""
