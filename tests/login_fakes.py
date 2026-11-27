"""In-memory Redis for login tests: commands plus Python twins of the login Lua scripts.

fakeredis/lupa are not installed, so each script is emulated by identity; the twins follow the Go Lua
line by line. Real-Redis byte compatibility is not exercised here. Other suites (e.g. the scene craft
proxy ledger) reuse the generic commands and register their own script twins in ``scripts``.
"""

import json
import re
import time

from xuanming.login import anti_addiction, locker, password_auth, queue, session_fence, sessions, tokens


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
    def __init__(self, *, clock_ms=None):
        self.data: dict[bytes, object] = {}
        self.expiry: dict[bytes, float] = {}
        self.fail = set()
        self._clock_ms = clock_ms
        self.scripts = {
            session_fence.COMMIT_LOGIN: self._commit_login,
            session_fence.CLOSE: self._close,
            session_fence.CHECK_ENTER_GAME: self._check_enter,
            session_fence.BIND_PLAYER: self._bind,
            session_fence.LEAVE: self._leave,
            session_fence.RENEW: self._renew,
            tokens.ISSUE_FAMILY: self._issue,
            tokens.VALIDATE_FAMILY_ACCESS: self._validate,
            tokens.ROTATE_REFRESH: self._rotate,
            tokens.REPLAY_REFRESH: self._replay,
            tokens.RETIRE_REFRESH: self._retire,
            sessions.MINT_LOGIN_GENERATION: self._mint,
            locker.RELEASE: self._release,
            locker.RENEW: self._lock_renew,
            queue.ENQUEUE_SCRIPT: self._queue_enqueue,
            queue.LOOKUP_SCRIPT: self._queue_lookup,
            queue.ADMIT_ENTRY_SCRIPT: self._queue_admit_entry,
            queue.ADMITTED_HOLD_SCRIPT: self._queue_admitted_hold,
            queue.ADMITTED_COUNT_SCRIPT: self._queue_admitted_count,
            queue.RESERVE_FAST_PATH_SCRIPT: self._queue_reserve_fast_path,
            password_auth.PASSWORD_QUOTA_BEGIN: self._password_quota_begin,
            password_auth.PASSWORD_QUOTA_FINISH: self._password_quota_finish,
            anti_addiction.COUNT_ATTEMPT_SCRIPT: self._count_attempt,
        }

    # -- plumbing --------------------------------------------------------------------------------
    def _now_ms(self) -> float:
        return float(self._clock_ms()) if self._clock_ms is not None else time.monotonic() * 1000

    def _wall_ms(self) -> int:
        return int(self._clock_ms()) if self._clock_ms is not None else time.time_ns() // 1_000_000

    def _alive(self, key):
        key = _b(key)
        deadline = self.expiry.get(key)
        if deadline is not None and deadline <= self._now_ms():
            self.data.pop(key, None)
            self.expiry.pop(key, None)
        return key if key in self.data else None

    def _pttl(self, key):
        key = self._alive(key)
        if key is None:
            return -2
        deadline = self.expiry.get(key)
        return -1 if deadline is None else max(1, int(deadline - self._now_ms()))

    def _px(self, key, ms):
        self.expiry[_b(key)] = self._now_ms() + int(ms)

    def _pxat_wall(self, key, wall_deadline_ms: int):
        delta = max(1, int(wall_deadline_ms) - self._wall_ms())
        self._px(key, delta)

    def _hash(self, key):
        key = _b(key)
        if self._alive(key) is None:
            self.data[key] = {}
        return self.data[key]

    def _zset(self, key):
        key = _b(key)
        if self._alive(key) is None:
            self.data[key] = _Zset()
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

    async def mget(self, *keys_):
        if "mget" in self.fail:
            raise ConnectionError("redis down")
        return [None if (alive := self._alive(k)) is None else self.data[alive] for k in keys_]

    async def set(self, key, value, *, nx=False, px=None, ex=None):
        if "set" in self.fail:
            raise ConnectionError("redis down")
        key = _b(key)
        if nx and self._alive(key) is not None:
            return None
        self.data[key] = _b(value)
        self.expiry.pop(key, None)
        if px:
            self._px(key, px)
        if ex:
            self._px(key, int(ex) * 1000)
        return True

    async def delete(self, *keys_):
        for key in keys_:
            self.data.pop(_b(key), None)
            self.expiry.pop(_b(key), None)

    async def ttl(self, key):
        pttl = self._pttl(key)
        if pttl < 0:
            return pttl
        return max(1, (pttl + 999) // 1000)

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

    async def zcard(self, key):
        if "zcard" in self.fail:
            raise ConnectionError("redis down")
        key = self._alive(key)
        return 0 if key is None else len(self.data[key])

    async def zrange(self, key, start, end):
        key = self._alive(key)
        if key is None:
            return []
        ordered = [m for m, _ in sorted(self.data[key].items(), key=lambda kv: (kv[1], kv[0]))]
        stop = len(ordered) if end < 0 else end + 1
        return ordered[start:stop]

    async def pexpire(self, key, ms):
        if self._alive(key) is not None:
            self._px(key, ms)

    async def expire(self, key, seconds):
        if self._alive(key) is None:
            return 0
        self._px(key, int(seconds) * 1000)
        return 1

    async def hmget(self, key, *fields):
        return [self._hget(key, field) for field in fields]

    async def hset(self, key, field, value):
        if "hset" in self.fail:
            raise ConnectionError("redis down")
        record = self._hash(key)
        added = _b(field) not in record
        record[_b(field)] = _b(value)
        return int(added)

    async def hdel(self, key, *fields):
        key = self._alive(key)
        if key is None:
            return 0
        return sum(self.data[key].pop(_b(field), None) is not None for field in fields)

    async def hgetall(self, key):
        if "hgetall" in self.fail:
            raise ConnectionError("redis down")
        key = self._alive(key)
        return {} if key is None else dict(self.data[key])

    async def hincrby(self, key, field, amount=1):
        if "hincrby" in self.fail:
            raise ConnectionError("redis down")
        record = self._hash(key)
        value = int(record.get(_b(field), b"0")) + int(amount)
        record[_b(field)] = str(value).encode()
        return value

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

    def _retire(self, keys_, argv):
        if self._type(keys_[0]) != "zset":
            return 0
        zset = self.data[keys_[0]]
        keep = int(argv[1])
        if len(zset) <= keep:
            return 0
        ordered = [m for m, _ in sorted(zset.items(), key=lambda kv: (kv[1], kv[0]))]
        protect = argv[4]
        to_drop = [m for m in ordered if m != protect][: len(zset) - keep]
        for token in to_drop:
            raw = self._alive(argv[2] + token) and self.data[argv[2] + token]
            if raw:
                data = json.loads(raw)
                family_key = argv[3] + data["family_id"].encode()
                if self._hget(family_key, "head") == token:
                    self.data.pop(family_key, None)
                    self.expiry.pop(family_key, None)
            self.data.pop(argv[2] + token, None)
            self.expiry.pop(argv[2] + token, None)
            zset.pop(token, None)
        return len(to_drop)

    # -- login queue twins -----------------------------------------------------------------------
    def _zrank(self, key, member):
        if self._alive(key) is None or member not in self.data[key]:
            return None
        ordered = [m for m, _ in sorted(self.data[key].items(), key=lambda kv: (kv[1], kv[0]))]
        return ordered.index(member)

    def _queue_enqueue(self, keys_, argv):
        if self._alive(keys_[3]) is not None:
            self.data[keys_[2]] = argv[0]
            self._px(keys_[2], argv[2])
            total = 0 if self._alive(keys_[0]) is None else len(self.data[keys_[0]])
            return [2, 0, total]
        queued = self._alive(keys_[0]) is not None and argv[0] in self.data[keys_[0]]
        kept = queued and self._type(keys_[1]) == "hash" and self._hget(keys_[1], argv[8]) is None
        if not kept:
            max_len = int(argv[3])
            current_len = 0 if self._alive(keys_[0]) is None else len(self.data[keys_[0]])
            if not queued and max_len > 0 and current_len >= max_len:
                return [4, 0, 0]
            self._zset(keys_[0])[argv[0]] = int(argv[1])
            self.data.pop(keys_[1], None)
            self.expiry.pop(keys_[1], None)
            self.data[keys_[1]] = {
                b"account": argv[4], b"device": argv[5], argv[7]: argv[6], b"createdAt": argv[1],
            }
        self.data[keys_[2]] = argv[0]
        self._px(keys_[2], argv[2])
        idle = int(argv[2])
        for k in (keys_[0], keys_[1]):
            if self._pttl(k) < idle:
                self._px(k, idle)
        rank = self._zrank(keys_[0], argv[0])
        total = len(self.data[keys_[0]])
        return [1 if kept else 3, rank, total]

    def _queue_lookup(self, keys_, argv):
        if self._alive(keys_[2]) is None or self.data[keys_[2]] != argv[0]:
            return [1, 0, 0, b""]
        if self._alive(keys_[3]) is not None:
            slot = self.data.pop(keys_[3])
            self.expiry.pop(keys_[3], None)
            if self._type(keys_[0]) == "hash":
                self.data[keys_[0]][argv[6]] = slot
                self.data[keys_[0]][argv[7]] = argv[9]
            return [2, 0, 0, slot]
        if self._type(keys_[0]) != "hash":
            return [1, 0, 0, b""]
        meta = self.data[keys_[0]]
        if meta.get(argv[2]) != argv[1]:
            return [1, 0, 0, b""]
        if meta.get(argv[3]) is not None:
            taken = meta.get(argv[6])
            retake_until = int(meta.get(argv[7]) or b"0")
            if taken is not None and int(argv[8]) < retake_until:
                return [6, 0, 0, taken]
            return [5, 0, 0, b""]
        rank = self._zrank(keys_[1], argv[0])
        if rank is None:
            return [4, 0, 0, b""]
        idle = int(argv[4])
        for k in (keys_[0], keys_[1], keys_[2]):
            ttl = self._pttl(k)
            if ttl != -2 and ttl < idle:
                self._px(k, int(argv[5]))
        return [3, rank, len(self.data[keys_[1]]), b""]

    def _queue_admit_entry(self, keys_, argv):
        in_queue = self._alive(keys_[3]) is not None and argv[5] in self.data[keys_[3]]
        if not in_queue:
            if self._type(keys_[1]) == "hash" and self._hget(keys_[1], argv[2]) == argv[3]:
                return 5
            return 1
        if self._type(keys_[1]) != "hash":
            self.data[keys_[3]].pop(argv[5], None)
            return 2
        if self._hget(keys_[1], argv[2]) is not None:
            self.data[keys_[3]].pop(argv[5], None)
            return 3
        now_ms = int(argv[7])
        budget = int(argv[8])
        live = sum(1 for score in (self.data[keys_[2]].values() if self._alive(keys_[2]) is not None else ())
                   if score > now_ms)
        if live >= budget:
            return 6
        self.data[keys_[1]][argv[2]] = argv[3]
        self.data[keys_[0]] = argv[0]
        self._px(keys_[0], int(argv[1]))
        self._zset(keys_[2])[argv[5]] = int(argv[4])
        if self._pttl(keys_[2]) < int(argv[6]):
            self._px(keys_[2], int(argv[6]))
        self.data[keys_[3]].pop(argv[5], None)
        return 4

    def _queue_admitted_hold(self, keys_, argv):
        self._zset(keys_[0])[argv[1]] = int(argv[0])
        if self._pttl(keys_[0]) < int(argv[2]):
            self._px(keys_[0], int(argv[2]))
        return 1

    def _queue_admitted_count(self, keys_, argv):
        now_ms = int(argv[0])
        if self._alive(keys_[0]) is None:
            return 0
        zset = self.data[keys_[0]]
        for m in [k for k, v in zset.items() if v <= now_ms]:
            zset.pop(m, None)
        return len(zset)

    def _queue_reserve_fast_path(self, keys_, argv):
        now_ms, budget = int(argv[0]), int(argv[1])
        zset = self._zset(keys_[0])
        for m in [k for k, v in zset.items() if v <= now_ms]:
            zset.pop(m, None)
        if len(zset) >= budget:
            return 0
        zset[argv[2]] = int(argv[3])
        if self._pttl(keys_[0]) < int(argv[4]):
            self._px(keys_[0], int(argv[4]))
        return 1

    # -- password quota twins --------------------------------------------------------------------
    def _load_quota_state(self, key, argv):
        fp = argv[0].decode()
        limit, window, lock = int(argv[1]), int(argv[2]), int(argv[3])
        token = argv[4].decode()
        now = self._wall_ms()
        if not (1 <= limit <= 100 and 1000 <= window <= 3_600_000 and 1000 <= lock <= 3_600_000
                and len(token) == 32 and re.fullmatch(r"[0-9a-f]+", token)):
            return None, False, now, fp, limit, window, lock, token
        exists = self._alive(key) is not None
        if not exists:
            return {"n": 0, "w": 0, "l": 0, "t": "", "e": 0, "d": "", "r": "", "u": 0}, False, now, fp, limit, window, lock, token
        if self._type(key) != "hash" or len(self.data[key]) != 10 or self._pttl(key) < 0:
            return None, True, now, fp, limit, window, lock, token
        raw = self.data[key]
        if raw.get(b"v") != b"1" or raw.get(b"fp") != argv[0]:
            return None, True, now, fp, limit, window, lock, token
        state = {
            "n": int(raw[b"n"]), "w": int(raw[b"w"]), "l": int(raw[b"l"]),
            "t": raw[b"t"].decode(), "e": int(raw[b"e"]),
            "d": raw[b"d"].decode(), "r": raw[b"r"].decode(), "u": int(raw[b"u"]),
        }
        return state, True, now, fp, limit, window, lock, token

    def _save_quota_state(self, key, fp: str, s: dict):
        deadline = max(s["w"], s["l"], s["e"], s["u"])
        self.data[key] = {
            b"v": b"1", b"fp": fp.encode(),
            b"n": str(s["n"]).encode(), b"w": str(s["w"]).encode(), b"l": str(s["l"]).encode(),
            b"t": s["t"].encode(), b"e": str(s["e"]).encode(),
            b"d": s["d"].encode(), b"r": s["r"].encode(), b"u": str(s["u"]).encode(),
        }
        self._pxat_wall(key, deadline)

    def _password_quota_begin(self, keys_, argv):
        s, _exists, now, fp, _limit, _window, _lock, token = self._load_quota_state(keys_[0], argv)
        if s is None:
            return [5, 0]
        if s["w"] > 0 and s["w"] <= now:
            s["n"] = s["w"] = 0
        if s["l"] > 0 and s["l"] <= now:
            s["l"] = 0
        if s["l"] > now:
            return [2, 0]
        if s["t"]:
            if s["t"] == token:
                return [1, s["e"]] if s["e"] > now else [4, 0]
            if s["e"] > now:
                return [3, 0]
        if s["d"] == token:
            return [4, 0]
        s["t"], s["e"], s["d"], s["r"], s["u"] = token, now + 30_000, "", "", 0
        self._save_quota_state(keys_[0], fp, s)
        return [1, s["e"]]

    def _password_quota_finish(self, keys_, argv):
        s, exists, now, fp, limit, window, lock, token = self._load_quota_state(keys_[0], argv)
        if s is None:
            return [5, 0]
        outcome = argv[5].decode()
        if outcome not in ("success", "mismatch", "abort"):
            return [6, 0]
        if not exists:
            return [4, 0]
        if s["d"] == token:
            if s["u"] <= now:
                return [4, 0]
            return [1, 0] if s["r"] == outcome else [6, 0]
        if s["t"] != token or s["e"] <= now:
            return [4, 0]
        if s["w"] > 0 and s["w"] <= now:
            s["n"] = s["w"] = 0
        if s["l"] > 0 and s["l"] <= now:
            s["l"] = 0
        if outcome == "mismatch":
            if s["n"] == 0:
                s["w"] = now + window
            s["n"] += 1
            if s["n"] >= limit:
                s["n"] = s["w"] = 0
                s["l"] = now + lock
        elif outcome == "success":
            s["n"] = s["w"] = s["l"] = 0
        s["d"], s["r"], s["u"] = token, outcome, s["e"]
        s["t"], s["e"] = "", 0
        self._save_quota_state(keys_[0], fp, s)
        return [1, 0]

    # -- misc twins ------------------------------------------------------------------------------
    def _count_attempt(self, keys_, argv):
        current = int(self.data[keys_[0]]) if self._alive(keys_[0]) is not None else 0
        nxt = current + 1
        self.data[keys_[0]] = str(nxt).encode()
        if nxt == 1:
            self._px(keys_[0], int(argv[0]) * 1000)
        return nxt

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
