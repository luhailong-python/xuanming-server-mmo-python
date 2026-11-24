"""player_locator 核心 10 段 Redis Lua 脚本，逐字同步自 Go ``session_cas.go``。"""

from __future__ import annotations

REPLACE_SESSION_AND_CANCEL_LEASE_LUA = """
local current = redis.call("GET", KEYS[1])
if ARGV[1] == "missing" then
    if current then return 0 end

    -- commitLeaseExpiry 已删 session、但外部 Gate/Scene 副作用尚未全部成功时，
    -- processing claim 是唯一的清理所有权。此时建立新 session 会撤销旧 claim，
    -- 造成旧场景永久残留；若旧调用迟到，还可能误清新 location。fail-closed
    -- 拒绝首次写入，直到旧 worker 成功 ack。
    if redis.call("ZSCORE", KEYS[3], ARGV[5])
        or redis.call("HGET", KEYS[4], ARGV[5])
        or redis.call("HGET", KEYS[5], ARGV[5]) then
        return 2
    end
else
    if (not current) or current ~= ARGV[2] then return 0 end
end

-- 同一 gate 上顶号时两个 gate 相等:跳过 SREM 只 SADD。顺序不能改成先 SADD 再 SREM,
-- 那会把同 gate 顶号的成员删掉(TestGateIndex_ReplaceLoginOnSameGateKeepsMember 钉住)。
-- missing 模式在登记任何清理前返回 2、CAS 失败返回 0,这两条早退路径都不碰索引。
if ARGV[6] ~= "" and ARGV[6] ~= ARGV[7] then
    redis.call("SREM", KEYS[7], ARGV[5])
    if redis.call("SCARD", KEYS[7]) == 0 then redis.pcall("SREM", KEYS[6], ARGV[6]) end
end
if ARGV[7] ~= "" then
    redis.call("SADD", KEYS[8], ARGV[5])
    redis.call("SADD", KEYS[6], ARGV[7])
end
redis.call("ZREM", KEYS[2], ARGV[5])
redis.call("ZREM", KEYS[3], ARGV[5])
redis.call("HDEL", KEYS[4], ARGV[5])
redis.call("HDEL", KEYS[5], ARGV[5])
if tonumber(ARGV[4]) > 0 then
    redis.call("SET", KEYS[1], ARGV[3], "PX", ARGV[4])
else
    redis.call("SET", KEYS[1], ARGV[3])
end
return 1
"""

SET_DISCONNECTING_LUA = """
local current = redis.call("GET", KEYS[1])
if (not current) or current ~= ARGV[1] then return 0 end

redis.call("ZADD", KEYS[2], ARGV[4], ARGV[5])
redis.call("ZREM", KEYS[3], ARGV[5])
redis.call("HDEL", KEYS[4], ARGV[5])
redis.call("HDEL", KEYS[5], ARGV[5])
if ARGV[6] ~= "" then
    redis.call("SREM", KEYS[7], ARGV[5])
    if redis.call("SCARD", KEYS[7]) == 0 then redis.pcall("SREM", KEYS[6], ARGV[6]) end
end
if tonumber(ARGV[3]) > 0 then
    redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
else
    redis.call("SET", KEYS[1], ARGV[2])
end
return 1
"""

DELETE_SESSION_IF_UNCHANGED_LUA = """
local current = redis.call("GET", KEYS[1])
if ARGV[1] == "missing" then
    if current then return 0 end
else
    if (not current) or current ~= ARGV[2] then return 0 end
end

redis.call("ZREM", KEYS[2], ARGV[3])
redis.call("ZREM", KEYS[3], ARGV[3])
redis.call("HDEL", KEYS[4], ARGV[3])
redis.call("HDEL", KEYS[5], ARGV[3])
if ARGV[4] ~= "" then
    redis.call("SREM", KEYS[7], ARGV[3])
    if redis.call("SCARD", KEYS[7]) == 0 then redis.pcall("SREM", KEYS[6], ARGV[4]) end
end
redis.call("DEL", KEYS[1])
return 1
"""

CLAIM_EXPIRED_LEASES_LUA = """
local recovered = redis.call("ZRANGEBYSCORE", KEYS[2], "-inf", ARGV[1], "LIMIT", 0, ARGV[3])
for _, player_id in ipairs(recovered) do
    redis.call("ZREM", KEYS[2], player_id)
    redis.call("ZADD", KEYS[1], ARGV[1], player_id)
    redis.call("HDEL", KEYS[3], player_id)
end

local expired = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, ARGV[3])
local result = {}
for _, player_id in ipairs(expired) do
    redis.call("ZREM", KEYS[1], player_id)
    redis.call("ZADD", KEYS[2], ARGV[2], player_id)

    local token = ARGV[4] .. ":" .. player_id
    redis.call("HSET", KEYS[3], player_id, token)

    table.insert(result, player_id)
    table.insert(result, token)
end
return result
"""

PIN_LEASE_CLAIM_PAYLOAD_LUA = """
if redis.call("HGET", KEYS[2], ARGV[1]) ~= ARGV[2] then return false end

local payload = redis.call("GET", KEYS[1])
if payload then
    redis.call("HSET", KEYS[3], ARGV[1], payload)
    return payload
end
payload = redis.call("HGET", KEYS[3], ARGV[1])
if payload then return payload end
return ""
"""

COMMIT_LEASE_EXPIRY_LUA = """
if redis.call("HGET", KEYS[4], ARGV[1]) ~= ARGV[2] then return 0 end

local current = redis.call("GET", KEYS[1])
if current and current ~= ARGV[3] then
    redis.call("ZREM", KEYS[3], ARGV[1])
    redis.call("HDEL", KEYS[4], ARGV[1])
    redis.call("HDEL", KEYS[5], ARGV[1])
    return 2
end

redis.call("DEL", KEYS[1])
return 1
"""

ACK_LEASE_CLAIM_LUA = """
if redis.call("HGET", KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[3], ARGV[1])
return 1
"""

RENEW_LEASE_CLAIMS_LUA = """
local renewed = 0
for i = 2, #ARGV, 2 do
    local player_id = ARGV[i]
    local token = ARGV[i + 1]
    if redis.call("HGET", KEYS[2], player_id) == token then
        redis.call("ZADD", KEYS[1], ARGV[1], player_id)
        renewed = renewed + 1
    end
end
return renewed
"""

REARM_LEASE_CLAIM_LUA = """
if redis.call("HGET", KEYS[4], ARGV[1]) ~= ARGV[2] then return 0 end

local current = redis.call("GET", KEYS[1])
if (not current) or current ~= ARGV[3] then
    redis.call("ZREM", KEYS[3], ARGV[1])
    redis.call("HDEL", KEYS[4], ARGV[1])
    redis.call("HDEL", KEYS[5], ARGV[1])
    return 2
end

redis.call("ZADD", KEYS[2], ARGV[4], ARGV[1])
redis.call("PERSIST", KEYS[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("HDEL", KEYS[4], ARGV[1])
redis.call("HDEL", KEYS[5], ARGV[1])
return 1
"""

ENQUEUE_OFFLINE_CLEANUP_LUA = """
if redis.call("GET", KEYS[1]) then return 0 end
redis.call("ZADD", KEYS[2], ARGV[1], ARGV[2])
redis.call("HSET", KEYS[3], ARGV[2], ARGV[3])
return 1
"""
