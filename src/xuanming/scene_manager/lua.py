"""scene_manager 两域 Redis Lua 脚本集合，与 Go 参考实现逐段对齐。

严格分层：
- 控制域脚本仅操作 ``SceneControlRedis`` 的 ``{ctl:<zone>}`` 键；
- 玩家域脚本仅操作 ``PlayerRedis`` 的 ``{<player_id>}`` 键；
两域绝不在同一段 Lua 脚本中混用。
"""

from __future__ import annotations

# ==============================================================================
# 一、SceneControlRedis（控制域 {ctl:<zone>}）Lua 脚本
# ==============================================================================

_LUA_SCENE_REGISTRATION_GUARD = """
local prior = redis.call('GET', KEYS[5])
if prior then
    local record = cjson.decode(prior)
    if record.token ~= ARGV[2] then
        return {'conflict'}
    end
    if record.reply[1] == 'registered' and redis.call('GET', KEYS[1]) ~= ARGV[4] then
        return {'conflict'}
    end
    return record.reply
end
if redis.call('EXISTS', KEYS[1], KEYS[6]) > 0 then
    return {'conflict'}
end
"""

_LUA_SCENE_REGISTRATION_COMMIT = """
if ARGV[6] ~= '' and redis.call('ZREM', KEYS[7], ARGV[6]) == 0 then
    return {'fenced'}
end
redis.call('SET', KEYS[1], ARGV[4])
redis.call('INCR', KEYS[2])
redis.call('SADD', KEYS[3], ARGV[1])
if ARGV[5] ~= '' then
    redis.call('SET', KEYS[4], ARGV[5])
end
"""

_LUA_SCENE_REGISTRATION_RECORD = """
local reply = {'registered'}
redis.call('SET', KEYS[5], cjson.encode({token = ARGV[2], reply = reply}), 'EX', ARGV[3])
return reply
"""

_LUA_WORLD_CHANNEL_WITHIN_DESIRED = """
local desired = tonumber(redis.call('HGET', KEYS[9], ARGV[7]) or '')
if not desired or desired < 1 then
    desired = tonumber(ARGV[8])
end
if desired and redis.call('SCARD', KEYS[8]) >= desired then
    return {'over_desired'}
end
"""

# 登记按需副本/镜像（与 Go scene_registration.go luaRegisterInstance 一致）
LUA_REGISTER_INSTANCE = (
    _LUA_SCENE_REGISTRATION_GUARD
    + _LUA_SCENE_REGISTRATION_COMMIT
    + """
redis.call('ZADD', KEYS[8], ARGV[7], ARGV[1])
if ARGV[8] == '1' then
    redis.call('SET', KEYS[9], '1')
end
if KEYS[11] then
    redis.call('SET', KEYS[10], ARGV[9])
    redis.call('SADD', KEYS[11], ARGV[1])
end
"""
    + _LUA_SCENE_REGISTRATION_RECORD
)

# 登记大世界频道（与 Go scene_registration.go luaRegisterWorldChannel 一致）
LUA_REGISTER_WORLD_CHANNEL = (
    _LUA_SCENE_REGISTRATION_GUARD
    + _LUA_WORLD_CHANNEL_WITHIN_DESIRED
    + _LUA_SCENE_REGISTRATION_COMMIT
    + """
redis.call('SADD', KEYS[8], ARGV[1])
"""
    + _LUA_SCENE_REGISTRATION_RECORD
)

# 登记结局未知时的作废核对脚本
LUA_ABORT_SCENE_REGISTRATION = """
local prior = redis.call('GET', KEYS[1])
if prior then
    local record = cjson.decode(prior)
    if record.token ~= ARGV[1] then
        return {'conflict'}
    end
    if record.reply[1] == 'registered' and redis.call('GET', KEYS[2]) ~= ARGV[3] then
        return {'conflict'}
    end
    return record.reply
end
local reply = {'aborted'}
redis.call('SET', KEYS[1], cjson.encode({token = ARGV[1], reply = reply}), 'EX', ARGV[2])
return reply
"""

# 仅在 scene:{ctl:<z>}:<id>:node 仍存在时预占成员（避免 destroy-while-entering 孤儿键）
LUA_ATOMIC_ADD_SCENE_MEMBER = """
if redis.call('EXISTS', KEYS[1]) == 0 then
    return -1
end
redis.call('SADD', KEYS[2], ARGV[1])
if KEYS[3] ~= nil then
    redis.call('SADD', KEYS[3], ARGV[1])
end
return redis.call('SCARD', KEYS[2])
"""

# 幂等加入场景/节点成员集合
LUA_ADD_SCENE_MEMBER = """
for i = 1, #KEYS do
    redis.call('SADD', KEYS[i], ARGV[1])
end
return 1
"""

# 幂等移出场景/节点成员集合
LUA_REMOVE_SCENE_MEMBER = """
for i = 1, #KEYS do
    redis.call('SREM', KEYS[i], ARGV[1])
end
return 1
"""

# 原子挑人数最少且仍存活的大世界频道并预占成员
LUA_RESERVE_BEST_WORLD_CHANNEL = """
local n = #ARGV - 1
local player = ARGV[n + 1]
local bestIdx = nil
local bestCount = nil

for i = 1, n do
    local sceneNodeKey = KEYS[(i - 1) * 2 + 1]
    local membersKey = KEYS[(i - 1) * 2 + 2]
    if redis.call('GET', sceneNodeKey) == ARGV[i] then
        local count = redis.call('SCARD', membersKey)
        if bestCount == nil or count < bestCount then
            bestIdx = i
            bestCount = count
        end
    end
end

if bestIdx == nil then
    return nil
end

local bestMembersKey = KEYS[(bestIdx - 1) * 2 + 2]
local bestNodeKey = KEYS[2 * n + bestIdx]
redis.call('SADD', bestMembersKey, player)
redis.call('SADD', bestNodeKey, player)
local newCount = redis.call('SCARD', bestMembersKey)
return {tostring(bestIdx), ARGV[bestIdx], tostring(newCount)}
"""

_LUA_SCENE_CLAIM_PRELUDE = """
local pendingToken = redis.call('HGET', KEYS[7], 'token')
if pendingToken then
    if pendingToken == ARGV[2] then
        return cjson.decode(redis.call('HGET', KEYS[7], 'reply'))
    end
    return {'pending', '', '', '', ''}
end
"""

_LUA_SCENE_CLAIM_OWNER_CHECK = """
local nodeId = redis.call('GET', KEYS[1])
if ARGV[6] == '' then
    if nodeId then
        return {'stale', '', '', '', ''}
    end
    nodeId = ''
elseif nodeId ~= ARGV[6] then
    return {'stale', '', '', '', ''}
end
"""

_LUA_SCENE_CLAIM_IDLE_GUARD = """
if redis.call('SCARD', KEYS[2]) > 0 then
    redis.call('ZADD', KEYS[3], 'XX', ARGV[3], ARGV[1])
    return {'busy', '', '', '', ''}
end
"""

_LUA_SCENE_CLAIM_COMMIT = """
local agonesGs = redis.call('GET', KEYS[6]) or ''
local source = redis.call('GET', KEYS[5]) or ''
local mirror = redis.call('GET', KEYS[4]) or ''
local members = redis.call('SMEMBERS', KEYS[2])
redis.call('DEL', KEYS[1], KEYS[2], KEYS[4], KEYS[5], KEYS[6])
redis.call('ZREM', KEYS[3], ARGV[1])
local unlistFrom = 9
if nodeId ~= '' then
    redis.call('SREM', KEYS[9], ARGV[1])
    if ARGV[7] == '1' then
        local count = tonumber(redis.call('GET', KEYS[10]) or '0') or 0
        if count > 0 then
            redis.call('DECR', KEYS[10])
        end
    end
    unlistFrom = 11
end
local unlistTo = #KEYS - (tonumber(ARGV[8] or '0') or 0)
for i = unlistFrom, unlistTo do
    redis.call('SREM', KEYS[i], ARGV[1])
end
local reply = {'claimed', nodeId, agonesGs, source, mirror}
for i = 1, #members do
    reply[i + 5] = members[i]
end
redis.call('HSET', KEYS[7], 'token', ARGV[2], 'kind', ARGV[4], 'reason', ARGV[5], 'claimed_at', ARGV[3],
    'reply', cjson.encode(reply))
redis.call('ZADD', KEYS[8], ARGV[3], ARGV[1])
return reply
"""

_LUA_SCENE_CLAIM_DEAD_NODE_TASK_CHECK = """
local deadNodeTaskQueuedAt = redis.call('ZSCORE', KEYS[#KEYS], ARGV[9])
if not deadNodeTaskQueuedAt or tonumber(deadNodeTaskQueuedAt) ~= tonumber(ARGV[10]) then
    return {'withdrawn', '', '', '', ''}
end
"""

# 空闲副本销毁认领脚本（有人即回 busy 并重置空闲时钟）
LUA_ATOMIC_DESTROY_INSTANCE = (
    _LUA_SCENE_CLAIM_PRELUDE
    + _LUA_SCENE_CLAIM_OWNER_CHECK
    + _LUA_SCENE_CLAIM_IDLE_GUARD
    + _LUA_SCENE_CLAIM_COMMIT
)

# 强制销毁副本/镜像/回滚认领脚本（不看人数，按期望属主 CAS）
LUA_ATOMIC_DESTROY_INSTANCE_FORCE = (
    _LUA_SCENE_CLAIM_PRELUDE
    + _LUA_SCENE_CLAIM_OWNER_CHECK
    + _LUA_SCENE_CLAIM_COMMIT
)

# 死节点孤儿副本销毁认领脚本（先核对收尾任务未被撤，再核对属主仍是死节点）
LUA_ATOMIC_DESTROY_ORPHAN_OF_DEAD_NODE = (
    _LUA_SCENE_CLAIM_PRELUDE
    + _LUA_SCENE_CLAIM_DEAD_NODE_TASK_CHECK
    + _LUA_SCENE_CLAIM_OWNER_CHECK
    + _LUA_SCENE_CLAIM_COMMIT
)

# 完成拆除任务后按 token 清理待办记录
LUA_FINISH_SCENE_TEARDOWN = """
if redis.call('HGET', KEYS[1], 'token') ~= ARGV[2] then
    return 0
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
"""

# 摘掉没有任务 Hash 的悬空待办条目
LUA_DROP_DANGLING_TEARDOWN = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
return redis.call('ZREM', KEYS[2], ARGV[1])
"""

# 原子分配并对齐大世界分线 1-based 线号（与 Go world_channel_directory.go 一致）
LUA_ALLOC_WORLD_CHANNEL_LINE_NUMBERS = """
if #ARGV == 0 then
    return {}
end

local wanted = {}
for i = 1, #ARGV do
    wanted[ARGV[i]] = true
end

local kept = {}
local used = {}
local flat = redis.call('HGETALL', KEYS[1])
for i = 1, #flat, 2 do
    local sceneId = flat[i]
    local lineNo = tonumber(flat[i + 1])
    if wanted[sceneId] and lineNo and lineNo >= 1 and lineNo <= 4294967295
        and lineNo == math.floor(lineNo) and not used[lineNo] then
        kept[sceneId] = lineNo
        used[lineNo] = true
    else
        redis.call('HDEL', KEYS[1], sceneId)
    end
end

local out = {}
local candidate = 1
for i = 1, #ARGV do
    local sceneId = ARGV[i]
    local lineNo = kept[sceneId]
    if not lineNo then
        while used[candidate] do
            candidate = candidate + 1
        end
        lineNo = candidate
        used[lineNo] = true
        kept[sceneId] = lineNo
        redis.call('HSET', KEYS[1], sceneId, tostring(lineNo))
    end
    out[#out + 1] = sceneId
    out[#out + 1] = tostring(lineNo)
end
return out
"""

# 写入死节点收尾任务并可选同脚本摘负载集（与 Go reentry_barrier.go luaEnqueueDeadNodeReconcile 一致）
LUA_ENQUEUE_DEAD_NODE_RECONCILE = """
if ARGV[4] ~= '' then
    redis.call('ZREM', KEYS[4], ARGV[4])
end
if ARGV[3] == 'index' then
    redis.call('SUNIONSTORE', KEYS[2], KEYS[2], KEYS[3])
else
    for i = 6, #ARGV do
        redis.call('SADD', KEYS[2], ARGV[i])
    end
end
if ARGV[5] ~= '' then
    redis.call('SET', KEYS[5], ARGV[5])
else
    redis.call('DEL', KEYS[5])
end
redis.call('ZADD', KEYS[1], 'NX', ARGV[2], ARGV[1])
return redis.call('SCARD', KEYS[2])
"""

# 判死同一实例重新注册时，原子撤销收尾任务并清除 death_at（与 Go luaWithdrawDeadNodeForReturningInstance 一致）
LUA_WITHDRAW_DEAD_NODE_FOR_RETURNING_INSTANCE = """
local dead = redis.call('GET', KEYS[3])
if ARGV[2] ~= '' and dead and dead == ARGV[2] then
    redis.call('ZREM', KEYS[1], ARGV[1])
    redis.call('DEL', KEYS[2], KEYS[3], KEYS[4])
    return 1
end
if redis.call('EXISTS', KEYS[4]) == 1 then
    return 0
end
return -1
"""

# 结束死节点收尾任务并清理计数键（与 Go luaFinishDeadNodeReconcile 一致）
LUA_FINISH_DEAD_NODE_RECONCILE = """
local queuedAt = redis.call('ZSCORE', KEYS[4], ARGV[1])
if not queuedAt or tonumber(queuedAt) ~= tonumber(ARGV[3]) then
    return 1
end
if ARGV[2] ~= '1' and redis.call('SCARD', KEYS[1]) > 0 then
    return 0
end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[5])
redis.call('ZREM', KEYS[4], ARGV[1])
return 1
"""

# 释放世界频道初始化互斥锁（属主校验）
LUA_RELEASE_WORLD_INIT_LOCK = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""


# ==============================================================================
# 二、PlayerRedis（玩家域 {<player_id>}）Lua 脚本
# ==============================================================================

# 观察 SQL 归属修订下界（20 位定宽十进制串比较）
LUA_OBSERVE_LOCATION_REVISION = """
local floor = redis.call('GET', KEYS[1])
if floor ~= false and (string.len(floor) ~= 20 or string.find(floor, '%D')) then return -1 end
if floor ~= false and floor > ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
return 1
"""

# 原子读取 (location, location_revision) 快照
LUA_READ_LOCATION_SNAPSHOT = (
    "return {redis.call('GET',KEYS[1]) or '',redis.call('GET',KEYS[2]) or ''}"
)

_LUA_MINT_TARGET_CHECK = """
local function owner_epoch_dec_gt(a, b)
    if #a ~= #b then
        return #a > #b
    end
    for i = 1, #a do
        local x, y = string.byte(a, i), string.byte(b, i)
        if x ~= y then
            return x > y
        end
    end
    return false
end
local mint_target = ARGV[9]
if type(ARGV[1]) ~= "string" or not (ARGV[1] == "0" or string.match(ARGV[1], "^[1-9]%d*$"))
    or type(mint_target) ~= "string" or not string.match(mint_target, "^[1-9]%d*$")
    or not owner_epoch_dec_gt(mint_target, ARGV[1])
    or owner_epoch_dec_gt(mint_target, "9007199254740990") then
    return redis.error_reply("owner epoch mint target invalid")
end
"""

_LUA_PLACEMENT_REVISION_CHECK = """
local requested_revision = ARGV[6]
if requested_revision ~= nil and requested_revision ~= '' then
    if redis.call('GET',KEYS[5]) ~= requested_revision then return 0 end
    local tag = redis.call('GET',KEYS[4]) or ''
    if redis.call('GET',KEYS[2]) == ARGV[2] and tag == ARGV[7] then
    elseif tag ~= ARGV[8] then return 0 end
end
"""

_LUA_WRITE_PLACEMENT_REVISION = """
if requested_revision ~= nil and requested_revision ~= '' and ARGV[7] ~= '' then
    redis.call('SET',KEYS[4],ARGV[7])
elseif KEYS[4] ~= nil then
    redis.call('DEL',KEYS[4])
end
"""

_LUA_LOCATION_EXPECT_CHECK = """
local expect_mode = ARGV[4]
if expect_mode == "missing" then
    if redis.call("EXISTS", KEYS[2]) == 1 then
        return 0
    end
elseif expect_mode == "present" then
    if redis.call("GET", KEYS[2]) ~= ARGV[5] then
        return 0
    end
elseif expect_mode ~= nil and expect_mode ~= "" and expect_mode ~= "any" then
    return 0
end
"""

# 铸造新 owner_epoch 并原子写 location + location_revision（与 Go luaMintEpochAndSetLocation 一致）
LUA_MINT_EPOCH_AND_SET_LOCATION = (
    _LUA_MINT_TARGET_CHECK
    + _LUA_PLACEMENT_REVISION_CHECK
    + """
local cur = redis.call("GET", KEYS[1])
if cur == false then
    cur = "0"
end
if cur ~= ARGV[1] then
    if ARGV[3] ~= "" and cur == ARGV[9]
        and redis.call("GET", KEYS[2]) == ARGV[2]
        and redis.call("GET", KEYS[3]) == ARGV[3] then
        redis.call("SET", KEYS[1], cur)
        return -tonumber(cur)
    end
    return 0
end
"""
    + _LUA_LOCATION_EXPECT_CHECK
    + """
if ARGV[3] ~= "" and redis.call("GET", KEYS[3]) ~= ARGV[3] then
    return -1
end
redis.call("SET", KEYS[1], ARGV[9])
redis.call("SET", KEYS[2], ARGV[2])
"""
    + _LUA_WRITE_PLACEMENT_REVISION
    + """
return tonumber(ARGV[9])
"""
)

# 不铸造 epoch，在 owner_epoch 与 location 期望均匹配时更新 location（同节点换图/重连身份写）
LUA_SET_LOCATION_IF_EPOCH = (
    _LUA_PLACEMENT_REVISION_CHECK
    + """
local cur = redis.call("GET", KEYS[1])
if cur == false then
    cur = "0"
end
if cur ~= ARGV[1] then
    return 0
end
if redis.call("GET", KEYS[2]) == ARGV[2] and (requested_revision == nil or requested_revision == '' or redis.call('GET',KEYS[4]) == ARGV[7]) then
    return 1
end
"""
    + _LUA_LOCATION_EXPECT_CHECK
    + """
redis.call("SET", KEYS[2], ARGV[2])
"""
    + _LUA_WRITE_PLACEMENT_REVISION
    + """
return 1
"""
)

_LUA_CPP_HANDOFF_FENCE_HELPERS = """
local function bytecmp(a,b)
    if #a~=#b then if #a>#b then return 1 else return -1 end end
    for i=1,#a do
        local x,y=string.byte(a,i),string.byte(b,i)
        if x~=y then if x>y then return 1 else return -1 end end
    end
    return 0
end
local function uint(s, zero)
    if type(s)~='string' or #s==0 or #s>20 or not string.match(s,'^%d+$') then return false end
    if s=='0' then return zero==true end
    if string.sub(s,1,1)=='0' then return false end
    return bytecmp(s,'18446744073709551615')<=0
end
local function cmp(a,b)
    return bytecmp(a,b)
end
local function nonce(s,zero)
    return type(s)=='string' and #s==32 and not string.find(s,'[^0-9a-f]')
        and (zero or s~=string.rep('0',32))
end
local function fence(s)
    if type(s)~='string' then return nil end
    local e,i,n,a,k,t=string.match(s,'^([^|]*)|([^|]*)|([^|]*)|([^|]*)|([^|]*)|([^|]*)$')
    if not uint(e) or not uint(n,true) or not nonce(a,true) or not nonce(t)
        or #i>256 or string.find(i,'[%c%s]') or (k~='H' and k~='F' and k~='R') then return nil end
    if n=='0' then
        if k~='R' or i~='' or a~=string.rep('0',32) then return nil end
    elseif i=='' or a==string.rep('0',32) then return nil end
    return {e=e,i=i,n=n,a=a,k=k,t=t}
end
"""

# 推路由/重定向失败后的单调回滚（与 Go luaRollbackPlayerPlacement 一致）
LUA_ROLLBACK_PLAYER_PLACEMENT = (
    _LUA_CPP_HANDOFF_FENCE_HELPERS
    + """
local void_epoch = ARGV[11]
local void_requested = void_epoch ~= nil and void_epoch ~= ''
local tag_is_placement, tag_is_observed = true, true
if ARGV[8] ~= nil and ARGV[8] ~= '' then
    if redis.call('GET',KEYS[5]) ~= ARGV[8] then return 0 end
    local tag = redis.call('GET',KEYS[4]) or ''
    tag_is_placement = tag == ARGV[9] or tag == ARGV[10]
    tag_is_observed = void_requested and tag == ARGV[13]
    if not tag_is_placement and not tag_is_observed then return 0 end
end
local loc = redis.call("GET", KEYS[1])
local cur = redis.call("GET", KEYS[2])
if cur == false then
    cur = "0"
end
local function restore_location()
    if ARGV[2] == "" then
        redis.call("DEL", KEYS[1])
    else
        redis.call("SET", KEYS[1], ARGV[2])
    end
    if KEYS[4] ~= nil then
        if ARGV[10] ~= nil and ARGV[10] ~= '' then redis.call('SET',KEYS[4],ARGV[10])
        else redis.call('DEL',KEYS[4]) end
    end
end
if tag_is_placement and loc == ARGV[1] and cur == ARGV[3] then
    if ARGV[5] ~= "" and redis.call("GET", KEYS[3]) ~= ARGV[5] then
        return 3
    end
    if ARGV[4] == "bump" and ARGV[5] == "" then
        if KEYS[6] == nil or not uint(ARGV[3]) then
            return redis.error_reply("ROUTE_ROLLBACK fence shape")
        end
        local raw = redis.call("HGET", KEYS[6], "handoff_fence")
        if raw ~= false then
            local held = fence(raw)
            if not held then
                return 5
            end
            local order = cmp(held.e, ARGV[3])
            if order == 0 then
                return 4
            end
            if order > 0 then
                return 5
            end
        end
    end
    local after = 0
    if ARGV[4] == "bump" then
        after = redis.call("INCR", KEYS[2])
    end
    restore_location()
    if ARGV[6] ~= "" then
        redis.call("SET", KEYS[3], ARGV[6], "EX", ARGV[7])
    end
    if ARGV[4] == "bump" then
        return -after
    end
    return 1
end
if tag_is_placement and ARGV[4] == "bump" and tonumber(cur) == tonumber(ARGV[3]) + 1 then
    if (ARGV[2] == "" and loc == false) or (ARGV[2] ~= "" and loc == ARGV[2]) then
        return 2
    end
end
if void_requested and tag_is_observed and ARGV[4] == "bump" and cur == void_epoch and (loc or "") == ARGV[12] then
    if ARGV[5] ~= "" then
        if redis.call("GET", KEYS[3]) == ARGV[5] then
            redis.call("DEL", KEYS[3])
        end
        return 7
    end
    if ARGV[14] == nil or ARGV[14] == "" then
        return redis.error_reply("ROUTE_ROLLBACK void shape")
    end
    redis.call("SET", KEYS[2], ARGV[14])
    restore_location()
    return 6
end
return 0
"""
)

# 原文 CAS + 可选释放标记原子复核删除 location（与 Go luaDeletePlayerLocationIfUnchanged 一致）
LUA_DELETE_PLAYER_LOCATION_IF_UNCHANGED = """
local cur = redis.call('GET', KEYS[1])
if cur == false then
    return 0
end
if (redis.call('GET',KEYS[5]) or '') ~= ARGV[4] then return -1 end
if ARGV[5] ~= '' and redis.call('GET',KEYS[6]) ~= ARGV[5] then return -1 end
if cur ~= ARGV[1] then
    return -1
end
if ARGV[2] == 'released' then
    local owner = redis.call('GET', KEYS[2])
    if owner ~= false and owner ~= ARGV[3] then
        return -2
    end
    local mark = redis.call('GET', KEYS[3])
    if mark == false then
        return -2
    end
    local e = string.match(mark, '^(%d+):%d+$')
    if e == nil then
        return -2
    end
    e = (string.gsub(e, '^0+(%d)', '%1'))
    if e ~= ARGV[3] then
        return -2
    end
elseif ARGV[2] ~= 'any' then
    return redis.error_reply('LEAVE_SCENE unknown delete mode')
end
redis.call('DEL', KEYS[1], KEYS[4], KEYS[5])
return 1
"""

# 离场等待持有者释放计时（使用 Redis 服务端 TIME）
LUA_NOTE_LEAVE_RELEASE_WAIT = """
local t = redis.call('TIME')
local nowMs = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local v = redis.call('GET', KEYS[1])
if v then
    local scope, first = string.match(v, '^(.*):(%d+)$')
    if scope ~= nil and scope == ARGV[1] then
        local waited = nowMs - tonumber(first)
        if waited < 0 then
            waited = 0
        end
        return waited
    end
end
redis.call('SET', KEYS[1], ARGV[1] .. ':' .. string.format('%.0f', nowMs), 'EX', ARGV[2])
return 0
"""

# 按值条件删除 EnterScene 去重占位
LUA_DELETE_ENTER_SCENE_DEDUPE_IF_VALUE = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
"""

# 将 EnterScene 去重占位 CAS 升级为完整成功响应缓存（并核对修订下界）
LUA_COMPLETE_ENTER_SCENE_DEDUPE = """
if KEYS[2] ~= nil and redis.call('GET', KEYS[2]) ~= ARGV[4] then return 0 end
if redis.call("GET", KEYS[1]) == ARGV[1] then
    redis.call("SET", KEYS[1], ARGV[2], "EX", ARGV[3])
    return 1
end
return 0
"""

# 世界频道死节点改派 CAS（与 Go world_init.go 属主改派同域同槽 {ctl:<zone>}）
LUA_REASSIGN_SCENE_NODE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[1], ARGV[2])
redis.call('SREM', KEYS[2], ARGV[3])
local oldCount = tonumber(redis.call('GET', KEYS[3]) or '0') or 0
if oldCount > 0 then
    redis.call('DECR', KEYS[3])
end
redis.call('SADD', KEYS[4], ARGV[3])
redis.call('INCR', KEYS[5])
if KEYS[6] ~= nil and ARGV[4] ~= '' and ARGV[4] ~= '0' then
    redis.call('SET', KEYS[6], ARGV[4])
end
return 1
"""
