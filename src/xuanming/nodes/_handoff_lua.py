"""从 C++ handoff_sql_barrier.h 移植的 Redis 同槽原子协议。"""

_HELPERS = """
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
local function plus2(s)
    local out,carry='',2
    for i=#s,1,-1 do
        local value=string.byte(s,i)-48+carry
        out=string.char(48+value%10)..out
        carry=math.floor(value/10)
    end
    if carry>0 then out=string.char(48+carry)..out end
    if not uint(out) then return nil end
    return out
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
local function sameholder(a,b)
    return a.e==b.e and a.i==b.i and a.n==b.n and a.a==b.a
end
local function types3()
    if (#KEYS~=3 and #KEYS~=4) or KEYS[1]==KEYS[2] or KEYS[1]==KEYS[3] or KEYS[2]==KEYS[3] then return false end
    for i,key in ipairs(KEYS) do
        if key=='' then return false end
        for j=1,i-1 do if key==KEYS[j] then return false end end
    end
    local b=redis.call('TYPE',KEYS[2]).ok
    local m=redis.call('TYPE',KEYS[3]).ok
    return redis.call('TYPE',KEYS[1]).ok=='string' and (b=='none' or b=='hash')
        and (m=='none' or m=='string') and (b=='none' or redis.call('PTTL',KEYS[2])==-1)
end
local function prior()
    local raw=redis.call('HGET',KEYS[2],'handoff_fence')
    if raw==false then return '',nil end
    local parsed=fence(raw)
    if not parsed then return nil,nil end
    return raw,parsed
end
local function marker(s,e)
    local owner,time=string.match(s or '','^(%d+):(%d+)$')
    return owner==e and uint(time)
end
"""

REGISTER = _HELPERS + """
if #ARGV~=3 or not types3() then return redis.error_reply('HANDOFF register shape') end
local candidate=fence(ARGV[2])
local raw,old=prior()
if not uint(ARGV[1]) or not candidate or candidate.k~='H' or candidate.e~=ARGV[1] or raw==nil then
    return redis.error_reply('HANDOFF register record') end
if redis.call('GET',KEYS[1])~=ARGV[1] then return -1 end
if raw==ARGV[2] then return 1 end
if raw~=ARGV[3] then return -2 end
if old then
    local order=cmp(candidate.e,old.e)
    if order<0 or (order==0 and (old.n=='0' or candidate.i~=old.i or cmp(candidate.n,old.n)<=0)) then return -2 end
end
local mark=redis.call('GET',KEYS[3])
if mark then
    local e,t=string.match(mark,'^(%d+):(%d+)$')
    if not uint(e) or not uint(t) or cmp(e,candidate.e)>0 then return redis.error_reply('HANDOFF register marker') end
end
redis.call('HSET',KEYS[2],'handoff_fence',ARGV[2])
if mark then redis.call('DEL',KEYS[3]) end
return 1
"""

CANCEL_REGISTRATION = _HELPERS + """
if #ARGV~=4 or not types3() then return redis.error_reply('HANDOFF cancel registration shape') end
local candidate,next=fence(ARGV[2]),fence(ARGV[4])
local raw,old=prior()
if not uint(ARGV[1]) or not candidate or not next or raw==nil or candidate.e~=ARGV[1]
    or candidate.k~='H' or next.k~='R' or not sameholder(candidate,next) or candidate.t==next.t then
    return redis.error_reply('HANDOFF cancel registration record') end
local owner=redis.call('GET',KEYS[1])
if not uint(owner) or cmp(owner,candidate.e)<0 then return redis.error_reply('HANDOFF cancel registration owner') end
if raw==ARGV[4] then return 1 end
if raw~=ARGV[2] then
    if raw~=ARGV[3] then return 0 end
    if old then
        local order=cmp(candidate.e,old.e)
        if order<0 or (order==0 and (old.n=='0' or candidate.i~=old.i or cmp(candidate.n,old.n)<=0)) then return 0 end
    end
end
local mark=redis.call('GET',KEYS[3])
if mark then
    local e,t=string.match(mark,'^(%d+):(%d+)$')
    if not uint(e) or not uint(t) then return redis.error_reply('HANDOFF cancel registration marker') end
end
redis.call('HSET',KEYS[2],'handoff_fence',ARGV[4])
return 1
"""

CONFIRM = _HELPERS + """
if #ARGV~=2 or not types3() then return redis.error_reply('HANDOFF confirm shape') end
local expected=fence(ARGV[2])
local raw=prior()
if not expected or expected.k~='H' or expected.e~=ARGV[1] or raw==nil then return redis.error_reply('HANDOFF confirm record') end
if redis.call('GET',KEYS[1])~=ARGV[1] or raw~=ARGV[2] then return -1 end
return 0
"""

REVOKE = _HELPERS + """
if #ARGV~=4 or not types3() then return redis.error_reply('HANDOFF revoke shape') end
local held,published,next=fence(ARGV[1]),fence(ARGV[2]),fence(ARGV[3])
local raw=prior()
if not held or not published or not next or raw==nil or held.k~='H'
    or not sameholder(held,published) or not sameholder(held,next)
    or (published.k~='F' and ARGV[2]~=ARGV[1]) or (published.k=='F' and published.t==held.t)
    or (next.k~='H' and next.k~='R') or next.t==held.t or next.t==published.t then
    return redis.error_reply('HANDOFF revoke record') end
local owner=redis.call('GET',KEYS[1])
if not uint(owner) then return redis.error_reply('HANDOFF revoke owner') end
if next.k=='H' and owner~=held.e then return 0 end
if raw~=ARGV[1] and raw~=ARGV[2] and raw~=ARGV[3] then return 0 end
local mark=redis.call('GET',KEYS[3])
if mark then
    local e,t=string.match(mark,'^(%d+):(%d+)$')
    if not uint(e) or not uint(t) then return redis.error_reply('HANDOFF revoke marker') end
end
local stamp=string.match(mark or '', '^%d+:(%d+)$')
redis.call('HSET',KEYS[2],'handoff_fence',ARGV[3])
if mark and ARGV[4]~='' and stamp==ARGV[4] then redis.call('DEL',KEYS[3]) end
return 1
"""

RELEASE = _HELPERS + """
if #ARGV~=5 or not types3() then return redis.error_reply('HANDOFF release shape') end
local held,next=fence(ARGV[4]),fence(ARGV[5])
local raw=prior()
if not held or not next or raw==nil or held.k~='H' or next.k~='F' or not sameholder(held,next)
    or held.t==next.t or held.e~=ARGV[1] or not marker(ARGV[2],ARGV[1]) or ARGV[3]~='300' then
    return redis.error_reply('HANDOFF release record') end
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
if raw==ARGV[5] then return 1 end
if raw~=ARGV[4] then return -2 end
redis.call('HSET',KEYS[2],'handoff_fence',ARGV[5])
redis.call('SET',KEYS[3],ARGV[2],'EX',ARGV[3])
return 1
"""

CREDENTIAL = _HELPERS + """
if #ARGV~=5 or not types3() then return redis.error_reply('HANDOFF credential shape') end
local observed,next=fence(ARGV[4]),fence(ARGV[5])
local raw=prior()
if not observed or not next or raw==nil or next.k~='F' or not sameholder(observed,next)
    or observed.t==next.t or observed.e~=ARGV[1] or not marker(ARGV[2],ARGV[1]) or ARGV[3]~='300' then
    return redis.error_reply('HANDOFF credential record') end
if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
if raw==ARGV[5] then return 1 end
if raw~=ARGV[4] then return -2 end
redis.call('HSET',KEYS[2],'handoff_fence',ARGV[5])
redis.call('SET',KEYS[3],ARGV[2],'EX',ARGV[3])
return 1
"""

PUBLISH = _HELPERS + """
if #ARGV~=5 or not types3() then return redis.error_reply('HANDOFF_SQL shape') end
local held,next=fence(ARGV[4]),fence(ARGV[5])
local raw=prior()
if not held or not next or raw==nil or held.k~='H' or next.k~='F' or not sameholder(held,next)
    or held.t==next.t or held.e~=ARGV[1] or not marker(ARGV[2],ARGV[1]) or ARGV[3]~='300' then
    return redis.error_reply('HANDOFF_SQL record') end
if redis.call('GET',KEYS[1])~=ARGV[1] then return -1 end
if raw==ARGV[5] then return 1 end
if raw~=ARGV[4] then return -2 end
redis.call('HSET',KEYS[2],'handoff_fence',ARGV[5])
redis.call('SET',KEYS[3],ARGV[2],'EX',ARGV[3])
return 1
"""

JUDGE = _HELPERS + """
if #KEYS~=4 or #ARGV~=4 then return redis.error_reply('HANDOFF outcome shape') end
-- 将前三键映射为 owner/barrier/mark，第四键是只读 location。
local location=KEYS[4]
if redis.call('TYPE',location).ok~='none' and redis.call('TYPE',location).ok~='string' then
    return redis.error_reply('HANDOFF location type') end
if not types3() then return redis.error_reply('HANDOFF outcome keys') end
local held,published,next=fence(ARGV[1]),fence(ARGV[2]),fence(ARGV[3])
local raw=prior()
if not held or not published or not next or raw==nil or not sameholder(held,published)
    or not sameholder(held,next) or held.k~='H' or next.k~='R'
    or (published.k~='F' and ARGV[2]~=ARGV[1]) or (published.k=='F' and published.t==held.t)
    or next.t==held.t or next.t==published.t then return redis.error_reply('HANDOFF outcome record') end
if raw~=ARGV[1] and raw~=ARGV[2] and raw~=ARGV[3] then return redis.error_reply('HANDOFF outcome superseded') end
local owner=redis.call('GET',KEYS[1])
if not uint(owner) then return redis.error_reply('HANDOFF outcome owner') end
local mark=redis.call('GET',KEYS[3])
if mark then
    local e,t=string.match(mark,'^(%d+):(%d+)$')
    if not uint(e) or not uint(t) then return redis.error_reply('HANDOFF outcome marker') end
end
local stamp=string.match(mark or '', '^%d+:(%d+)$')
redis.call('HSET',KEYS[2],'handoff_fence',ARGV[3])
if stamp==ARGV[4] then redis.call('DEL',KEYS[3]) end
return {owner,redis.call('GET',location)}
"""

RESUME = _HELPERS + """
if #KEYS~=4 or #ARGV~=4 then return redis.error_reply('HANDOFF resume shape') end
local location=KEYS[4]
local t=redis.call('TYPE',location).ok
if t~='none' and t~='string' then return redis.error_reply('HANDOFF resume location') end
if not types3() then return redis.error_reply('HANDOFF resume keys') end
local old,next=fence(ARGV[1]),fence(ARGV[2])
local raw=prior()
if not old or not next or raw==nil or old.k~='R' or next.k~='H'
    or old.i~=next.i or old.n~=next.n or old.a~=next.a or old.t==next.t
    or next.e~=ARGV[3] or (next.e~=old.e and (plus2(old.e)==nil or cmp(next.e,plus2(old.e))<0)) then return redis.error_reply('HANDOFF resume record') end
if redis.call('GET',KEYS[1])~=ARGV[3] or (redis.call('GET',location) or '')~=ARGV[4] then return 0 end
if redis.call('EXISTS',KEYS[3])~=0 then return 0 end
if raw==ARGV[2] then return 1 end
if raw~=ARGV[1] then return 0 end
redis.call('HSET',KEYS[2],'handoff_fence',ARGV[2])
return 1
"""
