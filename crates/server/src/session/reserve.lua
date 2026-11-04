-- KEYS: session reservation.
-- ARGV: session TTL seconds, IDKit flow ID. Payload storage happens separately.
assert(#KEYS == 1 and #ARGV == 2, 'invalid reservation arguments')

local session_key = KEYS[1]
local session_ttl_seconds = tonumber(ARGV[1])
local idkit_flow_id = ARGV[2]

assert(session_ttl_seconds and session_ttl_seconds > 0, 'invalid reservation TTL')

if redis.call('EXISTS', session_key) == 1 then
    return 0
end

redis.call('HSET', session_key, 'reserved', '1')
redis.call('HSET', session_key, 'idkit_flow_id', idkit_flow_id)
redis.call('EXPIRE', session_key, session_ttl_seconds)

return 1
