-- KEYS: session, request, status, response, legacy telemetry, legacy client, legacy flow.
-- ARGV: session TTL seconds, data TTL seconds, IDKit flow ID, opaque request payload, token digest if needed, existence policy.
assert(#KEYS == 7 and #ARGV == 6, 'invalid creation arguments')

local session_ttl = tonumber(ARGV[1])
local data_ttl = tonumber(ARGV[2])
assert(session_ttl and session_ttl > 0 and session_ttl == math.floor(session_ttl), 'invalid session TTL')
assert(data_ttl and data_ttl > 0 and data_ttl == math.floor(data_ttl), 'invalid data TTL')

if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end

local writes = {
    {'SETEX', KEYS[2], data_ttl, ARGV[4]},
    {'SETEX', KEYS[3], data_ttl, 'initialized'},
    {'HSET', KEYS[1], 'reserved', '1', 'idkit_flow_id', ARGV[3], 'response_read_token_digest', ARGV[5], 'allow_existence_check', ARGV[6]},
    {'EXPIRE', KEYS[1], session_ttl},
    {'DEL', KEYS[4], KEYS[5], KEYS[6], KEYS[7]},
}
local rollback = {'DEL', KEYS[1], KEYS[2], KEYS[3]}

-- Lua does not roll back errors. Check every command's permissions before writing.
assert(redis.acl_check_cmd(unpack(rollback)), 'creation cleanup not permitted')
for _, command in ipairs(writes) do
    assert(redis.acl_check_cmd(unpack(command)), 'creation command not permitted')
end

for _, command in ipairs(writes) do
    local result = redis.pcall(unpack(command))
    if type(result) == 'table' and result.err then
        redis.call(unpack(rollback))
        return redis.error_reply('request creation failed')
    end
end

return 1
