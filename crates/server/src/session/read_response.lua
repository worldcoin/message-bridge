-- Authorize before observing status or consuming a response.
local expected = redis.call('HGET', KEYS[1], 'response_read_token_digest')
if expected and expected ~= '' and expected ~= ARGV[1] then
    return {0, false, false}
end
local status = redis.call('GET', KEYS[2])
local response = redis.call('GETDEL', KEYS[3])
return {1, status, response}
