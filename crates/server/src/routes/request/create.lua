-- KEYS: request, status, response, flow, response token, HEAD protection.
-- ARGV: payload, initial status, data TTL, token enabled, token hash,
--       token TTL, HEAD protection enabled, HEAD protection TTL.
if redis.call("EXISTS", unpack(KEYS)) > 0 then
    return 0
end

if ARGV[4] == "1" then
    redis.call("SET", KEYS[5], ARGV[5], "EX", ARGV[6])
end
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[3])
if ARGV[7] == "1" then
    redis.call("SET", KEYS[6], "false", "EX", ARGV[8])
end
redis.call("SET", KEYS[2], ARGV[2], "EX", ARGV[3])
return 1
