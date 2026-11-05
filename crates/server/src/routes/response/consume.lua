-- KEYS: response token, status, response, flow, telemetry support, client name.
-- ARGV: observed token present, observed token hash, authenticated.
local token = redis.call("GET", KEYS[1])
if (ARGV[1] == "0" and token) or
   (ARGV[1] == "1" and token ~= ARGV[2]) then
    return {0, false} -- Credential changed; recheck it in Rust.
end
if token then
    if redis.call("EXISTS", KEYS[2], KEYS[3]) == 0 then
        return {404, false}
    end
    if #token ~= 32 then
        return {500, false}
    end
    if ARGV[3] ~= "1" then
        return {401, false}
    end
end

local status = redis.call("GET", KEYS[2])
local flow = redis.call("GET", KEYS[4])
local telemetry = redis.call("GET", KEYS[5])
local client = redis.call("GET", KEYS[6])
local response = redis.call("GETDEL", KEYS[3])
if response then
    redis.call("DEL", KEYS[2], KEYS[4], KEYS[5], KEYS[6])
    -- Keep the token as authorization and an ID reservation until it expires.
end
return {200, {status, response, flow, telemetry, client}}
