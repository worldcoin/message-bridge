-- KEYS: session, key to check for existence.
if redis.call('HGET', KEYS[1], 'allow_existence_check') == 'false' then
    return 0
end
return redis.call('EXISTS', KEYS[2])
