-- Signals
-- 
-- From time to time You need to synchronize Your application that
-- are connected to tarantool and run in different hosts.
-- 
-- You can use signals that are passed through tarantool
-- 
-- example:
-- 
-- the first host says:
--     
--     signals.send('my-signal', 'hello world')
-- 
-- the other host can catch the signal usng:
-- 
--     -- will sleep until signal will be received
--     local result = signals.wait('my-signal')
-- 
--     -- will sleep until signal will be received or timeout exceeded
--     local result = signals.wait('my-signal', 2)
-- 
-- Notes:
-- 
--     1. signals aren't safe: if producer generates too many signals and
--     there are no consumers to handle the signals some signals can be lost
--     2. signals is stored in memory (not in tarantool's spaces)
--     3. don't send 'nil' through signals: the value used as timeout indicator
--     4. if you send one signal and there are several consumers that are
--        waiting the signal, then only one of them will receive the signal


signals = {

    channel     = {},

    wait    = function(name, timeout)
        if timeout ~= nil then
            timeout = tonumber(timeout)
            if timeout < 0 then
                timeout = 0
            end
        end

        if signals.channel[ name ] == nil then
            signals.channel[ name ] = box.ipc.channel(1)
        end

        local res
        if timeout ~= nil then
            res = signals.channel[ name ]:get(timeout)
        else
            res = signals.channel[ name ]:get()
        end

        if res ~= nil then
            return res
        end
    end,

    send    = function(name, value)
        if signals.channel[ name ] == nil then
            signals.channel[ name ] = box.ipc.channel(1)
        end

        signals.channel[ name ]:put(value, 0)
    end
}

