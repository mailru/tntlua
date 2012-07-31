-- cnt.lua

--
-- Simple counter.
--

--
-- Increment counter identified by primary key.
-- Create counter if not exists.
-- Returns updated value of the counter.
--
function cnt_inc(space, ...)
    local key = {...}
    local cnt_index = #key

    local tuple
    while true do
        tuple = box.update(space, key, '+p', cnt_index, 1)
        if tuple ~= nil then break end
        local data = {...}
        table.insert(data, 1)
        tuple = box.insert(space, unpack(data))
        if tuple ~= nil then break end
    end

    return tuple[cnt_index]
end

--
-- Decrement counter identified by primary key.
-- Delete counter if it decreased to zero.
-- Returns updated value of the counter.
--
function cnt_dec(space, ...)
    local key = {...}
    local cnt_index = #key

    local tuple = box.select(space, 0, ...)
    if tuple == nil then return 0 end
    if tuple[cnt_index] == 1 then
        box.delete(space, ...)
        return 0
    else
        tuple = box.update(space, key, '-p', cnt_index, 1)
        return tuple[cnt_index]
    end
end
