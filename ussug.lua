function ussug_get(...)
    local data = {...}
    local ret = {}
    for _, v in pairs(data) do
        local selected = { box.select(0, 0, v) }
        for _, tuple in pairs(selected) do
            table.insert(ret, tuple)
        end
    end
    return ret
end

function ussug_insert(id, str)
    id = box.unpack("l", id)
    box.replace(0, id, str)
end
