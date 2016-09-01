function ussug_get(...)
    local data = {...}
    local ret = {}
    for _, v in pairs(data) do
        local selected = { box.select_limit(0, 0, 0, 1, v) }
        for _, tuple in pairs(selected) do
            table.insert(ret, tuple)
        end
    end
    if #ret > 0 then
        return ret
    end
    return
end

function ussug_insert(id, str)
    local id = box.unpack("l", id)
    box.replace(0, id, str)
end
