-- Race conditions in this files are possible. It is ok by biz logic.
function ussender_add(user_id, sender_id)
    local user_id = box.unpack("i", user_id)
    local sender_id = box.unpack("l", sender_id)

    local selected = { box.select_limit(0, 0, 0, 1, user_id) }
    if #selected == 0 then
        box.insert(0, user_id, sender_id)
    else
        local fun, param, state = selected[1]:pairs()
        state, _ = fun(param, state) -- skip the first element of tuple
        for state, v in fun, param, state do
            local cur_id = box.unpack("l", v)
            if cur_id == sender_id then
                return
            end
        end
        box.update(0, user_id, "!p", -1, sender_id)
    end

end

function ussender_select(user_id)
    local user_id = box.unpack("i", user_id)
    local ret = {box.select_limit(0, 0, 0, 1, user_id)}
    if #ret == 0 then
        return {user_id}
    end
    return ret
end

function ussender_delete(user_id)
    local user_id = box.unpack("i", user_id)
    box.delete(0, user_id)
end
