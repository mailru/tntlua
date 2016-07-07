function ussender_add(user_id, sender_id)
    user_id = box.unpack("i", user_id)
    sender_id = box.unpack("l", sender_id)

    selected = { box.select(0, 0, user_id) }
    if #selected == 0 then
        box.insert(0, user_id, sender_id)
    else
        for i=1, #selected[1]-1 do
            cur_id = box.unpack("l", selected[1][i])
            if cur_id == sender_id then
                return
            end
        end
        box.update(0, user_id, "!p", -1, sender_id)
    end

end

function ussender_select(user_id)
    user_id = box.unpack("i", user_id)
    ret = {box.select(0, 0, user_id)}
    if #ret == 0 then
        return {user_id}
    end
    return ret
end

function ussender_delete(user_id)
    user_id = box.unpack("i", user_id)
    box.delete(0, user_id)
end
