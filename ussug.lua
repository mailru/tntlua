function add_sender_for_user(user_id, ...)
    user_id = box.unpack("i", user_id)
    local data = {...}
    for _, v in pairs(data) do
        status, res = pcall(box.auto_increment, 0, v)
        if not status then
            res = box.select(0, 1, v)
        end

        res = { res }

        pkey = box.unpack('l',res[1][0])

        box.counter.inc(1, user_id, pkey)
    end
end
