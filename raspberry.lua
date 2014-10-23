local FLAG_REPLACE_STAT         = 1
local FLAG_REPLACE_SENDERS      = 2
local FLAG_REPLACE_ONE_SENDER   = 4

local function flag_is_set(set, flag)
    return set % (2 * flag) >= flag
end

function senders2_get_stat(uid)
    uid = box.unpack('i', uid)

    local tuple = box.select(0, 0, uid)

    if not tuple then
        return { 0, 0, 0, 0, 0 }
    end

    local stat = { box.unpack('wwwww', tuple[1]) }

    local senders = {}

    local format = ''
    for i = 1, box.unpack('w', tuple[2]) do
        format = format .. 'www'
    end

    local cur_senders = { box.unpack(format, tuple[3]) }
    for i = 1, #cur_senders, 3 do
        table.insert(senders, { cur_senders[i], cur_senders[i + 1], cur_senders[i + 2] })
    end

    return stat, unpack(senders)
end

function senders2_update_stat(uid, update_flags, total, unread, ignore, delete, read, ...)
    -- ... = sid_1, total_1, sid_2, total_2, sid_n, total_n

    uid, update_flags = box.unpack('i', uid), box.unpack('i', update_flags)

    local tuple = box.select(0, 0, uid)

    -- update stat

    local stat = { box.unpack('i', total), box.unpack('i', unread), box.unpack('i', ignore), box.unpack('i', delete), box.unpack('i', read) }

    if tuple and not flag_is_set(update_flags, FLAG_REPLACE_STAT) then
        local cur_stat = { box.unpack('wwwww', tuple[1]) }

        for i in pairs(cur_stat) do
            stat[i] = stat[i] + cur_stat[i]
        end
    end

    local packed_stat = box.pack('wwwww', unpack(stat))

    -- update senders

    local senders_count, packed_senders = 0, ''
    local temp = {...}

    if #temp > 0 then
        local senders = {}

        for i = 1, #temp, 2 do
            local sid = box.unpack('i', temp[i])
            senders[sid] = { sid, box.unpack('i', temp[i + 1]), 0 }
        end

        if tuple then
            local format = ''
            for i = 1, box.unpack('w', tuple[2]) do
                format = format .. 'www'
            end

            local cur_senders = { box.unpack(format, tuple[3]) }
            for i = 1, #cur_senders, 3 do
                local sid, total, flags = cur_senders[i], cur_senders[i + 1], cur_senders[i + 2]

                if senders[sid] then
                    if not flag_is_set(update_flags, FLAG_REPLACE_SENDERS) and not flag_is_set(update_flags, FLAG_REPLACE_ONE_SENDER) then
                        senders[sid][2] = senders[sid][2] + total
                    end
                    senders[sid][3] = flags
                else
                    if not flag_is_set(update_flags, FLAG_REPLACE_SENDERS) then
                        senders[sid] = { sid, total, flags }

                    elseif flags ~= 0 then
                        senders[sid] = { sid, 0, flags }
                    end
                end
            end
        end

        for k, v in pairs(senders) do
            senders_count = senders_count + 1
            packed_senders = packed_senders .. box.pack('www', unpack(v))
        end
    else
        if tuple and not flag_is_set(update_flags, FLAG_REPLACE_SENDERS) then
            senders_count = box.unpack('w', tuple[2])
            packed_senders = tuple[3]
        end
    end

    box.replace(0, uid, packed_stat, box.pack('w', senders_count), packed_senders)
end

function senders2_update_flags(uid, ...)
    -- ... = sid_1, flags_1, sid_2, flags_2, sid_n, flags_n

    uid = box.unpack('i', uid)

    local tuple = box.select(0, 0, uid)

    -- update stat

    local packed_stat

    if tuple then
        packed_stat = tuple[1]
    else
        packed_stat = box.pack('wwwww', 0, 0, 0, 0, 0)
    end

    -- update senders

    local senders = {}

    if tuple then
        local format = ''
        for i = 1, box.unpack('w', tuple[2]) do
            format = format .. 'www'
        end

        local cur_senders = { box.unpack(format, tuple[3]) }
        for i = 1, #cur_senders, 3 do
            local sid = cur_senders[i]
            senders[sid] = { sid, cur_senders[i + 1], cur_senders[i + 2] }
        end
    end

    local temp = {...}
    for i = 1, #temp, 2 do
        local sid, flags = box.unpack('i', temp[i]), box.unpack('i', temp[i + 1])
        if senders[sid] then
            senders[sid][3] = flags
        else
            senders[sid] = { sid, 0, flags }
        end
    end

    local senders_count, packed_senders = 0, ''

    for k, v in pairs(senders) do
        senders_count = senders_count + 1
        packed_senders = packed_senders .. box.pack('www', unpack(v))
    end

    box.replace(0, uid, packed_stat, box.pack('w', senders_count), packed_senders)
end
