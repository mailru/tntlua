local BACKUP_V1_SPACENO         = 1             -- comment out this line to disable backup

local FLAG_REPLACE_STAT         = 1
local FLAG_REPLACE_SENDERS      = 2
local FLAG_REPLACE_ONE_SENDER   = 4

local TUPLE_USER_STAT           = 1
local TUPLE_SENDERS_COUNT       = 2
local TUPLE_SENDSRS_STAT        = 3
local TUPLE_VERSION             = 4

local function flag_is_set(set, flag)
    return set % (2 * flag) >= flag
end

function senders2_get_stat(uid)
    uid = box.unpack('i', uid)

    local tuple = box.select(0, 0, uid)
    local version = tuple and #tuple > TUPLE_VERSION and box.unpack('w', tuple[TUPLE_VERSION]) or 1

    if not tuple then
        return { 0, 0, 0, 0, 0 }
    end

    local stat = { box.unpack('wwwww', tuple[TUPLE_USER_STAT]) }

    local senders = {}

    local format = ''
    for i = 1, box.unpack('w', tuple[TUPLE_SENDERS_COUNT]) do
        if version < 2 then
            format = format .. 'www'
        else
            format = format .. 'wwwww'
        end
    end

    local cur_senders = { box.unpack(format, tuple[TUPLE_SENDSRS_STAT]) }
    local step = version < 2 and 3 or 5

    for i = 1, #cur_senders, step do
        table.insert(senders, { cur_senders[i], cur_senders[i + 1], cur_senders[i + 2] })
    end

    return stat, unpack(senders)
end

function senders2_get_stat_2(uid)
    uid = box.unpack('i', uid)

    local tuple = box.select(0, 0, uid)
    local version = tuple and #tuple > TUPLE_VERSION and box.unpack('w', tuple[TUPLE_VERSION]) or 1

    if not tuple then
        return { 0, 0, 0, 0, 0 }
    end

    local stat = { box.unpack('wwwww', tuple[TUPLE_USER_STAT]) }

    local senders = {}

    local format = ''
    for i = 1, box.unpack('w', tuple[TUPLE_SENDERS_COUNT]) do
        if version < 2 then
            format = format .. 'www'
        else
            format = format .. 'wwwww'
        end
    end

    local cur_senders = { box.unpack(format, tuple[TUPLE_SENDSRS_STAT]) }
    local step = version < 2 and 3 or 5

    for i = 1, #cur_senders, step do
        if version < 2 then
            table.insert(senders, { cur_senders[i], cur_senders[i + 1], cur_senders[i + 2], 0, 0 })
        else
            table.insert(senders, { cur_senders[i], cur_senders[i + 1], cur_senders[i + 2], cur_senders[i + 3], cur_senders[i + 4] })
        end
    end

    return stat, unpack(senders)
end

function senders2_update_stat(uid, update_flags, total, unread, ignore, delete, read, ...)
    -- ... = sid_1, total_1, sid_2, total_2, sid_n, total_n

    uid, update_flags = box.unpack('i', uid), box.unpack('i', update_flags)

    local list = {...}

    local tuple = box.select(0, 0, uid)
    local version = tuple and #tuple > TUPLE_VERSION and box.unpack('w', tuple[TUPLE_VERSION]) or 1

    --if tuple and #list < 1 and not flag_is_set(update_flags, FLAG_REPLACE_SENDERS) then
    --    return -- don't update
    --end

    -- backup
    if tuple and version == 1 and BACKUP_V1_SPACENO then
        box.replace(BACKUP_V1_SPACENO, uid, tuple[1], tuple[2], tuple[3])
    end

    -- update stat

    local stat = { box.unpack('i', total), box.unpack('i', unread), box.unpack('i', ignore), box.unpack('i', delete), box.unpack('i', read) }

    if tuple and not flag_is_set(update_flags, FLAG_REPLACE_STAT) then
        local cur_stat = { box.unpack('wwwww', tuple[TUPLE_USER_STAT]) }

        for i in pairs(cur_stat) do
            stat[i] = stat[i] + cur_stat[i]
        end
    end

    local packed_stat = box.pack('wwwww', unpack(stat))

    -- update senders

    local senders_count, packed_senders = 0, ''

    if #list > 0 then
        local senders = {}

        for i = 1, #list, 2 do
            local sid = box.unpack('i', list[i])
            senders[sid] = { sid, box.unpack('i', list[i + 1]), 0, 0, 0 }
        end

        if tuple then
            local format = ''
            for i = 1, box.unpack('w', tuple[TUPLE_SENDERS_COUNT]) do
                if version < 2 then
                    format = format .. 'www'
                else
                    format = format .. 'wwwww'
                end
            end

            local cur_senders = { box.unpack(format, tuple[TUPLE_SENDSRS_STAT]) }
            local step = version < 2 and 3 or 5

            for i = 1, #cur_senders, step do
                local sid, total, flags, fld1, fld2 = cur_senders[i], cur_senders[i + 1], cur_senders[i + 2], 0, 0
                if version >= 2 then
                    fld1, fld2 = cur_senders[i + 3], cur_senders[i + 4]
                end

                if senders[sid] then
                    if not flag_is_set(update_flags, FLAG_REPLACE_SENDERS) and not flag_is_set(update_flags, FLAG_REPLACE_ONE_SENDER) then
                        senders[sid][2] = senders[sid][2] + total
                    end
                    senders[sid][3] = flags
                    senders[sid][4] = fld1
                    senders[sid][5] = fld2
                else
                    if not flag_is_set(update_flags, FLAG_REPLACE_SENDERS) then
                        senders[sid] = { sid, total, flags, fld1, fld2 }

                    elseif flags ~= 0 then
                        senders[sid] = { sid, 0, flags, fld1, fld2 }
                    end
                end
            end
        end

        for k, v in pairs(senders) do
            senders_count = senders_count + 1
            packed_senders = packed_senders .. box.pack('wwwww', unpack(v))
            version = 2
        end
    else
        if tuple and not flag_is_set(update_flags, FLAG_REPLACE_SENDERS) then
            senders_count = box.unpack('w', tuple[2])
            packed_senders = tuple[3]
        end
    end

    box.replace(0, uid, packed_stat, box.pack('w', senders_count), packed_senders, box.pack('w', version))
end

function senders2_update_flags(uid, ...)
    -- ... = sid_1, flags_1, sid_2, flags_2, sid_n, flags_n

    uid = box.unpack('i', uid)

    local list = {...}

    local tuple = box.select(0, 0, uid)
    local version = tuple and #tuple > TUPLE_VERSION and box.unpack('w', tuple[TUPLE_VERSION]) or 1

    if tuple and #list < 1 then
        return -- don't update
    end

    -- backup
    if tuple and version == 1 and BACKUP_V1_SPACENO then
        box.replace(BACKUP_V1_SPACENO, uid, tuple[1], tuple[2], tuple[3])
    end

    -- update stat

    local packed_stat

    if tuple then
        packed_stat = tuple[TUPLE_USER_STAT]
    else
        packed_stat = box.pack('wwwww', 0, 0, 0, 0, 0)
    end

    -- update senders

    local senders = {}

    if tuple then
        local format = ''
        for i = 1, box.unpack('w', tuple[TUPLE_SENDERS_COUNT]) do
            if version < 2 then
                format = format .. 'www'
            else
                format = format .. 'wwwww'
            end
        end

        local cur_senders = { box.unpack(format, tuple[TUPLE_SENDSRS_STAT]) }
        local step = version < 2 and 3 or 5

        for i = 1, #cur_senders, step do
            local sid = cur_senders[i]
            if version < 2 then
                senders[sid] = { sid, cur_senders[i + 1], cur_senders[i + 2], 0, 0 }
            else
                senders[sid] = { sid, cur_senders[i + 1], cur_senders[i + 2], cur_senders[i + 3], cur_senders[i + 4] }
            end
        end
    end

    for i = 1, #list, 2 do
        local sid, flags = box.unpack('i', list[i]), box.unpack('i', list[i + 1])
        if senders[sid] then
            senders[sid][3] = flags
        else
            senders[sid] = { sid, 0, flags, 0, 0 }
        end
    end

    local senders_count, packed_senders = 0, ''

    for k, v in pairs(senders) do
        senders_count = senders_count + 1
        packed_senders = packed_senders .. box.pack('wwwww', unpack(v))
        version = 2
    end

    box.replace(0, uid, packed_stat, box.pack('w', senders_count), packed_senders, box.pack('w', version))
end

function senders2_update_flags_2(uid, ...)
    -- ... = sid_1, flags_1, fld1_1, fld2_1, sid_2, flags_2, fld1_2, fld2_2, sid_n, flags_n, fld1_n, fld2_n

    uid = box.unpack('i', uid)

    local list = {...}

    local tuple = box.select(0, 0, uid)
    local version = tuple and #tuple > TUPLE_VERSION and box.unpack('w', tuple[TUPLE_VERSION]) or 1

    if tuple and #list < 1 then
        return -- don't update
    end

    -- backup
    if tuple and version == 1 and BACKUP_V1_SPACENO then
        box.replace(BACKUP_V1_SPACENO, uid, tuple[1], tuple[2], tuple[3])
    end

    -- update stat

    local packed_stat

    if tuple then
        packed_stat = tuple[TUPLE_USER_STAT]
    else
        packed_stat = box.pack('wwwww', 0, 0, 0, 0, 0)
    end

    -- update senders

    local senders = {}

    if tuple then
        local format = ''
        for i = 1, box.unpack('w', tuple[TUPLE_SENDERS_COUNT]) do
            if version < 2 then
                format = format .. 'www'
            else
                format = format .. 'wwwww'
            end
        end

        local cur_senders = { box.unpack(format, tuple[TUPLE_SENDSRS_STAT]) }
        local step = version < 2 and 3 or 5

        for i = 1, #cur_senders, step do
            local sid = cur_senders[i]
            if version < 2 then
                senders[sid] = { sid, cur_senders[i + 1], cur_senders[i + 2], 0, 0 }
            else
                senders[sid] = { sid, cur_senders[i + 1], cur_senders[i + 2], cur_senders[i + 3], cur_senders[i + 4] }
            end
        end
    end

    for i = 1, #list, 4 do
        local sid, flags, fld1, fld2 = box.unpack('i', list[i]), box.unpack('i', list[i + 1]), box.unpack('i', list[i + 2]), box.unpack('i', list[i + 3])
        if senders[sid] then
            senders[sid][3], senders[sid][4], senders[sid][5] = flags, fld1, fld2
        else
            senders[sid] = { sid, 0, flags, fld1, fld2 }
        end
    end

    local senders_count, packed_senders = 0, ''

    for k, v in pairs(senders) do
        senders_count = senders_count + 1
        packed_senders = packed_senders .. box.pack('wwwww', unpack(v))
        version = 2
    end

    box.replace(0, uid, packed_stat, box.pack('w', senders_count), packed_senders, box.pack('w', version))
end
