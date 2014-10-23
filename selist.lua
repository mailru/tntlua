function box.auto_increment_uniq(spaceno, uniq, ...)
    local tuple = box.select(spaceno, 1, uniq)

    if tuple then
        local format = 'i'

        if #tuple[0] == 8 then
            format = 'l'
        end

        return box.replace(spaceno, box.unpack(format, tuple[0]), uniq, ...)
    end

    return box.auto_increment(spaceno, uniq, ...)
end

function selist2_add_sender(mask, name_ru, name_en, cat)
    return box.auto_increment_uniq(0, mask, name_ru, name_en, box.unpack('i', cat))
end

function get_sender_list_by_offset(offset, limit)
    offset = box.unpack('i', offset)
    limit = box.unpack('i', limit)

    return { box.select_limit(0, 0, offset, limit) }
end

function get_sender_list_by_ids(...)
    local ret = { }
    for i, v in ipairs({...}) do
        local tuple = box.select(0, 0, box.unpack('i', v))
        table.insert(ret, tuple)
    end
    return unpack(ret)
end
