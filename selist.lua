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

local function mysplit(inputstr, sep)
        if sep == nil then
                sep = "%s"
        end
        local t={} ; i=1
        for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
                t[i] = str
                i = i + 1
        end
        return t
end

local function find_second_level_domain(mask)
    local splited = mysplit(mask, '@')
    local email_domain_part = splited[#splited]
    local domain_parts = mysplit(email_domain_part, '.')
    if #domain_parts == 0 then
        error('In mask ' .. mask .. ' no domain found')
    end
    if #domain_parts == 1 then
        error('In mask ' .. mask .. ' only first level domain found')
    end
    return domain_parts[#domain_parts - 1] .. '.' .. domain_parts[#domain_parts]
end

function selist2_add_sender(mask, name_ru, name_en, cat)
    return box.auto_increment_uniq(0, mask, name_ru, name_en, box.unpack('i', cat), find_second_level_domain(mask)):transform(5,1)
end

function get_sender_list_by_offset(offset, limit)
    offset = box.unpack('i', offset)
    limit = box.unpack('i', limit)

    local ret = {}
    local orig = {box.select_limit(0, 0, offset, limit)}
    for _, tuple in pairs(orig) do
        table.insert(ret, tuple:transform(5, 1))
    end

    return unpack(ret)
end

function get_sender_list_by_ids(...)
    local ret = { }
    for i, v in ipairs({...}) do
        local tuples = {box.select(0, 0, box.unpack('i', v))}
        for _, tuple in pairs(tuples) do
            table.insert(ret, tuple:transform(5, 1))
        end
    end
    return unpack(ret)
end

function selist2_search_by_mask(mask)
    local ret = { }
    local orig = {box.select(0, 1, mask)}
    for _, tuple in pairs(orig) do
        table.insert(ret, tuple:transform(5, 1))
    end
    return unpack(ret)
end

function selist2_search_by_domain(domain)
    local ret = { }
    local orig = {box.select(0, 2, domain)}
    for _, tuple in pairs(orig) do
        table.insert(ret, tuple:transform(5, 1))
    end
    return unpack(ret)
end

function selist2_get_exceptions()
    return box.select(1, 0)
end
