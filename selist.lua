local DISABLE_SEARCH_BY_DOMAIN = false

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

local function find_third_level_domain(email_domain_part)
    local domain_parts = mysplit(email_domain_part, '.')
    if #domain_parts <= 2 then
        return nil
    end
    return domain_parts[#domain_parts - 2] .. '.' .. domain_parts[#domain_parts - 1] .. '.' .. domain_parts[#domain_parts]
end

local function find_second_level_domain(email_domain_part)
    local domain_parts = mysplit(email_domain_part, '.')
    if #domain_parts <= 1 then
        return nil
    end
    return domain_parts[#domain_parts - 1] .. '.' .. domain_parts[#domain_parts]
end

function string.ends(str, end_of_str)
   return end_of_str=='' or string.sub(str, -string.len(end_of_str)) == end_of_str
end

local function need_third_level(domain)
    local third_level_domains = {box.select(2, 0)}
    for _, i in pairs(third_level_domains) do
        if string.ends(domain, "." .. i[0]) then
            return true
        end
    end
    return false
end

function selist2_add_sender(mask, name_ru, name_en, cat)
    local splited = mysplit(mask, '@')
    local email_domain_part = splited[#splited]

    local domain_to_store = ""
    if need_third_level(email_domain_part) then
        domain_to_store = find_third_level_domain(email_domain_part)
    else
        domain_to_store = find_second_level_domain(email_domain_part)
    end
    if not domain_to_store then
        error('Cannot find needed domain level')
    end
    return box.auto_increment_uniq(0, mask, name_ru, name_en, box.unpack('i', cat), domain_to_store):transform(5,1)
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

function selist2_search_by_domain(...)
    if DISABLE_SEARCH_BY_DOMAIN then
        return unpack({})
    end

    return _selist2_search_by_domain(...)
end

function selist2_search_by_domain_unswitchable(...)
    return _selist2_search_by_domain(...)
end

function _selist2_search_by_domain(domain)
    local domain_to_find = find_third_level_domain(domain)

    local ret = { }
    local ret_size = 0
    local orig = {}

    if domain_to_find then
        orig = {box.select(0, 2, domain_to_find)}
        for _, tuple in pairs(orig) do
            table.insert(ret, tuple:transform(5, 1))
            ret_size = ret_size + 1
        end
        if ret_size > 0 then
            return unpack(ret)
        end
    end

    domain_to_find = find_second_level_domain(domain)
    if not domain_to_find  then
        error('error string sent as domain')
    end

    orig = {box.select(0, 2, domain_to_find)}
    for _, tuple in pairs(orig) do
        table.insert(ret, tuple:transform(5, 1))
    end
    return unpack(ret)
end

function selist2_get_exceptions()
    return box.select(1, 0)
end

function selist2_get_thirdlevel_domains()
    return box.select(2, 0)
end

-- maillistadmin only

function selist2_get_senders_count(...)
    return _selist2_get_senders_count(_selist2_parse_params_count(...))
end

function selist2_get_senders(...)
    return _selist2_get_senders(_selist2_parse_params(...))
end

function selist2_get_top_domains_count(...)
    return _selist2_get_top_domains_count(_selist2_parse_params_count(...))
end

function selist2_get_top_domains(...)
    return _selist2_get_top_domains(_selist2_parse_params(...))
end

function _selist2_parse_params_count(filter_cat)
    filter_cat = box.unpack('i', filter_cat)
    filter_cat = filter_cat ~= 0xFFFFFFFF and filter_cat or false

    return filter_cat
end

function _selist2_parse_params(offset, limit, filter_cat, sort_order, sort_reverse)
    offset = box.unpack('i', offset)
    limit = box.unpack('i', limit)

    filter_cat = box.unpack('i', filter_cat)
    filter_cat = filter_cat ~= 0xFFFFFFFF and filter_cat or false

    sort_reverse = box.unpack('i', sort_reverse) ~= 0

    return offset, limit, filter_cat, sort_order, sort_reverse
end

function _selist2_get_senders_count(filter_cat)
    local count = 0

    if filter_cat then
        count = box.space[0].index[3]:count(filter_cat)
    else
        _selist2_iterate_tuples(false, function(tuple)
            count = count + 1
        end)
    end

    return count
end

function _selist2_get_senders(offset, limit, filter_cat, sort_order, sort_reverse)
    local ret_list = {}

    local sort_index = { name = 1, domain = 2, name_ru = 4, name_rus = 4, name_en = 5, name_eng = 5, cat = 3 }
    local index = sort_index[sort_order] or 0

    _selist2_iterate_tuples(filter_cat, index, function(tuple)
        table.insert(ret_list, tuple)
    end)

    return _selist2_unpack_result(ret_list, offset, limit, sort_reverse)
end

function _selist2_get_top_domains_count(filter_cat)
    local count = 0
    local uniq_domains = {}

    _selist2_iterate_tuples(filter_cat, function(tuple)
        if not uniq_domains[tuple[5]] then
            count = count + 1
            uniq_domains[tuple[5]] = 1
        end
    end)

    return count
end

function _selist2_get_top_domains(offset, limit, filter_cat, sort_order, sort_reverse)
    local uniq_domains = {}

    _selist2_iterate_tuples(filter_cat, function(tuple)
        uniq_domains[tuple[5]] = (uniq_domains[tuple[5]] or 0) + 1
    end)

    local ret_list = {}

    local k, v
    for k, v in pairs(uniq_domains) do
        table.insert(ret_list, box.tuple.new({ k, v }))
    end

    if sort_order == 'domain' then
        if sort_reverse then
            table.sort(ret_list, function(a, b) return a[0] > b[0] end)
        else
            table.sort(ret_list, function(a, b) return a[0] < b[0] end)
        end
    else
        if sort_reverse then
            table.sort(ret_list, function(a, b) return a[1] < b[1] end)
        else
            table.sort(ret_list, function(a, b) return a[1] > b[1] end)
        end
    end

    return _selist2_unpack_result(ret_list, offset, limit)
end

function _selist2_iterate_tuples(filter_cat, index, handler)
    if not handler then
        handler = index
        index = 0
    end

    local tuple
    for tuple in box.space[0].index[index]:iterator() do
        if not filter_cat or filter_cat == box.unpack('i', tuple[4]) then
            handler(tuple)
        end
    end
end

function _selist2_unpack_result(result, offset, limit, sort_reverse)
    if offset >= #result then
        return
    end

    local limit_from, limit_to = offset + 1, math.min(#result, offset + limit)

    if not sort_reverse then
        return unpack(result, limit_from, limit_to)
    end

    limit_from, limit_to = #result + 1 - limit_to, #result + 1 - limit_from

    local reverse = {}

    local v
    for _, v in ipairs({ unpack(result, limit_from, limit_to) }) do
        table.insert(reverse, 0, v)
    end

    return unpack(reverse)
end
