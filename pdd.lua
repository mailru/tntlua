function get_mxstatus(pdd_domain)
    local tup = box.select(0, 0, pdd_domain)
    if not tup then
        return {0, 0}
    end

    if #tup < 3 then
        error("Not enough elements of tuple")
    end

    local flags = tup[1]
    return {1, flags}
end

function get_pdd(pdd_domain)
    local tup = box.select(0, 0, pdd_domain)
    if not tup then
        return {0, 0, ''}
    end

    if #tup < 3 then
        error("Not enough elements of tuple")
    end

    local flags = tup[1]
    local defuser = tup[2]

    return {1, flags, defuser}
end
