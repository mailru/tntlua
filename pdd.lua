function get_mxstatus(pdd_domain)
    local tup = box.select(0, 0, pdd_domain)
    if not tup then
        return {0, 0}
    end

    local flags = 0
    if #tup >= 2 then
        flags = tup[1]
    end

    return {1, flags}
end

function get_pdd(pdd_domain)
    local tup = box.select(0, 0, pdd_domain)
    if not tup then
        return {0, 0, ''}
    end

    local flags = 0
    if #tup >= 2 then
        flags = tup[1]
    end

    local defuser = ''
    if #tup >= 3 then
        defuser = tup[2]
    end

    return {1, flags, defuser}
end
