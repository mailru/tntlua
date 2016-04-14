function get_mxstatus(pdd_domain)
    local tup = box.select(0, 0, pdd_domain)
    if not tup then
        return {0, 0}
    end
    return {1, tup[1]}
end

function get_pdd(pdd_domain)
    local tup = box.select(0, 0, pdd_domain)
    if not tup then
        return {0, 0, ''}
    end
    return {1, tup[1], tup[2]}
end
