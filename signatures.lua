-- signatures.lua
--
--
-- space[3].enabled = 1
-- space[3].index[0].type = "TREE"
-- space[3].index[0].unique = 1
-- space[3].index[0].key_field[0].fieldno = 0
-- space[3].index[0].key_field[0].type = "NUM"
-- space[3].index[0].key_field[1].fieldno = 1
-- space[3].index[0].key_field[1].type = "NUM"


local space_no = 3
local params_n = 3

local function store_signatures(user_id, signatures)
    for sig_idx, sig in pairs(signatures) do
        box.insert(space_no,
                   user_id,
                   sig_idx,
                   sig.plain,
                   sig.html,
                   sig.is_default)
    end
end

local function delete_signatures(user_id, max_idx)
    for i = 0, max_idx do
        box.delete(space_no, user_id, i)
    end
end

function select_signatures(user_id)
    return box.select(space_no, 0, user_id)
end

-- delete all old user signatures and store new
function replace_signatures(user_id, ...)
    if user_id == nil then
        error('user_id is required')
    end
    local sig_list = {...}
    local sig_list_len = table.getn(sig_list)
    if sig_list_len % params_n ~= 0 then
        error('illegal parameters: var arguments list should contain triples plain html is_default')
    end

    local signatures = {}
    local got_default = false

    for i = 0, sig_list_len / params_n - 1 do
        signatures[i] = {
            plain = sig_list[params_n * i + 1],
            html = sig_list[params_n * i + 2],
            is_default = box.unpack('i', sig_list[params_n * i + 3]),
        }
        local sig = signatures[i]
        if sig.is_default == 1 then
            if got_default then
                error('illegal parameters: only one default signature expected')
            else
                got_default = true
            end
        elseif sig.plain == '' and sig.html == '' then
            signatures[i] = nil
        end
    end
    if got_default == false and sig_list_len ~= 0 then
        error('illegal parameters: need default signature')
    end

    -- we need to know max idx of signature to delete all
    local max_tuple = box.select_reverse_range(space_no, 0, 1, user_id)
    if max_tuple ~= nil then
        delete_signatures(user_id, box.unpack('i', max_tuple[1]))
    end

    store_signatures(user_id, signatures)
end
