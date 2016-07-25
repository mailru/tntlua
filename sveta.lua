-- sveta.lua
--
--
-- space[4].enabled = 1
-- space[4].index[0].type = "TREE"
-- space[4].index[0].unique = 1
-- space[4].index[0].key_field[0].fieldno = 0
-- space[4].index[0].key_field[0].type = "NUM"
-- space[4].index[0].key_field[1].fieldno = 1
-- space[4].index[0].key_field[1].type = "STR"

local space_no = 4
local delete_chunk = 100

function add_query(user_id, query)
    if string.len(query) > 1024 then
        error("too long query")
    end
    local tuple = box.update(space_no, {user_id, query}, '+p=p', 2, 1, 3, box.time())

    if tuple == nil then
        return box.insert(space_no, {user_id, query, 1, box.time()})
    end

    return tuple
end

function delete_query(user_id, query)
    return box.delete(space_no, user_id, query)
end

function delete_all(user_id)
   while true do
       -- delete by chuncks of dalete_chunk len
       local tuples = {box.select_limit(space_no, 0, 0, delete_chunk, user_id)}
       if( table.getn(tuples) == 0 ) then
           break
       end
       for _, tuple in ipairs(tuples) do
           box.delete(space_no, user_id, tuple[1])
       end
   end
end

function select_queries(user_id)
    return box.select(space_no, 0, user_id)
end
