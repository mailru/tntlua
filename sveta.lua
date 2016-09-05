-- sveta.lua
--
--
-- space[4].enabled = 1
-- space[4].index[0].type = "HASH"
-- space[4].index[0].unique = 1
-- space[4].index[0].key_field[0].fieldno = 0
-- space[4].index[0].key_field[0].type = "NUM"
-- space[4].index[0].key_field[1].fieldno = 1
-- space[4].index[0].key_field[1].type = "STR"
-- space[4].index[1].type = "AVLTREE"
-- space[4].index[1].unique = 0
-- space[4].index[1].key_field[0].fieldno = 0
-- space[4].index[1].key_field[0].type = "NUM"
-- space[4].index[1].key_field[1].fieldno = 3
-- space[4].index[1].key_field[1].type = "NUM"
-- space[4].index[2].type = "AVLTREE"
-- space[4].index[2].unique = 0
-- space[4].index[2].key_field[0].fieldno = 0
-- space[4].index[2].key_field[0].type = "NUM"
-- space[4].index[2].key_field[1].fieldno = 4
-- space[4].index[2].key_field[1].type = "NUM"
-- space[4].index[2].key_field[2].fieldno = 2
-- space[4].index[2].key_field[2].type = "NUM"
--
-- tuple structure:
-- uid, query, total_count, last_ts, 2_week_count, latest_counter(today), ... , earliest_counter(2 weeks ago)

local space_no = 4
local delete_chunk = 100
local max_query_size = 1024
local seconds_per_day = 86400
local two_weeks = 14
local index_of_user_id = 0
local index_of_query  = 1
local index_of_total_counter = 2
local index_of_last_ts = 3
local index_of_2_week_counter = 4
local index_of_latest_counter = 5
local index_of_earliest_counter = 18

local timeout = 0.005
local max_attempts = 5

function add_query(user_id, query)
    if string.len(query) > max_query_size then
        error("too long query")
    end
    uid = box.unpack('i', user_id)

    local tuple = box.update(space_no, {uid, query}, "+p=p+p+p", index_of_total_counter, 1, index_of_last_ts, box.time(), index_of_2_week_counter, 1, index_of_latest_counter, 1)

    if tuple == nil then
        local new_tuple = {}
        new_tuple[index_of_user_id] = uid
        new_tuple[index_of_query] = query
        new_tuple[index_of_total_counter] = 1
        new_tuple[index_of_last_ts] = box.time()
        new_tuple[index_of_2_week_counter] = 1
        new_tuple[index_of_latest_counter] = 1
        for i=index_of_latest_counter+1,index_of_earliest_counter do
            new_tuple[i] = 0
        end
        return box.insert(space_no, new_tuple)
    end

    return tuple
end

function delete_query(user_id, query)
    return box.delete(space_no, box.unpack('i', user_id), query)
end

function delete_all(user_id)
    uid = box.unpack('i', user_id)
    while true do
        -- delete by chuncks of dalete_chunk len
        local tuples = {box.select_limit(space_no, 1, 0, delete_chunk, uid)}
        if( table.getn(tuples) == 0 ) then
            break
        end
        for _, tuple in ipairs(tuples) do
            box.delete(space_no, uid, tuple[1])
        end
    end
end

local function select_queries_by_index(uid, limit, index)
    resp = {}
    local i = 0
    for tuple in box.space[space_no].index[index]:iterator(box.index.REQ, uid) do
        if i == lim then break end
        table.insert(resp, {tuple[index_of_user_id], tuple[index_of_query], tuple[index_of_total_counter], tuple[index_of_last_ts], tuple[index_of_2_week_counter]})
        i = i + 1
    end
    return unpack(resp)
end

function select_recent(user_id, limit)
    uid = box.unpack('i', user_id)
    lim = box.unpack('i', limit)
    return select_queries_by_index(uid, limit, 1)
end

function select_2_week_popular(user_id, limit)
    uid = box.unpack('i', user_id)
    lim = box.unpack('i', limit)
    return select_queries_by_index(uid, limit, 2)
end

local function move_counters(tuple)
    new_tuple = {}
    new_tuple[0] = box.unpack('i',tuple[0])
    new_tuple[1] = tuple[1]
    for i = 2,index_of_earliest_counter do
        new_tuple[i] = box.unpack('i',tuple[i])
    end
    new_tuple[index_of_2_week_counter] = new_tuple[index_of_2_week_counter] - new_tuple[index_of_earliest_counter]
    for i=index_of_earliest_counter,index_of_latest_counter+1,-1 do
        new_tuple[i] = new_tuple[i - 1]
    end
    new_tuple[index_of_latest_counter] = 0
    return box.replace(space_no, new_tuple)
end

local function move_all_counters()
    local n = 0
    print('start moving counters')
    local start_time = box.time()
    for t in box.space[space_no].index[0]:iterator(box.index.ALL) do
        local count = 0
        while true do
            local status, result = pcall(move_counters, t)
            if status then
                --success
                break
            else
            --exception
                count = count + 1
                if count == max_attempts then
                    print('max attempts reached for moving counters for user ', t[0], ' query ', t[1])
                    break
                end
                box.fiber.sleep(timeout)
            end
        end

        n = n + 1
        if n == 100 then
            box.fiber.sleep(timeout)
            n = 0
        end
    end
    print('finish moving counters. elapsed ', box.time() - start_time, ' seconds')
end

local function move_all_counters_fiber()
    while true do
        local time = box.time()
        local sleep_time = math.ceil(time/seconds_per_day)*seconds_per_day - time + 1
        print('sleep for ', sleep_time, ' seconds')
        box.fiber.sleep(sleep_time)
        move_all_counters()
    end
end

box.fiber.wrap(move_all_counters_fiber)
