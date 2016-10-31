-- sveta.lua
-- implements aggregation of abstract data by update date and number of updates
-- grouping by id and data
-- allows desc sorting by date or by (2_week_count + total_count),
-- where 2_week_count is number of updates for last two weeks,
-- this updates automatically by fiber every 24 hours
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
-- space[4].index[3].type = "AVLTREE"
-- space[4].index[3].unique = 1
-- space[4].index[3].key_field[0].fieldno = 3
-- space[4].index[3].key_field[0].type = "NUM"
-- space[4].index[3].key_field[1].fieldno = 0
-- space[4].index[3].key_field[1].type = "NUM"
-- space[4].index[3].key_field[2].fieldno = 1
-- space[4].index[3].key_field[2].type = "STR"
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

local old_query_days = 60

local timeout = 0.005
local max_attempts = 5

local n_fibers = 10
local move_channel = box.ipc.channel(n_fibers)
local delete_channel = box.ipc.channel(n_fibers)

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

function add_old_query(user_id, query, timestamp)
    if string.len(query) > max_query_size then
        error("too long query")
    end
    local uid = box.unpack('i', user_id)
    local ts = box.unpack('i', timestamp)

    if ts > box.time() then
        error("unable to add query in future")
    end

    local days_ago = math.ceil(box.time()/seconds_per_day) - math.ceil(ts/seconds_per_day)

    local tuple = box.select(space_no, 0, uid, query)

    if tuple == nil then
        local new_tuple = {}
        new_tuple[index_of_user_id] = uid
        new_tuple[index_of_query] = query
        new_tuple[index_of_total_counter] = 1
        new_tuple[index_of_last_ts] = ts
        for i=index_of_latest_counter,index_of_earliest_counter do
            new_tuple[i] = 0
        end
        if( days_ago < two_weeks ) then
            new_tuple[index_of_2_week_counter] = 1
            new_tuple[index_of_latest_counter + days_ago] = 1
        end
        return box.insert(space_no, new_tuple)
    else
        new_ts = math.max(ts, box.unpack('i', tuple[index_of_last_ts]))
        if( days_ago < two_weeks ) then
            return box.update(space_no, {uid, query}, "+p=p+p+p", index_of_total_counter, 1,index_of_last_ts, new_ts,
                              index_of_2_week_counter, 1, index_of_latest_counter + days_ago, 1)
        else
            return box.update(space_no, {uid, query}, "+p=p", index_of_total_counter, 1, index_of_last_ts, new_ts)
        end
    end
end

function delete_query(user_id, query)
    return box.delete(space_no, box.unpack('i', user_id), query)
end

function delete_all(user_id)
    uid = box.unpack('i', user_id)
    while true do
        -- delete by chuncks of dalete_chunk len
        local tuples = {box.select_limit(space_no, 1, 0, delete_chunk, uid)}
        if( #tuples == 0 ) then
            break
        end
        for _, tuple in ipairs(tuples) do
            box.delete(space_no, uid, tuple[1])
        end
    end
end

local function select_queries_by_index(uid, limit, index)
    local resp = {}
    local i = 0
    for tuple in box.space[space_no].index[index]:iterator(box.index.REQ, uid) do
        if i == limit then break end
        table.insert(resp, {tuple[index_of_user_id], tuple[index_of_query], tuple[index_of_total_counter], tuple[index_of_last_ts], tuple[index_of_2_week_counter]})
        i = i + 1
    end
    return unpack(resp)
end

function select_recent(user_id, limit)
    local uid = box.unpack('i', user_id)
    local lim = box.unpack('i', limit)
    return select_queries_by_index(uid, lim, 1)
end

function select_2_week_popular(user_id, limit)
    local uid = box.unpack('i', user_id)
    local lim = box.unpack('i', limit)
    return select_queries_by_index(uid, lim, 2)
end

local function move_counters(tuple)
    local new_tuple = {}
    for i = index_of_user_id,index_of_2_week_counter do
        new_tuple[i] = tuple[i]
    end
    new_tuple[index_of_latest_counter] = box.pack('i', 0)
    for i=index_of_latest_counter+1,index_of_earliest_counter do
        new_tuple[i] = tuple[i - 1]
    end
    new_tuple[index_of_2_week_counter] = box.pack('i', box.unpack('i', new_tuple[index_of_2_week_counter]) - box.unpack('i', tuple[index_of_earliest_counter]))

    return box.replace(space_no, new_tuple)
end

local function move_counters_fiber()
    local tpl = move_channel:get()
    while true do
        local count = 0
        local status, result = pcall(move_counters, tpl)
        if status then
        --success
           tpl = move_channel:get()
        else
        --exception
            count = count + 1
            if count == max_attempts then
                print('max attempts reached for moving counters for user ', tpl[0], ' query ', tpl[1])
                tpl = move_channel:get()
            else
                box.fiber.sleep(timeout)
            end
        end
    end
end

local function delete_old_queries_fiber()
    local tpl = delete_channel:get()
    while true do
        local count = 0
        local status, result = pcall(box.delete, space_no, tpl[0], tpl[1])
        if status then
        --success
           tpl = delete_channel:get()
        else
        --exception
            count = count + 1
            if count == max_attempts then
                print('max attempts reached for deleting query for user ', tpl[0], ' query ', tpl[1])
                tpl = delete_channel:get()
            else
                box.fiber.sleep(timeout)
            end
        end
    end
end

local function move_all_counters()
    local n = 0
    print('start moving counters')
    local start_time = box.time()
    local tuples = {box.select_range(space_no, 3, n_fibers, box.time() - (two_weeks + 1) * seconds_per_day)}
    local last_tuple = nil
    while true do
        if #tuples == 0 then
            break
        end
        for _, t in pairs(tuples) do
            move_channel:put(t)
            n = n + 1
            if n % 1000 == 0 then
                box.fiber.sleep(0)
            end
            last_tuple = t
        end
        tuples = {box.select_range(space_no, 3, n_fibers, last_tuple[index_of_last_ts], last_tuple[index_of_user_id], last_tuple[index_of_query])}
        tuples[1] = nil
    end
    print('finish moving counters. elapsed ', box.time() - start_time, ' seconds moved ', n, ' tuples')
end

local function delete_old_queries()
    local n = 0
    print('start delete old queries')
    local start_time = box.time()
    local tuples = {box.select_reverse_range(space_no, 3, n_fibers, box.time() - old_query_days * seconds_per_day)}
    local last_tuple = nil
    while true do
        if #tuples == 0 then
            break
        end
        for _, t in pairs(tuples) do
            delete_channel:put(t)
            n = n + 1
            if n % 1000 == 0 then
                box.fiber.sleep(0)
            end
            last_tuple = t
        end
        tuples = {box.select_reverse_range(space_no, 3, n_fibers, last_tuple[index_of_last_ts], last_tuple[index_of_user_id], last_tuple[index_of_query])}
    end
    print('finish delete old queries. elapsed ', box.time() - start_time, ' seconds, ', n, ' delete requests')
end

local function move_and_delete_fiber()
    while true do
        local time = box.time()
        local sleep_time = math.ceil(time/seconds_per_day)*seconds_per_day - time + 1
        print('move_and_delete_fiber: sleep for ', sleep_time, ' seconds')
        box.fiber.sleep(sleep_time)
        move_all_counters()
        delete_old_queries()
    end
end

if sveta_started_fibers ~= nil then
    for _, fid in pairs(sveta_started_fibers) do
        box.fiber.kill(fid)
    end
end

sveta_started_fibers = {}
local fiber = box.fiber.wrap(move_and_delete_fiber)
table.insert(sveta_started_fibers, box.fiber.id(fiber))
for i = 1, n_fibers do
    fiber = box.fiber.wrap(move_counters_fiber)
    table.insert(sveta_started_fibers, box.fiber.id(fiber))
    fiber = box.fiber.wrap(delete_old_queries_fiber)
    table.insert(sveta_started_fibers, box.fiber.id(fiber))
end
