--
-- rima.lua
--

--
-- Task manager for imap collector.
-- Task's key is a user email address.
-- Rima can manage some tasks with the same key.
-- Tasks with identical keys will be groupped and managed as one bunch of tasks.
--
-- Producers can adds tasks by rima_put() calls.
-- Consumer request a bunch of tasks (with common key) by calling rima_get().
-- When Rima gives a task to worker it locks the key until worker calls rima_done(key).
-- Rima does not return task with already locked keys.
--

--
-- Space 0: Task Queue (task's data)
--   Tuple: { task_id (NUM64), key (STR), task_description (NUM), add_time (NUM) }
--   Index 0: TREE { task_id }
--   Index 1: TREE { key, task_id }
--
-- Space 2: Task Queue (with priority, locks e.t.c.)
--   Tuple: { key (STR), priority (NUM), is_locked (NUM), lock_time (NUM), lock_source (STR), serial_num (NUM) }
--   Index 0: TREE { key }
--   Index 1: TREE { priority, is_locked, lock_time }
--   Index 2: TREE { priority, is_locked, serial_num }
--   Index 3: TREE { serial_num }
--
-- Space 3: Mail Fetcher Queue (Special queue for fast single message loading)
--   Tuple: { task_id (NUM64), key (STR), task_description (NUM), add_time (NUM) }
--   Index 0: TREE { task_id }
--   Index 1: TREE { key }
--

local EXPIRATION_TIME = 30 * 60 -- seconds
local TASKS_BATCH = 1000
local FAST_TASKS_BATCH = 1000

local function next_queue_id()
    local next_id = 1
    local max_id = box.space[2].index[3]:max()

    if max_id ~= nil then
        next_id = box.unpack('i', max_id[5]) + 1
    end

    return next_id
end

--
-- Insert task data into the queue
--
local function insert_task_data(key, data, new_prio, ts)

    local first_task = box.select_limit(0, 1, 0, 1, key)
    if first_task == nil then
        box.auto_increment(0, key, data, ts)
    else
        if new_prio == 0 and data == first_task[2] then
            -- optimisation: no need another same task
        else
            box.auto_increment(0, key, data, ts)
        end
    end
end

--
-- Put task to the queue.
--
local function rima_put_impl(key, data, prio, ts)
    -- first: insert task data
    insert_task_data(key, data, prio, ts)

    -- second: insert or update key into queue
    local pr = box.select_limit(2, 0, 0, 1, key)
    if pr == nil then
        box.insert(2, key, prio, 0, box.time(), '', next_queue_id())
    elseif box.unpack('i', pr[1]) < prio then
        box.update(2, key, "=p", 1, prio)
    else
    end

    return 1
end

function rima_put(key, data) -- deprecated
    rima_put_impl(key, data, 512, box.time())
end

function rima_put_with_prio(key, data, prio)
    prio = box.unpack('i', prio)

    rima_put_impl(key, data, prio, box.time())
end

function rima_put_with_prio_and_ts(key, data, prio, ts)
    prio = box.unpack('i', prio)
    ts = box.unpack('i', ts)

    rima_put_impl(key, data, prio, ts)
end

function rima_put_sync(key, data, prio)
    prio = box.unpack('i', prio)

    return rima_put_impl(key, data, prio, box.time())
end

--
-- Put fetch single mail task to the queue.
--
function rima_put_fetchmail(key, data)
    box.auto_increment(3, key, data, box.time())
end

local function get_prio_key_with_lock(prio, source)
    local v = box.select_limit(2, 2, 0, 1, prio, 0)
    if v == nil then return nil end

    if source == nil then source = "" end

    -- lock the key
    local key = v[0]
    box.update(2, key, "=p=p=p", 2, 1, 3, box.time(), 4, source)

    return key
end

local function get_key_data(key)
    local result = { key }

    local tuples = { box.select_limit(0, 1, 0, TASKS_BATCH, key) }
    for _, tuple in pairs(tuples) do
        tuple = box.delete(0, box.unpack('l', tuple[0]))
        if tuple ~= nil then
            table.insert(result, { box.unpack('i', tuple[3]), tuple[2] } )
        end
    end

    return result
end

--
-- Request tasks from the queue.
--
function rima_get_ex(prio, source)
    prio = box.unpack('i', prio)

    local key = get_prio_key_with_lock(prio, source)
    if key == nil then return end

    local tasks = get_key_data(key)
    if table.getn(tasks) == 1 then
        -- if only email in table, it means there are no tasks
        rima_done(key)
        return
    end

    return unpack(tasks)
end

--
-- Request fetch single mail tasks from the queue.
--
function rima_get_fetchmail()
    local tuple = box.select_range(3, 0, 1)
    if tuple == nil then return end

    local key = tuple[1]

    local result = {}
    local n = 0

    local tuples = { box.select_limit(3, 1, 0, FAST_TASKS_BATCH, key) }
    for _, tuple in pairs(tuples) do
        tuple = box.delete(3, box.unpack('l', tuple[0]))
        if tuple ~= nil then
            table.insert(result, { box.unpack('i', tuple[3]), tuple[2] })
            n = 1
        end
    end

    if n == 0 then return end
    return key, unpack(result)
end

--
-- Request tasks from the queue for concrete user.
--
function rima_get_user_tasks(key, source)
    local lock_acquired = rima_lock(key, source)
    if lock_acquired == 0 then
        local pr = box.select_limit(2, 0, 0, 1, key)
        if pr[4] ~= source and source ~= "force_run" then
            return
        end
        lock_acquired = 1
    end

    return unpack(get_key_data(key))
end

--
-- Notify manager that tasks for that key was completed.
-- Rima unlocks key and next rima_get() may returns tasks with such key.
-- In case of non-zero @unlock_delay user unlock is defered for @unlock_delay seconds (at least).
--
function rima_done(key, unlock_delay)
    if unlock_delay ~= nil then unlock_delay = box.unpack('i', unlock_delay) end

    local pr = box.select_limit(2, 0, 0, 1, key)
    if pr == nil then return end

    if unlock_delay ~= nil and unlock_delay > 0 then
        box.update(2, key, "=p=p", 2, 1, 3, box.time() - EXPIRATION_TIME + unlock_delay)
    elseif box.select_limit(0, 1, 0, 1, key) == nil then
        -- no tasks for this key in the queue
        box.delete(2, key)
    else
        box.update(2, key, "=p=p", 2, 0, 3, box.time())
    end
end

--
-- Explicitly lock tasks for the key.
--
function rima_lock(key, source)
    local pr = box.select_limit(2, 0, 0, 1, key)
    if pr ~= nil and box.unpack('i', pr[2]) > 0 then
        -- already locked, pr[2] - is_locked
        return 0
    end

    if source == nil then source = "" end

    -- lock the key
    if pr ~= nil then
        box.update(2, key, "=p=p=p", 2, 1, 3, box.time(), 4, source)
    else
        box.insert(2, key, 0, 1, box.time(), source, next_queue_id())
    end

    return 1
end

--
-- Delete info and all tasks for user
--

function rima_delete_user(email)
    local something_deleted = 0
    repeat
        something_deleted = 0

        local tuple = box.delete(2, email)
        if tuple ~= nil then something_deleted = 1 end

        local tuples = { box.select_limit(3, 1, 0, 1000, email) }
        for _, tuple in pairs(tuples) do
            tuple = box.delete(3, box.unpack('l', tuple[0]))
            something_deleted = 1
        end

        tuples = { box.select_limit(0, 1, 0, 1000, email) }
        for _, tuple in pairs(tuples) do
            tuple = box.delete(0, box.unpack('l', tuple[0]))
            something_deleted = 1
        end
    until something_deleted == 0
end

--
-- Run expiration of tuples
--

local function is_expired(args, tuple)
    if tuple == nil or #tuple <= args.fieldno then
        return nil
    end

    -- expire only locked keys
    if box.unpack('i', tuple[2]) == 0 then return false end

    local field = tuple[args.fieldno]
    local current_time = box.time()
    local tuple_expire_time = box.unpack('i', field) + args.expiration_time
    return current_time >= tuple_expire_time
end

local function delete_expired(spaceno, args, tuple)
    rima_done(tuple[0])
end

dofile('expirationd.lua')

expirationd.run_task('expire_locks', 2, is_expired, delete_expired, {fieldno = 3, expiration_time = EXPIRATION_TIME})
