--
-- Implementation of queueing API.
--
-- A task in a queue can be in one of the following states:
-- READY      initial state, the task is ready for execution
-- DELAYED    initial state, the task will become ready
--            for execution when a timeout expires
-- TAKEN      the task is being  worked on
-- BURIED     the task is dead: it's either complete, or
--            cancelled or otherwise should not be worked on
--
-- The following methods are supported:
--
-- ------------------
-- - Producer methods
-- ------------------
-- box.queue.start(sno)
-- start queue in space sno
--
-- box.queue.put(sno, delay, <tuple data>)
-- Creates a new task and stores it in the queue.
--
-- sno          designates the queue (one queue per space).
-- timeout      if not 0, task execution is postponed
--              for the given timeout (in seconds)
-- <tuple data> the rest of the task parameters, are stored
--              in the tuple data and describe the task itself
--
-- The task is added to the end of the queue.
-- This method returns a 64 bit integer id of the newly
-- created task.
--
-- box.queue.push(sno, <tuple data>)
--
-- Same as above, but puts the task at the beginning of
-- the queue. Returns a 64 bit id of the newly created task.
--
-- box.queue.delete(sno, id)
-- Delets a taks by id.
-- Returns the contents of the deleted task.
--
-- ------------------
-- - Consumer methods
-- ------------------
--
-- box.queue.take(sno, timeout)
--
-- Finds a task for execution and marks the task as TAKEN.
-- Returns the task id and contents of the task.
-- A task in reserved to the consumer which issued
-- the request and will not be given to any other
-- consumer.
-- 'timeout' is considered if the queue is empty
-- If timeout is 0, the request immediately returns nil.
-- If timeout is not given, the caller is suspended
-- until a task appears in the queue.
-- Otherwise, the caller is suspended until
-- for the duration of the timeout (in seconds).
--
-- box.queue.release(sno, id, delay)
-- If the task is assigned to the consumer issuing
-- the request, it's put back to the queue, in READY
-- state. If delay is given, next execution
-- of the task is delayed for delay seconds.
-- If the task has not been previously taken
-- by the consumer, an error is raised.
--
-- box.queue.ack(sno, id)
-- Mark the task as complete and delete it,
-- as long as it was TAKEN by the consumer issuing
-- the request.
-- ---------------------------------------------
-- - How to configure a space to support a queue
-- ---------------------------------------------
-- space[0].enabled = true
--
-- space[0].index[0].type = TREE
-- space[0].index[0].unique = true
-- space[0].index[0].key_field[0].fieldno = 0
-- space[0].index[0].key_field[0].type = NUM64
--
-- space[0].index[1].type = TREE
-- space[0].index[1].unique = false
-- space[0].index[1].key_field[0].fieldno = 1
-- space[0].index[1].key_field[0].type = NUM
--
-- ---------------------------------------------
-- Background expiration of tasks taken by
-- detached consumers.
------------------------------------------------
-- There is a background fiber which puts all tasks
-- for which there is no active consumer back to
-- READY state.
-- --------------------------------------------------
-- Task metadata
-- --------------------------------------------------
-- The following task metadata is maintained by the
-- queue module and can be inspected at any time:
--
-- t[0]    unixtime + counter -- time in seconds when
--         the task was added to the queue, primary key
-- t[1]    fiber id of the fiber assigned to the task.
--
--  Tasks are executed in the order of addition.
--  Delayed tasks get addition time from the future.
--  Prioritized tasks have addition time from the past.
--
-- t[2]    task state R - ready, T - taken, B - buried
--
-- t[3]    task execution counter. Useful for
--         detecting 'poisoned' tasks, which
--         were returned to the queue too many times.
--

box.queue = {}

local box_queue_id = 0

-- space column description
local c_id = 0
local c_fid = 1
local c_state = 2
local c_count = 3

-- critical: these functions work with both, lua numbers (doubles)
-- and int64
local function lshift(number)
    return number * 2^32
end

local function rshift(number)
    return number / 2^32
end

function box.queue.id(delay)
-- IEEE 754 8-byte double has 53 bits for precision. Ensure
-- the auto-increment step fits into a Lua number nicely
    box_queue_id = box_queue_id + 2^12
-- shift os.time(), which is unix time, to the high part of 64 bit number
-- and ensure uniqueness of id
-- sic: we do the shifting while the number is still lua number,
-- to get a value within the Lua number range. This helps dealing
-- with queue ids from Tarantool command line
    if delay == nil then
        delay = 0
    end
    return tonumber64(lshift(os.time() + delay) + bit.tobit(box_queue_id))
end

function box.queue.put(sno, delay, ...)
    sno, delay = tonumber(sno), tonumber(delay)
    local id = box.queue.id(delay)
    return box.insert(sno, id, 0, 'R', 0, ...)
end

function box.queue.push(sno, ...)
    sno = tonumber(sno)
    local minid = box.unpack('l', box.space[sno].index[0]:min()[0])
    local id = lshift(rshift(minid) - 1)
    return box.insert(sno, id, 0, 'R', 0, ...)
end

function box.queue.delete(sno, id)
    sno, id = tonumber(sno), tonumber64(id)
    return box.space[sno]:delete(id)
end

function box.queue.take(sno, timeout)
    sno, timeout = tonumber(sno), tonumber(timeout)
    if timeout == nil then
        timeout = 60*60*24*365 -- one year
    end
-- find a ready task
    local task = nil
    while true do
        task = box.select(sno, 1, 0)
        if task ~= nil then
            break
        end
        if timeout > 0 then
            box.fiber.sleep(1)
            timeout = timeout - 1
        else
            return nil
        end
    end
    return box.update(sno, task[0], "=p=p+p",
                      c_fid, box.fiber.id(),
                      c_state, 'T', -- taken
                      c_count, 1)
end

local function consumer_find_task(sno, id)
    local task = box.select(sno, 0, id)
    if task == nil then
        error("task not found")
    end
    if box.unpack('i',task[c_fid]) ~= box.fiber.id() then
        error("the task does not belong to the consumer " .. box.fiber.id() .. ", but to " .. box.unpack('i',task[c_fid]) )
    end
    return task
end

function box.queue.ack(sno, id)
    sno, id = tonumber(sno), tonumber64(id)
    local task = consumer_find_task(sno, id)
    box.space[sno]:delete(id)
end

function box.queue.release(sno, id, delay)
    if delay == nil then
        delay = 0
    end
    sno, id, delay = tonumber(sno), tonumber64(id), tonumber(delay)
    local task = consumer_find_task(sno, id)
    -- we only change id if we need to adjust delay
    local newid
    if delay ~= 0 then
        newid = box.queue.id(delay)
    else
        newid = id
    end
    return box.update(sno, id, "=p=p=p", c_id, newid, c_fid, 0, c_state, 'R')
end

-- return abandoned tasks to the work queue
local function queue_expire(sno)
    box.fiber.detach()
    box.fiber.name("box.queue "..sno)
    local idx = box.space[sno].index[1].idx
    while true do
        local i = 0
        for it, tuple in idx.next, idx, box.fiber.id() do
            fid  = tuple[c_fid]
            if box.fiber.find(fid) == nil then
                local id = tuple[0]
                box.update(sno, id, "=p=p", c_fid, 0, c_state, 'R')
                break
            end
            i = i + 1
            if i == 100 then
                -- sleep without resetting the iterator
                box.fiber.sleep(1)
                i = 0
            end
        end
        -- sleep before the next iteration
        box.fiber.sleep(1)
    end
end

function box.queue.start(sno)
    sno = tonumber(sno)
    box.fiber.resume(box.fiber.create(queue_expire), sno)
end
