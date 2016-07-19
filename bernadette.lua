local fiber = require 'fiber'
local log = require 'log'
local task_state = require 'queue.abstract.state'

if not queue then
    -- Needed for testing
    queue = require 'queue'
end

function show_error(str)
    box.error(box.error.PROC_LUA, str)
end

if not queue then
    show_error("Failed to load queue module")
end

if not queue.tube.bernadette then
    queue.start()
end

-- ============================================================================== --

-- not local for tests
-- maximum number of deferred messages supported
MAX_TASKS = 200

-- timeout for take() requests, in seconds
TAKE_TIMEOUT = 30

-- ============================================================================== --

local FIELD_UID = 1
local FIELD_UIDL = 2
local FIELD_TASK_ID = 3
local FIELD_SEND_DATE = 4
local FIELD_DATA = 5

-- return codes descrition
local ERR_SUCC = 0
local ERR_IN_PROCESS = 1
local ERR_NO_SUCH_TASK = 2
local ERR_TOO_MANY_TASKS = 3
local ERR_INVALID_TIMESTAMP = 4

-- XXX: Don't modify default storage engine!
-- fifottl haven't got an interface to set an engine but same engine
-- for both spaces is required for transactions
box.once('bernadette_init', function ()
    queue.create_tube('bernadette', 'fifottl')

    local relations = box.schema.space.create('relations')

    -- { uid, message_id } => task_id
    -- PS: message_id == uidl
    relations:create_index('uid_uidl', {
        type = 'HASH',
        parts = { FIELD_UID, 'NUM', FIELD_UIDL, 'STR' },
        unique = true,
    })

    relations:create_index('task_id', {
        type = 'HASH',
        parts = { FIELD_TASK_ID, 'NUM' },
        unique = true,
    })

    relations:create_index('uid', {
        type = 'TREE',
        parts = { FIELD_UID, 'NUM', FIELD_SEND_DATE, 'NUM' },
        unique = false,
    })
end)

function render_error(err_code)
    return { err_code, 0, "" }
end

-- ============================================================================== --

-- Input params
-- Not local for testing
ReplaceParams = {}
function ReplaceParams:new(user_id, old_msg_id, new_msg_id, send_date, data)
    if user_id == nil or user_id == 0 then
        show_error("bernadette_replace: user id is required")
    end

    if new_msg_id == nil or new_msg_id == "" then
        show_error("bernadette_replace: invalid new_msg_id")
    end

    local time = math.floor(fiber.time())
    if send_date == nil or send_date < time then
        log.warn("bernadette_replace: invalid send_date: " .. send_date .. ", at least " .. time .. " is required")
        return render_error(ERR_INVALID_TIMESTAMP)
    end

    if old_msg_id == "" then
        old_msg_id = nil -- to skip check like <old_msg_id == nil or old_msg_id == nil>
    end

    if data == "" then
        data = nil -- don't store empty data
    end

    local params = {
        __uid = user_id,
        __old_uidl = old_msg_id,
        __send_date = send_date,
        __delay = send_date - time,

        __new_uidl = new_msg_id,  -- could be nil
        __data = data,            -- could be nil
    }

    setmetatable(params, self)
    self.__index = self
    return params
end

-- This wrappers (getters) are required to prevent uninitialied variables usage
function ReplaceParams:uid() return self.__uid end
function ReplaceParams:old_uidl() return self.__old_uidl end
function ReplaceParams:new_uidl() return self.__new_uidl end
function ReplaceParams:send_date() return self.__send_date end
function ReplaceParams:delay() return self.__delay end
function ReplaceParams:data() return self.__data end

-- ============================================================================== --

-- Not local for testing
Task = {}
function Task:new(tuple)
    if tuple == nil then
        tuple = {}
    end

    local task = {
        __user_id     = tuple[FIELD_UID],
        __uidl        = tuple[FIELD_UIDL],
        __task_id     = tuple[FIELD_TASK_ID],
        __send_date   = tuple[FIELD_SEND_DATE],
        __data        = tuple[FIELD_DATA],
    }

    setmetatable(task, self)
    self.__index = self
    return task
end

function Task:from_params(params, task_id)
    self.__user_id = params:uid()
    self.__uidl = params:new_uidl()
    self.__task_id = task_id
    self.__send_date = params:send_date()
    self.__data = params:data()
end

function Task:serialize()
    local tuple = {}
    tuple[FIELD_UID] = self:user_id()
    tuple[FIELD_UIDL] = self:uidl()
    tuple[FIELD_TASK_ID] = self:id()
    tuple[FIELD_SEND_DATE] = self:send_date()
    tuple[FIELD_DATA] = self:data()

    return tuple
end

function Task:user_serialize()
    return {
        self:uidl(),
        self:send_date(),
        self:data() or "",
    }
end

function Task:user_serialize_with_uid()
    return {
        self:user_id(),
        self:uidl(),
        self:send_date(),
        self:data() or "",
    }
end

function Task:initialized() return self.__user_id ~= nil end
function Task:user_id() return self.__user_id end
function Task:uidl() return self.__uidl end
function Task:id() return self.__task_id end
function Task:send_date() return self.__send_date end
function Task:data() return self.__data end

-- ============================================================================== --

-- not local for testing
function get_task_status(task)
    local ok, ret = pcall(function(task_id)
        local queued_task = queue.tube.bernadette:peek(task_id)
        return queued_task[2] -- { task_id, task_status }
    end, task:id())

    if not ok or ret == task_state.DONE then
        -- a task is pruned from the queue when it's executed, so state DONE may be hard to see
        return ERR_NO_SUCH_TASK
    end

    if ret == task_state.READY or ret == task_state.DELAYED then
        -- ready tasks still can be deleted
        return ERR_SUCC
    end

    if ret == task_state.BURIED then
        -- task is broken ?
        log.warn("Buried task #" .. task:id() .. " found, uid = " .. task:user_id() .. ", uidl == " .. task:uidl())
        return ERR_SUCC
    end

    if ret == task_state.TAKEN then
        return ERR_IN_PROCESS
    end

    show_error("Unexpected task state found: " .. task_state)
end

function find_task_by_uidl(uid, uidl)
    return Task:new( box.space.relations.index.uid_uidl:get({ uid, uidl }) )
end

-- Function will select all tasks of a specific user in the send_date order
function select_user_tasks(uid, msg_id)
    local index = box.space.relations.index.uid
    if msg_id and msg_id ~= "" then
        index = box.space.relations.index.uid_uidl
    else
        msg_id = nil
    end

    local ret = index:select({ uid, msg_id }, { limit = MAX_TASKS })
    for i = 1, #ret do
        local t = Task:new(ret[i])
        ret[i] = t:user_serialize()
    end

    return unpack(ret)
end

function bernadette_delete_real(task)
    -- delete task from queue
    -- delete task from relations space
    -- XXX: call this function right after queue:peek() (or under transaction) !!!
    -- Otherwise task can be consumed with queue:take() by another fiber !!!

    queue.tube.bernadette:delete(task:id())
    box.space.relations.index.task_id:delete(task:id())
end

function bernadette_delete_impl(uid, msg_id, ignore_in_process)
    local task = find_task_by_uidl(uid, msg_id)

    if not task:initialized() then
        return render_error(ERR_NO_SUCH_TASK)
    end

    local status = get_task_status(task)
    if ignore_in_process and status == ERR_IN_PROCESS then
        status = ERR_SUCC
    end

    if status ~= ERR_SUCC then
        return render_error(status)
    end

    bernadette_delete_real(task)

    return {
        ERR_SUCC,
        task:send_date(),
        task:data() or "",
    }
end

function bernadette_put_real(params)
    local task_data = queue.tube.bernadette:put(nil, { delay = params:delay() })
    if task_data == nil or #task_data < 2 then
        show_error("Can't put new task in queue")
    end

    local new_task = Task:new()
    new_task:from_params(params, task_data[1]) -- { task_id, task_status } in task_data

    box.space.relations:replace(new_task:serialize())
end

function bernadette_replace_impl(params)
    -- this function should be wrapped with transaction
    local existed_task
    if params:old_uidl() then
        existed_task = find_task_by_uidl(params:uid(), params:old_uidl())
        if not existed_task:initialized() then
            return render_error(ERR_NO_SUCH_TASK)
        end

        local task_status = get_task_status(existed_task)
        if task_status ~= ERR_SUCC then
            return render_error(task_status)
        end

        bernadette_delete_real(existed_task)
    end

    -- Check limit for a number of tasks.
    -- This limit can be modified by admins, so queue can contain a number of
    -- tasks over limit. Send an error in this case.
    -- We should do this after old task deleting to be sure old task never never will be executed
    local existed_tasks = box.space.relations.index.uid:count(params:uid())
    if existed_tasks >= MAX_TASKS then
        return render_error(ERR_TOO_MANY_TASKS)
    end

    bernadette_put_real(params)

    return {
        ERR_SUCC,
        existed_task and existed_task:send_date() or 0,
        existed_task and existed_task:data() or "",
    }
end

function bernadette_make_transaction(callback, ...)
    box.begin()

    local ok, ret = pcall(callback, ...)

    if not ok then
        box.rollback()
        show_error(ret)
    else
        box.commit()
        return ret
    end
end

--
-- Function inserts new task into queue. If old_msg_id given, old task will be replaced.
--
function bernadette_replace(user_id, old_msg_id, new_msg_id, send_date, data)
    local params = ReplaceParams:new(user_id, old_msg_id, new_msg_id, send_date, data)
    if not params.uid then
        -- object wasn't blessed => an error occured
        return params
    end

    return bernadette_make_transaction(function (params)
        return bernadette_replace_impl(params)
    end, params)
end

--
-- Function tries to peek a task from queue. Message id can be <""> or nil.
-- All the tasks of user user_id iwill be returned in the required order
--
function bernadette_peek(user_id, message_id)
    if user_id == nil or user_id == "" then
        show_error("bernadette_peek: Invalid user_id")
    end

    return select_user_tasks(user_id, message_id)
end

function bernadette_delete_x(user_id, message_id, ignore_in_process)
    if user_id == nil or user_id == "" or message_id == nil or message_id == "" then
        show_error("bernadette_delete: Invalid user_id or uidl")
    end

    return bernadette_make_transaction(function (user_id, message_id, ignore_in_process)
        return bernadette_delete_impl(user_id, message_id, ignore_in_process)
    end, user_id, message_id, ignore_in_process)
end

function bernadette_delete(user_id, message_id)
    return bernadette_delete_x(user_id, message_id, false)
end

function bernadette_force_delete(user_id, message_id)
    return bernadette_delete_x(user_id, message_id, true)
end

--
-- Function tries to select any task from queue.
-- If there is is no free tasks found during TAKE_TIMEOUT, nil will be returned.
--
function bernadette_take()
    local queued_task = queue.tube.bernadette:take(TAKE_TIMEOUT)
    if queued_task == nil then
        return nil
    end

    local task = Task:new(box.space.relations.index.task_id:get({ queued_task[1] }))
    if not task:initialized() then
        log.error("Task with id " .. queued_task[1] .. " not found in relations space")
        show_error("Invalid task #" .. queued_task[1] .. " found in queue") -- just in case
    end

    return task:user_serialize_with_uid()
end

--
-- Function releases task into queue by id.
-- Function returns true if everything is OK, and false othewise.
--
function bernadette_release(user_id, msg_id, delay)
    local task = Task:new(box.space.relations.index.uid_uidl:get({ user_id, msg_id }))
    if not task:initialized() then
        -- Task can be deleted by anyone else
        return false
    end

    local ok, ret = pcall(function(id)
        queue.tube.bernadette:release(id, { delay = delay, })
    end, task:id())

    if not ok then
        show_error(ret)
    end

    return true
end

--
-- Function removes task from the queue using ack()
--
function bernadette_ack(user_id, msg_id)
    local task = Task:new(box.space.relations.index.uid_uidl:get({ user_id, msg_id }))
    if not task:initialized() then
        return false
    end

    return bernadette_make_transaction(function (task_id)
        queue.tube.bernadette:ack(task_id)
        box.space.relations.index.task_id:delete(task_id)
        return true
    end, task:id())
end

box.schema.func.create('bernadette_peek', { if_not_exists = true })
box.schema.func.create('bernadette_replace', { if_not_exists = true })
box.schema.func.create('bernadette_delete', { if_not_exists = true })
box.schema.func.create('bernadette_force_delete', { if_not_exists = true })

box.schema.func.create('bernadette_ack', { if_not_exists = true })
box.schema.func.create('bernadette_take', { if_not_exists = true })
box.schema.func.create('bernadette_release', { if_not_exists = true })
