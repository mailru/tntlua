local queue = require 'queue'
local fiber = require 'fiber'

local function show_error(str)
    box.error(box.error.PROC_LUA, str)
end

if not queue then
    show_error("Failed to load queue module")
end

if not queue.tube.deferrs then
    queue.start()
end

box.once('deferrs_init', function ()
    queue.create_tube('deferrs', 'fifottl')
end)

local function get_task_id(task)
    return task[1]
end

local function get_task_data(task)
    return task[3]
end

function deferr_put(uid, release_time, data)
    local ok, ret = pcall(function(uid, release_time, data)
        local delay = release_time - math.floor(fiber.time())
        if delay < 0 then
            show_error("Invalid release_time found for user " .. uid .. ", data == " .. data)
        end
        return queue.tube.deferrs:put({ uid, release_time, data }, { delay = delay })
    end, uid, release_time, data)

    if not ok then
        show_error(str) -- unexpected error. Pass it to capron
    end

    return get_task_id(ret)
end

function deferr_delete(id)
    local ok, ret = pcall(function (task_id)
        return queue.tube.deferrs:delete(task_id)
    end, id)

    if not ok then
        return nil
    end

    return get_task_data(ret) -- a tuple: { uid, release_time, data }
end

function deferr_peek(id)
    local ok, ret = pcall(function (task_id)
        return queue.tube.deferrs:peek(task_id) -- task state will not be changed
    end, id)

    if not ok then
        return nil
    end

    return get_task_data(ret) -- a tuple: { uid, release_time, data }
end
