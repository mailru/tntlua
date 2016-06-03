if not is_test or not is_test() then
    box.error(box.error.PROC_LUA, "Run this module only on the test environment!")
end

queue = require 'queue'

dofile 'bernadette.lua'

local fiber = require 'fiber'
local task_state = require 'queue.abstract.state'

local test_user_id = 1
local function cleanup()
    local ids = {}
    for _, t in box.space.relations.index.uid:pairs({ test_user_id }, { iterator = box.index.ALL }) do
        table.insert(ids, t[2])
    end

    for _, id in ipairs(ids) do
        box.space.relations.index.uid_uidl:delete({ test_user_id, id })
    end

    if task_id ~= nil then
        pcall(function (id)
            queue.tube.bernadette:delete(id)
        end, task_id)
    end
end

cleanup()
local test = (require 'tap').test('bernadette test')
test:plan(12)

local error_n_checks = 5
local function check_error(test, err, err_no, err_desc)
    test:istable(err, "error table found")
    test:is(#err, 3, "number of elements in error is correct")
    test:is(err[1], err_no, "valid error number for '" .. err_desc .. "' error")
    test:is(err[2], 0, "empty send date for error")
    test:is(err[3], "", "empty data for error")
end

test:test('replace_params test', function (test)
    test:plan(8)

    local time = math.floor(fiber.time())

    local params_n_checks = 6
    local function check_params(test, params, expected_vals)
        test:is(params:uid(), expected_vals[1], "correct uid")
        test:is(params:old_uidl(), expected_vals[2], "correct old msg id")
        test:is(params:new_uidl(), expected_vals[3], "correct new msg id")
        test:is(params:send_date(), expected_vals[4], "correct send date")
        test:is(params:delay(), expected_vals[4] - time, "correct delay")
        test:is(params:data(), expected_vals[5], "correct data")
    end

    test:test('valid params', function(test)
        test:plan(params_n_checks + 2)
        local s, r  = pcall(function (test)
            local args = { 1, "", "1", time + 10, "xxx" }
            local params = ReplaceParams:new(unpack(args))

            args[2] = nil -- old_msg_id will not be used
            check_params(test, params, args)
            return params
        end, test)

        test:is(s, true, "exception wasn't generated")
        test:istable(r, "params are ok")
    end)

    test:test('valid params without data', function(test)
        test:plan(params_n_checks + 2)
        local s, r  = pcall(function (test)
            local args = { 1, "", "1", time + 10, "" }
            local params = ReplaceParams:new(unpack(args))

            args[2] = nil -- old_msg_id will not be used
            args[5] = nil -- data will not be used
            check_params(test, params, args)
            return params
        end, test)

        test:is(s, true, "exception wasn't generated")
        test:istable(r, "params are ok")
    end)

    test:test('valid params without data with old_msg_id', function(test)
        test:plan(params_n_checks + 2)
        local s, r  = pcall(function (test)
            local args = { 1, "2", "1", time + 10, "" }
            local params = ReplaceParams:new(unpack(args))

            args[5] = nil -- data will not be used
            check_params(test, params, args)
            return params
        end, test)

        test:is(s, true, "exception wasn't generated")
        test:istable(r, "params are ok")
    end)

    test:test('valid params without data', function(test)
        test:plan(params_n_checks + 2)
        local s, r  = pcall(function (test)
            local args = { 1, "", "1", time + 10, "" }
            local params = ReplaceParams:new(unpack(args))

            args[2] = nil -- old_msg_id will not be used
            args[5] = nil -- data will not be used
            check_params(test, params, args)
            return params
        end, test)

        test:is(s, true, "exception wasn't generated")
        test:istable(r, "params are ok")
    end)

    test:test('valid params with old_msg_id', function (test)
        test:plan(params_n_checks + 2)
        local s, r  = pcall(function (test)
            local args = { 1, "2", "1", time + 10, "xxx" }
            local params = ReplaceParams:new(unpack(args))
            check_params(test, params, args)
            return params
        end, test)

        test:is(s, true, "exception wasn't generated")
        test:istable(r, "params are ok")
    end)

    test:test('invalid time', function (test)
        test:plan(error_n_checks + 4)
        local s, r = pcall(function (test)
            local args = { 1, "2", "1", time - 1, "xxx" }
            local params = ReplaceParams:new(unpack(args))
            test:istable(params, "table was returned")
            test:isnil(params.uid, "table wasn't blessed")
            check_error(test, params, 4, "invalid timestamp")
            return params
        end, test)

        test:is(s, true, "exception wasn't generated")
        test:istable(r, "error message is ok")
    end)

    test:test('invalid user id', function (test)
        test:plan(2)

        local s, r = pcall(function (test)
            local params = ReplaceParams:new()
            test:fail("exception should be")
        end, test)

        test:is(s, false, "exception is expected")
        test:is(r, "bernadette_replace: user id is required", "correct error message")
    end)

    test:test('invalid new msg id', function (test)
        test:plan(2)

        local s, r = pcall(function (test)
            local params = ReplaceParams:new(1, "")
            test:fail("exception should be")
        end, test)

        test:is(s, false, "exception is expected")
        test:is(r, "bernadette_replace: invalid new_msg_id", "correct error message")
    end)
end)

local tasks_n_checks = 11
local function check_task(test, task, args)
    test:istable(task, "task is table")
    test:ok(task:initialized(), "task is initialized")

    test:is(task:user_id(), args[1], "user id is ok")
    test:is(task:uidl(), args[2], "uidl is ok")
    test:is(task:id(), args[3], "message id is ok")
    test:is(task:send_date(), args[4], "send date is ok")
    test:is(task:data(), args[5], "data is ok")

    local serialized = task:serialize()
    test:istable(serialized, "serialization complete ok")
    test:is_deeply(serialized, args, "serialized values are ok")

    serialized = task:user_serialize()
    test:istable(serialized, "user serialization ok")
    test:is_deeply(serialized, { task:uidl(), task:send_date(), task:data() or "" }, "user serialized values are ok")
end

test:test('task test', function (test)
    test:plan(4)
    test:test("empty task", function (test)
        test:plan(3)
        local task = Task:new()
        test:istable(task, "task ok")
        test:is(#task, 0, "task contains no data")
        test:ok(not task:initialized(), "task is not initialized")
    end)

    test:test("valid task", function (test)
        local data = { 1, "324", 135, 100500, "xxx" }
        local task = Task:new(data)
        test:plan(tasks_n_checks)
        check_task(test, task, data)
    end)

    test:test("valid task without one field", function (test)
        local data = { 1, "324", 135, 100500, "xxx" }
        data[3] = nil

        local task = Task:new(data)
        test:plan(tasks_n_checks)
        check_task(test, task, data)
    end)

    test:test("test from_params() call", function (test)
        test:plan(tasks_n_checks + 4)
        local time = math.floor(fiber.time())
        local args = { 1, "", "1", time + 10, "" }
        local params = ReplaceParams:new(unpack(args))

        local task = Task:new()
        test:istable(task, "task created ok")
        test:isnt(task:initialized(), "task is not initialized")

        task:from_params(params, 1222)

        test:istable(task, "task is still valid")
        test:ok(task:initialized(), "task is initialized now")

        check_task(test, task, { args[1], args[3], 1222, args[4], nil })
    end)
end)

test:test("task status test", function (test)
    -- simple mock for task statuses is implemented
    local Queue = {}
    function Queue:new()
        local x = {}
        setmetatable(x, self)
        self.__index = self
        return x
    end

    function Queue:set_for_peek(id, t)
        if not self.for_peek then
            self.for_peek = {}
        end
        self.for_peek[id] = t
    end

    function Queue:peek(task_id)
        -- will be called from code tested
        if self.for_peek[task_id] == nil then
            box.error(box.error.PROC_LUA, "Invalid task id")
        end
        return self.for_peek[task_id]
    end

    local qq = Queue:new()

    local tests = {
        { { 100, task_state.DONE }, 2 },        -- no such task
        { { 200, task_state.READY }, 0 },       -- succ
        { { 300, task_state.DELAYED }, 0 },     -- succ
        { { 400, task_state.BURIED }, 0 },      -- succ
        { { 500, task_state.TAKEN }, 1 },       -- in process
    }

    for _, v in ipairs(tests) do
        qq:set_for_peek(v[1][1], v[1])
    end

    local old_q = queue
    queue = {
        tube = {
            bernadette = qq,
        }
    }

    local task = Task:new()

    -- needed for correct logging in main file
    task.__uidl = 0
    task.__user_id = 1

    test:plan(2 * (#tests + 2))
    for _, v in ipairs(tests) do
        task.__task_id = v[1][1]
        local s, r = pcall(function(task)
            return get_task_status(task)
        end, task)
        test:ok(s, "task " .. task:id() .. " success")
        test:is(r, v[2], "task " .. task:id() .. " ret ok")
    end

    task.__task_id = 288
    local s, r = pcall(function(task)
        return get_task_status(task)
    end, task)

    test:ok(s, "not existed task")
    test:is(r, 2, "no such task")

    qq:set_for_peek(199, { 199, 'xxxx' })
    task.__task_id = 199
    local s, r = pcall(function(task)
        return get_task_status(task)
    end, task)

    test:ok(not s, "Invalid task status: exception")
    test:isstring(r, "exception message")

    queue = old_q
end)

test:test("find task by uidl test", function (test)
    test:plan(3)

    test:test("no data in tarantool", function (test)
        local s, r = pcall(function (test)
            return find_task_by_uidl(test_user_id, "1")
        end)

        test:plan(3)

        test:ok(s, "no exception while no data in tarantool")
        test:istable(r, "correct return value")
        test:ok(not r:initialized(), "task is not initialized")

        cleanup()
    end)

    test:test("correct data in tarantool", function (test)
        local data = { test_user_id, "1", 100, 100, "xxx" }
        box.space.relations:insert(data)

        local s, r = pcall(function ()
            return find_task_by_uidl(test_user_id, "1")
        end)

        test:plan(tasks_n_checks + 1)
        test:ok(s, "no exception for data came")

        check_task(test, r, data)

        cleanup()
    end)

    test:test("correct data with skipped field", function (test)
        local data = { test_user_id, "1", 100, 100, "xxx" }
        data[5] = nil -- remove "data"
        box.space.relations:insert(data)

        local s, r = pcall(function ()
            return find_task_by_uidl(test_user_id, "1")
        end)

        test:plan(tasks_n_checks + 1)
        test:ok(s, "no exception for data came")

        check_task(test, r, data)

        cleanup()
    end)
end)

test:test("select user tasks test", function (test)
    test:plan(6)

    local call_tests = 2
    local function check_call(test, uid, uidl)
        local s, r = pcall(function (uid, uidl)
            return { select_user_tasks(uid, uidl or "") }
        end, uid, uidl)

        test:ok(s, "no exception happened")
        test:istable(r, "table returned from function")

        return r
    end

    local function sortFunc(a, b)
        return a[4] < b[4] -- sort by send date
    end

    local function check_list(test, list)
        local results = {}
        for _, v in ipairs(list) do
            box.space.relations:insert(v)
        end
        table.sort(list, sortFunc)

        for _, v in ipairs(list) do
            if #results >= MAX_TASKS then
                break
            end

            local t = Task:new(v)
            table.insert(results, t:user_serialize())
        end

        test:plan(call_tests + #results * 3 + 1)

        local r = check_call(test, test_user_id)
        if not test:is(#r, #results, "Correct number of elements") then
            return
        end

        for i, v in ipairs(r) do
            test:istable(v, "Valid return value, i == " .. i)
            test:is(#v, 3, "Valid number of elements, i == " .. i)
            test:is_deeply(v, results[i], "expected return value, i == " .. i)
        end
    end

    test:test("no tasks found in tarantool", function (test)
        test:plan(call_tests + 1)
        local r = check_call(test, test_user_id)
        test:is(#r, 0, "no tuples were returned from tarantool")
    end)

    test:test("single task in tarantool", function (test)
        check_list(test, {
            { test_user_id, "1", 100, 100, "xxx" },
        })
        cleanup()
    end)

    test:test("multiple number of tasks in tarantool", function (test)
        check_list(test, {
            { test_user_id, "1", 100, 100, "xxx" },
            { test_user_id, "2", 110, 200, "xxx" },
            { test_user_id, "4", 120, 50, "xxx" },
        })
        cleanup()
    end)

    test:test("too many tasks in tarantool", function (test)
        local tasks = {}
        local max_tasks = MAX_TASKS
        MAX_TASKS = 10

        for i = 1, MAX_TASKS + 39 do
            table.insert(tasks, { test_user_id, tostring(i), i + 198, math.random(5000000), "xxxx " .. i })
        end

        check_list(test, tasks)

        MAX_TASKS = max_tasks
        cleanup()
    end)

    test:test("only one message is required", function (test)
        test:plan(call_tests + 4)
        box.space.relations:insert({ test_user_id, "1", 100, 100, "xxx" })
        local r = check_call(test, test_user_id, "1")

        test:istable(r, "table was returned")
        test:is(#r, 1, "1 message is expected")
        test:is(#r[1], 3, "3 fields in a message")
        test:is_deeply(r[1], { "1", 100, "xxx" }, "valid msg")

        cleanup()
    end)

    test:test("required message not found in tarantool", function (test)
        test:plan(call_tests + 2)
        local r = check_call(test, test_user_id, "1")
        test:istable(r, "table was returned")
        test:is(#r, 0, "no data is expected")
        cleanup()
    end)
end)

test:test("real delete test", function (test)
    test:plan(2)

    local call_tests = 2
    local function check_call(test, task_id, task_exists)
        local task = Task:new()
        task.__task_id = task_id
        local s, r = pcall(function (task)
            bernadette_delete_real(task)
            return nil
        end, task)

        if task_exists then
            test:ok(s, "no exception happened")
            test:isnil(r, "nothing returned from function")
        else
            test:ok(not s, "exception happened")
            test:isstring(r, "error message shawn")
        end

        return r
    end

    test:test("nothing to delete", function (test)
        test:plan(call_tests)
        check_call(test, 123, false)
    end)

    test:test("deletion complete", function (test)
        local t = queue.tube.bernadette:put(nil, { release = 1000 })
        box.space.relations:insert({ test_user_id, "1", t[1], 100, "xxx" })

        test:plan(call_tests + 3)
        check_call(test, t[1], true)

        local s, r = pcall(function (task)
            return queue.tube.bernadette:peek(task)
        end, t[1])

        test:ok(not s, "no task is expected in tarantool")
        test:isstring(r, "exception is expected")

        r = box.space.relations.index.task_id:select({t[1]})
        test:is(#r, 0, "no message is expected in tarantool")

        cleanup(t[1])
    end)
end)

test:test("delete impl test", function (test)
    test:plan(4)

    test:test("no task", function (test)
        local e, r = pcall(function ()
            return bernadette_delete_impl(test_user_id, "1", false)
        end)

        test:plan(4)
        test:ok(e, "no exception happened")
        test:istable(r, "table returned")
        test:is(#r, 3, "valid number of elements")
        test:is_deeply(r, { 2, 0, "" }, "valid data")
    end)

    test:test("invalid status", function (test)
        local t = queue.tube.bernadette:put(nil, { release = 0 }) -- task will be 'ready'
        queue.tube.bernadette.raw.space.index.task_id:update({t[1]}, {{'=',2,'t'}}) -- task was taken
        box.space.relations:insert({ test_user_id, "1", t[1], 100, "xxx" })

        local e, r = pcall(function ()
            return bernadette_delete_impl(test_user_id, "1", false)
        end)

        test:plan(4)
        test:ok(e, "no exception happened")
        test:istable(r, "table returned")
        test:is(#r, 3, "valid number of elements")
        test:is_deeply(r, { 1, 0, "" }, "valid data")
        cleanup(t[1])
    end)

    test:test("ignore task in process", function (test)
        local t = queue.tube.bernadette:put(nil, { release = 0 }) -- task will be 'ready'
        queue.tube.bernadette.raw.space.index.task_id:update({t[1]}, {{'=',2,'t'}}) -- task was taken
        box.space.relations:insert({ test_user_id, "1", t[1], 100, "xxx" })

        local e, r = pcall(function ()
            return bernadette_delete_impl(test_user_id, "1", true)
        end)

        test:plan(4)
        test:ok(e, "no exception happened")
        test:istable(r, "table returned")
        test:is(#r, 3, "valid number of elements")
        test:is_deeply(r, { 0, 100, "xxx" }, "valid data")
        cleanup(t[1])
    end)

    test:test("success", function (test)
        local t = queue.tube.bernadette:put(nil, { release = 100 })
        box.space.relations:insert({ test_user_id, "1", t[1], 100, "xxx" })

        local e, r = pcall(function ()
            return bernadette_delete_impl(test_user_id, "1", false)
        end)

        test:plan(5)
        test:ok(e, "no exception happened")
        test:istable(r, "table returned")
        test:is(#r, 3, "valid number of elements")
        test:is_deeply(r, { 0, 100, "xxx" }, "valid data")

        local x = box.space.relations:select({ test_user_id, "1" })
        test:is(#x, 0, "no data is expected in tarantool")

        cleanup(t[1])
    end)
end)

test:test("put_real test", function (test)
    local time = math.floor(fiber.time())
    local args = { test_user_id, "2", "22", time + 10, "xxx" }
    local params = ReplaceParams:new(unpack(args))

    local e, r = pcall(function (params)
        bernadette_put_real(params)
        return nil
    end, params)

    test:plan(9)
    test:ok(e, "no exception happened")
    test:isnil(r)

    local x = box.space.relations.index.uid_uidl:select({ test_user_id, "22" })
    test:istable(x, "select done")
    test:is(#x, 1, "valid number of return args")
    test:is(#x[1], 5, "valid number of args in returned tuple")
    test:is(x[1][1], test_user_id, "valid uid")
    test:is(x[1][2], "22", "valid msg id")
    test:is(x[1][4], time + 10, "valid timestamp")
    test:is(x[1][5], "xxx", "valid data")

    cleanup()
end)

test:test("replace_impl test", function (test)
    local time = math.floor(fiber.time())
    test:plan(2)

    local extra_cases = 4
    local function call_func(test, params, expected)
        local e, r = pcall(function (params)
            return bernadette_replace_impl(params)
        end, params)

        test:ok(e, "no exception happened")
        test:istable(r, "table in return value")
        test:is(#r, 3, "valid number of returned args")
        test:is_deeply(r, expected, "valid values")
    end

    test:test("old_msg_id given", function (test)
        test:plan(5)

        local args = { test_user_id, "2", "22", time + 10, "xxx" }
        local params = ReplaceParams:new(unpack(args))

        test:test("old_msg_id not found", function (test)
            test:plan(extra_cases)
            call_func(test, params, { 2, 0, "" })
        end)

        test:test("old_msg_id is in process", function (test)
            local t = queue.tube.bernadette:put(nil, { release = 0 }) -- task will be 'ready'
            queue.tube.bernadette.raw.space.index.task_id:update({t[1]}, {{'=',2,'t'}}) -- task was taken
            box.space.relations:insert({ test_user_id, "2", t[1], time + 100, "xxx" })

            test:plan(extra_cases)
            call_func(test, params, { 1, 0, "" })

            cleanup(t[1])
        end)

        test:test("old_msg_id is ok, all is ok", function (test)
            local t = queue.tube.bernadette:put(nil, { release = 0 }) -- task will be 'ready'
            box.space.relations:insert({ test_user_id, "2", t[1], time + 120, "zzz" })

            test:plan(extra_cases + 5)
            call_func(test, params, { 0, time + 120, "zzz" })

            local x = box.space.relations:select({ test_user_id, "2" })
            test:istable(x, "return value test")
            test:is(#x, 0, "tuple not exists")

            x = box.space.relations:select({ test_user_id, "22" })
            test:istable(x, "return value test")
            test:is(#x, 1, "tuple exists")
            x = { x[1]:unpack() } -- can't modify tuple
            x[3] = 1 -- task id
            test:is_deeply(x, { test_user_id, "22", 1, time + 10, "xxx" }, "tuple contains valid values")

            cleanup(t[1])
        end)

        test:test("too many tasks, fail expected", function (test)
            local max_tasks = MAX_TASKS
            MAX_TASKS = 10

            local t = queue.tube.bernadette:put(nil, { release = 0 }) -- task will be 'ready'
            box.space.relations:insert({ test_user_id, params:old_uidl(), t[1], math.random(5000000), "xxxxggg" }) -- insert real task

            for i = 1, MAX_TASKS + 9 do
                box.space.relations:insert({ test_user_id, tostring(i + 235325), i + 118 + t[1], math.random(5000000), "xxxx " .. i })
            end

            test:plan(extra_cases)
            call_func(test, params, { 3, 0, "" }) -- too many tasks

            MAX_TASKS = max_tasks
            cleanup(t[1])
        end)

        test:test("too many tasks, success expected", function (test)
            local max_tasks = MAX_TASKS
            MAX_TASKS = 10

            local t = queue.tube.bernadette:put(nil, { release = 0 }) -- task will be 'ready'
            local tuple = { test_user_id, params:old_uidl(), t[1], math.random(5000000), "xxxxy" }
            box.space.relations:insert(tuple) -- insert real task

            for i = 1, MAX_TASKS - 1 do
                box.space.relations:insert({ test_user_id, tostring(i + 23235235), i + 118 + t[1], math.random(5000000), "xxxx " .. i })
            end

            test:plan(extra_cases + 2)
            call_func(test, params, { 0, tuple[4], tuple[5] }) -- too many tasks

            local x = box.space.relations:select({ test_user_id, params:old_uidl() })
            test:is(#x, 0, "old tuple should be removed")

            x = box.space.relations:select({ test_user_id, params:new_uidl() })
            test:is(#x, 1, "new tuple should be inserted")

            MAX_TASKS = max_tasks
            cleanup(t[1])
        end)
    end)

    test:test("old_msg_id not given", function (test)
        test:plan(2)

        local args = { test_user_id, "", "22", time + 10, "xxx" }
        local params = ReplaceParams:new(unpack(args))

        test:test("too many tasks", function (test)
            local max_tasks = MAX_TASKS
            MAX_TASKS = 10

            for i = 1, MAX_TASKS + 9 do
                box.space.relations:insert({ test_user_id, tostring(i), i + 198, math.random(5000000), "xxxx " .. i })
            end

            test:plan(extra_cases)
            call_func(test, params, { 3, 0, "" }) -- too many tasks

            MAX_TASKS = max_tasks
            cleanup()
        end)

        test:test("success", function (test)
            test:plan(extra_cases)
            call_func(test, params, { 0, 0, "" }) -- success

            cleanup()
        end)
    end)

    cleanup()
end)

test:test("replace test", function (test)
    test:plan(3)

    local begin = box.begin
    local commit = box.commit
    local rollback = box.rollback

    local begin_called, commit_called, rollback_called = false, false, false
    box.begin = function () begin_called = true end
    box.commit = function () commit_called = true end
    box.rollback = function () rollback_called = true end

    local function __cleanup()
        begin_called, commit_called, rollback_called = false, false, false
    end

    local time = math.floor(fiber.time())

    test:test("error happened", function (test)
        test:plan(6)
        local args = { test_user_id, "", "1", time - 1, "xxx" }

        local s, r = pcall(function ()
            return bernadette_replace(unpack(args))
        end)

        test:ok(s, "exception wasn't generated")
        test:istable(r, "error message is ok")
        test:is_deeply(r, { 4, 0, "" }, "error returned successfully")
        test:ok(not begin_called, "begin() wasn't called")
        test:ok(not commit_called, "commit() wasn't called")
        test:ok(not rollback_called, "rollback() wasn't called")

        __cleanup()
        cleanup()
    end)

    test:test("transaction success", function (test)
        test:plan(6)
        local args = { test_user_id, "", "1", time + 100, "xxx" }

        local s, r = pcall(function ()
            return bernadette_replace(unpack(args))
        end)

        test:ok(s, "exception wasn't generated")
        test:istable(r, "error message is ok")
        test:is_deeply(r, { 0, 0, "" })
        test:ok(begin_called, "begin() was called")
        test:ok(commit_called, "commit() was called")
        test:ok(not rollback_called, "rollback() wasn't called")

        __cleanup()
        cleanup()
    end)

    test:test("transaction fail", function (test)
        test:plan(5)

        local args = { test_user_id, "2", "1", time + 100, "xxx" }
        local err_f = render_error
        render_error = function (x)
            show_error("test")
        end

        local s, r = pcall(function ()
            return bernadette_replace(unpack(args))
        end)

        test:ok(not s, "exception was generated")
        test:isstring(r, "error message is ok")
        test:ok(begin_called, "begin() was called")
        test:ok(not commit_called, "commit() wasn't called")
        test:ok(rollback_called, "rollback() was called")

        __cleanup()
        cleanup()

        render_error = err_f
    end)

    box.begin = begin
    box.commit = commit
    box.rollback = rollback
end)

test:test("peek test", function (test)
    test:plan(2)

    test:test("invalid user_id", function (test)
        local e, r = pcall(function ()
            return bernadette_peek()
        end)

        test:plan(2)
        test:ok(not e, "exception is expected")
        test:isstring(r, "error message is expected")
    end)

    test:test("valid user_id", function (test)
        local t = queue.tube.bernadette:put(nil, { release = 0 }) -- task will be 'ready'
        local tuple = { test_user_id, "22", t[1], math.random(5000000), "xxxxy" }
        box.space.relations:insert(tuple) -- insert real task

        local e, r = pcall(function ()
            return { bernadette_peek(test_user_id) }
        end)

        test:plan(3)
        test:ok(e, "exception isn't expected")
        test:istable(r, "table is expected")
        test:is_deeply(r, {{ tuple[2], tuple[4], tuple[5] }}, "valid return valud")

        cleanup(t[1])
    end)
end)

test:test("delete test", function (test)
    test:plan(4)

    test:test("invalid user_id", function (test)
        local e, r = pcall(function ()
            return bernadette_delete()
        end)

        test:plan(2)
        test:ok(not e, "exception is expected")
        test:isstring(r, "error message is expected")
    end)

    test:test("invalid message_id", function (test)
        local e, r = pcall(function ()
            return bernadette_delete(test_user_id)
        end)

        test:plan(2)
        test:ok(not e, "exception is expected")
        test:isstring(r, "error message is expected")
    end)

    local begin = box.begin
    local commit = box.commit
    local rollback = box.rollback

    local begin_called, commit_called, rollback_called = false, false, false
    box.begin = function () begin_called = true end
    box.commit = function () commit_called = true end
    box.rollback = function () rollback_called = true end

    local function __cleanup()
        begin_called, commit_called, rollback_called = false, false, false
    end

    local time = math.floor(fiber.time())

    test:test("valid transaction", function (test)
        test:plan(6)

        local t = queue.tube.bernadette:put(nil, { release = 0 }) -- task will be 'ready'
        local tuple = { test_user_id, "22", t[1], math.random(5000000), "xxxxy" }
        box.space.relations:insert(tuple) -- insert real task

        local s, r = pcall(function ()
            return bernadette_delete(test_user_id, tuple[2])
        end)

        test:ok(s, "exception wasn't generated")
        test:istable(r, "error message is ok")
        test:is_deeply(r, { 0, tuple[4], tuple[5] })
        test:ok(begin_called, "begin() was called")
        test:ok(commit_called, "commit() was called")
        test:ok(not rollback_called, "rollback() wasn't called")

        __cleanup()
        cleanup(t[1])
    end)

    test:test("invalid transaction", function (test)
        local err_f = render_error
        render_error = function (x)
            show_error("test")
        end

        local s, r = pcall(function ()
            return bernadette_delete(test_user_id, "1")
        end)

        test:plan(5)
        test:ok(not s, "exception was generated")
        test:isstring(r, "error message is ok")
        test:ok(begin_called, "begin() was called")
        test:ok(not commit_called, "commit() wasn't called")
        test:ok(rollback_called, "rollback() was called")

        __cleanup()
        cleanup()

        render_error = err_f
    end)

    box.begin = begin
    box.commit = commit
    box.rollback = rollback
end)

test:check()
