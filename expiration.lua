
-- ========================================================================= --
-- local support functions
-- ========================================================================= --

-- This function create new table with constants members. The runntime errro
-- will be rised if attempting to change table members.
local function finalize_table(table)
    return setmetatable ({}, {
			     __index = table,
			     __newindex = function(table_arg,
						   name_arg,
						   value_arg)
				 error("attempting to change constant " .. 
				       tostring(name_arg) ..
				       " to "
				       .. tostring(value_arg), 2)
			     end
			     })
end

-- get fiber id function
local function get_fid(fiber)
    local fid = 0
    if fiber ~= nil then
	fid = fiber:id()
    end
    return fid
end

-- get field
local function get_field(tuple, field_no)
    if tuple == nil then
	return nil
    end

    if #tuple <= field_no then
	return nil
    end

    return tuple[field_no]
end

-- ========================================================================= --
-- Constants
-- ========================================================================= --

local task_constatns = finalize_table(
{
    -- default value of number of tuples will be checked by one itaration
    default_tuples_per_iter = 1024,
    -- default value of time requed for full index scann (in seconds)
    default_full_scann_time = 3600,
    -- maximal worker delay (seconds)
    max_delay = 1,
    -- check worker intarval
    check_interval = 1,
})
--local task_constatns = finalize_table(task_constatns)


-- ========================================================================= --
-- Task local functions
-- ========================================================================= --

-- ------------------------------------------------------------------------- --
-- Task fibers
-- ------------------------------------------------------------------------- --

local curr_task = nil

local function is_tuple_expired(task, tuple)
    field = get_field(tuple, task.field_no)
    if field == nil or #field ~= 4 then
	return true
    end

    local current_time = os.time()
    local tuple_expire_time = box.unpack("i", field)
    return current_time >= tuple_expire_time
end

local function process_expired_tuple(task, tuple)
    -- delete tuple
    local key = box.unpack("i", tuple[0])
    box.delete(task.space_no, key)
    task.tuples_expired = task.tuples_expired + 1

    -- put to cemetery if needed
    local email = get_field(tuple, 1)
    if task.cemetery_space_no ~= nil and email ~= nil then
	box.replace(task.cemetery_space_no, email, os.time())
    end
end

local function worker_loop()
    -- save current task
    local task = curr_task
    -- detach guardian from creator and attach it to sched process
    box.fiber.detach()

    local scann_space = box.space[task.space_no]
    local scann_index = box.space[task.space_no].index[0]
    while true do
	local checks = 0

	-- full index scann loop
	for itr, tuple in scann_index.idx.next, scann_index.idx do
	    checks = checks + 1

	    -- do main work
	    if is_tuple_expired(task, tuple) then
		process_expired_tuple(task, tuple)
	    end

	    -- check, can worker go to sleep
	    if checks >= task.tuples_per_iter then
		checks = 0
		if scann_space:len() > 0 then
		    local delay = (task.tuples_per_iter * task.full_scann_time)
			/ scann_space:len()
		    
		    if delay > task_constatns.max_delay then
			delay = task_constatns.max_delay
		    end
		    box.fiber.sleep(delay)
		end
	    end
	end

	if scann_space:len() == 0 then
	    -- space is empty, nothig to do
	    box.fiber.sleep(task_constatns.max_delay)
	elseif task.tuples_per_iter > scann_space:len() then
	    -- space is empty, nothig to do
	    box.fiber.sleep(task_constatns.max_delay)	    
	end
    end
end

local function guardian_loop()
    -- save current task
    local task = curr_task
    -- detach guardian from creator and attach it to sched process
    box.fiber.detach()
    
    print("expiration: task '" .. task.name .. "' started")
    -- create worker fiber
    curr_task = task
    task.worker_fiber = box.fiber.create(worker_loop)
    box.fiber.resume(task.worker_fiber)

    while true do
	if task.worker_fiber:id() == 0 then
	    print("expiration: task '" .. task.name .. "' restarted")
	    task.restarts = task.restarts + 1
	    -- create worker fiber
	    curr_task = task
	    task.worker_fiber = box.fiber.create(worker_loop)
	    result = box.fiber.resume(task.worker_fiber)
	end
	box.fiber.sleep(task_constatns.check_interval)
    end
end


-- ------------------------------------------------------------------------- --
-- Task managemet
-- ------------------------------------------------------------------------- --

-- task list
local task_list = {}

-- create new expiration task
local function create_task(name)
    local task = {}
    task.name = name
    task.start_time = os.time()
    task.guardian_fiber = nil
    task.worker_fiber = nil
    task.space_no = nil
    task.cemetery_space_no = nil
    task.field_no = nil
    task.tuples_expired = 0
    task.restarts = 0
    task.tuples_per_iter = task_constatns.default_tuples_per_iter
    task.full_scann_time = task_constatns.default_full_scann_time
    return task
end

-- get task for table
local function get_task(name)
    if name == nil then
	error("task name undefined")
    end
    
    -- check, does the task exist
    if task_list[name] == nil then
	error("task '" .. name .. "' doesn't exist")
    end

    return task_list[name]
end

-- run task
local function run_task(task)
    -- save running task in local variable
    curr_task = task
    -- start guardian task
    task.guardian_fiber = box.fiber.create(guardian_loop)
    box.fiber.resume(task.guardian_fiber)
end

-- kill task
local function kill_task(task)
    if task.guardian_fiber ~= nil and task.guardian_fiber:id() ~= 0 then
	-- kill guardian fiber
	box.fiber.cancel(task.guardian_fiber)
    end
    if task.worker_fiber ~= nil and task.worker_fiber:id() ~= 0 then
	-- kill worker fiber
	box.fiber.cancel(task.worker_fiber)
    end
end


-- ========================================================================= --
-- Expriration daemon global variables
-- ========================================================================= --

-- main table
exprd = {
    -- enable/disable debug functions
    _debug = false,
}


-- ========================================================================= --
-- Expriration daemon management functions
-- ========================================================================= --

--
-- Run named task
-- params:
--    name -- is taks's name
--    space_no -- task's processing space
--    field_no -- tuple's expire timestamp (integer unix time)
--    cemetery_space_no -- cemetery space for expired tuples
--    tuples_per_iter -- number of tuples will be checked by one itaration
--    full_scann_time -- time requed for full index scann (in seconds)
--
function exprd.run_task(name,
		       space_no,
		       field_no,
		       cemetery_space_no,
		       tuples_per_iter,
		       full_scann_time)
    if name == nil then
	error("task name undefined")
    end

    -- check, does the task exist
    if task_list[name] ~= nil then
	error("task '" .. name .. "' already running")
    end
    local task = create_task(name)

    --
    -- check
    --

    -- check expiration space number (required)
    if space_no == nil then
	error("expiration space must be specified")
    elseif space_no < 0 or space_no >= 256 then
	error("invalid expiration space number")
    end
    task.space_no = space_no

    -- check expiration field number (required)
    if field_no == nil then
	error("expiration field number must be specified")
    elseif field_no < 0 then
	error("invalid expiration field numbe")
    end
    task.field_no = field_no

    -- check cemetery space number (not required)
    if cemetery_space_no ~= nil then
	if cemetery_space_no < 0 or cemetery_space_no >= 256 then
	    error("invalid cemetery space number")
	end
	task.cemetery_space_no = cemetery_space_no
    end

    -- check tuples per iteration (not required)
    if tuples_per_iter ~= nil then
	if tuples_per_iter <= 0 then
	    error("invalid tuples per iteration parametr")
	end
	task.tuples_per_iter = tuples_per_iter
    end

    -- check full scann time
    if full_scann_time ~= nil then
	if full_scann_time <= 0 then
	    error("invalid full scann time")
	end
	task.full_scann_time = full_scann_time
    end

    --
    -- run task
    --

    -- put the task to table
    task_list[name] = task
    -- run
    run_task(task)
end

--
-- Kill named task
-- params:
--    name -- is taks's name
--
function exprd.kill_task(name)
    kill_task(get_task(name))
    task_list[name] = nil
end

--
-- Print task list in TSV table format
-- params:
--   print_head -- print table head
--
function exprd.show_task_list(print_head)
    if print_head == nil or print_head == true then
	print("name" .. "\t" ..
	      "exp.sn" .. "\t" ..
	      "cem.sn" .. "\t" ..
	      "expired" .. "\t" ..
	      "time")
	print("-----------------------------------")
    end
    for i, task in pairs(task_list) do
	print(task.name .. "\t" ..
	      task.space_no .. "\t" ..
	      task.cemetery_space_no .. "\t" ..
	      task.tuples_expired .. "\t" ..
	      math.floor(os.time() - task.start_time))
    end
end

function exprd.task_details(name)
    local task = get_task(name)
    print("name:                  ", task.name)
    print("start time:            ", math.floor(task.start_time))
    print("working time:          ", math.floor(os.time() - task.start_time))
    print("space number:          ", task.space_no)
    print("field number:          ", task.field_no)
    print("cemetery space number: ", task.cemetery_space_no)
    print("tuples per iteration:  ", task.tuples_per_iter)
    print("full index scann time: ", task.full_scann_time)
    print("tuples expited:        ", task.tuples_expired)
    print("restars:               ", task.restarts)
    print("guardian fid:          ", get_fid(task.guardian_fiber))
    print("worker fid:            ", get_fid(task.worker_fiber))
end


-- ========================================================================= --
-- Expriration module test functions
-- ========================================================================= --

-- generate email string
local function get_email(uid)
    local email = "test_" .. uid .. "@sex.com"
    return email
end

-- insert entry to space
local function add_entry(space_no, uid, email, expiration_time)
    box.replace(space_no, uid, email, expiration_time)
end

-- put test tuples
function exprd.put_test_tuples(space_no, total)
    if not exprd._debug then
	error("expiration daemon module's debug disabled")
    end

    local time = math.floor(os.time())
    for i = 0, total do
	add_entry(space_no, i, get_email(i), time + i)
    end

    -- tuple w/o expiration date
    uid = total + 1
    add_entry(space_no, uid, get_email(uid), "")

    -- tuple w/ invalid expiration date
    uid = total + 2
    add_entry(space_no, uid, get_email(uid), "some string in exp field")
end

-- print test tuples
function exprd.print_test_tuples(space_no)
    if not exprd._debug then
	error("expiration daemon module's debug disabled")
    end

    local index = box.space[space_no].index[0]
    for itr, tuple in index.idx.next, index.idx do
	print(tuple)
    end
end
