-- 
-- Implementation of queueing API.
-- 
-- Faster and simplified version of queue.
--   No delayed tasks
--   No ttl
--   No ttr
--
--
-- A task in a queue can be in one of the following states:
-- READY      initial state, the task is ready for execution
-- TAKEN      the task is being  worked on
-- BURIED (TODO) the task is dead: it's either complete, or
--            cancelled or otherwise should not be worked on
-- 
-- The following methods are supported:
-- 
-- ------------------
-- - Producer methods
-- ------------------
-- box.fqueue.enable(sno)
--   start queue in space sno
-- box.fqueue.disable(sno)
--   stop queue in space sno
--
-- box.fqueue.put(sno, tube, prio, delay, ttr, ttl, <tuple data>)
--   Creates a new task and stores it in the queue.
-- 
-- sno          space number
-- tube         queue number (default 0)
-- prio         priority of task (default 0x7ff) the lower value the higher priority
-- delay        if not 0, task execution is postponed
--              for the given timeout (in seconds, may be fractional)
-- ttr          Time-To-Release in seconds (default 300s)
--              How many time give to task to be in state TAKEN until return into READY
-- ttl          Time-To-Live in seconds (default infinity)
--              How long task should remain in the queue until discarded
-- <tuple data> the rest of the task parameters, are stored
--              in the tuple data and describe the task itself
--
-- The task is added to the end of the queue.
-- This method returns a created tuple
-- created task.
--
-- box.fqueue.delete(sno, id)
-- Delets a taks by id.
-- Returns the contents of the deleted task.
--
-- ------------------
-- - Consumer methods
-- ------------------
-- 
-- box.fqueue.take(sno, tube, timeout)
-- 
-- Finds a task for execution and marks the task as TAKEN. 
-- Returns the task tuple.
-- A task is reserved to the consumer which issued
-- the request and will not be given to any other
-- consumer. 
-- 'timeout' is considered if the queue is empty
-- If timeout is 0, the request immediately returns nil.
-- If timeout is not given, the caller is suspended
-- until a task appears in the queue.
-- Otherwise, the caller is suspended until
-- for the duration of the timeout (in seconds). 
--
-- box.fqueue.release(sno, id, prio, delay, ttr, ttl)
-- If the task is assigned to the consumer issuing
-- the request, it's put back to the queue, in READY
-- state. If delay is given, next execution 
-- of the task is delayed for delay seconds.
-- If the task has not been previously taken 
-- by the consumer, an error is raised.
-- If passed any of prio, ttr, ttl, then they are updated
--
-- box.fqueue.ack(sno, id)
-- Mark the task as complete and delete it,
-- as long as it was TAKEN by the consumer issuing
-- the request. 
-- ---------------------------------------------
-- - How to configure a space to support a queue 
-- ---------------------------------------------
--
--
-- space[X].enabled = 1
-- space[X].index[0].type = "TREE"
-- space[X].index[0].unique = 1
-- space[X].index[0].key_field[0].fieldno = 0
-- space[X].index[0].key_field[0].type = "NUM64"
-- 
-- space[X].index[1].type = "TREE"
-- space[X].index[1].unique = 1
-- space[X].index[1].key_field[0].fieldno = 1    # tube id
-- space[X].index[1].key_field[0].type = "NUM"
-- space[X].index[1].key_field[1].fieldno = 3    # status
-- space[X].index[1].key_field[1].type = "STR"
-- space[X].index[1].key_field[2].fieldno = 2    # prio
-- space[X].index[1].key_field[2].type = "NUM"
-- space[X].index[1].key_field[3].fieldno = 0    # id
-- space[X].index[1].key_field[3].type = "NUM64"
-- 
-- space[X].index[2].type = "TREE"
-- space[X].index[2].unique = false
-- space[X].index[2].key_field[0].fieldno = 4    # runat
-- space[X].index[2].key_field[0].type = "NUM64"

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
-- 
-- The following task metadata is maintained by the
-- queue module and can be inspected at any time:
-- 
--   id     : i64
--   tube   : i32    [ default 0]
--   prio   : i32    [ default 0x7fff ]
--   status : STR[1] { one of 'R' 'T' 'D' 'B' }
--   runat  : i64    {system field} Nearest time in epoch microseconds when task state should be changed
--   ttr    : i64    microseconds
--   ttl    : i64    microseconds
--   taken  : i32    how many times task was taken
--   buried : i32    how many times task was buried
--

if box.fqueue == nil then
	box.fqueue = {}
	box.fqueue.taken = {}
	box.fqueue.sequence = {}
	box.fqueue.enabled = {}
else
	local taken = box.fqueue.taken
	local enabled = box.fqueue.enabled
	box.fqueue = {}
	box.fqueue.sequence = {}
	if taken then
		box.fqueue.taken = taken
	else
		box.fqueue.taken = {}
	end
	if enabled then
		box.fqueue.enabled = enabled
	else
		box.fqueue.enabled = {}
	end
end

-- space column description

local c_id      = 0 -- id, int64, used as sequence
local c_tube    = 1 -- tube id. int32
local c_prio    = 2 -- priority of task, int32
local c_status  = 3 -- status of task. available values: [ 'R' - ready, 'D' - delayed, 'T' - taken, 'B' - buried ]

local c_taken   = 4 -- count of how many times task was taken
local c_buried  = 5 -- count of how many times task was buried

local i_pk      = 0
local i_ready   = 1

function box.fqueue.id(sno)
	sno = tonumber(sno)
	if box.fqueue.sequence[ sno ] then
		box.fqueue.sequence[ sno ] = box.fqueue.sequence[ sno ] + 1ULL
	else
		local tuple = box.space[sno].index[i_pk]:max()
		local id = 2^25 -- 33554432
		if tuple ~= nil then
			id = box.unpack('l',tuple[0]) + 0ULL
		end
		box.fqueue.sequence[ sno ] = id + 1ULL
	end
	return box.fqueue.sequence[ sno ]
end

function box.fqueue.put(sno, tube, prio, ...)
	sno, tube, prio= tonumber(sno), tonumber(tube), tonumber(prio)
	if not box.fqueue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	if prio == nil then prio = 0x7fff end
	if tube == nil then tube = 0 end
	local status = 'R'
	local id = box.fqueue.id(sno)
	--                     id, tube, prio, status, taken, buried,
	return box.insert(sno, id, tube, prio, status, 0,     0,      ...)
end

function box.fqueue.delete(sno, id)
	sno, id = tonumber(sno), tonumber64(id)
	if not box.fqueue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	box.fqueue.taken[ box.pack('l',id) ] = nil
	return box.space[sno]:delete(id)
end


function box.fqueue.take(sno, tube)
	sno, tube = tonumber(sno), tonumber(tube)
	if not box.fqueue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	if tube == nil then tube = 0 end
	local x,one_ready = box.space[sno].index[i_ready]:next_equal( tube,'R' )
	if one_ready == nil then return end
	print("Selected "..tostring( box.unpack('l',one_ready[0]) ).." for take ")
	box.fqueue.taken[ one_ready[0] ] = box.fiber.id()
	return box.update(
		sno, one_ready[0],
			"=p+p",
			-- c_fid, box.fiber.id(),
			c_status, 'T', -- taken
			c_taken, 1
	)
end

local function consumer_check_task(sno, pid)
	if not box.fqueue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	local taken_by = box.fqueue.taken[ pid ]
	print("Task "..tostring(box.unpack('l',pid)).." taken by " .. tostring(taken_by))
	local task
	if taken_by == box.fiber.id() then
		-- don't select task. if it will be release, then select it inside release
		-- task = box.select(sno, i_pk, id)
		return true
	elseif taken_by then
		error(string.format( "Task taken by %d. Not you (%d)", taken_by, box.fiber.id() ))
	else
		--for k,v in pairs(box.fqueue.taken) do
		--	print( string.format("Task %s taken by %d", tostring( k ), v) )
		--end
		error("Task "..tostring(box.unpack('l',pid)).." not taken by any")
	end
end


function box.fqueue.ack( sno, id )
	sno, id = tonumber(sno), tonumber64(id)
	local pid = box.pack('l',id)
	consumer_check_task(sno,pid)
	box.fqueue.taken[ pid ] = nil
	box.space[sno]:delete( id )
end

function box.fqueue.release( sno, id, prio )
	sno, id, prio = tonumber(sno), tonumber64(id), tonumber(prio)
	local pid = box.pack('l',id)
	consumer_check_task(sno,pid)
	local task = box.select(sno, i_pk, id)
	-- if something is set, then update and recalculate
	if prio == nil then
		prio = box.unpack('i',task[c_prio])
	end
	
	box.fqueue.taken[ pid ] = nil
	return box.update(sno, id,
		"=p=p=p=p=p",
		c_prio, prio,
		c_status, 'R',
		c_runat, runat,
		c_ttr, ttr,
		c_ttl, ttl
	)

end

function box.fqueue.collect(sno)
	sno = tonumber(sno)
	if not box.fqueue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	local idx = box.space[sno].index[i_ready]
	do
		local it,tuple = idx:next_equal( tube,'T' )
		while it do
			if tuple[c_status] ~= 'T' then break end
			if not box.fqueue.taken[ tuple[0] ] or not box.fiber.find( box.fqueue.taken[ tuple[0] ] ) then
				print(string.format( "Return task %s to ready. No fiber %d", tostring( box.unpack('l', tuple[0]) ), box.fqueue.taken[ tuple[0] ] ))
				box.fqueue.taken[ tuple[0] ] = nil
				box.update( sno,tuple[0],
					"=p",
					c_status, 'R'
				)
			end
			it,tuple = idx:next(it)
		end
	end
	-- collectgarbage("step")
	return
end

local function queue_watcher(sno)
	sno = tonumber(sno)
	box.fiber.detach()
	box.fiber.name("box.fqueue.watcher["..sno.."]")
	print("Starting queue watcher for space "..sno)
	while true do
		-- box.fiber.testcancel()
		--for i = 1,100,1 do
		box.fqueue.collect(sno)
		--end
		box.fiber.sleep(0.1)
		--box.fiber.sleep(0)
	end
end

function box.fqueue.enable(sno)
	sno = tonumber(sno)
	if box.fqueue.enabled[sno] then error("Space ".. sno .. " queue already enabled") end
	
	local fiber = box.fiber.create(queue_watcher)
	
	print("created fiber ", box.fiber.id(fiber))
	box.fqueue.enabled[sno] = box.fiber.id( fiber )
	box.fiber.resume(fiber, sno)
	return
end

function box.fqueue.disable(sno)
	sno = tonumber(sno)
	if not box.fqueue.enabled[sno] then
		print("Space ".. sno .. " queue not enabled")
		return
	end
	local fiber = box.fiber.find( box.fqueue.enabled[sno] )
	if not fiber then
		print("Cannot find fiber by id " .. box.fqueue.enabled[sno])
		box.fqueue.enabled[sno] = nil
		return
	end
	print("found created fiber: ", box.fiber.id(fiber))
	box.fiber.cancel( fiber )
	box.fqueue.enabled[sno] = nil
end

