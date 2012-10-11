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
-- box.queue.enable(sno)
--   start queue in space sno
-- box.queue.disable(sno)
--   stop queue in space sno
--
-- box.queue.put(sno, tube, prio, delay, ttr, ttl, <tuple data>)
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
-- box.queue.delete(sno, id)
-- Delets a taks by id.
-- Returns the contents of the deleted task.
--
-- ------------------
-- - Consumer methods
-- ------------------
-- 
-- box.queue.take(sno, tube, timeout)
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
-- box.queue.release(sno, id, prio, delay, ttr, ttl)
-- If the task is assigned to the consumer issuing
-- the request, it's put back to the queue, in READY
-- state. If delay is given, next execution 
-- of the task is delayed for delay seconds.
-- If the task has not been previously taken 
-- by the consumer, an error is raised.
-- If passed any of prio, ttr, ttl, then they are updated
--
-- box.queue.ack(sno, id)
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

if box.queue == nil then
	box.queue = {}
	box.queue.taken = {}
	box.queue.sequence = {}
	box.queue.enabled = {}
else
	local taken = box.queue.taken
	local enabled = box.queue.enabled
	box.queue = {}
	box.queue.sequence = {}
	if taken then
		box.queue.taken = taken
	else
		box.queue.taken = {}
	end
	if enabled then
		box.queue.enabled = enabled
	else
		box.queue.enabled = {}
	end
end

-- space column description


local c_id      = 0 -- id, int64, used as sequence
local c_tube    = 1 -- tube id. int32
local c_prio    = 2 -- priority of task, int32
local c_status  = 3 -- status of task. available values: [ 'R' - ready, 'D' - delayed, 'T' - taken, 'B' - buried ]
local c_runat   = 4 -- time, when task should be accounted as ready. microseconds. int64. box.time64()
local c_ttr     = 5 -- Time-To-Release. time amount task allowed to be taken. microseconds. int64.
local c_ttl     = 6 -- Time-To-Live. time amount task allowed to be in queue. microseconds. int64.


local c_taken   = 7 -- count of how many times task was taken
local c_buried  = 8 -- count of how many times task was buried

local i_pk      = 0
local i_ready   = 1
local i_run     = 2

function box.queue.id(sno)
	sno = tonumber(sno)
	if box.queue.sequence[ sno ] then
		box.queue.sequence[ sno ] = box.queue.sequence[ sno ] + 1ULL
	else
		local tuple = box.space[sno].index[i_pk]:max()
		local id = 2^25 -- 33554432
		if tuple ~= nil then
			id = box.unpack('l',tuple[0]) + 0ULL
		end
		box.queue.sequence[ sno ] = id + 1ULL
	end
	return box.queue.sequence[ sno ]
end

function box.queue.put(sno, tube, prio, delay, ttr, ttl, ...)
	sno, tube, prio, delay, ttr, ttl = tonumber(sno), tonumber(tube), tonumber(prio), tonumber(delay), tonumber(ttr), tonumber(ttl)
	if not box.queue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	if prio == nil then prio = 0x7fff end
	if delay == nil then delay = 0 end
	if tube == nil then tube = 0 end
	if ttr == nil or ttr == 0 then ttr = 300 end
	local status = 'R'
	local runat = 0ULL
	
	if delay and ttl and delay > ttl then
		error("Delay can't be longer than TTL")
	end
	
	if ttl ~= nil and ttl > 0 then
		ttl = box.time64() + tonumber64( ttl * 1E6 )
	else
		ttl = 0xffffffffffffffffULL
	end
	
	if delay > 0 then
		status = 'D'
		runat = box.time64() + tonumber64( delay * 1E6 )
	else
		runat = ttl
	end
	if ttr ~= nil and ttr > 0 then
		ttr = tonumber64( ttr * 1E6 )
	else
		ttr = 300000000ULL -- 300 seconds
	end
	local id = box.queue.id(sno)
	--                     id, tube, prio, status, runat  ttr, ttl, taken, buried,
	return box.insert(sno, id, tube, prio, status, runat, ttr, ttl, 0,     0,      ...)
end

function box.queue.putdef(sno, tube, ...)
	return box.queue.put(sno,tube,nil,nil,nil,nil,...)
end


function box.queue.push(sno, prio, ttr, ...)
	error("TODO: index search")
end

function box.queue.delete(sno, id)
	sno, id = tonumber(sno), tonumber64(id)
	if not box.queue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	box.queue.taken[ box.pack('l',id) ] = nil
	return box.space[sno]:delete(id)
end


function box.queue.take(sno, tube, timeout)
	sno, tube, timeout = tonumber(sno), tonumber(tube), tonumber(timeout)
	if not box.queue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	if tube == nil then tube = 0 end
	local idx = box.space[sno].index[i_ready].idx
	local x,one_ready = box.space[sno].index[i_ready]:next_equal( tube,'R' )
	if one_ready == nil then return end
	
	box.queue.taken[ one_ready[0] ] = box.fiber.id()
	-- task may remain in status T for an amount of ttr microseconds. so, put time + ttr into runat
	return box.update(
		sno, one_ready[0],
			"=p=p+p",
			-- c_fid, box.fiber.id(),
			c_status, 'T', -- taken
			c_runat, box.time64() + box.unpack('l', one_ready[ c_ttr ]),
			c_taken, 1
	)
end

-- original version of take. Should be slightly rewrittend
function box.queue.take1(sno, tube, timeout)
	sno, tube, timeout = tonumber(sno), tonumber(tube), tonumber(timeout)
	if not box.queue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	-- if timeout == nil then timeout = 60*60*24*365 end
	if timeout == nil then timeout = 60 end
	if tube == nil then tube = 0 end
	local sleep = 0;
	-- don't sleep for all the timeout. Split it into parts
	if timeout > 0 then
		sleep = timeout / 10;
		if sleep > 1 then
			sleep = 0.1
		end
	end
	local finish = box.time() + timeout;
	local idx = box.space[sno].index[i_ready].idx
	-- print("Running take at "..box.time()..", until "..finish.."\n")
	
	local one_ready
	while true do
		local x
		x,one_ready = box.space[sno].index[i_ready]:next_equal( tube,'R' )
		if one_ready then
			break
		end
		-- print("No task found. Sleeping for "..sleep.."\n");
		if box.time() >= finish then break end
		box.fiber.sleep( sleep )
	end
	if one_ready == nil then return end
	box.queue.taken[ one_ready[0] ] = box.fiber.id()
	-- task may remain in status T for an amount of ttr microseconds. so, put time + ttr into runat
	return box.update(
		sno, one_ready[0],
			"=p=p+p",
			-- c_fid, box.fiber.id(),
			c_status, 'T', -- taken
			c_runat, box.time64() + box.unpack('l', one_ready[ c_ttr ]),
			c_taken, 1
	)
end

local function consumer_check_task(sno, id)
	sno, id = tonumber(sno), tonumber64(id)
	if not box.queue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	local taken_by = box.queue.taken[ box.pack('l',id) ]
	local task
	if taken_by == box.fiber.id() then
		-- don't select task. if it will be release, then select it inside release
		-- task = box.select(sno, i_pk, id)
		return true
	elseif taken_by then
		error(string.format( "Task taken by %d. Not you (%d)", taken_by, box.fiber.id() ))
	else
		error("Task not taken by any")
	end
end


function box.queue.ack( sno, id )
	sno, id = tonumber(sno), tonumber64(id)
	consumer_check_task(sno,id)
	box.queue.taken[ box.pack('l',id) ] = nil
	box.space[sno]:delete( id )
end

function box.queue.release( sno, id, prio, delay, ttr, ttl )
	sno, id, prio, delay, ttr, ttl = tonumber(sno), tonumber64(id), tonumber(prio), tonumber(delay), tonumber(ttr), tonumber(ttl)
	consumer_check_task(sno,id)
	local task = box.select(sno, i_pk, id)
	-- if something is set, then update and recalculate
	if delay and ttl and delay > ttl then
		error("Delay can't be longer than TTL")
	end
	
	if prio == nil then
		prio = box.unpack('i',task[c_prio])
	end
	
	if ttr ~= nil and ttr > 0 then
		ttr = tonumber64( ttr * 1E6 )
	else
		ttr = box.unpack('l',task[c_ttr])
	end
	
	if ttl ~= nil and ttl > 0 then
		ttl = box.time64() + tonumber64( ttl * 1E6 )
	else
		ttl = box.unpack('l',task[c_ttl])
	end
	
	local status, runat
	if delay and delay > 0 then
		status = 'D'
		runat = box.time64() + tonumber64( delay * 1E6 )
	else
		status = 'R'
		runat = ttl
	end
	
	box.queue.taken[ box.pack('l',id) ] = nil
	
	return box.update(sno, id,
		"=p=p=p=p=p",
		c_prio, prio,
		c_status, status,
		c_runat, runat,
		c_ttr, ttr,
		c_ttl, ttl
	)

end

function box.queue.collect(sno)
	sno = tonumber(sno)
	if not box.queue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	local idx = box.space[sno].index[i_run]
	
	do
		local it,tuple
		while true do
			it,tuple = idx:next(it)
			-- it,tuple = idx.next(idx,it)
			if it == nil then break end
			local runat = box.unpack('l',tuple[c_runat])
			if runat > box.time64() then
				break
			end
			print(tuple[c_status] .. ": " .. tostring( runat/1E6 ) .. "\n")
			if tuple[c_status] == 'D' then
				-- update tuple, set status = R, runat = ttl
				if ( box.unpack('l',tuple[c_ttl]) > box.time64() ) then
					print("Turn delayed into ready: ", tuple)
					box.update( sno,tuple[0],
							"=p=p",
							c_status, 'R',
							c_runat, box.unpack('l',tuple[c_ttl])
					)
				else
					print("Remove out delayed since TTL passed by: ", tuple )
					-- box.space[sno]:delete(tuple[0])
					box.delete( sno, tuple[0] )
				end
			elseif tuple[c_status] == 'R' then
				-- puff! Task run out of time. Delete
				print("Remove out of TTL: ", tuple )
				-- box.space[sno]:delete(tuple[0])
				box.delete( sno, tuple[0] )
			elseif tuple[c_status] == 'T' then
				-- TTR Expired. Give task back
				print("Turn taken into ready because of TTR: ", tuple)
				box.queue.taken[ tuple[0] ] = nil
				box.update( sno,tuple[0],
						"=p=p",
						c_status, 'R',
						c_runat, box.unpack('l',tuple[c_ttl])
				)
			end
		end
	end
	-- collectgarbage("step")
	return
end

local function queue_watcher(sno)
	sno = tonumber(sno)
	box.fiber.detach()
	box.fiber.name("box.queue.watcher["..sno.."]")
	print("Starting queue watcher for space "..sno)
	while true do
		-- box.fiber.testcancel()
		--for i = 1,100,1 do
		box.queue.collect(sno)
		--end
		box.fiber.sleep(0.001)
		--box.fiber.sleep(0)
	end
end

function box.queue.enable(sno)
	sno = tonumber(sno)
	if box.queue.enabled[sno] then error("Space ".. sno .. " queue already enabled") end
	
	local fiber = box.fiber.create(queue_watcher)
	
	print("created fiber ", box.fiber.id(fiber))
	box.queue.enabled[sno] = box.fiber.id( fiber )
	box.fiber.resume(fiber, sno)
	return
end

function box.queue.disable(sno)
	sno = tonumber(sno)
	if not box.queue.enabled[sno] then
		print("Space ".. sno .. " queue not enabled")
		return
	end
	local fiber = box.fiber.find( box.queue.enabled[sno] )
	if not fiber then
		print("Cannot find fiber by id " .. box.queue.enabled[sno])
		box.queue.enabled[sno] = nil
		return
	end
	print("found created fiber: ", box.fiber.id(fiber))
	box.fiber.cancel( fiber )
	box.queue.enabled[sno] = nil
end

