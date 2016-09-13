-- 
-- Implementation of queueing API.
--
-- A task in a queue can be in one of the following states:
-- READY initial state, the task is ready for execution
-- DELAYED initial state, the task will become ready
-- for execution when a timeout expires
-- TAKEN the task is being worked on
-- BURIED the task is dead: it's either complete, or
-- cancelled or otherwise should not be worked on
-- 
-- The following methods are supported:
-- 
-- ------------------
-- - Producer methods
-- ------------------
-- box.queue.enable(sno)
-- start queue in space sno
-- box.queue.disable(sno)
-- stop queue in space sno
--
-- box.queue.put(sno, tube, prio, delay, ttr, ttl, <tuple data>)
-- Creates a new task and stores it in the queue.
-- 
-- sno space number
-- tube queue number (default 0)
-- prio priority of task (default 0x7ff) the lower value the higher priority
-- delay if not 0, task execution is postponed
-- for the given timeout (in seconds, may be fractional)
-- ttr Time-To-Release in seconds (default 300s)
-- How many time give to task to be in state TAKEN until return into READY
-- ttl Time-To-Live in seconds (default infinity)
-- How long task should remain in the queue until discarded
-- <tuple data> the rest of the task parameters, are stored
-- in the tuple data and describe the task itself
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
-- id : i64
-- tube : i32 [ default 0]
-- prio : i32 [ default 0x7fff ]
-- status : STR[1] { one of 'R' 'T' 'D' 'B' }
-- runat : i64 {system field} Nearest time in epoch microseconds when task state should be changed
-- ttr : i64 microseconds
-- ttl : i64 microseconds
-- retry : i32 how many times task can be taken - deleted when reaches 0
--


if box.queue == nil then
	box.queue = {}
	box.queue.sequence = {}
	box.queue.enabled = {}
	box.queue.stat = {}
	box.queue.cnt = {}
	box.queue.in_collect = -1
else
	local enabled = box.queue.enabled
	local old = box.queue
	box.queue = {}
	box.queue.sequence = {}
	box.queue.stat = old.stat
	box.queue.cnt = old.cnt
	box.queue.in_collect = old.in_collect
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

local c_retry   = 7 -- count of how many times task can be retaken

-- local sno_cnt   = 1 -- counter space

local i_pk      = 0
local i_ready   = 1
local i_run     = 2

function box.queue.cmd_counter(tube, cmd)
	if box.queue.stat[tube] == nil then
		-- box.queue.stat[tube] = { added = 0; deleted = 0; }
		box.queue.stat[tube] = { added = 0; deleted = 0; ack = 0; release = 0; release_deleted = 0; delayed_ttl_exp_deleted = 0; ttl_exp_deleted = 0; ttr_exp_deleted = 0; take = 0; limits = 0; }
	end
	box.queue.stat[tube][cmd] = box.queue.stat[tube][cmd] + 1
end

function box.queue.id(sno)
	sno = tonumber(sno)
	if box.queue.sequence[ sno ] then
		box.queue.sequence[ sno ] = box.queue.sequence[ sno ] + 1ULL
	else
		local tuple = box.space[sno].index[i_pk]:max()
		local id = 1
		if tuple ~= nil then
			id = box.unpack('l',tuple[0]) + 0ULL
		end
		box.queue.sequence[ sno ] = id + 1ULL
	end
	return box.queue.sequence[ sno ]
end

function box.queue.put(sno, tube, limits, prio, delay, ttr, ttl, retry, ...)
	sno, tube, limits, prio, delay, ttr, ttl, retry = tonumber(sno), tonumber(tube), tonumber(limits), tonumber(prio), tonumber(delay), tonumber(ttr), tonumber(ttl), tonumber(retry)
	if not box.queue.enabled[sno] then 
		error("Space ".. sno .. " queue not enabled") 
	end	
	if prio == nil then prio = 0x7fff end
	if tube == nil then tube = 0 end
	if limits == nil then limits = 500000 end
	if delay == nil then delay = 0 end
	if retry == nil then retry = 5 end
	if ttr == nil or ttr == 0 then ttr = 300 end
	local status = 'R'
	local runat = 0ULL
		
	if box.queue.tube_stat(tube) >= limits then
		box.queue.cmd_counter(tube, "limits" )
		error("Limits reached")
	end

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
	-- box.counter.inc( sno_cnt, tube )
	box.queue.cnt_inc( tube )
	box.queue.cmd_counter(tube, "added" )
	local tuple = box.insert(sno, id, tube, prio, status, runat, ttr, ttl, retry,   ...)
	-- return tonumber64(id)
	-- local tuple = box.insert(sno, id, tube, prio, status, runat, ttr, ttl, retry,   ...)
	return tonumber64(id)
end

function box.queue.delete(sno, id)
	sno, id = tonumber(sno), tonumber64(id)
	if not box.queue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	local tuple = box.space[sno]:delete(id)
	if tuple then 
		-- box.counter.dec( sno_cnt, tuple[c_tube] )
		box.queue.cnt_dec( box.unpack('i',tuple[c_tube]) )
		box.queue.cmd_counter( box.unpack('i',tuple[c_tube]) , "deleted" )
	end
	return tuple
end

function box.queue.take(sno, tube)
	sno, tube = tonumber(sno), tonumber(tube)
	if not box.queue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	if tube == nil then tube = 0 end
	local x,one_ready = box.space[sno].index[i_ready]:next_equal( tube,'R' )
	if one_ready == nil then return end
	box.queue.cmd_counter(tube, "take" )
	-- task may remain in status T for an amount of ttr microseconds. so, put time + ttr into runat
	return box.update(
		sno, one_ready[0],
			"=p=p",
			-- c_fid, box.fiber.id(),
			c_status, 'T', -- taken
			c_runat, box.time64() + box.unpack('l', one_ready[ c_ttr ])
	)
end

function box.queue.prolong(sno, id, ttr)
	sno, id, ttr = tonumber(sno), tonumber64(id), tonumber(ttr)
	if not box.queue.enabled[sno] then
		error("Space " .. sno .. " queue not enabled")
	end

	if ttr == nil or ttr <= 0 then
		error("Invalid ttr (" .. ttr .. ") specified (only positive values allowed)")
	end

	return box.update(sno, id, "=p", c_runat, box.time64() + ttr*1E6)
end

function box.queue.tube_select(sno, tube, offset, limit, ...)
	sno, tube, offset, limit = tonumber(sno), tonumber(tube), tonumber(offset), tonumber(limit)
	if not box.queue.enabled[sno] then
		error("Space " .. sno .. " queue not enabled")
	end

	if tube == nil then
		error("tube not specified")
	end

	if offset == nil or offset < 0 then
		offset = 0
	end

	if limit == nil or limit <= 0 or limit > 2000 then
		limit = 2000
	end

	return box.select_limit(sno, i_ready, offset, limit, tube, ...)
end

function box.queue.ack( sno, id )
	sno, id = tonumber(sno), tonumber64(id)
	local tuple = box.space[sno]:delete(id)
	if tuple then 
		-- box.counter.dec( sno_cnt, tuple[c_tube] )
		box.queue.cnt_dec( box.unpack('i',tuple[c_tube]) )
		box.queue.cmd_counter( box.unpack('i',tuple[c_tube]) , "ack" )
	end
	return tuple
end

function box.queue.release( sno, id, prio, delay, ttr, ttl, retry )
	sno, id, prio, delay, ttr, ttl, retry = tonumber(sno), tonumber64(id), tonumber(prio), tonumber(delay), tonumber(ttr), tonumber(ttl), tonumber(retry)
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
	
	if retry == nil then
		retry = box.unpack('i',task[c_retry]) - 1
	end
	
	local status, runat
	if delay and delay > 0 then
		status = 'D'
		runat = box.time64() + tonumber64( delay * 1E6 )
	else
		status = 'R'
		runat = ttl
	end
	
	if retry > 0 then
		box.queue.cmd_counter( box.unpack('i',task[c_tube]) , "release" )
		return box.update(sno, id,
			"=p=p=p=p=p=p",
			c_prio, prio,
			c_status, status,
			c_runat, runat,
			c_ttr, ttr,
			c_ttl, ttl,
			c_retry, retry
		)
	else 
		-- box.counter.dec( sno_cnt, task[c_tube] )
		box.queue.cnt_dec( box.unpack('i',task[c_tube]) )
		box.queue.cmd_counter( box.unpack('i',task[c_tube]) , "release_deleted" )
		return box.space[sno]:delete(id)
	end
end




-- box.fiber.wrap(function()
	-- while true
		-- local r,e = pcall(function ()
			-- ... your code
			
		-- end)
		-- if not r then
			-- print("error in code"..e)
		-- end
	-- end

-- end)

function box.queue.collect(sno)
	sno = tonumber(sno)
	if not box.queue.enabled[sno] then error("Space ".. sno .. " queue not enabled") end
	local idx = box.space[sno].index[i_run]
	
	box.queue.in_collect = 0;
	do
		local c = 0;
		local it,tuple
		while true do
			c = c+1;
			if c%1000 == 0 then
				if( c > 50000) then print( "Big count for collect: "..c) end
				box.queue.in_collect = c;
				box.fiber.sleep(0);
			end
			it,tuple = idx:next(it)
			if it == nil then break end
			local runat = box.unpack('l',tuple[c_runat])
			if runat > box.time64() then
				break
			end
			-- print(tuple[c_status] .. ": " .. tostring( runat/1E6 ) .. "\n")
			if tuple[c_status] == 'D' then
				-- update tuple, set status = R, runat = ttl
				if ( box.unpack('l',tuple[c_ttl]) > box.time64() ) then
					-- print("Turn delayed into ready: ", tuple)
					box.update( sno,tuple[0],
							"=p=p",
							c_status, 'R',
							c_runat, box.unpack('l',tuple[c_ttl])
					)
				else
					-- print("Remove out delayed since TTL passed by: ", tuple )
					-- box.space[sno]:delete(tuple[0])
					box.delete( sno, tuple[0] )
					-- box.counter.dec( sno_cnt, tuple[c_tube] )
					box.queue.cnt_dec( box.unpack('i',tuple[c_tube]) )
					box.queue.cmd_counter( box.unpack('i',tuple[c_tube]) , "delayed_ttl_exp_deleted" )
				end
			elseif tuple[c_status] == 'R' then
				-- puff! Task run out of time. Delete
				-- print("Remove out of TTL: ", tuple )
				-- box.space[sno]:delete(tuple[0])
				box.delete( sno, tuple[0] )
				-- box.counter.dec( sno_cnt, tuple[c_tube] )
				box.queue.cnt_dec( box.unpack('i',tuple[c_tube]) )
				box.queue.cmd_counter( box.unpack('i',tuple[c_tube]) , "ttl_exp_deleted" )
			elseif tuple[c_status] == 'T' then
				-- TTR Expired. Give task back
				-- print("Turn taken into ready because of TTR: ", tuple)
				-- box.queue.taken[ tuple[0] ] = nil
				local retry = box.unpack('i',tuple[c_retry]) - 1
				if ( retry > 0 ) then
					box.update( sno,tuple[0],
							"=p=p=p",
							c_status, 'R',
							c_runat, box.unpack('l',tuple[c_ttl]),
							c_retry, retry
					)
				else
					-- box.counter.dec( sno_cnt, tuple[c_tube] )
					box.queue.cnt_dec( box.unpack('i',tuple[c_tube]) )
					box.queue.cmd_counter( box.unpack('i',tuple[c_tube]) , "ttr_exp_deleted" )
					box.space[sno]:delete(tuple[0])
				end
			end
		end
	end
	
	box.queue.in_collect = -1;
	
end

local function queue_watcher(sno)
	sno = tonumber(sno)
	box.fiber.detach()
	box.fiber.name("box.queue.watcher["..sno.."]")
	print("Starting queue watcher for space "..sno)
	while true do
		if box.cfg.replication_source == nil then
			local r,e = pcall(box.queue.collect,sno)
			if not r then print("collect died: "..e) end
		end
		box.fiber.sleep(1.0)
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

function box.queue.tube_stat(tube)
	tube = tonumber(tube)
	if box.queue.cnt[ tube ] == nil then 
		return 0
	else
		return box.queue.cnt[ tube ]
	end
end

function box.queue.total_stat()
	local stats = {}
	for tube,cnt in pairs(box.queue.cnt) do
		table.insert(stats, {tube, cnt})
	end
	return stats
end

function box.queue.cmd_stat_OLD()
	local stats = {}
	for tube,cnt in pairs(box.queue.stat) do 
		table.insert(stats, box.tuple.new({tonumber(tube), cnt["added"], cnt["deleted"] }) )
	end
	box.queue.stat = {} -- reset stats
	return stats
end

function box.queue.cmd_stat()
	local stats = {}
	for tube,cnt in pairs(box.queue.stat) do 
		local added = cnt["added"]
		local ack = cnt["ack"]
		local deleted = cnt["deleted"]
		local release = cnt["release"]
		local release_deleted = cnt["release_deleted"]
		local delayed_ttl_exp_deleted = cnt["delayed_ttl_exp_deleted"]
		local ttl_exp_deleted = cnt["ttl_exp_deleted"]
		local ttr_exp_deleted = cnt["ttr_exp_deleted"]
		local take = cnt["take"]
		local limits = cnt["limits"]
		if added == nil then added = 0 end
		if ack == nil then ack = 0 end
		if deleted == nil then deleted = 0 end
		if release == nil then release = 0 end
		if release_deleted == nil then release_deleted = 0 end
		if delayed_ttl_exp_deleted == nil then delayed_ttl_exp_deleted = 0 end
		if ttl_exp_deleted == nil then ttl_exp_deleted = 0 end
		if ttr_exp_deleted == nil then ttr_exp_deleted = 0 end
		if take == nil then take = 0 end
		if limits == nil then limits = 0 end
		-- print('tube:', tonumber(tube), ' added', added, ' deleted',deleted, ' ack',ack, ' release',release, ' release_deleted',release_deleted, ' delayed_ttl_exp_deleted',delayed_ttl_exp_deleted, ' ttl_exp_deleted',ttl_exp_deleted, ' ttr_exp_deleted',ttr_exp_deleted);
		-- table.insert(stats, box.tuple.new({tonumber(tube), added, deleted }) )
		table.insert(stats, box.tuple.new({tonumber(tube), added, deleted, ack, release, release_deleted, delayed_ttl_exp_deleted, ttl_exp_deleted, ttr_exp_deleted, take, limits }) )
	end
	box.queue.stat = {} -- reset stats
	return stats
end


function box.queue.cnt_init()
	local i = box.space[0].index[1];
	local it
	local key = 0;
	box.queue.cnt = {};
	local cnt0 = box.space[0].index[1]:count( 0 )
	if cnt0 > 0 then box.queue.cnt[ 0 ] = cnt0 end
	while (true) do
		it = i:iterator(box.index.GT,key)
		local tuple = it();
		if (not tuple) then break end
		box.queue.cnt[ box.unpack('i',tuple[1]) ] = box.space[0].index[1]:count( tuple[1] )
		key = tuple[1];
	end
	-- for tube,cnt in pairs(box.queue.cnt) do print( tube, ' ', cnt ) end
end

function box.queue.cnt_inc(tube)
	if box.queue.cnt[ tube ] ~= nil then
		box.queue.cnt[ tube ] = box.queue.cnt[ tube ] + 1
	else
		box.queue.cnt[ tube ] = 1
	end
	return box.queue.cnt[ tube ]
end

function box.queue.cnt_dec(tube)
	if box.queue.cnt[ tube ] == nil then return 0 end
	box.queue.cnt[ tube ] = box.queue.cnt[ tube ] - 1
	if box.queue.cnt[ tube ] <= 0 then 
		box.queue.cnt[ tube ] = nil
		return 0
	end
	return box.queue.cnt[ tube ]
end

-- ------------------------------------------

print( 'init.lua loaded' )

if not box.queue.enabled[0] then 
	box.queue.enable(0)
end

box.queue.cnt_init()
