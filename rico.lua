--
-- rico.lua
--

--
-- Remote Imap collector COunters. Stores some scheduling information needed for collectors.
--

--
-- Space 0: Stores last collect time, last success collect time and so on.
--   Tuple: { coll_id (NUM), last_time (NUM), last_ok (NUM), old_threshold (NUM), last_fullsync (NUM), success_collects_num (NUM) }
--   Index 0: TREE { coll_id }
--
-- Space 1: Stores last collect time, last success collect time and so on for another type of collectors (pop3).
--   Tuple: { coll_id (NUM), last_time (NUM), last_ok (NUM), old_threshold (NUM), last_fullsync (NUM), success_collects_num (NUM) }
--   Index 0: TREE { coll_id }
--

function rico_get(coll_id)
	coll_id = box.unpack('i', coll_id)
	local t = box.select_limit(0, 0, 0, 1, coll_id)
	if t ~=nil then return t:transform(0,1) end
end

function rico_get_lasttimes(from_coll_id, to_coll_id)
	from_coll_id = box.unpack('i', from_coll_id)
	to_coll_id = box.unpack('i', to_coll_id)

	local result = {}
	for t in box.space[0].index[0]:iterator(box.index.GE, from_coll_id) do
		if box.unpack('i', t[0]) >= to_coll_id then break end
		table.insert(result, { t:slice(0, 2) })
	end
	if #result > 0 then return result end
	return unpack(result)
end

function rico_reset(coll_id)
	coll_id = box.unpack('i', coll_id)
        box.replace(0, coll_id, box.time(), box.time(), box.time(), box.time(), 0)
end

function rico_update_success(coll_id, need_update_old_threshold, need_update_last_fullsync)
	coll_id = box.unpack('i', coll_id)
	need_update_old_threshold = box.unpack('i', need_update_old_threshold)
	need_update_last_fullsync = box.unpack('i', need_update_last_fullsync)

	local status, t
	if need_update_old_threshold ~= 0 and need_update_last_fullsync ~= 0 then
		status, t = pcall(box.update, 0, { coll_id }, "=p=p=p=p+p", 1, box.time(), 2, box.time(), 3, box.time(), 4, box.time(), 5, 1)
	elseif need_update_old_threshold ~= 0 then
		status, t = pcall(box.update, 0, { coll_id }, "=p=p=p+p", 1, box.time(), 2, box.time(), 3, box.time(), 5, 1)
	elseif need_update_last_fullsync ~= 0 then
		status, t = pcall(box.update, 0, { coll_id }, "=p=p=p+p", 1, box.time(), 2, box.time(), 4, box.time(), 5, 1)
	else
		status, t = pcall(box.update, 0, { coll_id }, "=p=p+p", 1, box.time(), 2, box.time(), 5, 1)
	end

	if not status or t == nil then
		box.replace(0, coll_id, box.time(), box.time(), box.time(), box.time(), 1)
	end
end

function rico_update_failure(coll_id, need_update_last_fullsync)
	coll_id = box.unpack('i', coll_id)
	need_update_last_fullsync = box.unpack('i', need_update_last_fullsync)

	local status, t
	if need_update_last_fullsync ~= 0 then
		status, t = pcall(box.update, 0, { coll_id }, "=p=p", 1, box.time(), 4, box.time())
	else
		status, t = pcall(box.update, 0, { coll_id }, "=p", 1, box.time())
	end

	if not status or t == nil then
		box.replace(0, coll_id, box.time(), box.time(), box.time(), box.time(), 0)
	end
end

function rico_drop(coll_id)
	coll_id = box.unpack('i', coll_id)
	local t = box.delete(0, coll_id)
end



------------------------------
--    POP3                  --
------------------------------

function rico_get_pop3(coll_id)
	coll_id = box.unpack('i', coll_id)
	local t = box.select_limit(1, 0, 0, 1, coll_id)
	if t ~=nil then return t:transform(0,1) end
end

function rico_get_lasttimes_pop3(from_coll_id, to_coll_id)
	from_coll_id = box.unpack('i', from_coll_id)
	to_coll_id = box.unpack('i', to_coll_id)

	local result = {}
	for t in box.space[1].index[0]:iterator(box.index.GE, from_coll_id) do
		if box.unpack('i', t[0]) >= to_coll_id then break end
		table.insert(result, { t:slice(0, 2) })
	end
	if #result > 0 then return result end
	return unpack(result)
end

function rico_reset_pop3(coll_id)
    coll_id = box.unpack('i', coll_id)
    box.replace(1, coll_id, box.time(), box.time(), box.time(), box.time(), 0)
end

function rico_update_success_pop3(coll_id, need_update_old_threshold, need_update_last_fullsync)
	coll_id = box.unpack('i', coll_id)
	need_update_old_threshold = box.unpack('i', need_update_old_threshold)
	need_update_last_fullsync = box.unpack('i', need_update_last_fullsync)

	local status, t
	if need_update_old_threshold ~= 0 and need_update_last_fullsync ~= 0 then
		status, t = pcall(box.update, 1, { coll_id }, "=p=p=p=p+p", 1, box.time(), 2, box.time(), 3, box.time(), 4, box.time(), 5, 1)
	elseif need_update_old_threshold ~= 0 then
		status, t = pcall(box.update, 1, { coll_id }, "=p=p=p+p", 1, box.time(), 2, box.time(), 3, box.time(), 5, 1)
	elseif need_update_last_fullsync ~= 0 then
		status, t = pcall(box.update, 1, { coll_id }, "=p=p=p+p", 1, box.time(), 2, box.time(), 4, box.time(), 5, 1)
	else
		status, t = pcall(box.update, 1, { coll_id }, "=p=p+p", 1, box.time(), 2, box.time(), 5, 1)
	end

	if not status or t == nil then
		box.replace(1, coll_id, box.time(), box.time(), box.time(), box.time(), 1)
	end
end

function rico_update_failure_pop3(coll_id, need_update_last_fullsync)
	coll_id = box.unpack('i', coll_id)
	need_update_last_fullsync = box.unpack('i', need_update_last_fullsync)

	local status, t
	if need_update_last_fullsync ~= 0 then
		status, t = pcall(box.update, 1, { coll_id }, "=p=p", 1, box.time(), 4, box.time())
	else
		status, t = pcall(box.update, 1, { coll_id }, "=p", 1, box.time())
	end

	if not status or t == nil then
		box.replace(1, coll_id, box.time(), box.time(), box.time(), box.time(), 0)
	end
end

function rico_drop_pop3(coll_id)
	coll_id = box.unpack('i', coll_id)
	local t = box.delete(1, coll_id)
end
