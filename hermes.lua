--
-- hermes.lua
--

--
-- Folder bindings for imap collector.
--

--
-- Space 0: Folder Bindings and IMAP Collector State.
--   Tuple: { coll_id (NUM), rmt_fld_id (STR), fld_id (NUM), uid_validity (NUM), up_uid (NUM), down_uid (NUM), up_date (NUM), down_date (NUM), modseq (STR), state (STR) }
--   Index 0: TREE { coll_id, rmt_fld_id }
--
-- Space 1: Errors that occurred during synchronization of local changes on the remote side.
--   Tuple: { email (STR), error_index (NUM), description (STR) }
--   Index 0: TREE { email, error_index }
--
--   Description field contains some tarantool independent information about a problem.
--   Example:
--     'test@mail.ru': {1, 'error while appending message on storage'}
--

function hermes_get(coll_id)
	coll_id = box.unpack('i', coll_id)
	return box.select_limit(0, 0, 0, 1000000, coll_id)
end

function hermes_drop(coll_id)
	coll_id = box.unpack('i', coll_id)
	local tuples = { box.select_limit(0, 0, 0, 1000000, coll_id) }
	for _, tuple in pairs(tuples) do
		box.delete(0, coll_id, tuple[1])
	end
end

function hermes_rebase(coll_id)
	coll_id = box.unpack('i', coll_id)
	for tuple in box.space[0].index[0]:iterator(box.index.EQ, coll_id) do
		box.update(0, { coll_id, tuple[1] }, "=p", 2, -1)
	end
end

function hermes_update(coll_id, rmt_fld_id, fld_id, uid_validity, up_uid, down_uid, up_date, down_date)
	coll_id = box.unpack('i', coll_id)
	fld_id = box.unpack('i', fld_id)
	uid_validity = box.unpack('i', uid_validity)
	up_uid = box.unpack('i', up_uid)
	down_uid = box.unpack('i', down_uid)
	up_date = box.unpack('i', up_date)
	down_date = box.unpack('i', down_date)

	local status, t = pcall(box.update, 0, { coll_id, rmt_fld_id }, "=p=p=p=p=p=p", 2, fld_id, 3, uid_validity, 4, up_uid, 5, down_uid, 6, up_date, 7, down_date)
	if not status or t == nil then
		box.replace(0, coll_id, rmt_fld_id, fld_id, uid_validity, up_uid, down_uid, up_date, down_date)
	end
end

function hermes_drop_by_rmt_fld(coll_id, rmt_fld_id)
	coll_id = box.unpack('i', coll_id)
	box.delete(0, coll_id, rmt_fld_id)
end

function hermes_drop_by_fld(coll_id, fld_id)
	coll_id = box.unpack('i', coll_id)
	fld_id = box.unpack('i', fld_id)
	local tuples = { box.select_limit(0, 0, 0, 1000000, coll_id) }
	for _, tuple in pairs(tuples) do
		if box.unpack('i', tuple[2]) == fld_id then
			box.delete(0, coll_id, tuple[1])
		end
	end
end

function hermes_update_up_uid(coll_id, rmt_fld_id, up_uid, up_date)
	coll_id = box.unpack('i', coll_id)
	up_uid = box.unpack('i', up_uid)
	up_date = box.unpack('i', up_date)
	local t = box.update(0, { coll_id, rmt_fld_id }, "=p=p", 4, up_uid, 6, up_date)
	if t ~= nil then return 1 else return 0 end
end

function hermes_update_rmt_fld(coll_id, fld_id, new_rmt_fld_id)
	coll_id = box.unpack('i', coll_id)
	fld_id = box.unpack('i', fld_id)
	local tuples = { box.select_limit(0, 0, 0, 1000000, coll_id) }
	for _, tuple in pairs(tuples) do
		if box.unpack('i', tuple[2]) == fld_id then
			box.update(0, { coll_id, tuple[1] }, "=p", 1, new_rmt_fld_id)
			return 1
		end
	end
	return 0
end

function hermes_update_modseq(coll_id, rmt_fld_id, modseq)
	coll_id = box.unpack('i', coll_id)
	local t = box.update(0, { coll_id, rmt_fld_id }, "=p", 8, modseq)
	if t ~= nil then return 1 else return 0 end
end

function hermes_update_state(coll_id, rmt_fld_id, state)
	coll_id = box.unpack('i', coll_id)
	local status, t = pcall(box.update, 0, { coll_id, rmt_fld_id }, "=p", 9, state)
	if not status then
		t = box.update(0, { coll_id, rmt_fld_id }, "=p=p", 8, "", 9, state)
	end
	if t ~= nil then return 1 else return 0 end
end

local function get_key_cardinality_and_max_msgid(key)
	-- TODO: optimize (replace by calculation cardinality of a key in the index)
	local data = { box.select(1, 0, key) }
	local n = #data
	if n == 0 then
		return 0, 0
	end
	return n, box.unpack('i', data[n][1])
end

function hermes_error_add(key, err_content)
	local done = nil
	while not done do
		local _, max_id = get_key_cardinality_and_max_msgid(key)
		done = box.insert(1, key, max_id + 1, err_content)
	end
end

function hermes_errors_del(key, ...)
	local local_indexes = {...}
	for i = 1, #local_indexes do
		box.delete(1, key, box.unpack('i', local_indexes[i]))
	end
end

function hermes_error_replace(key, err_no, new_content)
	err_no = box.unpack('i', err_no)
	box.replace(1, key, err_no, new_content)
end

function hermes_errors_get(key)
	return { box.select(1, 0, key) }
end
