--
-- irina.lua
--

--
-- Space 0: Remote IMAP Collector Accounts (Instant and Usual)
--   Tuple: { email (STR), user_id (NUM), is_instant (NUM), is_expirable (NUM), inst_from (NUM), shard_id (NUM) }
--   Index 0: HASH { email }
--   Index 1: TREE { is_instant, shard_id }
--
-- Space 1: Remote IMAP Collector Listeners
--   Tuple: { shard_id (NUM), addr (STR) }
--   Index 0: TREE { shard_id }
--

local N_SHARDS = 1024

local function send_instant_cmd(addr, cmd)
	local msg = "[instant: " .. addr .. ", cmd: " .. cmd .. "]"

	local ind = addr:find(":")
	if ind == nil then return "invalid address " .. msg end
	local host = addr:sub(0, ind - 1)
	local port = tonumber(addr:sub(ind + 1))

	local s = box.socket.tcp()
	if s == nil then return "can not create socket " .. msg end

	if not s:connect(host, port, 0.1) then
		local _, errstr = s:error()
		s:close()
		return "can not connect " .. msg .. ": " .. errstr
	end

	local bytes_sent, status, errno, errstr = s:send(cmd, 0.1)
	if bytes_sent ~= #cmd then
		local _, errstr = s:error()
		s:close()
		return "can not send data " .. msg .. ": " .. errstr
	end

	local response = s:readline()

	if response == nil then
		local _, errstr = s:error()
		s:close()
		return "empty response " .. msg .. ": " .. errstr
	end

	s:close()

	return "instant response " .. msg .. ": " .. response
end

local function get_collector_address(shardid)
	local v = box.select_limit(1, 0, 0, 1, shardid)
	if v == nil then return nil end
	return v[1]
end

--
-- Send signals to Instant Remote IMAP Collector daemon
--
local function send_change_status(email, userid, shardid, enabled, expirable)
	local addr = get_collector_address(shardid)
	if addr == nil then return "shard does not binded" end

	local data = email .. " " .. userid .. " " .. enabled .. " " .. expirable
	return send_instant_cmd(addr, data)
end
local function send_add_shard(addr, shardid)
	local data = "add_shard " .. shardid
	return send_instant_cmd(addr, data)
end
local function send_del_shard(addr, shardid)
	local data = "del_shard " .. shardid
	return send_instant_cmd(addr, data)
end

local function bind_shards_to_addr(errors, shards, addr, notify_added_shard, notify_deleted_shard)
	local need_wait = false
	for _, shardid in pairs(shards) do
		local prev_addr = get_collector_address(shardid)
		box.replace(1, shardid, addr)
		if prev_addr ~= nil and notify_deleted_shard ~= 0 then
			local msg = send_del_shard(prev_addr, shardid)
			table.insert(errors, box.tuple.new({msg}))
			need_wait = true
		end
	end

	if notify_added_shard == 0 then return end

	-- give the time for applying shard deletion
	if need_wait then box.fiber.sleep(5) end

	for _, shardid in pairs(shards) do
		local msg = send_add_shard(addr, shardid)
		table.insert(errors, box.tuple.new({msg}))
	end
end

local function unbind_shards_from_addr(errors, shards, addr, notify_added_shard, notify_deleted_shard)
	for _, shardid in pairs(shards) do
		local prev_addr = get_collector_address(shardid)
		box.delete(1, shardid)
		if prev_addr ~= nil and notify_deleted_shard ~= 0 then
			local msg = send_del_shard(prev_addr, shardid)
			table.insert(errors, box.tuple.new({msg}))
		end
	end
end

local function get_shards_distribution()
	local n_addrs = 0
	local addrs = {}
	for t in box.space[1].index[0]:iterator(box.index.ALL) do
		local curr_shardid = box.unpack('i', t[0])
		local curr_addr = t[1]
		if addrs[curr_addr] == nil then
			addrs[curr_addr] = {}
			n_addrs = n_addrs + 1
		end
		table.insert(addrs[curr_addr], curr_shardid)
	end
	return n_addrs, addrs
end

local function get_minmax_loaded_addrs(addrs)
	local min_addr = nil
	local min_n = N_SHARDS + 1
	local max_addr = nil
	local max_n = 0
	for addr, shards in pairs(addrs) do
		local n = table.getn(shards)
		if n > max_n then
			max_addr = addr
			max_n = n
		end
		if n < min_n then
			min_addr = addr
			min_n = n
		end
	end
	return min_addr, max_addr
end

function irina_add_collector_for(addr, shardid, notify_added_shard, notify_deleted_shard)
	shardid = tonumber(shardid)
	notify_added_shard = tonumber(notify_added_shard) or 1
	notify_deleted_shard = tonumber(notify_deleted_shard) or 1

	if shardid < 0 or shardid >= N_SHARDS then
		return "invalid shard id"
	end

	local prev_addr = get_collector_address(shardid)
	if prev_addr ~= nil and prev_addr == addr then
		return "shard already binded to this addr"
	end

	local errors = {}
	bind_shards_to_addr(errors, { shardid }, addr, notify_added_shard, notify_deleted_shard)
	return unpack(errors)
end

function irina_add_collector(addr, notify_added_shard, notify_deleted_shard)
	notify_added_shard = tonumber(notify_added_shard) or 1
	notify_deleted_shard = tonumber(notify_deleted_shard) or 1

	local n_addrs, addrs = get_shards_distribution()

	if n_addrs == 0 then
		local new_shards = {}
		for i = 0, N_SHARDS-1 do table.insert(new_shards, i) end
		local errors = {}
		bind_shards_to_addr(errors, new_shards, addr, notify_added_shard, notify_deleted_shard)
		return unpack(errors)
	end

	if addrs[addr] ~= nil then
		return "already added"
	end

	-- Ignore small shards (created by irina_add_collector_for) in resharding process.
	-- It may have special meaning (test shard or another).
	local n_manual_shards = 0
	do
		local min_threshold = math.floor(N_SHARDS / n_addrs)
		local manual_addrs = {}
		for a, s in pairs(addrs) do
			if table.getn(s) < min_threshold then
				table.insert(manual_addrs, a)
			end
		end
		for _, a in pairs(manual_addrs) do
			local manual_shards = addrs[a]
			n_manual_shards = n_manual_shards + table.getn(manual_shards)
			addrs[a] = nil
			n_addrs = n_addrs - 1
		end
	end

	local new_count = (N_SHARDS - n_manual_shards) / (n_addrs + 1)

	local new_shards = {}
	for i = 1, new_count do
		local _, addr = get_minmax_loaded_addrs(addrs)
		local shardid = table.remove(addrs[addr], 1)
		table.insert(new_shards, shardid)
	end

	local errors = {}
	bind_shards_to_addr(errors, new_shards, addr, notify_added_shard, notify_deleted_shard)
	return unpack(errors)
end

function irina_del_collector(addr, notify_added_shard, notify_deleted_shard)
	notify_added_shard = tonumber(notify_added_shard) or 1
	notify_deleted_shard = tonumber(notify_deleted_shard) or 1

	local n_addrs, addrs = get_shards_distribution()

	if addrs[addr] == nil then
		return "no such addr"
	end

	local errors = {}
	local shards = addrs[addr]
	addrs[addr] = nil
	n_addrs = n_addrs - 1
	unbind_shards_from_addr(errors, shards, addr, notify_added_shard, notify_deleted_shard)

	if n_addrs == 0 then return unpack(errors) end

	for _, shardid in pairs(shards) do
		local new_addr, _ = get_minmax_loaded_addrs(addrs)
		table.insert(addrs[new_addr], shardid)
		bind_shards_to_addr(errors, {shardid}, new_addr, notify_added_shard, notify_deleted_shard)
	end

	return unpack(errors)
end

function irina_show_shards_distribution()
	local n, addrs = get_shards_distribution()

	local unused = {}
	for i = 0, N_SHARDS-1 do unused[i] = 1 end

	local ret = {}
	for k, v in pairs(addrs) do
		table.sort(v)
		for _, shardid in pairs(v) do unused[shardid] = 0 end
		local shards_str = table.concat(v, ",")
		table.insert(ret, box.tuple.new({k, shards_str}))
	end

	local unbinded = {}
	for k, v in pairs(unused) do
		if v ~= 0 then table.insert(unbinded, k) end
	end

	if table.getn(unbinded) > 0 then
		table.sort(unbinded)
		local shards_str = table.concat(unbinded, ",")
		table.insert(ret, box.tuple.new({"unbinded_shards", shards_str}))
	end

	return unpack(ret)
end

function irina_get_shards(addr)
	local result = {}
	for t in box.space[1].index[0]:iterator(box.index.ALL) do
		local curr_shardid = box.unpack('i', t[0])
		if t[1] == addr then table.insert(result, curr_shardid) end
	end
	return unpack(result)
end

local function update_record(email, set_instant, set_expirable)
	box.update(0, email, "=p=p=p", 2, set_instant, 3, set_expirable, 4, box.time())
end

function irina_add_user(email, userid, is_instant, shardid)
	userid = box.unpack('i', userid)
	is_instant = box.unpack('i', is_instant)
	shardid = box.unpack('i', shardid)

	local need_send = false
	local tuple = box.select_limit(0, 0, 0, 1, email)
	if tuple == nil then
		box.insert(0, email, userid, is_instant, 0, box.time(), shardid)
		need_send = (is_instant == 1)
	elseif is_instant == 1 then
		local is_old_instant, is_old_expirable = box.unpack('i', tuple[2]), box.unpack('i', tuple[3])
		if (is_old_instant == 0 or is_old_expirable == 1) then
			need_send = true
			shardid = box.unpack('i', tuple[5])
			update_record(email, is_instant, 0)
		end
	elseif box.unpack('i', tuple[2]) == 1 then
		need_send = true
		shardid = box.unpack('i', tuple[5])
		update_record(email, 0, 0)
	end

	if need_send then send_change_status(email, userid, shardid, is_instant, 0) end
end

function irina_del_user(email)
	local tuple = box.delete(0, email)
	if tuple == nil then return end

	local userid = box.unpack('i', tuple[1])
	local is_old_instant = box.unpack('i', tuple[2])
	local shardid = box.unpack('i', tuple[5])
	if is_old_instant == 1 then send_change_status(email, userid, shardid, 0, 0) end
end

local function set_flags_impl(tuple, cond, set_instant, set_expirable)
	local email = tuple[0]
	local userid = box.unpack('i', tuple[1])
	local is_instant = box.unpack('i', tuple[2])
	local is_expirable = box.unpack('i', tuple[3])
	local shardid = box.unpack('i', tuple[5])

	if not cond(is_instant, is_expirable) then return end
	update_record(email, set_instant, set_expirable)

	if is_instant ~= set_instant or is_expirable ~= set_expirable then
		send_change_status(email, userid, shardid, set_instant, set_expirable)
	end
end

local function set_flags(email, cond, set_instant, set_expirable)
	local tuple = box.select_limit(0, 0, 0, 1, email)
	if tuple == nil then return end
	set_flags_impl(tuple, cond, set_instant, set_expirable)
end

function irina_set_instant(email)
	set_flags(email,
		function(i, e) return i == 0 or e == 1 end,
		1, 0)
end

function irina_del_instant(email)
	set_flags(email,
		function(i, e) return i == 1 and e == 0 end,
		0, 0)
end

function irina_set_online(email)
	set_flags(email,
		function(i, e) return i == 0 or e == 1 end,
		1, 1)
end

function irina_get_instant_users_ex(shardid)
	shardid = box.unpack('i', shardid)
	local result = {}
	for tuple in box.space[0].index[1]:iterator(box.index.EQ, 1, shardid) do
		table.insert(result, { tuple[0], box.unpack('i', tuple[1]), box.unpack('i', tuple[3]) })
	end
	if #result > 0 then return result end
	return unpack(result)
end

function irina_get_usual_users(shardid)
	shardid = box.unpack('i', shardid)
	local result = {}
	for tuple in box.space[0].index[1]:iterator(box.index.EQ, 0, shardid) do
		table.insert(result, { tuple[0], box.unpack('i', tuple[1]) })
	end
	if #result > 0 then return result end
	return unpack(result)
end

local function is_expired(args, tuple)
	if tuple == nil or #tuple <= args.fieldno then return nil end
	local is_expirable = box.unpack('i', tuple[3])
	if is_expirable == 0 then return false end

	local field = box.unpack('i', tuple[args.fieldno])
	return box.time() >= field + args.expiration_time
end

local function clean_expired(spaceno, args, tuple)
	set_flags_impl(tuple,
		function (i, e) return i == 1 and e == 1 end,
		0, 0)
end

dofile('expirationd.lua')

expirationd.run_task('expire_instant', 0, is_expired, clean_expired, {fieldno = 4, expiration_time = 5*60})
