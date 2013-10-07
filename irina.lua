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

local function parse_addr(addr)
	local ind = addr:find(":")
	if ind == nil then return nil end

	return addr:sub(0, ind - 1), tonumber(addr:sub(ind + 1))
end

local function send_collector_cmd(addr, cmd)
	local s = box.socket.tcp()
	if s == nil then
		print("can not create collector socket")
		return false
	end

	local host, port = parse_addr(addr)

	if not s:connect(host, port, 0.1) then
		local _, errstr = s:error()
		print("can not connect to collector[" .. host .. ":" .. port .. "]: " .. errstr)
		s:close()
		return false
	end

	local bytes_sent, status, errno, errstr = s:send(cmd, 0.1)
	if bytes_sent ~= #cmd then
		local _, errstr = s:error()
		print("can not send data to collector[" .. host .. ":" .. port .. "]: " .. errstr)
		s:close()
		return false
	end

	s:close()
	return true
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
	if addr == nil then return end

	local data = email .. " " .. userid .. " " .. enabled .. " " .. expirable
	send_collector_cmd(addr, data)
end
local function send_add_shard(addr, shardid)
	local data = "add_shard " .. shardid
	send_collector_cmd(addr, data)
end
local function send_del_shard(addr, shardid)
	local data = "del_shard " .. shardid
	send_collector_cmd(addr, data)
end

local function get_table_size(t)
	local n = 0
	for _, _ in pairs(t) do n = n + 1 end
	return n
end

function irina_add_collector(addr, notify_added_shard)
	notify_added_shard = tonumber(notify_added_shard)

	local addrs = {}
	for tuple in box.space[1].index[0]:iterator(box.index.ALL) do
		local curr_addr = tuple[1]
		if curr_addr == addr then
			print("already has collector with addr " .. addr)
			return
		end
		if addrs[curr_addr] == nil then addrs[curr_addr] = 1
		else addrs[curr_addr] = addrs[curr_addr] + 1 end
	end

	local n_collectors = get_table_size(addrs)

	if n_collectors == 0 then
		for i = 0, 1023 do
			print("bind shard #" .. i .. " to " .. addr)
			box.insert(1, i, addr)
			if notify_added_shard ~= 0 then send_add_shard(addr, i) end
		end
		return
	end

	local new_count = 1024 / (n_collectors + 1)

	for tuple in box.space[1].index[0]:iterator(box.index.ALL) do
		local curr_shardid = box.unpack('i', tuple[0])
		local curr_addr = tuple[1]

		if addrs[curr_addr] > new_count then
			addrs[curr_addr] = addrs[curr_addr] - 1
			print("rebind shard #" .. curr_shardid .. " from " .. curr_addr .. " to " .. addr)
			box.replace(1, curr_shardid, addr)
			send_del_shard(curr_addr, curr_shardid)
			if notify_added_shard ~= 0 then
				box.fiber.sleep(0.1)
				send_add_shard(addr, curr_shardid)
			end
		end
	end
end

function irina_del_collector(addr, notify_deleted_shard)
	notify_deleted_shard = tonumber(notify_deleted_shard)

	local addrs = {}
	for tuple in box.space[1].index[0]:iterator(box.index.ALL) do
		local curr_addr = tuple[1]
		addrs[curr_addr] = 1
	end

	if addrs[addr] == nil then
		print("collector with addr " .. addr .. " does not exist")
		return
	end

	local n_collectors = get_table_size(addrs)

	if n_collectors == 1 then
		for i = 0, 1023 do
			print("unbind shard #" .. i .. " from " .. addr)
			box.delete(1, i)
			if notify_deleted_shard ~= 0 then send_del_shard(addr, i) end
		end
		return
	end

	addrs[addr] = nil
	local addr_shards = irina_get_shards_impl(addr)
	local n_addr_shards = get_table_size(addr_shards)

	while n_addr_shards > 0 do
		for curr_addr, _ in pairs(addrs) do
			if n_addr_shards > 0 then
				local shardid = nil
				for k, v in pairs(addr_shards) do
					shardid = v
					table.remove(addr_shards, k)
					n_addr_shards = n_addr_shards - 1
					do break end
				end

				print("rebind shard #" .. shardid .. " from " .. addr .. " to " .. curr_addr)
				box.replace(1, shardid, curr_addr)
				if notify_deleted_shard ~= 0 then
					send_del_shard(addr, shardid)
					box.fiber.sleep(0.1)
				end
				send_add_shard(curr_addr, shardid)
			end
		end
	end
end

function irina_get_shards_impl(addr)
	local result = {}
	for tuple in box.space[1].index[0]:iterator(box.index.ALL) do
		local curr_shardid = box.unpack('i', tuple[0])
		if tuple[1] == addr then table.insert(result, curr_shardid) end
	end
	return result
end

function irina_get_shards(addr)
	local result = irina_get_shards_impl(addr)
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
			update_record(email, is_instant, 0)
		end
	end

	if need_send then send_change_status(email, userid, shardid, 1, 0) end
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
-- do not remove instant status

--[[	set_flags(email,
		function(i, e) return i == 1 and e == 0 end,
		0, 0)]]
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
	return unpack(result)
end

function irina_get_usual_users(shardid)
	shardid = box.unpack('i', shardid)
	local result = {}
	for tuple in box.space[0].index[1]:iterator(box.index.EQ, 0, shardid) do
		table.insert(result, { tuple[0], box.unpack('i', tuple[1]) })
	end
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
