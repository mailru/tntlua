-- spammerdb.lua

local function get_value(v, stype)
    return math.floor(v / (4 ^ stype)) % 4
end

local function set_value(v, stype, old_value)
    return old_value + v * 4 ^ stype
end

local function unpack_data(data)
	-- data has format {2bit}{2bit}{2bit}{2bit}{mtime for each non-zero 2bit}*
	-- local vt = { box.unpack('bi*', tuple[2]) }
	local vt = { string.byte(data) }
	for i = 1, math.floor((#data - 1) / 4) do
		table.insert(vt, box.unpack('i', string.sub(data, -2 + 4 * i, 1 + 4 * i)))
	end
	return vt
end

local function pack_data(vt)
	vt[1] = box.pack('b', vt[1])
	for i = 2, #vt do vt[i] = box.pack('i', vt[i]) end
	return table.concat(vt)
end

function spammerdb_get(userid, stype, ...)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	stype = box.unpack('i', stype)
	local emails = {...}

	for i, email in ipairs(emails) do
		if #email > 0 then
			local tuple = box.select(0, 0, userid, email)
			if tuple ~= nill then
				local v = string.byte(tuple[2]) -- box.unpack('b', tuple[2])
				v = get_value(v, stype)
				if v > 0 then
					return { box.pack('b', v), box.pack('i', i) }
				end
			end
		end
	end
	return { box.pack('b', 0), box.pack('i', 0) }
end

function spammerdb_getall(userid)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)

	local result = {}

	local tuples = { box.select(0, 0, userid) }
	for _, tuple in pairs(tuples) do
		local vt = unpack_data(tuple[2])
		local j = 2
		for stype = 0, 3 do
			local v = get_value(vt[1], stype)
			if v > 0 then
				table.insert(result, { tuple[1], box.pack('b', v), stype, vt[j] })
				j = j + 1
			end
		end
	end
	return unpack(result)
end

function spammerdb_set(userid, stype, email, value, mtime)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	stype = box.unpack('i', stype)
	value = string.byte(value) --box.unpack('b', value)
	mtime = box.unpack('i', mtime)

	local tuple = box.select(0, 0, userid, email)
	if tuple ~= nil then
		local vt = unpack_data(tuple[2])
		local v = get_value(vt[1], stype)
		-- mark already exists - do nothing
		if v == value then return end

		local new_v = set_value(value - v, stype, vt[1])
		if new_v == 0 then
			-- last spam mark for this email deleted
			box.delete(0, userid, email)
		else
			local new = { new_v }
			local j = 2
			for st = 0, 3 do
				local v = get_value(vt[1], st)
				if st == stype then
					if value == 0 then
						-- remove and skip old mtime
						j = j + 1
					else
						table.insert(new, mtime)
						-- skip old mtime if exists
						if v ~= 0 then j = j + 1 end
					end
				elseif v > 0 then
					-- copy mtime for unchanged mark
					table.insert(new, vt[j])
					j = j + 1
				end
			end
			box.replace(0, userid, email, pack_data(new))
		end
	elseif value > 0 then
		box.insert(0, userid, email, pack_data({ set_value(value, stype, 0), mtime }))
	end
end

--
-- Delete all spammerdb lines for user @userid.
-- Returns flag of deletion success.
--
function spammerdb_delall(userid)
        -- client sends integers encoded as BER-strings
        userid = box.unpack('i', userid)

        local tuples = { box.select(0, 0, userid) }
        for _, tuple in pairs(tuples) do
                box.delete(0, userid, tuple[1])
        end

        return box.pack('i', #tuples)
end
