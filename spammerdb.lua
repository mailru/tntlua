-- spammerdb.lua

local function get_value(flags, stype)
    return math.floor(flags / (4 ^ stype)) % 4
end

local function set_value(v, stype, old_flags)
    return old_flags + v * 4 ^ stype
end

function spammerdb_get(userid, stype, ...)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	stype = box.unpack('i', stype)
	local emails = {...}

	for i, email in ipairs(emails) do
		if #email > 0 then
			local tuple = box.select(0, 0, userid, email)
			if tuple ~= nil then
				local flags = string.byte(tuple[2]) -- box.unpack('b', tuple[2])
				local v = get_value(flags, stype)
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
		local flags = string.byte(tuple[2]) -- box.unpack('b', tuple[2])
		for stype = 0, 3 do
			local v = get_value(flags, stype)
			if v > 0 then
				table.insert(result, { tuple[1], box.pack('b', v), stype })
			end
		end
	end
	return unpack(result)
end

function spammerdb_set(userid, stype, email, value)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	stype = box.unpack('i', stype)
	value = string.byte(value) --box.unpack('b', value)

	local tuple = box.select(0, 0, userid, email)
	if tuple ~= nil then
		local flags = string.byte(tuple[2]) -- box.unpack('b', tuple[2])
		local v = get_value(flags, stype)
		-- mark already exists - do nothing
		if v == value then return end

		local new_flags = set_value(value - v, stype, flags)
		if new_flags == 0 then
			-- last spam mark for this email deleted
			box.delete(0, userid, email)
		else
			box.replace(0, userid, email, box.pack('b', new_flags))
		end
	elseif value > 0 then
		local new_flags = set_value(value, stype, 0)
		box.insert(0, userid, email, box.pack('b', new_flags))
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
