--
-- bida2.lua
--
-- BIDA (ver. 2)
--
-- Space 0: simple birthday storage
--   Tuple: { user_id (INT), date (INT) }
--   Index 0: TREE { date, user_id }
--

local limit = 7000

function bida2_get_users_by_birthday(birthday, userid_offset)
	birthday = box.unpack('i', birthday)
	userid_offset = box.unpack('i', userid_offset)

	--[[
	We are using user id as an offset in out tarantool request.
	From documentation:
		localhost> lua box.select_range(4, 1, 2, '1')
		---
		 - '1': {'1'}
		 - '2': {'2'}
		...
	That means, that select_range() function can return users with unexpected birthday => we should filter them out
	--]]
	local tuples = { box.select_range(0, 0, limit, birthday, userid_offset) }

	local result = {}
	for _, tuple in pairs(tuples) do
		if box.unpack('i', tuple[1]) == birthday then
			table.insert(result, tuple[0])
		end
	end

	return unpack(result)
end

function bida2_delete_user(userid)
	userid = box.unpack('i', userid)
	local tuples, tuple
	tuples = { box.select(0, 1, userid) }
	for _, tuple in pairs(tuples) do
		box.delete(0, tuple[1], tuple[0])
	end
end
