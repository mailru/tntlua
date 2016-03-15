--
-- bida2.lua
--
-- BIDA (ver. 2)
--
-- Space 0: simple birthday storage
--   Tuple: { user_id (INT), date (INT) }
--   Index 0: HASH { user_id }
--   Index 1: TREE { date, user_id }
--

local limit = 7000

function bida2_get_users_by_birthday(birthday, userid_offset)
	birthday = box.unpack('i', birthday)
	userid_offset = box.unpack('i', userid_offset)

	local tuples = { box.select_range(0, 1, limit, birthday, userid_offset) }

	local result = {}
	for _, tuple in pairs(tuples) do
		if box.unpack('i', tuple[1]) == birthday then
			table.insert(result, tuple[0])
		end
	end

	return unpack(result)
end
