--
-- bida.lua
--

--
-- simple birthday storage
--

local limit = 7000

function get_users_by_birthday(birthday, email_offset)
	birthday = box.unpack('i', birthday)

	local tuples = { box.select_range(0, 1, limit, birthday, email_offset) }

	local result = {}
	for _, tuple in pairs(tuples) do
		if box.unpack('i', tuple[1]) == birthday then
			table.insert(result, tuple[0])
		end
	end

	return unpack(result)
end
