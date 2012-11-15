--
-- livads.lua
--

dofile('cnt.lua')

local max_results = 10

--
-- Search keys with prefix @prefix.
-- If found more then @max_results then returns 'MAX_RESULT, -1'.
-- Else returns found keys.
--
function livads_search(prefix)
	local t = { box.select_range(0, 0, max_results + 1, prefix) }
	local i = table.getn(t)

	while i > 0 and string.find(t[i][0], prefix, 1, true) ~= 1 do
		table.remove(t)
		i = i - 1
	end

	if i > max_results then
		-- FIXME return user-constructed tuple without inserting to table
		return box.replace(0, "MAX_RESULT", -1)
	end
	return unpack(t)
end
