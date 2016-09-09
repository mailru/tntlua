-- norepres.lua

--
-- Tuple: userid (NUM), digest (STR), uidl (NUM64), time (NUM)
-- Index0: userid, digest. (TREE)
-- Index1: time. (TREE)
--

--
-- Delete all noreply reminders for user @userid.
-- Returns flag of deletion success.
--
function norepres_delall(userid)
        -- client sends integers encoded as BER-strings
        userid = box.unpack('i', userid)

        local tuples = { box.select(0, 0, userid) }
        for _, tuple in pairs(tuples) do
                box.delete(0, userid, tuple[1])
        end

        return box.pack('i', #tuples)
end

function norepres_del(userid, digest)
        -- client sends integers encoded as BER-strings
        userid = box.unpack('i', userid)

        local tuple = box.delete(0, userid, digest)
        if tuple ~= nil then return 1 else return 0 end
end

function norepres_getold(time, limit)
        -- client sends integers encoded as BER-strings
	time = box.unpack('i', time)
	limit = box.unpack('i', limit)

	local tuples = { box.select_range(0, 1, limit) }

	local ret = {}
	for _, tuple in pairs(tuples) do
		if box.unpack('i', tuple[3]) < time then
		    table.insert(ret, { tuple[0], tuple[1],
		                    box.unpack('i', string.sub(tuple[2], 0, 4)),
		                    box.unpack('i', string.sub(tuple[2], 5, 8)),
		                    tuple[3] })
		end
	end
	return unpack(ret)
end
