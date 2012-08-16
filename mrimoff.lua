-- mrimoff.lua

--
-- Stored procedures for Mail.Ru Agent offline messages storage.
--

--
-- index0 - { userid, msgid } (TREE, unique)
--

local function get_key_cardinality_and_max_msgid(index_no, ...)
	local key = ...
	-- TODO: optimize (replace by calculation cardinality of a key in the index)
	local data = { box.select(0, index_no, key) }
	local n = #data
	if n == 0 then
		return 0, 0
	end
	return n, box.unpack('i', data[n][1])
end

--
-- Add offline message @msg for user @userid.
-- If storage already contains @limit messages do nothing.
-- Returns tuple { "number of messages for this user", "id for added message" }
-- "id for added message" == 0 if no message was added.
--
function mrim_add(userid, msg)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)

	local limit = 1000

	while true do
		local n_msgs, max_msgid = get_key_cardinality_and_max_msgid(0, userid)

		if n_msgs >= limit then
			return { box.pack('i', n_msgs), box.pack('i', 0) }
		end

		local msgid = max_msgid + 1
		local status, result = pcall(box.insert, 0, userid, msgid, msg)
		if status then
			return { box.pack('i', n_msgs + 1), box.pack('i', msgid) }
		else
			--exception
			box.fiber.sleep(0.001)
		end
	end
end

--
-- Delete offline message with id @msgid for user @userid.
-- Returns flag of deletion success.
--
function mrim_del(userid, msgid)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	msgid = box.unpack('i', msgid)

	local del = box.delete(0, userid, msgid)
	if del ~= nil then
		return box.pack('i', 1)
	end
	return box.pack('i', 0)
end

--
-- Delete all offline messages for user @userid.
-- Returns flag of deletion success.
--
function mrim_del_all(userid)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)

	local msgs = { box.select(0, 0, userid) }
	for _, msg in pairs(msgs) do
		box.delete(0, msg[0], msg[1])
	end

	return box.pack('i', #msgs)
end

--
-- Get no more then @limit messages for user @userid
-- sorted ascending by addition order.
-- Returns tuple { "total number of messages for user" }
-- followed by tuples with requested user messages.
--
function mrim_get(userid, limit)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	limit = box.unpack('i', limit)

	-- TODO: use one select request for calculation of n_msgs and getting no more then @limit msgs
	local n_msgs, _ = get_key_cardinality_and_max_msgid(0, userid)

	return box.pack('i', n_msgs), box.select_limit(0, 0, 0, limit, userid)
end
