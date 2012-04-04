-- mrimoff.lua

--
-- Stored procedures for Mail.Ru Agent offline messages storage.
--

--
-- index0 - msgid (TREE, unique)
-- index1 - { userid, msgid } (TREE, unique)
--

local function get_key_cardinality(index_no, ...)
	local key = ...
	-- TODO: optimize (replace by calculation cardinality of a key in the index)
	return #{ box.select(0, index_no, key) }
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
	local n_msgs = get_key_cardinality(1, userid)

	if n_msgs >= limit then
		return { box.pack('i', n_msgs), box.pack('l', tonumber64(0)) }
	end

	local max_tu = box.space[0].index[0]:max()
	local msgid = nil
	if max_tu ~= nil then
		msgid = box.unpack('l', max_tu[0]) + 1
	else
		msgid = tonumber64(1)
	end
	box.insert(0, msgid, userid, msg)

	return { box.pack('i', n_msgs + 1), box.pack('l', msgid) }
end

--
-- Delete offline message with id @msgid for user @userid.
-- Returns flag of deletion success.
--
function mrim_del(userid, msgid)
	-- client sends integers encoded as BER-strings
	msgid = box.unpack('l', msgid)

	local del = box.delete(0, msgid)
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

	local msgs = { box.select(0, 1, userid) }
	for _, msg in ipairs(msgs) do
		box.delete(0, msg[0])
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

	local n_msgs = get_key_cardinality(1, userid)

	return box.pack('i', n_msgs), box.select_limit(0, 1, 0, limit, userid)
end
