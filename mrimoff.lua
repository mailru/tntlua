-- mrim.lua

--
-- index0 - msgid (unique)
-- index1 - { userid, msgid } (unique)
--

function mrim_add(userid, msg)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	--print("mrim_add "..userid)

	local limit = 1000
	--FIXME: optimize
	local n_msgs = #{ box.select(0, 1, userid) }

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

	return { box.pack('i', n_msgs + 1), box.pack('l', msgid) } -- count + msgid
end

function mrim_del(userid, msgid)
	-- client sends integers encoded as BER-strings
	msgid = box.unpack('l', msgid)
	--print("mrim_del "..box.unpack('i', userid).."   "..tostring(msgid))
	local del = box.delete(0, msgid)
	if del ~= nil then
		return box.pack('i', 1)
	end
	return box.pack('i', 0)
end

function mrim_get(userid, limit)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	limit = box.unpack('i', limit)
	--print("mrim_get "..userid.."   "..limit)

	--FIXME: optimize
	local n_msgs = #{ box.select(0, 1, userid) }

	return box.pack('i', n_msgs), box.select_limit(0, 1, 0, limit, userid)
end
