
dofile('fqueue.lua')

box.fqueue.enable(0)

function calinda_put(userid, msgid, header, part)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	msgid = box.unpack('l', msgid)

	box.fqueue.put(0, 0, nil, userid, msgid, header, part)
end
