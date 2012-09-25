-- msglinks.lua

-- index0: userid+toid
-- index1: userid+fromid+linkid
-- tuple: userid,fromid,linkid,toid,created

function msglinks_set(userid, fromid, linkid, toid, created)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	linkid = box.unpack('i', linkid)
	created = box.unpack('i', created)

	local old_link = box.select(0, 1, userid, fromid, linkid)
	if old_link ~= nil then box.delete(0, userid, old_link[3]) end

	box.insert(0, userid, fromid, linkid, toid, created)
end
