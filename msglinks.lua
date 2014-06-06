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

function msglinks_delete(userid, numbers, ...)
	userid = box.unpack('i', userid)
	numbers = box.unpack('i', numbers) -- num of draft ids

	if numbers == 0 then
		-- delete all
		for tuple in box.space[0].index[0]:iterator(box.index.EQ, userid) do
			box.delete(0, userid, tuple[3])
		end
	else
		local draft_ids = {...}
		for _, draft_id in ipairs(draft_ids) do
			box.delete(0, userid, draft_id)
		end
	end
end

--
-- Run expiration of tuples
--

local function is_expired(args, tuple)
	if tuple == nil or #tuple <= args.fieldno then
		return nil
	end

	local field = tuple[args.fieldno]
	local current_time = box.time()
	local tuple_expire_time = box.unpack('i', field) + args.expiration_time
	return current_time >= tuple_expire_time
end

local function delete_expired(spaceno, args, tuple)
	box.delete(0, tuple[0], tuple[3])
end

dofile('expirationd.lua')

expirationd.run_task('expire_msglinks', 0, is_expired, delete_expired, {fieldno = 4, expiration_time = 30*24*3600})
