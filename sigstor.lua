-- sigstor.lua

--
-- Stored procedures for Mail.Ru Antispam signature reverse index.
--

--
-- index0 - { digest, msgid, userid } (TREE, unique)
--

local signature_type_to_spaceno = { at = 0, im = 1, me = 2, sh = 3, i2 = 4, ur = 5, em = 6, tr = 7, bl = 8 }

local function get_spaceno(sigtype)
	local v = signature_type_to_spaceno[sigtype]
	if v ~= nil then return v end
	return 0
end

--
-- Add signatures from message @msgid for user @userid.
--
function sigstor_add(userid, msgid, signatures)
	-- client sends integers encoded as BER-strings
	userid = box.unpack('i', userid)
	msgid = box.unpack('l', msgid)
	local time = os.time()

	-- signatures is a binary string consists of 18-bytes signature (2 byte types and 16 byte digests)
	for v = 0, signatures:len() - 1, 18 do
		local t = signatures:sub(v + 1, v + 2)
		local digest = signatures:sub(v + 3, v + 18)
		-- convert signature type to space number
		local spaceno = get_spaceno(t)
		box.insert(spaceno, digest, msgid, userid, time)
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
	local current_time = os.time()
	local tuple_expire_time = box.unpack('i', field) + args.expiration_time
	return current_time >= tuple_expire_time
end

local function delete_expired(spaceno, args, tuple)
	box.delete(spaceno, tuple[0], tuple[1], tuple[2])
end

dofile('expirationd.lua')

for t, spaceno in pairs(signature_type_to_spaceno) do
	expirationd.run_task(t, spaceno, is_expired, delete_expired, {fieldno = 3, expiration_time = 7*24*60*60})
end
