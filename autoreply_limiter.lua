--
-- autoreply_limiter.lua
-- Developed by Mail.Ru
--

--
-- Index: userid (NUM), email (STR)
--

local limiter_period = 12*60*60

function autoreply_limiter_check(userid, email)
  userid = box.unpack('i', userid)
  local time = box.time()

  local tuple = box.select(0, 0, userid, email)
  if tuple == nil then
    box.insert(0, userid, email, time)
    return 1
  else
    local timeOld = box.unpack('i', tuple[2])
    if (time - timeOld) >= limiter_period then
      box.replace(0, userid, email, time)
      return 1
    else
      return 0
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
	box.delete(0, box.unpack('i', tuple[0]), tuple[1])
end

dofile('expirationd.lua')

expirationd.run_task('expire_limits', 0, is_expired, delete_expired, {fieldno = 2, expiration_time = limiter_period})
