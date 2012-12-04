--
-- autoreply_limiter.lua
-- Developed by Mail.Ru
--

--
-- Index: userid (NUM), email (STR)
--

local limiter_period = 4*24*3600

function autoreply_limiter_check(userid, email)
  userid = box.unpack('i', userid)
  local time = os.time()

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
