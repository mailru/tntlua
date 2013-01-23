
-- tuple : (userid, email)
-- function returns list of mailer

function list_mailer(userid)
  userid = box.unpack('i', userid)
  
  local result = { }
  local tuples = { box.select(0, 0, userid) }
  if tuples == nil then
    return
  else
    for _, tuple in pairs(tuples) do
      table.insert(result, tuple[1])
    end
    return unpack(result)
  end
end
