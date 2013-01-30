
-- tuple : (listid, email)
-- function returns list of mailer

function list_mailer_get(listid)
  listid = box.unpack('i', listid)

  local result = { }
  local tuples = { box.select(0, 0, listid) }
  if tuples == nil then
    return
  else
    for _, tuple in pairs(tuples) do
      table.insert(result, tuple[1])
    end
    return unpack(result)
  end
end
