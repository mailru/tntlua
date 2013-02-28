
-- space 0:
-- tuple : (listid, email)
-- space 1:
-- tuple : (listid, subj_prefix)

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

function list_mailer_get_ext(listid)
	listid = box.unpack('i', listid)

	local properties = box.select(1, 0, listid)

	local subj_prefix = ""
	if properties ~= nil then subj_prefix = properties[1] end

	local result = { subj_prefix }

	local tuples = { box.select(0, 0, listid) }
	if tuples == nil then return end

	for _, tuple in pairs(tuples) do
		table.insert(result, tuple[1])
	end

	return unpack(result)
end
