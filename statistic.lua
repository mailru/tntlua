field_count = 10

function increment_or_insert(space, key, field)
    res = box.update(space, key, '+p', field, 1)
    if res == nil
    then
    --insert new tuple
	tuple = {}
	for i = 2, field_count do tuple[i] = 0 end
	tuple[1] = os.date("%d_%m_%y")
	tuple[tonumber(field)] = 1
	box.insert(space, key, unpack(tuple))
    end
    return
end
