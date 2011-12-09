field_count = 10
timeout = 0.006
max_attempts = 5

function increment_or_insert(space, key, field)
    retry = true
    count = 0
    while retry do
	status, result = pcall(box.update, space, key, '+p', field, 1)
	if status then
	--success update or tuple is not exist
	    retry = false
	    if result == nil then
	    --insert new tuple
		tuple = {}
		for i = 2, field_count do tuple[i] = 0 end
		tuple[1] = string.sub(key, -8)
		tuple[tonumber(field)] = 1
		box.insert(space, key, unpack(tuple))
	    end
	else
	--exception
	    count = count + 1
	    if count == max_attempts then
		print("max attempts reached for space="..space.." key="..key.." field="..field)
		break
	    end
	    box.fiber.sleep(timeout)
	end
    end
end
