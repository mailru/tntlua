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

function increment_or_insert_2(space, key, field, element1, element2)
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
        for i = 2, field_count + 2 do tuple[i] = 0 end
        tuple[1] = string.sub(key, -8)
        tuple[tonumber(field)] = 1
        tuple[field_count + 1] = element1
        tuple[field_count + 2] = element2
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

field_subject = 2
field_first = 3
field_last = 4

function update_or_insert(space, key, subject, timestamp)
    retry = true
    count = 0
    while retry do
    status, result = pcall(box.update, space, key, '=p', field_last, timestamp)
    if status then
    --success update or tuple is not exist
        retry = false
        if result == nil then
        --insert new tuple
        tuple = {}
        for i = 2, field_last do tuple[i] = 0 end
        tuple[1] = string.sub(key, -8)
        tuple[field_subject] = subject
        tuple[field_first] = timestamp
        tuple[field_last] = timestamp
        box.insert(space, key, unpack(tuple))
        end
    else
    --exception
        count = count + 1
        if count == max_attempts then
        print("max attempts reached for space="..space.." key="..key)
        break
        end
        box.fiber.sleep(timeout)
    end
    end
end
