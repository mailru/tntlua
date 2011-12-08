field_count = 10
timeout = 0.05

function increment_or_insert(space, key, field)
    retry = true
    while retry do
	    status, result = pcall(box.update, space, key, '+p', field, 1)
	if status then
	--success update or tuple is not exist
	    retry = false
	    if result == nil then
            --insert new tuple
            tuple = {}
            for i = 2, field_count do tuple[i] = 0 end
                tuple[1] = os.date("%d_%m_%y")
                tuple[tonumber(field)] = 1
                box.insert(space, key, unpack(tuple))
            end
        else
        --exception
            if result == "Tuple is marked as read-only" then
                box.fiber.sleep(timeout)
            else
                retry = false
                print("space="..space.." key="..key.." field="..field.." :"..result)
            end
        end
    end
end
