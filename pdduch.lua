function add_user(user_id, stub)
	local max_tuple = box.space[0].index[1]:max()
	local max_id = 0
	if max_tuple ~= nil then
		max_id = box.unpack( 'i', max_tuple[1] )
	end
	local ok, err = pcall( box.insert, 0, user_id, max_id + 1 )
	if not ok and string.find( err, 'Duplicate key' ) == nil then
		return err
	end
end

function get_first_chunk(count)
	count = box.unpack('i', count)
	return box.select_limit(0, 1, 0, count)
end

function get_next_chunk(last_id, last_user_id, count)
	last_id = box.unpack('i', last_id)
	last_user_id = box.unpack('i', last_user_id)
	count = box.unpack('i', count)
	return box.select_range(0, 1, count, last_id, last_user_id)
end

function remove_users(max_id)
	max_id = box.unpack('i', max_id)
	local stop = false
	local removed = true
	while ( not stop and removed ) do
		local tuples = { box.select_limit(0, 1, 0, 1000) }
		removed = false
		for _, tuple in pairs(tuples) do
			local id = box.unpack( 'i', tuple[1] )
			if ( id > max_id ) then
				stop = true
				break
			end
			tuple = box.delete( 0, box.unpack('i', tuple[0]) )
			removed = true
		end
	end
end

function get_count()
	return box.space[0]:len()
end

-- delete_all
function delete_older_than()
	while (true) do
		local tuples = { box.select_limit(0, 1, 0, 1000) }
		local removed = false
		for _, tuple in pairs(tuples) do
			removed = true
			tuple = box.delete( 0, box.unpack('i', tuple[0]) )
		end
		if not removed then
			break
		end
	end
end
