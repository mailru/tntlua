local dkim_space = 0
local from_space = 1
local envfrom_space = 2
local sender_ip_space = 3
local dkim_msgtype_ts_space = 4
local dkim_senderip_space = 5

local field_last = 4
local field_count = 10
local timeout = 0.006
local max_attempts = 5

local function increment_stat3(space, key, subject, timestamp)
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
                tuple[2] = subject
                tuple[3] = timestamp
                tuple[4] = timestamp
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

-- BEG deprecated interface

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

function update_or_insert(space, key, subject, timestamp)
    increment_stat3(space, key, subject, timestamp)
end

-- END deprecated interface

local function increment_stat(space, key, users, spam_users, prob_spam_users, inv_users)
    retry = true
    count = 0
    while retry do
        status, result = pcall(box.update, space, key, '+p+p+p+p', 2, users, 3, spam_users, 4, prob_spam_users, 5, inv_users)
        if status then
        --success update or tuple is not exist
            retry = false
            if result == nil then
            --insert new tuple
                tuple = {}
                for i = 2, field_count do tuple[i] = 0 end
                tuple[1] = string.sub(key, -8)
                tuple[2] = users
                tuple[3] = spam_users
                tuple[4] = prob_spam_users
                tuple[5] = inv_users
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

local function increment_stat2(space, key, element1, element2, users, spam_users, prob_spam, inv_users)
    retry = true
    count = 0
    while retry do
        status, result = pcall(box.update, space, key, '+p+p+p+p', 2, users, 3, spam_users, 4, prob_spam_users, 5, inv_users)
        if status then
        --success update or tuple is not exist
            retry = false
            if result == nil then
            --insert new tuple
                tuple = {}
                for i = 2, field_count + 2 do tuple[i] = 0 end
                tuple[1] = string.sub(key, -8)
                tuple[2] = users
                tuple[3] = spam_users
                tuple[4] = prob_spam_users
                tuple[5] = inv_users
                tuple[field_count + 1] = element1
                tuple[field_count + 2] = element2
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

function mstat_add(
        msgtype, sender_ip, from_domain, envfrom_domain, dkim_domain, subject,
        users, spam_users, prob_spam_users, inv_users,
        timestamp)
    users           = box.unpack('i', users)
    spam_users      = box.unpack('i', spam_users)
    prob_spam_users = box.unpack('i', prob_spam_users)
    inv_users       = box.unpack('i', inv_users)
    timestamp       = box.unpack('i', timestamp)
    local time_str = os.date("_%d_%m_%y", timestamp)

    if envfrom_domain ~= "" then
        increment_stat(envfrom_space, envfrom_domain..time_str, users, spam_users, prob_spam_users, inv_users)
    end
    if from_domain ~= "" then
        increment_stat(from_space, from_domain..time_str, users, spam_users, prob_spam_users, inv_users)
    end
    if dkim_domain ~= "" then
        increment_stat(dkim_space, dkim_domain..time_str, users, spam_users, prob_spam_users, inv_users)
    end
    if sender_ip ~= "" then
        increment_stat(sender_ip_space, sender_ip..time_str, users, spam_users, prob_spam, inv_users)
    end
    if dkim_domain ~= "" and sender_ip ~= "" then
        increment_stat2(dkim_senderip_space, dkim_domain.."|"..sender_ip..time_str, dkim_domain, sender_ip, users, spam_users, prob_spam_users, inv_users)
    end
    if dkim_domain ~= "" and msgtype ~= "" then
        local element = dkim_domain..":"..msgtype..time_str
        increment_stat(dkim_space, element, users, spam_users, prob_spam_users, inv_users)
        if subject == "" then subject = " " end
        increment_stat3(dkim_msgtype_ts_space, element, subject, timestamp)
    end
end
