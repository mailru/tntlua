local dkim_space = 0
local from_space = 1
local envfrom_space = 2
local sender_ip_space = 3
local dkim_msgtype_ts_space = 4

local field_last = 4
local field_count = 10
local timeout = 0.006
local max_attempts = 5

local blacklist_tarantool_config = {
    host='127.0.0.1',
    port=33013,
    reconnect_interval=0,
    update_interval=60,
    space=1}

local function increment_stat3(space, key, subject, timestamp)
    local retry = true
    local count = 0
    while retry do
        local status, result = pcall(box.update, space, key, '=p', field_last, timestamp)
        if status then
        --success update or tuple is not exist
            retry = false
            if result == nil then
            --insert new tuple
                local tuple = {}
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
    if tostring(space) == '5' then
        return
    end
    local retry = true
    local count = 0
    while retry do
	local status, result = pcall(box.update, space, key, '+p', field, 1)
	if status then
	--success update or tuple is not exist
	    retry = false
	    if result == nil then
	    --insert new tuple
		local tuple = {}
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
    if tostring(space) == '5' then
        return
    end
    local retry = true
    local count = 0
    while retry do
    local status, result = pcall(box.update, space, key, '+p', field, 1)
    if status then
    --success update or tuple is not exist
        retry = false
        if result == nil then
        --insert new tuple
        local tuple = {}
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
    if tostring(space) == '5' then
        return
    end
    increment_stat3(space, key, subject, timestamp)
end

-- END deprecated interface

local function increment_stat(space, key, users, spam_users, prob_spam_users, inv_users)
    local retry = true
    local count = 0
    while retry do
        local status, result = pcall(box.update, space, key, '+p+p+p+p', 2, users, 3, spam_users, 4, prob_spam_users, 5, inv_users)
        if status then
        --success update or tuple is not exist
            retry = false
            if result == nil then
            --insert new tuple
                local tuple = {}
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

local function fetch_blacklist(conn, space)
    assert(conn:timeout(1):ping(), "Tarantool is unreachable")

    local response = {conn:select_range(space, 0, 9000)}
    print("Fetched "..#response.." rows from space "..space)

    local blacklist = {}
    for i = 1, #response do
        local domain = response[i][0]
        blacklist[domain] = true
    end
    return blacklist
end

local blacklist = {}
box.fiber.wrap(function ()
    local tnt = blacklist_tarantool_config
    print("New Connect to tarantool "..tnt.host..":"..tnt.port)
    local conn = box.net.box.new(tnt.host, tnt.port, tnt.reconnect_interval)

    while true do
        print("Update blacklist")
        local ok, result = pcall(fetch_blacklist, conn, tnt.space)
        if ok then
            blacklist = result
            print("Blacklist updated")
        else
            -- exception
            print("Update failed. Reason: "..result)
        end
        box.fiber.sleep(tnt.update_interval)
    end
end)

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

    -- Collect DKIM domains
    -- Multiple DKIM format: dkim_domain == '"mail.ru","bk.ru"'
    -- Single DKIM format: dkim_domain == 'mail.ru'
    local dkim_domains = {}
    if dkim_domain:sub(1, 1) == '"' and dkim_domain:sub(-1, -1) == '"' then
        local sep = ","
        local pattern = string.format("([^%s]+)", sep)
        dkim_domain:gsub(pattern, function(c) dkim_domains[#dkim_domains+1] = c:sub(2, -2) end)
    else
        dkim_domains[1] = dkim_domain
    end

    -- Check every DKIM for blacklist
    for i = 1, #dkim_domains do
        if blacklist[dkim_domains[i]] == true then
            print("DKIM:"..dkim_domains[i].." is blacklisted")
            return
        end
    end

    -- Update statistics
    if envfrom_domain ~= "" then
        increment_stat(envfrom_space, envfrom_domain..time_str, users, spam_users, prob_spam_users, inv_users)
    end
    if from_domain ~= "" then
        increment_stat(from_space, from_domain..time_str, users, spam_users, prob_spam_users, inv_users)
    end
    for i = 1, #dkim_domains do
        if dkim_domains[i] ~= "" then
            increment_stat(dkim_space, dkim_domains[i]..time_str, users, spam_users, prob_spam_users, inv_users)
        end
    end
    if sender_ip ~= "" then
        increment_stat(sender_ip_space, sender_ip..time_str, users, spam_users, prob_spam_users, inv_users)
    end
    for i = 1, #dkim_domains do
        if dkim_domains[i] ~= "" and msgtype ~= "" then
            local element = dkim_domains[i]..":"..msgtype..time_str
            increment_stat(dkim_space, element, users, spam_users, prob_spam_users, inv_users)
            if subject == "" then subject = " " end
            increment_stat3(dkim_msgtype_ts_space, element, subject, timestamp)
        end
    end
end
