-- Tuple of profile has always 2 fields.
-- Field 1 -- user-id
-- Field 2 -- key/value dict. Key is always a number.

proscribe_host = '10.161.41.111'
proscribe_port = 27017
keys_to_proscribe = {324}

-- Create a user, then init storage, create functions and then revoke ALL priveleges from user
local function init_storage(init_func, interface)
    local init_username = 'profile'
    box.schema.user.create(init_username, {if_not_exists = true})
    box.schema.user.grant(init_username, 'execute,read,write', 'universe', nil,
            {if_not_exists = true})
    box.session.su(init_username)

    init_func()

    for _, v in pairs(interface) do
        box.schema.func.create(v, {setuid = true, if_not_exists = true})
    end

    box.session.su('admin')
    box.schema.user.revoke(init_username, 'execute,read,write', 'universe')
end

-- Create a role, which can execute interface functions
local function init_role(role_name, interface)
    box.schema.role.create(role_name, {if_not_exists = true})
    for _, v in pairs(interface) do
        box.schema.role.grant(role_name, 'execute', 'function', v,
                {if_not_exists = true})
    end
end

-- Scheme initialization
local function init()
    local s = box.schema.create_space('profile', {
        if_not_exists = true,
    })
    s:create_index('primary', {
        type = 'tree',
	unique = true,
        parts = {1, 'unsigned'},
        if_not_exists = true,
    })
end

-- List of functions, which is possible to call from outside the box
local interface = {
    'profile_delete',
    'profile_get_all',
    'profile_multiget',
    'profile_multiset',
    'profile_set',
}

msgpack = require('msgpack')
socket = require('socket')
digest = require('digest')
pickle = require('pickle')

proscribe_socket = socket('AF_INET', 'SOCK_DGRAM', 'udp')

-- Function, which sends requested profile_key changes to special DWH daemon
local function send_to_proscribe(uid, profile_key, old_value, new_value)
	local fit = false
	for _, v in ipairs(keys_to_proscribe) do
		if v == profile_key then
			fit = true
		end
	end
	if not fit then
		return
	end

	if not old_value then
		old_value = ''
	end
	if not new_value then
		new_value = ''
	end
	if old_value == new_value then -- we doesnt want to send data without changes
		return
	end

	old_value = digest.base64_encode(old_value)
	new_value = digest.base64_encode(new_value)

	local body = pickle.pack('iliiAiA', 1, uid, profile_key, #old_value, old_value, #new_value, new_value)
	local packet = pickle.pack('iiiA', 1, #body, 0, body)
	proscribe_socket:sendto(proscribe_host, proscribe_port, packet)
end

-- Cast internal profile format to the format, requested by client-side.
local function cast_profile_to_return_format(profile)
	setmetatable(profile.data, msgpack.map_mt)
	return {profile.uid, profile.data}
end

local function store_profile(profile)
	local count = 0
	for k,v in pairs(profile.data) do count = count + 1 end

	-- We dont want to store empty profiles. Save space.
	if count == 0 then
		return box.space.profile:delete(profile.uid)
	end

	setmetatable(profile.data, msgpack.map_mt)
	return box.space.profile:replace({profile.uid, profile.data})
end

local function create_new_profile(user_id)
	local profile = {}
	profile.uid = user_id
	profile.data = {}
	return profile
end

local function load_profile(user_id)
	local tup = box.space.profile:select(user_id)
	-- In case no profile found, operate it as profile without keys/values
	if #tup == 0 then
		return create_new_profile(user_id)
	end
	local profile = {}
	profile.uid = user_id
	profile.data = tup[1][2] -- Index 1 is because we have only 1 tuple with such userid (index is unique). Second field of tuple is key/value dict.
	return profile
end

local function set_profile_key(profile, key, value)
	-- Do not store empty keys. We want to save space.
	if value == '' then
		value = nil
	end
	profile.data[key] = value
end

-- function profile_delete delete profile. Returns nothing
function profile_delete(user_id)
	box.space.profile:delete(user_id)
end

-- function profile_get_all returns full profile
function profile_get_all(user_id)
	local profile = load_profile(user_id)
	return cast_profile_to_return_format(profile)
end

-- function profile_multiget returns only requested keys from profile. Accepts user_id and then several keys
function profile_multiget(user_id, ...)
	local pref_list = {...}
	if #pref_list == 0 then
		return cast_profile_to_return_format(create_new_profile(user_id))
	end

	local profile = load_profile(user_id)

	-- Create a copy of profile. We select few keys, so it is faster to copy only needed keys, then clear not needed keys
	local profile_copy = create_new_profile(profile.uid)
	for _, v in ipairs(pref_list) do
		if profile.data[v] then
			profile_copy.data[v] = profile.data[v]
		end
	end

	return cast_profile_to_return_format(profile_copy)
end

-- function profile_multiset accepts user_id and then key, value, key, value, key, value, ... Returns full updated profile.
function profile_multiset(user_id, ...)
	local pref_list = {...}

	if #pref_list % 2 ~= 0 then
		error('Not even number of arguments')
	end
	
	local profile = load_profile(user_id)
	
	-- In case of no keys were passed, just return full profile
	if #pref_list == 0 then
		return cast_profile_to_return_format(profile)
	end

	local i, pref_key = next(pref_list)
    	local i, pref_value = next(pref_list, i)
	-- iterate all passed key/value pairs from arguments
    	while pref_key ~= nil and pref_value ~= nil do
		send_to_proscribe(profile.uid, pref_key, profile.data[pref_key], pref_value)
		set_profile_key(profile, pref_key, pref_value)
		i, pref_key = next(pref_list, i)
		i, pref_value = next(pref_list, i)
    	end

	store_profile(profile)
	return cast_profile_to_return_format(profile)
end

-- function profile_set set only one key. Returns full updated profile
function profile_set(user_id, key, value)
	return profile_multiset(user_id, key, value)
end

init_storage(init, interface)
init_role('profile_role', interface)
