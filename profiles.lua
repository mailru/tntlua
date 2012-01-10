--
-- Store and serve user preferences.
-- For every user, stores a list of preferences or attributes,
-- as a list of pairs -- attribute_name, attribute_value
--
-- It is assumed that each user may have a fairly large number
-- of preferences (e.g. 500), but keep most of them set to
-- default. Therefore, we only store non-default preferences.
--
-- The following storage schema is used:
--
-- space:
--        user_id pref_id pref_value pref_id pref_value ...
--
-- profile_get(user_id)
-- - returns a tuple with all user preferences
--
-- profile_set(user_id, pref_id, pref_value)
-- - sets a given preference.
--
-- profile_multiset(profile_id, ...)
-- - sets a given variable number of preferences.
--

-- namespace which stores all notifications
local space_no = 0


-- ========================================================================= --
-- Profile local functions
-- ========================================================================= --

local function print_profile(header, profile)
    if not profiles._debug then
	return
    end
    print(header)
    print("id = ", profile.id)
    for pref_id, pref_value in pairs(profile.prefs) do
	print("prefs[", pref_id, "] = ", pref_value)
    end
end

local function load_profile(profile_id)
    -- init empty profile
    local profile = {}
    profile.id = profile_id
    profile.prefs = {}

    -- try to find tuple
    local tuple = box.select(space_no, 0, profile_id)
    if tuple ~= nil then
	-- tuple exists, fill preference list
	local k, _ = tuple:next() -- skip user id
	while k ~= nil do
	    local pref_key, pref_value
	    -- get preference key
	    k, pref_key = tuple:next(k)
	    if k == nil then
		break
	    end
	    pref_key = box.unpack("i", pref_key)
	    -- get preference value
	    k, pref_value = tuple:next(k)
	    if k == nil then
		break
	    end
	    pref_value = box.unpack("i", pref_value)
	    -- put preference
	    profile.prefs[pref_key] = pref_value
	end
    end

    print_profile("load", profile)
    return profile
end

local function store_profile(profile)
    print_profile("store", profile)
    -- init profile tuple
    local tuple = { profile.id }
    -- put preference to tuple
    for pref_id, pref_value in pairs(profile.prefs) do
	-- insert preference id
	table.insert(tuple, pref_id)
	-- insert preference value
	table.insert(tuple, pref_value)
    end
    return box.replace(space_no, unpack(tuple))
end


-- ========================================================================= --
-- Profile interface
-- ========================================================================= --

profiles = {
    -- enable/disable debug functions
    _debug = true,
}

function profile_get(profile_id)
    return box.select(space_no, 0, profile_id)
end

function profile_set(profile_id, pref_key, pref_value)
    --
    -- check input params
    --

    -- profile id
    if profile_id == nil then
	error("profile's id undefied")
    end
    -- preference key
    if pref_key == nil then
	error("preference key undefined")
    end
    -- preference value
    if pref_value == nil then
	error("preference value undefined")
    end

    --
    -- process
    --

    -- load profile
    local profile = load_profile(profile_id)
    -- set preference
    profile.prefs[pref_key] = pref_value
    -- store result
    return store_profile(profile)
end

function profile_multiset(profile_id, ...)
    local pref_list = {...}
    local pref_list_len = table.getn(pref_list)

    --
    -- check input params
    --

    -- profile id
    if profile_id == nil then
	error("profile's id undefied")
    end
    -- preference list's length
    if pref_list_len == 0 then
        --- nothing to set, return the old tuple (if it exists)
        return box.select(space_no, 0, profile_id)
    end
    -- preference list's parity
    if pref_list_len % 2 ~= 0 then
        error("illegal parameters: var arguments list should contain pairs pref_key pref_value")
    end

    --
    -- process
    --

    -- load profile
    local profile = load_profile(profile_id)

    -- initialize iterator by pref argument list, all arguments go by pair
    -- id and value.
    local i, pref_key = next(pref_list)
    local i, pref_value = next(pref_list, i)
    while pref_key ~= nil and pref_value ~= nil do
	-- set new preference value
	profile.prefs[pref_key] = pref_value
        -- go to the next pair
        i, pref_key = next(pref_list, i)
        i, pref_value = next(pref_list, i)
    end

    -- store result
    return store_profile(profile)
end
