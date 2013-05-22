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
            -- get preference value
            k, pref_value = tuple:next(k)
            if k == nil then
                break
            end
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
    _debug = false,
}

function profile_get(profile_id)
    return box.select(space_no, 0, profile_id)
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
        -- erase preference if its new value is empty string
        if pref_value == '' then
            pref_value = nil
        end
        -- set new preference value
        profile.prefs[pref_key] = pref_value
        -- go to the next pair
        i, pref_key = next(pref_list, i)
        i, pref_value = next(pref_list, i)
    end

    -- store result
    return store_profile(profile)
end

function profile_set(profile_id, pref_key, pref_value)
    return profile_multiset(profile_id, pref_key, pref_value)
end

-- ========================================================================== --
-- helper function which loads profile from tuple
-- ========================================================================== --

-- this function was taken from profiles.lua:load_profile() and should be set in accordance with it
function load_profile2(tuple)
    -- init empty profile
    local profile = {}
    profile.id = tuple[0]
    profile.prefs = {}

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
            -- get preference value
            k, pref_value = tuple:next(k)
            if k == nil then
                break
            end
            -- put preference
            profile.prefs[pref_key] = pref_value
        end
    end

    return profile
end

-- ========================================================================== --
-- helper functions which help to delete empty preferences
-- ========================================================================== --

function profile_cleanup_empty_preferences()
    local cnt = 0
    local n = 0
    
    for tpl in box.space[space_no].index[0]:iterator(box.index.ALL) do
        local profile = load_profile2(tpl)
        local has_empty = false
        
        for k, v in pairs(profile.prefs) do
            if v == '' then
                profile.prefs[k] = nil
                has_empty = true
            end
        end

        if has_empty == true then
            store_profile(profile)
            cnt = cnt + 1
        end

        n = n + 1
        if n == 100 then
            box.fiber.sleep(0.001)
            n = 0
        end
    end

    print(cnt, ' tuples cleaned from empty keys')
    return cnt
end

-- ========================================================================== --
-- cleans up keys with empty string values in every profile
-- needs to be called once for old version of profiles.lua
-- new version no longer produces keys with empty string values
-- ========================================================================== --

function profile_cleanup_empty_preferences_all()
    while profile_cleanup_empty_preferences() ~= 0 do end
end

-- ========================================================================== --
-- helper functions which removes particular key in every profile
-- ========================================================================== --
function profile_hexify(str)
    return (
        string.gsub(str, "(.)",
            function (c) return string.format("%02X", string.byte(c)) end
        )
    )
end

function profile_cleanup_key(key)
    if type(key) ~= "string" then
        error("bad parameters")
    end

    local cnt = 0
    local n = 0

    for tpl in box.space[space_no].index[0]:iterator(box.index.ALL) do
        local profile = load_profile2(tpl)
        if profile.prefs[key] ~= nil then
            print('cleanup_key: ', profile.id, '[', profile_hexify(key), ']', ' = ', profile.prefs[key], ', orig tuple: ', tpl)
            profile.prefs[key] = nil
            store_profile(profile)
            cnt = cnt + 1            
        end

        n = n + 1
        if n == 100 then
            box.fiber.sleep(0.001)
            n = 0
        end
    end

    print(cnt, ' tuples cleaned from key [', profile_hexify(key), ']')
    return cnt
end

-- ========================================================================== --
-- cleans up particular key in every profile
-- ========================================================================== --

function profile_cleanup_key_all(key)
    if type(key) ~= "string" then
        error("bad parameters")
    end

    while profile_cleanup_key(key) ~= 0 do end
end
