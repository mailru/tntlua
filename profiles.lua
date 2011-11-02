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

-- namespace which stores all notifications
local space_no = 0

-- Find the tuple associated with user id or create a new
-- one.
function profile_find_or_insert(user_id)
    tuple = box.select(space_no, 0, user_id)
    if tuple ~= nil then
        return tuple
    end
    box.insert(space_no, user_id)
    -- we can't use the tuple returned by box.insert, since it could
    -- have changed since.
    return box.select(space_no, 0, user_id)
end

function profile_find_attribute(tuple, pref_id)
    i = 2
    k, v = tuple:next() -- skip user id
    while k ~= nil do
        k, v = tuple:next(k)
        if k ~= nil then
            if v == pref_id then
                return i
            end
            k, v = tuple:next(k)
            i = i + 2
        end
    end
    return nil
end

--
-- Update a preference, given its id. If it's being set for the first
-- time, simply append both pref_id and pref_value to the list of
-- preferences.
--
function profile_set(user_id, pref_id, pref_value)
    tuple = profile_find_or_insert(user_id)
    pref_fieldno = profile_find_attribute(tuple, pref_id)
    if pref_fieldno ~= nil then
        return box.update(space_no, user_id, "=p", pref_fieldno, pref_value)
    else
        tuple = {tuple:unpack()}
        table.insert(tuple, pref_id)
        table.insert(tuple, pref_value)
        return box.replace(space_no, unpack(tuple))
    end
end

function profile_multiset(user_id, ...)
    local pref_list = {...}
    local pref_list_len = table.getn(pref_list)

    if pref_list_len % 2 ~= 0 then
        error("illegal parameters: var arguments list should contain pairs pref_id pref_value")
    end

    if pref_list_len == 0 then
        --- nothing to set, return the old tuple (if it exists)
        return box.select(space_no, 0, user_id)
    end

    local old_tuple = profile_find_or_insert(user_id)
    local new_tuple = { old_tuple:unpack() }

    -- initialize iterator by pref argument list, all arguments go by pair
    -- id and value.
    local itr, pref_id = next(pref_list)
    local itr, pref_value = next(pref_list, itr)
    while pref_id ~= nil and pref_value ~= nil do
        pref_fieldno = profile_find_attribute(old_tuple, pref_id)
        if pref_fieldno ~= nil then
            -- this entry exists, update the old entry
            new_tuple[pref_fieldno + 1] = pref_value
        else
            -- this is a new entry, append it to the end of the tuple
            table.insert(new_tuple, pref_id)
            table.insert(new_tuple, pref_value)
        end

        -- go to the next pair
        itr, pref_id = next(pref_list, itr)
        itr, pref_value = next(pref_list, itr)
    end

    return box.replace(space_no, unpack(new_tuple))
end

function profile_get(user_id)
    return box.select(space_no, 0, user_id)
end
