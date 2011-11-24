--
-- A system to store and serve notifications in a social network.
-- Stores an association [user_id <-> list of notifications].
--
-- Each notification describes a recent event a user should get
-- informed about, and is represented by a notifcation_id.
--
-- Notification id is a fixed length string.
--
-- Notifications can belong to different types (photo evaluations,
-- unread messages, friend requests).
--
-- For each type, a FIFO queue of last ring_max notifications
-- is maintained, as well as the count of "unread" notifications.
--
-- The number of notification types can change.
--
-- The following storage schema is used:
--
-- space:
--        user_id total_unread type1_unread_count type1_ring
--                             type2_unread_count type2_ring
--                             type3_unread_count type3_ring
--                             ....
-- Supported operations include:
--
-- notification_push(user_id, ring_no, notification_id)
-- - pushes a notification into the ring buffer associated with the
--   type, unless it's already there, incrementing the count of unread
--   notifications associated  with the given ring, as well as the total
--   count. However, if unread_count is above ring_max, counters
--   are not incremented.
--   ring_no is indexed from 0 to ring_max - 1
--
-- notification_read(user_id, ring_no)
-- - read the notifications in the given ring.
--   This substracts the ring unread_count from the total
--   unread_count, and drops the ring unread_count to 0.
--
-- notification_total_unread_count(user_id)
-- - gets the total unread count
--
--  Removing duplicates
--  -------------------
--
--  On top of just maintaining FIFO queues for each type,
--  this set of Lua procedures ensures that each notification
--  in each FIFO is unique. When inserting a new notification id,
--  the FIFO is checked first whether the same id is already there.
--  If it's present in the queue, it's simply moved to the
--  beginning of the line, no duplicate is inserted.
--  Unread counters are incremented only if the old id was
--  already read.

-- namespace which stores all notifications
local space_no = 0
-- description of notification types
-- these ids describe notification id tail offset used by is_duplicate()
local notification_types = { 0, 0, 4, 0, 0 }
-- how many notifications of each type the associated
-- circular buffer keeps
local ring_max = 20
-- field index of the first ring in the tuple.
-- field 0 - user id, field 1 - total unread count, hence
local ring0_fieldno = 2

-- Find the tuple associated with user id or create a new
-- one.
function notification_find_or_insert(user_id)
    local tuple = box.select(space_no, 0, user_id)
    if tuple ~= nil then
        return tuple
    end
    -- create an empty ring for each notification type
    local rings = {}
    for i = 1, #notification_types do
        table.insert(rings, 0) -- unread count - 0
        table.insert(rings, "") -- notification list - empty
    end
    box.insert(space_no, user_id, 0, unpack(rings))
    -- we can't use the tuple returned by box.insert, since it could
    -- have changed since.
    return box.select(space_no, 0, user_id)
end

function notification_total_unread_count(user_id)
    return box.unpack('i', notification_find_or_insert(user_id)[1])
end

function notification_unread_count(user_id, ring_no)
    -- calculate the field no of the associated ring
    local fieldno = ring0_fieldno + tonumber(ring_no) * 2
    return box.unpack('i', notification_find_or_insert(user_id)[fieldno])
end

--
-- Find if id is already present in the given ring
-- return true and id offset when found,
-- false len(ring) when not found
--
local function is_duplicate(ring, id, tail_offset)
    id_len = #id
    id = string.sub(id, 1, id_len - tail_offset)
    for i=1, #ring, id_len do
        n_id = string.sub(ring, i, i+id_len-1 - tail_offset)
        if  n_id == id then
            -- we're going to insert a new notifiation,
            -- return the old notification offset relative to
            -- the new field size
            return true, i - 1 + id_len
        end
    end
    return false, ring_max * id_len
end

--
-- Append a notification to its ring (one ring per notification type)
-- Update 'unread' counters, unless they are already at their max.
--
function notification_push(user_id, ring_no, id)
    local fieldno = ring0_fieldno + tonumber(ring_no) * 2
    -- insert the new id at the beginning of ring, truncate the tail
    -- use box SPLICE operation for that
    local format = ":p:p"
    local args = { fieldno + 1, box.pack("ppp", 0, 0, id), fieldno + 1 }
    --
    -- check whether a duplicate id is already present
    --
    local tuple = notification_find_or_insert(user_id)
    local _, dup_offset = is_duplicate(tuple[fieldno+1], id,
                                       notification_types[ring_no+1])
    --
    -- if duplicate is there, dup_offset points at it.
    -- otherwise it points at ring tail
    --
    table.insert(args, box.pack("ppp", dup_offset, #id, ""))
    local unread_count = box.unpack('i', tuple[fieldno])
    -- check if the counters need to be updated
    if unread_count < ring_max and dup_offset > unread_count * #id then
    -- prepend ++total_unread_count, ++unread_count to our update operation
        format = "+p+p"..format
        local args1 = args
        args = { 1, 1, fieldno, 1 }
        for _, v in ipairs(args1) do table.insert(args, v) end
    end
    return box.update(space_no, user_id, format, unpack(args))
end

-- Read a notification.
-- Return the total unread count, all other unread counts,
-- and ring data in question.

function notification_read(user_id, ring_no)
    local unread_count = notification_unread_count(user_id, ring_no)
    local fieldno = ring0_fieldno + tonumber(ring_no) * 2
    local tuple = box.update(space_no, user_id, "+p+p",
                             1, -unread_count, fieldno, -unread_count)
    local return_fields = {}
    local k, v = tuple:next()
    local k, v = tuple:next(k) -- skip user id
    table.insert(return_fields, v) -- total unread count
    k, v = tuple:next(k) -- now at the first ring
    while k ~= nil do
        table.insert(return_fields, v) -- insert unread count
        k, v = tuple:next(k)
        if k ~= nil then
            k, v = tuple:next(k) -- skip this ring data
        end
    end
    table.insert(return_fields, tuple[fieldno+1]) -- insert ring data
    return return_fields
end
