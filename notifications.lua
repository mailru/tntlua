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
-- For each type, a FIFO queue of last fifo_max notifications
-- is maintained, as well as the count of "unread" notifications.
--
-- The number of notification types can change.
--
-- The following storage schema is used:
--
-- space:
--        user_id total_unread type1_unread_count type1_fifo
--                             type2_unread_count type2_fifo
--                             type3_unread_count type3_fifo
--                             ....
-- Supported operations include:
--
-- notification_add(user_id, fifo_no, notification_id)
-- - pushes a notification into the fifo queue associated with the
--   type, unless it's already there, incrementing the count of unread
--   notifications associated  with the given fifo, as well as the total
--   count. However, if unread_count is above fifo_max, counters
--   are not incremented.
--
-- notification_read(user_id, fifo_no)
-- - read the notifications in the given fifo.
--   This substracts the fifo unread_count from the total
--   unread_count, and drops the fifo unread_count to 0.
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
-- current total number of different notification types
local type_max = 5
-- how many notifications of each type the associated
-- FIFO keeps
local fifo_max = 20
-- field index of the first fifo in the tuple.
-- field 0 - user id, field 1 - total unread count, hence
local fifo0_fieldno = 2

-- Find the tuple associated with user id or create a new
-- one.
function notification_find_or_insert(user_id)
    tuple = box.select(space_no, 0, user_id)
    if tuple ~= nil then
        return tuple
    end
    -- create an empty fifo for each notification type
    fifos = {}
    for i = 1, type_max do
        table.insert(fifos, 0) -- unread count - 0
        table.insert(fifos, "") -- notification list - empty
    end
    box.insert(space_no, user_id, 0, unpack(fifos))
    -- we can't use the tuple returned by box.insert, since it could
    -- have changed since.
    return box.select(space_no, 0, user_id)
end

function notification_total_unread_count(user_id)
    return box.unpack('i', notification_find_or_insert(user_id)[1])
end

function notification_unread_count(user_id, fifo_no)
    -- calculate the field no of the associated fifo
    fieldno = fifo0_fieldno + tonumber(fifo_no) * 2
    return box.unpack('i', notification_find_or_insert(user_id)[fieldno])
end

--
-- Find if id is already present in the given fifo
-- return true and id offset when found,
-- false len(fifo) when not found
--
local function is_duplicate(fifo, id)
    for i=1, #fifo, #id do
        if string.sub(fifo, i, i+#id-1) == id then
            -- we're going to insert a new notifiation,
            -- return the old notification offset relative to
            -- the new field size
            return true, i - 1 + #id
        end
    end
    return false, fifo_max * #id
end

--
-- Append a notification to its fifo (one fifo per notification type)
-- Update 'unread' counters, unless they are already at their max.
--
function notification_push(user_id, fifo_no, id)
    fieldno = fifo0_fieldno + tonumber(fifo_no) * 2
    -- insert the new id at the beginning of fifo, truncate the tail
    -- use box SPLICE operation for that
    format = ":p:p"
    args = { fieldno + 1, box.pack("ppp", 0, 0, id), fieldno + 1 }
    --
    -- check whether a duplicate id is already present
    --
    tuple = notification_find_or_insert(user_id)
    _, dup_offset = is_duplicate(tuple[fieldno+1], id)
    --
    -- if duplicate is there, dup_offset points at it.
    -- otherwise it points at fifo tail
    --
    table.insert(args, box.pack("ppp", dup_offset, #id, ""))
    unread_count = box.unpack('i', tuple[fieldno])
    -- check if the counters need to be updated
    if unread_count < fifo_max and dup_offset > unread_count * #id then
    -- prepend ++total_unread_count, ++unread_count to our update operation
        format = "+p+p"..format
        args1 = args
        args = { 1, 1, fieldno, 1 }
        for _, v in ipairs(args1) do table.insert(args, v) end
    end
    return box.update(space_no, user_id, format, unpack(args))
end

-- read notifications

function notification_read(user_id, fifo_no)
    unread_count = notification_unread_count(user_id, fifo_no)
    fieldno = fifo0_fieldno + tonumber(fifo_no) * 2
    return box.update(space_no, user_id, "+p+p",
                      1, -unread_count, fieldno, -unread_count)[fieldno+1]
end
