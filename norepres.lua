-- norepres.lua

--
-- Delete all noreply reminders for user @userid.
-- Returns flag of deletion success.
--
function norepres_delall(userid)
        -- client sends integers encoded as BER-strings
        userid = box.unpack('i', userid)

        local tuples = { box.select(0, 0, userid) }
        for _, tuple in pairs(tuples) do
                box.delete(0, userid, tuple[1])
        end

        return box.pack('i', #tuples)
end
