
function get_sender_list_by_offset(offset, limit)
        offset = box.unpack('i', offset)
        limit = box.unpack('i', limit)

        return { box.select_limit(0, 0, offset, limit) }
end
