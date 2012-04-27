-- Assumes that spaceno has a TREE int32 (NUM) primary key
-- inserts a tuple after getting the next value of the
-- primary key and returns it back to the user
function box.auto_increment(spaceno, ...)
    spaceno = tonumber(spaceno)
    local max_tuple = box.space[spaceno].index[0].idx:max()
    local max = -1
    if max_tuple ~= nil then
        max = box.unpack('i', max_tuple[0])
    end
    return box.insert(spaceno, max + 1, ...)
end
