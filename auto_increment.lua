-- Assumes that spaceno has a TREE int32 (NUM) primary key
-- inserts a tuple after getting the next value of the
-- primary key and returns it back to the user
function box.auto_increment(spaceno, ...)
    max_tuple = box.space[spaceno].index[0].idx:max()
    if max_tuple ~= nil then
        max = box.unpack('i', max_tuple[0])
    else
        max = -1
    end
    return box.insert(spaceno, max + 1, ...)
end
