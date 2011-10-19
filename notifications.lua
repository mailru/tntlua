local space = 51
local default_version = 1

local typ_max = 5
local total_offset = 2
local counter_offset = 3
local notify_offset = counter_offset + typ_max
local notify_max_count = 20

local function to_table(t, typ, notify)
       local packed = t[notify_offset + typ]
       local r = {}
       if packed == "" then
               return r
       end
       local insert, sub, len = table.insert, string.sub, #notify
       for i = 1, # packed / len do
               local j = 1 + (i - 1) * len
               insert(r, sub(packed, j, j))
       end
       return r
end
local from_table = table.concat

local function inc(typ, v)
       if not v then
               v = 1
       end
       return { ['op'] = '+p',
                ['data'] = {counter_offset + typ, v} }
end

local function inc_total(v)
       if not v then
               v = 1
       end
       return { ['op'] = '+p',
                ['data'] = {total_offset, v} }
end

-- TODO: use splice
local function move(typ, arr, i)
       table.insert(arr, 1, table.remove(arr, i))
       return { ['op'] = '=p',
                ['data'] = { notify_offset + typ, from_table(arr) }
        } 
end

local function remove_tail(typ, arr)
       table.remove(arr)
end

local function insert(typ, arr, notify)
       table.insert(arr, notify)
       return { ['op'] = '=p',
                ['data'] = { notify_offset + typ, from_table(arr) } 
        }
end

local function notify_insert_op(t, typ, notify)
       local arr = to_table(t, typ, notify)
       local count = box.unpack('i', (t[typ + counter_offset + 1]))

       for i, v in ipairs(arr) do
               if v == notify then
                       if count < i then
                               return move(typ, arr, i)
                       else
                               return inc_total(), inc(typ), move(typ, arr, i)
                       end
               end
       end

       if #arr == notify_max_count then
               remove_tail(typ, arr) -- NB: this should be op
               return insert(typ, arr, notify)
       else
               return inc_total(), inc(typ), insert(typ, arr, notify)
       end
end

local function get_tuple(user_id)
       local t = box.select(space, 0, user_id)
       if not t then
               t = { user_id, default_version,
                     0, 0, 0, 0, 0, 0,
                     "", "", "", "", "" }
               pcall(box.insert, space, unpack(t))
               t = box.select(space, 0, user_id)
       end
       return t
end

local function glue_ops(ops)
       local op, data = {}, {}

       for i, v in ipairs(ops) do
               table.insert(op, v.op)
               for j, k in ipairs(v.data) do
                       table.insert(data, k)
               end
       end
       return table.concat(op), unpack(data)
end


local function args(user_id, typ)
       user_id, typ = tonumber(user_id), tonumber(typ)

       if not user_id then
               error("bad user_id")
       end
       if not typ or typ < 0 or typ > typ_max then
               error("bad type")
       end

       if not notify or #notify == 0 then
               error("bad notify")
       end

       return user_id, typ
end

function box.notify_insert(user_id, typ, notify)
       user_id, typ = check(user_id, typ)

       local t = get_tuple(user_id)
       local ops = {notify_insert_op(t, typ, notify)}
       box.update(space, user_id, glue_ops(ops))

       return box.select(space, 0, user_id)
end

function box.notify_get(user_id, typ)
       user_id, typ = check(user_id, typ)

       local t = get_tuple(user_id)
       local total, count = box.unpack('i', t[counter_offset]), box.unpack('i', t[typ + counter_offset + 1])

       local ops = {inc(typ, -count), inc(total, -count) }
       box.update(space, user_id, glue_ops(ops))

       return t[notify_offset + typ]
end
