if ffi == nil then
    if require == nil then
        error('You must restart tarantool')
    end
    ffi = require('ffi')
end

ffi.cdef[[
    unsigned perl_crc32(const char *key, unsigned len);
]]

local libperlcrc32 = ffi.load("perlcrc32")

function perl_crc32(key)
    if type(key) ~= "string" then
        error("String key is expected in perl_crc32: '" .. key .. "'")
    end

    return libperlcrc32.perl_crc32(key, #key)
end
