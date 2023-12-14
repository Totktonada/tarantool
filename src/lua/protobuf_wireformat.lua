local ffi = require('ffi')

local function encode_uint(data)
    local code = ''
    local byte
    data = ffi.cast('uint64_t', data)
    repeat
        byte = (data % 128) + 128
        data = data / 128
        if data == 0 then
            byte = byte - 128
        end
        code = code .. string.char(tonumber(byte))
    until data == 0
    return code
end

local function encode_sint(data, field_num)
    if data > 0 then
        return encode_uint(field_num) .. encode_uint(2 * data)
    else
        return encode_uint(field_num) .. encode_uint(2 * (-data) - 1)
    end
end

local function encode_float(data)
    local p = ffi.new('float[1]')
    p[0] = data
    return ffi.string(ffi.cast('char*', p), 4)
end

local function encode_double(data)
    local p = ffi.new('double[1]')
    p[0] = data
    return ffi.string(ffi.cast('char*', p), 8)
end

local function encode_fixed32(data)
    local code = ''
    local byte
    data = ffi.cast('uint32_t', data)
    repeat
        byte = data % 256
        data = data / 256
        code = code .. string.char(tonumber(byte))
    until string.len(code) == 4
    return code
end

local function encode_fixed64(data)
    local code = ''
    local byte
    data = ffi.cast('uint64_t', data)
    repeat
        byte = data % 256
        data = data / 256
        code = code .. string.char(tonumber(byte))
    until string.len(code) == 8
    return code
end

return{
    encode_uint = encode_uint,
    encode_sint = encode_sint,
    encode_float = encode_float,
    encode_fixed32 = encode_fixed32,
    encode_double = encode_double,
    encode_fixed64 = encode_fixed64,
}
