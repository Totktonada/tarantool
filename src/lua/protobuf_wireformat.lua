local ffi = require('ffi')

-- Internal function for protobuf integer encoding.
-- Used for VARINT encoding and tag encoding for other types.
local function internal_encode_uint(data)
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

-- Encode for any VARINT values except sintN
local function encode_uint(data, field_id)
    local wire_type = 0
    return internal_encode_uint(bit.lshift(field_id, 3) + wire_type) ..
        internal_encode_uint(data)
end

-- "ZigZag" encoding for sintN values in VARINT
local function encode_sint(data, field_id)
    if data > 0 then
        return encode_uint(2 * data, field_id)
    else
        return encode_uint(2 * (-data) - 1, field_id)
    end
end

-- Encode any I32 values except float
local function encode_fixed32(data, field_id)
    local wire_type = 5
    local code = ''
    local byte
    data = ffi.cast('uint32_t', data)
    repeat
        byte = data % 256
        data = data / 256
        code = code .. string.char(tonumber(byte))
    until string.len(code) == 4
    return internal_encode_uint(bit.lshift(field_id, 3) + wire_type) .. code
end

-- Encode float values from I32
local function encode_float(data, field_id)
    local wire_type = 5
    local p = ffi.new('float[1]')
    p[0] = data
    return internal_encode_uint(bit.lshift(field_id, 3) + wire_type) ..
        ffi.string(ffi.cast('char*', p), 4)
end

-- Encode any I64 values except double
local function encode_fixed64(data, field_id)
    local wire_type = 1
    local code = ''
    local byte
    data = ffi.cast('uint64_t', data)
    repeat
        byte = data % 256
        data = data / 256
        code = code .. string.char(tonumber(byte))
    until string.len(code) == 8
    return internal_encode_uint(bit.lshift(field_id, 3) + wire_type) .. code
end

-- Encode double value from I64
local function encode_double(data, field_id)
    local wire_type = 1
    local p = ffi.new('double[1]')
    p[0] = data
    return internal_encode_uint(bit.lshift(field_id, 3) + wire_type) ..
        ffi.string(ffi.cast('char*', p), 8)
end

-- Encode any LEN values
local function encode_len(data, field_id)
    local wire_type = 2
    return string.format('%s%s%s',
        internal_encode_uint(bit.lshift(field_id, 3) + wire_type),
        internal_encode_uint(string.len(data)), data)
end

return{
    encode_uint = encode_uint,
    encode_sint = encode_sint,
    encode_float = encode_float,
    encode_fixed32 = encode_fixed32,
    encode_double = encode_double,
    encode_fixed64 = encode_fixed64,
    encode_len = encode_len,
}
