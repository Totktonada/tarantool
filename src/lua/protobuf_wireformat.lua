local ffi = require('ffi')

local WIRE_TYPE_VARINT = 0
local WIRE_TYPE_I64 = 1
local WIRE_TYPE_LEN = 2
-- SGROUP (3) and EGROUP (4) are deprecated in proto3.
local WIRE_TYPE_I32 = 5

-- {{{ Helpers

-- 32-bit IEEE 754 representation of the given number.
local function as_float(data)
    local p = ffi.new('float[1]')
    p[0] = data
    return ffi.string(ffi.cast('char *', p), 4)
end

-- 64-bit IEEE 754 representation of the given number.
local function as_double(data)
    local p = ffi.new('double[1]')
    p[0] = data
    return ffi.string(ffi.cast('char *', p), 8)
end

-- Encode an integral value as VARINT without a tag.
--
-- Input: number (integral), cdata<int64_t>, cdata<uint64_t>.
--
-- This is a helper function to encode tag and data values.
local function encode_varint(data)
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

-- Encode a tag byte from the given field ID and the given
-- Protocol Buffers wire type.
--
-- This is the first byte in the Tag-Length-Value encoding.
local function encode_tag(field_id, wire_type)
    assert(wire_type >= 0 and wire_type <= 5)
    return encode_varint(bit.bor(bit.lshift(field_id, 3), wire_type))
end

-- }}} Helpers

-- {{{ encode_* functions

-- Encode an integral value as VARINT using the two's complement
-- encoding.
--
-- Input: number (integral), cdata<int64_t>, cdata<uint64_t>.
--
-- Use it for Protocol Buffers types: int32, int64, int64, uint64,
-- bool, enum.
local function encode_uint(data, field_id)
    return encode_tag(field_id, WIRE_TYPE_VARINT) .. encode_varint(data)
end

-- Encode an integral value as VARINT using the "ZigZag" encoding.
--
-- Input: number (integral), cdata<int64_t>, cdata<uint64_t>.
--
-- Use it for Protocol Buffers types: sint32, sint64.
local function encode_sint(data, field_id)
    local zz = data >= 0 and 2 * data or 2 * (-data) - 1
    return encode_uint(zz, field_id)
end

-- Encode an integral value as I32.
--
-- Input: number (integral), cdata<int64_t>, cdata<uint64_t>.
--
-- Use it for Protocol Buffers types: fixed32, sfixed32.
local function encode_fixed32(data, field_id)
    local code = ''
    local byte
    data = ffi.cast('uint32_t', data)
    repeat
        byte = data % 256
        data = data / 256
        code = code .. string.char(tonumber(byte))
    until string.len(code) == 4
    return encode_tag(field_id, WIRE_TYPE_I32) .. code
end

-- Encode a floating point value as I32.
--
-- Input: number.
--
-- Use it for Protocol Buffers types: float.
local function encode_float(data, field_id)
    return encode_tag(field_id, WIRE_TYPE_I32) .. as_float(data)
end

-- Encode an integral value as I36.
--
-- Input: number (integral), cdata<int64_t>, cdata<uint64_t>.
--
-- Use it for Protocol Buffers types: fixed64, sfixed64.
local function encode_fixed64(data, field_id)
    local code = ''
    local byte
    data = ffi.cast('uint64_t', data)
    repeat
        byte = data % 256
        data = data / 256
        code = code .. string.char(tonumber(byte))
    until string.len(code) == 8
    return encode_tag(field_id, WIRE_TYPE_I64) .. code
end

-- Encode a floating point value as I64.
--
-- Input: number.
--
-- Use it for Protocol Buffers types: double.
local function encode_double(data, field_id)
    return encode_tag(field_id, WIRE_TYPE_I64) .. as_double(data)
end

-- Encode a string value as LEN.
--
-- The string contains raw bytes to encode.
--
-- Use it for Protocol Buffers primitives: string, bytes, embedded
-- message, packed repeated fields.
local function encode_len(data, field_id)
    return string.format('%s%s%s',
        encode_tag(field_id, WIRE_TYPE_LEN),
        encode_varint(string.len(data)),
        data)
end

-- }}} encode_* functions

return{
    encode_uint = encode_uint,
    encode_sint = encode_sint,
    encode_float = encode_float,
    encode_fixed32 = encode_fixed32,
    encode_double = encode_double,
    encode_fixed64 = encode_fixed64,
    encode_len = encode_len,
}
