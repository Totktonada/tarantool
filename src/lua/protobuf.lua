local ffi = require('ffi')
local wireformat = require('internal.protobuf.wireformat')
local protocol_mt
-- These constants are used to define the boundaries of valid field ids.
-- Described in more detail here:
-- https://protobuf.dev/programming-guides/proto3/#assigning
local MIN_FIELD_ID = 1
local RESERVED_FIELD_ID_MIN = 19000
local RESERVED_FIELD_ID_MAX = 19999
local MAX_FIELD_ID = 2^29 - 1

local int64_t = ffi.typeof('int64_t')
local uint64_t = ffi.typeof('uint64_t')

local scalars = {}
local non_scalars = {}

-- Create a message object suitable to pass
-- into the protobuf.protocol function.
--
-- Accepts a name of the message and a message
-- definition in the following format.
--
-- message_def = {
--    <field_name> = {<field_type>, <field_id>},
--    <...>
-- }
local function message(message_name, message_def)
    local field_by_name = {}
    local field_by_id = {}
    for field_name, def in pairs(message_def) do
        local field_type = def[1]
        local field_id = def[2]
        local field_type, rep = string.gsub(field_type, 'repeated%s', '')
        if field_by_id[field_id] ~= nil then
            error(('Id %d in field %q was already used'):format(field_id,
                field_name))
        end
        if field_id < MIN_FIELD_ID or field_id > MAX_FIELD_ID then
            error(('Id %d in field %q is out of range [%d; %d]'):format(
                field_id, field_name, MIN_FIELD_ID, MAX_FIELD_ID))
        end
        if field_id >= RESERVED_FIELD_ID_MIN and
           field_id <= RESERVED_FIELD_ID_MAX then
           error(('Id %d in field %q is in reserved ' ..
               'id range [%d, %d]'):format(field_id, field_name,
               RESERVED_FIELD_ID_MIN, RESERVED_FIELD_ID_MAX))
        end
        local field_def = {
            type = field_type,
            name = field_name,
            id = field_id,
        }
        if rep ~= 0 then
            field_def['repeated'] = true
        end
        field_by_name[field_name] = field_def
        field_by_id[field_id] = field_def
    end
    return {
        type = 'message',
        name = message_name,
        field_by_name = field_by_name,
        field_by_id = field_by_id
    }
end

-- Create a enum object suitable to pass into
-- the protobuf.protocol function.
--
-- Accepts a name of an enum and an enum definition
-- in the following format.
--
-- enum_def = {
--     <value_name> = <value_id>,
--     <...>
-- }
local function enum(enum_name, enum_def)
    local id_by_value = {}
    local value_by_id = {}
    for value_name, value_id in pairs(enum_def) do
        if value_by_id[value_id] ~= nil then
            error(('Double definition of enum field %q by %d'):format(
                value_name, value_id))
        end
        local field_def = {type = 'int32', name = value_name}
        scalars['int32'].validate(field_def, value_id)
        id_by_value[value_name] = value_id
        value_by_id[value_id] = value_name
    end
    if value_by_id[0] == nil then
        error(('%q definition does not contain a field with id = 0'):
            format(enum_name))
    end
    return {
        type = 'enum',
        name = enum_name,
        id_by_value = id_by_value,
        value_by_id = value_by_id,
    }
end

-- Create a protocol object that stores message
-- data needed for encoding.
--
-- Accepts protocol definition using protobuf.message
-- and protobuf.enum functions as in example.
--
-- protocol_def = {
--     protocol.message(<message_name>, <message_def>),
--     protocol.enum(<enumc_name>, <enum_def>),
--     <...>
-- }
local function protocol(protocol_def)
    local return_protocol = {}
    -- Declaration table is used to check forward declarations
    -- false -- this type used as the field type in the message was not defined
    -- true -- this field type was defined
    local declarations = {}
    for _, def in pairs(protocol_def) do
        if declarations[def.name] then
            error(('Double definition of name %q'):format(def.name))
        end
        if def.type == 'message' then
            for _, field_def in pairs(def.field_by_id) do
                local standard = scalars[field_def.type] ~= nil
                local declared = declarations[field_def.type]
                if field_def.type == def.name then
                    error(('Message %q has a field of %q type ' ..
                        'Recursive definition is not allowed'):format(
                        def.name, field_def.type))
                elseif not standard and not declared then
                    declarations[field_def.type] = false
                end
            end
        end
        declarations[def.name] = true
        return_protocol[def.name] = def
    end
    -- Detects a message or a enum that is used as a field type in message
    -- but not defined in protocol. Allows a type be defined after usage
    for def_type, declared in pairs(declarations) do
        if not declared then
            error(('Type %q is not declared'):format(def_type))
        end
    end
    return setmetatable(return_protocol, protocol_mt)
end

local function is_number64(value)
    if type(value) == 'cdata' and (ffi.istype(int64_t, value) or
        ffi.istype(uint64_t, value)) then
        return true
    end
end

local function validate_length(value)
    local MAX_LEN = 2^32
    if string.len(value) > MAX_LEN then
        error("Too long string to be encoded")
    end
end

local function encode_len(field_id, value)
    local wire_type = 2
    return string.format('%s%s%s',
        wireformat.encode_uint(bit.lshift(field_id, 3) + wire_type),
        wireformat.encode_uint(string.len(value)),
        value)
end

local encode_field

local function code_rep_packed(protocol, name, field_name, field_id, data)
    local buf = {}
    protocol[name]['field_by_name'][field_name]['repeated'] = false
    for _, value in pairs(data) do
        table.insert(buf, string.sub(encode_field(protocol, field_name,
            name, value), 2))
    end
    protocol[name]['field_by_name'][field_name]['repeated'] = true
    return encode_len(field_id, table.concat(buf))
end

local function code_rep_unpacked(protocol, name, field_name, data)
    local buf = {}
    protocol[name]['field_by_name'][field_name]['repeated'] = false
    for _, value in pairs(data) do
        table.insert(buf, encode_field(protocol, field_name, name, value))
    end
    protocol[name]['field_by_name'][field_name]['repeated'] = true
    return table.concat(buf)
end

local function check_integer(field_def, value)
    if (type(value) == 'number' and math.ceil(value) ~= value) then
        error(('Input number value %f for %q is not integer'):format(
            value, field_def.name))
    elseif type(value) == 'cdata' and not is_number64(value) then
        error(('Input cdata value %q for %q field is not integer'):format(
            ffi.typeof(value), field_def.name))
    end
end

local function encode_enum(value, list_of_values, field_id, field_type)
    if type(value) ~= 'number' and list_of_values[value] == nil then
        error(('%q is not defined in %q enum'):format(value, field_type))
    end
    -- According to open enums semantics unknown enum values are encoded as
    -- numeric identifier. https://protobuf.dev/programming-guides/enum/
    if type(value) == 'number' then
        local field_def = {type = 'int32', id = field_id}
        scalars['int32'].validate(field_def, value)
        return scalars['int32'].encode(field_id, value)
    else
        return scalars['int32'].encode(field_id, list_of_values[value])
    end
end

encode_field = function(protocol, field_name, name, value)
    local field_by_name = protocol[name]['field_by_name']
    if field_by_name[field_name] == nil and
        field_name ~= '_unknown_fields' then
            error(('Wrong field name %q for %q protocol'):
                format(field_name, name))
    end
    if field_name == '_unknown_fields' then
        return table.concat(value)
    else
        local field_type = field_by_name[field_name]['type']
        local field_id = field_by_name[field_name]['id']
        local repeated = field_by_name[field_name]['repeated']
        local scalar_def = scalars[field_type]
        local non_scalars_def = protocol[field_type]
        if repeated then
            if type(value) ~= 'table' then
                error('For repeated fields table data are needed')
            elseif scalar_def ~= nil and scalar_def.encode_as_packed then
                return code_rep_packed(protocol, name, field_name,
                    field_id, value)
            else
                return code_rep_unpacked(protocol, name, field_name, value)
            end
        elseif scalar_def ~= nil then
            scalar_def.validate(field_by_name[field_name], value)
            return scalar_def.encode(field_id, value)
        elseif non_scalars_def ~= nil then
            return non_scalars[non_scalars_def['type']].encode(protocol,
                field_id, field_type, value)
        else
            assert(false)
        end
    end
end

local function validate_type(field_def, value, exp_type)
    if type(exp_type) == 'table' then
        local found = false
        for _, exp_t in pairs(exp_type) do
            if type(value) == exp_t then
                found = true
                break
            end
        end
        if not found then
            error(('Field %q of %q type gets %q type value. ' ..
                'Unsupported or colliding types'):format(field_def.name,
                field_def.type, type(value)))
        end
        return
    end
    assert(type(exp_type) == 'string')
    if type(value) ~= exp_type then
        error(('Field %q of %q type gets %q type value. ' ..
            'Unsupported or colliding types'):format(field_def.name,
            field_def.type, type(value)))
    end
    return
end

local function validate_value_error(field_def, value)
    error(('Input data for %q field is %q and do not fit in %q')
        :format(field_def.name, value, field_def.type))
end

local function validate_value(field_def, value, min, max)
    -- To avoid incorrect comparison unsigned cdata is not
    -- checked for lower limit
    if type(value) == 'cdata' and ffi.istype(uint64_t, value) then
        if value > max then
            validate_value_error(field_def, value)
        end
        return
    end
    if value < min or value > max then
        validate_value_error(field_def, value)
    end
end

scalars.float = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_FLOAT = 3.4028234E+38
        local MIN_FLOAT = -3.4028234E+38
        validate_type(field_def, value, 'number')
        validate_value(field_def, math.abs(value), MIN_FLOAT, MAX_FLOAT)
    end,
    encode = function(field_id, value)
        local wire_type = 5
        return wireformat.encode_uint(bit.lshift(field_id, 3) + wire_type) ..
            wireformat.encode_float(value)
    end,
}

scalars.fixed32 = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_FIXED32 = 2^32 - 1
        local MIN_FIXED32 = 0
        local MIN_CDATA = 0ULL
        local MAX_CDATA = 4294967295ULL
        validate_type(field_def, value, {'number', 'cdata'})
        validate_value(field_def, value, MIN_FIXED32, MAX_FIXED32)
        if type(value) == 'cdata' then
            if not is_number64(value) then
                error(('Input cdata value %q for %q field is not integer'):
                    format(ffi.typeof(value), field_def['name']))
            end
            validate_value(field_def, value, MIN_CDATA, MAX_CDATA)
        end
    end,
    encode = function(field_id, value)
        local wire_type = 5
        return wireformat.encode_uint(bit.lshift(field_id, 3) + wire_type) ..
            wireformat.encode_fixed32(value)
    end,
}

scalars.sfixed32 = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_SFIXED32 = 2^31 - 1
        local MIN_SFIXED32 = -2^31
        local MIN_CDATA = -2147483648LL
        local MAX_CDATA = 2147483647LL
        validate_type(field_def, value, {'number', 'cdata'})
        validate_value(field_def, value, MIN_SFIXED32, MAX_SFIXED32)
        if type(value) == 'cdata' then
            if not is_number64(value) then
                error(('Input cdata value %q for %q field is not integer'):
                    format(ffi.typeof(value), field_def['name']))
            end
            validate_value(field_def, value, MIN_CDATA, MAX_CDATA)
        end
    end,
    encode = function(field_id, value)
        local wire_type = 5
        return wireformat.encode_uint(bit.lshift(field_id, 3) + wire_type) ..
            wireformat.encode_fixed32(value)
    end,
}

scalars.double = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_DOUBLE = 1.7976931348623157E+308
        local MIN_DOUBLE = -1.7976931348623157E+308
        validate_type(field_def, value, 'number')
        validate_value(field_def, value, MIN_DOUBLE, MAX_DOUBLE)
    end,
    encode = function(field_id, value)
        local wire_type = 1
        return wireformat.encode_uint(bit.lshift(field_id, 3) + wire_type) ..
            wireformat.encode_double(value)
    end,
}

scalars.fixed64 = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_UINT64 = 18446744073709551615
        local MIN_UINT64 = 0
        local MIN_CDATA = 0ULL
        local MAX_CDATA = 18446744073709551615ULL
        validate_type(field_def, value, {'number', 'cdata'})
        if type(value) == 'number' then
            validate_value(field_def, value, MIN_UINT64, MAX_UINT64)
        end
        if type(value) == 'cdata' then
            if not is_number64(value) then
                error(('Input cdata value %q for %q field is not integer'):
                    format(ffi.typeof(value), field_def['name']))
            end
            validate_value(field_def, value, MIN_CDATA, MAX_CDATA)
        end
    end,
    encode = function(field_id, value)
        local wire_type = 1
        return wireformat.encode_uint(bit.lshift(field_id, 3) + wire_type) ..
            wireformat.encode_fixed64(value)
    end,
}

scalars.sfixed64 = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_SINT64 = 9223372036854775806
        local MIN_SINT64 = -9223372036854775807
        local MIN_CDATA = -9223372036854775807LL
        local MAX_CDATA = 9223372036854775806LL
        validate_type(field_def, value, {'number', 'cdata'})
        if type(value) == 'number' then
            validate_value(field_def, value, MIN_SINT64, MAX_SINT64)
        end
        if type(value) == 'cdata' then
            if not is_number64(value) then
                error(('Input cdata value %q for %q field is not integer'):
                    format(ffi.typeof(value), field_def['name']))
            end
            validate_value(field_def, value, MIN_CDATA, MAX_CDATA)
        end
    end,
    encode = function(field_id, value)
        local wire_type = 1
        return wireformat.encode_uint(bit.lshift(field_id, 3) + wire_type) ..
            wireformat.encode_fixed64(value)
    end,
}

scalars.string = {
    encode_as_packed = false,
    validate = function(field_def, value)
        validate_type(field_def, value, 'string')
        validate_length(value)
    end,
    encode = encode_len
}

scalars.bytes = scalars.string

scalars.int32 = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_INT32 = 2^31 - 1
        local MIN_INT32 = -2^31
        validate_type(field_def, value, {'number', 'cdata'})
        check_integer(field_def, value)
        validate_value(field_def, value, MIN_INT32, MAX_INT32)
    end,
    encode = function(field_id, value)
        if value >= 0 then
            return wireformat.encode_uint(bit.lshift(field_id, 3)) ..
                wireformat.encode_uint(value)
        elseif value < 0 then
            return wireformat.encode_uint(bit.lshift(field_id, 3)) ..
                wireformat.encode_uint(ffi.cast('uint64_t', value))
        end
    end,
}

scalars.sint32 = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_SINT32 = 2^31 - 1
        local MIN_SINT32 = -2^31
        validate_type(field_def, value, {'number', 'cdata'})
        check_integer(field_def, value)
        validate_value(field_def, value, MIN_SINT32, MAX_SINT32)
    end,
    encode = function(field_id, value)
        if value >= 0 then
            return wireformat.encode_uint(bit.lshift(field_id, 3)) ..
                wireformat.encode_uint(value)
        elseif value < 0 then
            return wireformat.encode_sint(value, bit.lshift(field_id, 3))
        end
    end,
}

scalars.uint32 = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_UINT32 = 2^32 - 1
        local MIN_UINT32 = 0
        validate_type(field_def, value, {'number', 'cdata'})
        check_integer(field_def, value)
        validate_value(field_def, value, MIN_UINT32, MAX_UINT32)
    end,
    encode = function(field_id, value)
        return wireformat.encode_uint(bit.lshift(field_id, 3)) ..
            wireformat.encode_uint(value)
    end,
}

scalars.int64 = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_INT64 = 2^63 - 1
        local MIN_INT64 = -2^63
        local MIN_CDATA = -9223372036854775808LL
        local MAX_CDATA = 9223372036854775807LL
        validate_type(field_def, value, {'number', 'cdata'})
        check_integer(field_def, value)
        if type(value) == 'number' then
            validate_value(field_def, value, MIN_INT64, MAX_INT64)
        end
        if type(value) == 'cdata' then
            if not is_number64(value) then
                error(('Input cdata value %q for %q field is not integer'):
                    format(ffi.typeof(value), field_def['name']))
            end
            validate_value(field_def, value, MIN_CDATA, MAX_CDATA)
        end
    end,
    encode = function(field_id, value)
        if value >= 0 then
            return wireformat.encode_uint(bit.lshift(field_id, 3)) ..
                wireformat.encode_uint(value)
        elseif value < 0 then
            return wireformat.encode_uint(bit.lshift(field_id, 3)) ..
                wireformat.encode_uint(ffi.cast('uint64_t', value))
        end
    end,
}

scalars.sint64 = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_SINT64 = 2^63 - 1
        local MIN_SINT64 = -2^63
        local MIN_CDATA = -9223372036854775808LL
        local MAX_CDATA = 9223372036854775807LL
        validate_type(field_def, value, {'number', 'cdata'})
        check_integer(field_def, value)
        if type(value) == 'number' then
            validate_value(field_def, value, MIN_SINT64, MAX_SINT64)
        end
        if type(value) == 'cdata' then
            if not is_number64(value) then
                error(('Input cdata value %q for %q field is not integer'):
                    format(ffi.typeof(value), field_def['name']))
            end
            validate_value(field_def, value, MIN_CDATA, MAX_CDATA)
        end
    end,
    encode = function(field_id, value)
        if value >= 0 then
            return wireformat.encode_uint(bit.lshift(field_id, 3)) ..
                wireformat.encode_uint(value)
        elseif value < 0 then
            return wireformat.encode_sint(value, bit.lshift(field_id, 3))
        end
    end,
}

scalars.uint64 = {
    encode_as_packed = true,
    validate = function(field_def, value)
        local MAX_UINT64 = 2^64 - 1
        local MIN_UINT64 = 0
        validate_type(field_def, value, {'number', 'cdata'})
        if type(value) == 'cdata' then
            if not is_number64(value) then
                error(('Input cdata value %q for %q field is not integer'):
                    format(ffi.typeof(value), field_def['name']))
            end
            return
        end
        check_integer(field_def, value)
        if value > MAX_UINT64 or value < MIN_UINT64 then
           error(('Input data for %q field is %d and do not fit in ' ..
               'uint_64'):format(field_def.name, tonumber(value)))
        end
    end,
    encode = function(field_id, value)
        return wireformat.encode_uint(bit.lshift(field_id, 3)) ..
            wireformat.encode_uint(value)
    end,
}

scalars.bool = {
    encode_as_packed = true,
    validate = function(field_def, value)
        validate_type(field_def, value, 'boolean')
    end,
    encode = function(field_id, value)
        local tag = wireformat.encode_uint(bit.lshift(field_id, 3))
        if value then
            return tag .. '\x01'
        else
            return tag .. '\x00'
        end
    end,
}

-- Encodes the entered data in accordance with the
-- selected protocol into binary format.
--
-- Accepts a protocol created by protobuf.protocol function,
-- a name of a message selected for encoding and
-- the data that needs to be encoded in the following format.
--
-- data = {
--     <field_name> = <value>,
--     <...>
-- }

local function encode(protocol, name, data)
    local buf = {}
    local message_def = protocol[name]
    if message_def == nil then
        error(('There is no message or enum named %q in the given protocol')
            :format(name))
    end
    if message_def.type ~= 'message' then
        assert(message_def.type == 'enum')
        error(('Attempt to encode enum %q as a top level message'):format(name))
    end
    for field_name, value in pairs(data) do
        table.insert(buf, encode_field(protocol, field_name, name, value))
    end
    return table.concat(buf)
end

-- This table contains encode functions for non scalar protobuf types
-- such as enum and message
non_scalars.enum = {
    encode = function(protocol, field_id, field_type, value)
        local list_of_values = protocol[field_type]['id_by_value']
        return encode_enum(value, list_of_values, field_id, field_type)
    end,
}

non_scalars.message = {
    encode = function(protocol, field_id, field_type, value)
        local encoded_msg = encode(protocol, field_type, value)
        validate_length(encoded_msg)
        return encode_len(field_id, encoded_msg)
    end,
}

protocol_mt = {
    __index = {
        encode = encode,
    }
}

return {
    message = message,
    enum = enum,
    protocol = protocol,
}
