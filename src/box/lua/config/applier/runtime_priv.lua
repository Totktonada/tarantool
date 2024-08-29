local function process_lua_call(user_or_role_data)
    local lua_call = {}
    local privileges = user_or_role_data.privileges or {}

    for _, privilege in ipairs(privileges) do
        local permissions = privilege.permissions or {}
        local has_execute = false
        for _, perm in ipairs(permissions) do
            if perm == "execute" then
                has_execute = true
                break
            end
        end
        if has_execute and privilege.lua_call ~= nil then
            for _, call in ipairs(privilege.lua_call) do
                lua_call[call] = true
            end
        end
    end
    return lua_call
end

local function apply_runtime_priv(config_module)
    local configdata = config_module._configdata
    local credentials = configdata:get('credentials') or {}
    local roles = credentials.roles or {}
    local users = credentials.users or {}

    local roles_data = {}
    for role_name, role_data in pairs(roles) do
        roles_data[role_name] = {
            lua_call = process_lua_call(role_data),
            roles = role_data.roles or {}
        }
    end

    local function merge_role_lua_call(role_data, role_privileges)
        if role_data == nil then
            return
        end
        for call, _ in pairs(role_data.lua_call) do
            role_privileges[call] = true
        end
        for _, deps_role in ipairs(role_data.roles) do
            merge_role_lua_call(roles_data[deps_role], role_privileges)
        end
    end

    for role_name, role_data in pairs(roles_data) do
        local role_privileges = {}
        merge_role_lua_call(role_data, role_privileges)
        roles_data[role_name].lua_call = role_privileges
    end

    box.internal.lua_call_runtime_priv_reset()
    for user_name, user_data in pairs(users) do
        local lua_call = process_lua_call(user_data)
        local user_roles = user_data.roles or {}
        for _, deps in ipairs(user_roles) do
            if roles_data[deps] ~= nil then
                for call, _ in pairs(roles_data[deps].lua_call) do
                    lua_call[call] = true
                end
            end
        end
        for func_name, _ in pairs(lua_call) do
            if func_name == "all" then
                box.internal.lua_call_runtime_priv_grant(user_name, '')
            else
                box.internal.lua_call_runtime_priv_grant(user_name, func_name)
            end
        end
    end
end

return {
    name = 'runtime_priv',
    apply = apply_runtime_priv
}
