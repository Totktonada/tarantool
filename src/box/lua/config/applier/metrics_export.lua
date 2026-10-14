local clock = require('clock')
local errno = require('errno')
local socket = require('socket')
local urilib = require('uri')
local iostream = require('internal.iostream')
local log = require('internal.config.utils.log')

-- A request line with the header fields.
local MAX_HEAD_SIZE = 8192
-- For a request to come and for its response to go.
local TIMEOUT = 60

-- The content type and the body of each format.
local formats = {
    json = {'application/json', function()
        return require('metrics.plugins.json').export()
    end},
    prometheus = {'text/plain', function()
        return require('metrics.plugins.prometheus').collect_http().body
    end},
}

-- Listening sockets of the HTTP servers.
local listeners = {}
-- The graphite configuration in effect.
local graphite_applied = {}

local function normalize_path(path)
    return '/' .. path:strip('/')
end

-- Returns the status, the content type and the body of the response.
local function respond(endpoints, head)
    local method, target = head:match('^(%S+) (%S+) HTTP/1%.%d\r?\n')
    local path = target and normalize_path(target:match('^[^?]*'))
    local endpoint = method == 'GET' and endpoints[path]
    if not endpoint then
        return 404, 'text/plain', ''
    end
    local content_type, export = unpack(formats[endpoint.format])
    local start = clock.monotonic()
    local ok, body = pcall(export)
    local status = ok and 200 or 500
    if endpoint.metrics ~= nil and endpoint.metrics.enabled then
        -- The histogram of metrics.http_middleware.
        require('metrics.http_middleware').get_default_collector():observe(
            clock.monotonic() - start,
            {path = path, method = method, status = status})
    end
    if not ok then
        log.error('metrics_export: %s: %s', path, body)
        return status, 'text/plain', ''
    end
    return status, content_type, body
end

-- Serves one request per connection: a scrape interval is long enough
-- to connect anew.
local function serve(endpoints, stream)
    local head = ''
    repeat
        local chunk = stream:read(TIMEOUT)
        if chunk == nil or chunk == '' then
            return
        end
        head = head .. chunk
    until head:find('\r?\n\r?\n') or #head >= MAX_HEAD_SIZE
    local status, content_type, body = respond(endpoints, head)
    -- The reason phrase is optional (RFC 9112).
    stream:write(('HTTP/1.1 %d \r\nContent-Type: %s\r\nContent-Length: %d' ..
                  '\r\nConnection: close\r\n\r\n%s'):format(
                 status, content_type, #body, body), TIMEOUT)
end

local function listen(endpoints, uri)
    local u = urilib.parse(uri.uri)
    local listener = socket.tcp_server(u.host, u.service, {
        name = 'metrics_export',
        handler = function(sock)
            serve(endpoints, iostream.new(sock:fd(), uri))
        end,
    })
    if listener == nil then
        error(('metrics_export: failed to listen on %s: %s'):format(
              uri.uri, errno.strerror()), 0)
    end
    table.insert(listeners, listener)
end

local function apply(config)
    -- The servers restart on every reload.
    for _, listener in ipairs(listeners) do
        listener:close()
    end
    listeners = {}
    local cfgs = config._configdata:get('metrics.export.http',
                                        {use_default = true})
    for _, cfg in ipairs(cfgs or {}) do
        local endpoints = {}
        for _, endpoint in ipairs(cfg.endpoints or {}) do
            endpoints[normalize_path(endpoint.path)] = endpoint
        end
        if next(endpoints) ~= nil then
            for _, uri in ipairs(cfg.listen) do
                listen(endpoints, uri)
            end
        end
    end

    local graphite = config._configdata:get('metrics.export.graphite',
                                            {use_default = true}) or {}
    -- The plugin stops all its senders at once, an application's too.
    if not table.equals(graphite, graphite_applied) then
        local plugin = require('metrics.plugins.graphite')
        plugin.stop()
        for _, cfg in ipairs(graphite) do
            plugin.init(cfg)
        end
        graphite_applied = graphite
    end
end

return {
    name = 'metrics_export',
    apply = apply,
}
