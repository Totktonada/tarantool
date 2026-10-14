local fiber = require('fiber')
local fio = require('fio')
local http_client = require('http.client')
local json = require('json')
local socket = require('socket')
local t = require('luatest')
local cbuilder = require('luatest.cbuilder')
local cluster = require('luatest.cluster')

local g = t.group()

local CERT_DIR = fio.pathjoin(fio.abspath(os.getenv('SOURCEDIR') or '.'),
                              'test/ssl_cert')
local CA_FILE = fio.pathjoin(CERT_DIR, 'ca.crt')
local SERVER_CERT_FILE = fio.pathjoin(CERT_DIR, 'server.crt')
local SERVER_KEY_FILE = fio.pathjoin(CERT_DIR, 'server.key')
local CLIENT_CERT_FILE = fio.pathjoin(CERT_DIR, 'client.crt')
local CLIENT_KEY_FILE = fio.pathjoin(CERT_DIR, 'client.key')

local ENDPOINTS = {
    {path = '/metrics/json', format = 'json'},
    {path = '/metrics/prometheus', format = 'prometheus'},
}

local function free_port()
    local s = socket.tcp_server('127.0.0.1', 0, function() end)
    local port = s:name().port
    s:close()
    return port
end

local function get(url, opts)
    opts = table.copy(opts or {})
    opts.no_proxy = '*'
    opts.timeout = 5
    return http_client.new():get(url, opts)
end

-- Returns the status of a raw request.
local function raw_status(port, request)
    local s = socket.tcp_connect('127.0.0.1', port)
    s:write(request)
    local response = s:read({delimiter = '\r\n'}, 5)
    s:close()
    return tonumber(response:match('^HTTP/1%.1 (%d+) '))
end

local function config(metrics)
    return cbuilder:new()
        :add_instance('i-001', {})
        :set_global_option('metrics', metrics)
        :config()
end

local function export_config(http)
    return config({include = {'some_counter'}, export = {http = http}})
end

local function start(cfg)
    local c = cluster:new(cfg)
    c:start()
    c['i-001']:exec(function()
        require('metrics').counter('some_counter'):inc(1, {label = 'ANY'})
    end)
    return c
end

local function assert_json(url, opts)
    local res = get(url, opts)
    t.assert_equals(res.status, 200, url)
    t.assert_equals(res.headers['content-type'], 'application/json')
    local metrics = json.decode(res.body)
    metrics[1].timestamp = nil
    t.assert_equals(metrics, {{
        metric_name = 'some_counter',
        label_pairs = {alias = 'i-001', label = 'ANY'},
        value = 1,
    }})
end

-- http.client raises on a failed TLS handshake.
local function assert_no_server(url, opts)
    local ok, res = pcall(get, url, opts)
    t.assert(not ok or res.status ~= 200, url)
end

g.test_http = function()
    local port = free_port()
    local url = ('http://127.0.0.1:%d/metrics'):format(port)
    local c = start(export_config({{
        listen = {{uri = ('127.0.0.1:%d'):format(port)}},
        endpoints = ENDPOINTS,
    }}))

    assert_json(url .. '/json?a=b')
    local res = get(url .. '/prometheus/')
    t.assert_equals(res.status, 200)
    t.assert_str_matches(res.body, '# HELP some_counter \n' ..
                                   '# TYPE some_counter counter\n' ..
                                   'some_counter{.*} 1\n')
    t.assert_equals(get(url).status, 404)
    t.assert_equals(raw_status(port, 'POST /metrics/json HTTP/1.1\r\n\r\n'),
                    404)
    t.assert_equals(raw_status(port, 'GET\r\n\r\n'), 404)

    c['i-001']:exec(function()
        require('metrics.plugins.json').export = function() error('boom') end
    end)
    t.assert_equals(get(url .. '/json').status, 500)
end

g.test_reload = function()
    local port = free_port()
    local listen = {{uri = ('127.0.0.1:%d'):format(port)}}
    local url = 'http://' .. listen[1].uri
    local c = start(export_config({
        {listen = listen, endpoints = {ENDPOINTS[1]}},
    }))
    assert_json(url .. '/metrics/json')
    t.assert_equals(get(url .. '/metrics/prometheus').status, 404)

    c:reload(export_config({{listen = listen, endpoints = ENDPOINTS}}))
    t.assert_equals(get(url .. '/metrics/prometheus').status, 200)

    local new_port = free_port()
    local new_listen = {{uri = tostring(new_port)}}
    local new_url = ('http://127.0.0.1:%d'):format(new_port)
    c:reload(export_config({{listen = new_listen, endpoints = ENDPOINTS}}))
    assert_json(new_url .. '/metrics/json')
    assert_no_server(url .. '/metrics/json')

    -- No endpoints, no server.
    c:reload(export_config({{listen = new_listen, endpoints = {}}}))
    assert_no_server(new_url .. '/metrics/json')
end

g.test_tls = function()
    local port = free_port()
    local listen = ('127.0.0.1:%d'):format(port)
    local url = ('https://%s/metrics/json'):format(listen)
    -- The certificate is issued for tarantool.io.
    local opts = {ca_file = CA_FILE, verify_host = false}
    local params = {
        transport = 'ssl',
        ssl_cert_file = SERVER_CERT_FILE,
        ssl_key_file = SERVER_KEY_FILE,
    }
    local server = {
        listen = {{uri = listen, params = params}},
        endpoints = ENDPOINTS,
    }
    local c = start(export_config({server}))
    assert_json(url, opts)
    assert_no_server(('http://%s/metrics/json'):format(listen))

    -- With a CA file, the server verifies the clients.
    params.ssl_ca_file = CA_FILE
    c:reload(export_config({server}))
    assert_no_server(url, opts)
    opts.ssl_cert = CLIENT_CERT_FILE
    opts.ssl_key = CLIENT_KEY_FILE
    assert_json(url, opts)

    -- A wrong file fails the connections.
    params.ssl_cert_file = '/no/such/file'
    c:reload(export_config({server}))
    assert_no_server(url, opts)
end

g.test_endpoint_metrics = function()
    local port = free_port()
    local url = ('http://127.0.0.1:%d/metrics'):format(port)
    start(config({
        include = {'http_server_request_latency'},
        export = {http = {{
            listen = {{uri = tostring(port)}},
            endpoints = {{
                path = '/metrics',
                format = 'prometheus',
                metrics = {enabled = true},
            }},
        }}},
    }))
    get(url)
    t.assert_str_contains(get(url).body,
                          'http_server_request_latency_count{path="/metrics"')
end

g.test_unix_socket = function()
    local c = start(export_config({{
        listen = {{uri = 'unix/:./{{ instance_name }}.sock'}},
        endpoints = ENDPOINTS,
    }}))
    local s = socket.tcp_connect('unix/', fio.pathjoin(c._dir, 'i-001.sock'))
    t.assert(s ~= nil)
    s:write('GET /metrics/json HTTP/1.0\r\n\r\n')
    local res = s:read(1024 * 1024, 5)
    s:close()
    t.assert_str_matches(res, 'HTTP/1.1 200 \r\n.*some_counter.*')
end

g.test_graphite = function()
    local receiver = socket('AF_INET', 'SOCK_DGRAM', 'udp')
    t.assert(receiver:bind('127.0.0.1', 0))
    local port = receiver:name().port
    local function graphite_config(prefix)
        return config({
            include = {'some_counter'},
            export = {graphite = {{
                prefix = prefix,
                host = '127.0.0.1',
                port = port,
                send_interval = 0.1,
            }}},
        })
    end
    -- Skips what is sent with another prefix.
    local function receive(prefix)
        local deadline = fiber.clock() + 5
        while fiber.clock() < deadline do
            if receiver:readable(0.5) then
                local data = receiver:recv()
                if data:startswith(prefix .. '.') then
                    return data
                end
            end
        end
    end

    -- The prefix of the plugin by default.
    local c = start(graphite_config(nil))
    t.assert_str_matches(receive('tarantool'),
                         'tarantool%.some_counter;.* 1 %d+.*\n')
    c:reload(graphite_config('two'))
    t.assert_str_matches(receive('two'), 'two%.some_counter;.* 1 %d+.*\n')

    c:reload(config({include = {'some_counter'}}))
    while receiver:readable(0.5) do
        receiver:recv()
    end
    t.assert_not(receiver:readable(1))
    receiver:close()
end
