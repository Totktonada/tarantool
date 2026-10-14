local fiber = require('fiber')
local fio = require('fio')
local socket = require('socket')
local t = require('luatest')

local iostream = require('internal.iostream')

local g = t.group()

local CERT_DIR = fio.pathjoin(fio.abspath(os.getenv('SOURCEDIR') or '.'),
                              'test/ssl_cert')

g.before_each(function(cg)
    cg.a, cg.b = socket.socketpair('AF_UNIX', 'SOCK_STREAM', 0)
end)

g.after_each(function(cg)
    -- Some tests close a socket themselves.
    pcall(cg.a.close, cg.a)
    pcall(cg.b.close, cg.b)
end)

g.test_plain = function(cg)
    local a, b = iostream.new(cg.a:fd()), iostream.new(cg.b:fd())
    t.assert_equals(a:write('hello', 1), true)
    t.assert_equals(b:read(1), 'hello')

    local data, err = b:read(0.01)
    t.assert_equals(data, nil)
    t.assert_equals(err.type, 'TimedOut')

    cg.a:close()
    t.assert_equals(b:read(1), '')
    data, err = b:write('hello', 1)
    t.assert_equals(data, nil)
    t.assert_str_contains(err.message, 'Broken pipe')
end

g.test_write_timeout = function(cg)
    local a = iostream.new(cg.a:fd())
    local ok, err = a:write(string.rep('x', 64 * 1024 * 1024), 0.01)
    t.assert_equals(ok, nil)
    t.assert_equals(err.type, 'TimedOut')
end

g.test_cancel = function(cg)
    local b = iostream.new(cg.b:fd())
    local fib = fiber.new(b.read, b, 60)
    fib:set_joinable(true)
    fiber.yield()
    fib:cancel()
    local _, data, err = fib:join()
    t.assert_equals(data, nil)
    t.assert_equals(err.type, 'FiberIsCancelled')
end

g.test_gc = function(cg)
    iostream.new(cg.a:fd())
    pcall(iostream.new, cg.a:fd(), {uri = 'localhost',
                                    params = {transport = 'ssl'}})
    collectgarbage()
    -- The socket outlives the streams.
    t.assert_equals(cg.a:write('x'), 1)
end

g.test_ssl = function(cg)
    local function new(params)
        return iostream.new(cg.b:fd(), {uri = 'localhost', params = params})
    end
    t.assert_error_msg_contains('Invalid URI table', iostream.new,
                                cg.b:fd(), {params = {}})
    t.assert_error_msg_equals('SSL certificate missing', new,
                              {transport = 'ssl'})
    t.assert_error_msg_equals(
        "Error loading SSL certificate '/no/such/file': " ..
        "No such file or directory", new, {
            transport = 'ssl',
            ssl_cert_file = '/no/such/file',
            ssl_key_file = fio.pathjoin(CERT_DIR, 'server.key'),
        })

    -- A server stream fails on a client that doesn't speak TLS.
    local b = new({
        transport = 'ssl',
        ssl_cert_file = fio.pathjoin(CERT_DIR, 'server.crt'),
        ssl_key_file = fio.pathjoin(CERT_DIR, 'server.key'),
    })
    cg.a:write('GET / HTTP/1.1\r\n\r\n')
    local data, err = b:read(1)
    t.assert_equals(data, nil)
    t.assert_equals(err.type, 'SSLError')
end
