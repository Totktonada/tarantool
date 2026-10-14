/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "lua/iostream.h"

#include <lauxlib.h>
#include <lua.h>

#include "coio.h"
#include "diag.h"
#include "fiber.h"
#include "core/iostream.h"
#include "lua/uri.h"
#include "lua/utils.h"
#include "uri/uri.h"

/** Lua type name of struct iostream. */
static const char *iostream_typename = "iostream";

/**
 * iostream.new(fd[, uri]) -> stream
 *
 * Creates a stream over an accepted nonblocking socket: encrypted if the
 * URI parameters say so as for iproto (transport = 'ssl', ssl_*), plain
 * otherwise. The stream doesn't own the socket and doesn't touch it when
 * Lua collects the stream.
 */
static int
lbox_iostream_new(struct lua_State *L)
{
	int fd = luaL_checkinteger(L, 1);
	/* No URI is a nil one: a plain stream. */
	lua_settop(L, 2);
	struct uri uri;
	struct iostream_ctx ctx;
	int rc = luaT_uri_create(L, 2, &uri);
	if (rc == 0) {
		rc = iostream_ctx_create(&ctx, IOSTREAM_SERVER, &uri);
		uri_destroy(&uri);
	}
	if (rc != 0)
		return luaT_error(L);
	struct iostream *io = lua_newuserdata(L, sizeof(*io));
	iostream_clear(io);
	luaL_getmetatable(L, iostream_typename);
	lua_setmetatable(L, -2);
	rc = iostream_create(io, fd, &ctx);
	/* The stream keeps a reference to the SSL context. */
	iostream_ctx_destroy(&ctx);
	return rc == 0 ? 1 : luaT_error(L);
}

/**
 * Waits until a stream that returned the given status can go on: with TLS,
 * a read may need to write and the reverse. Returns 0 if it can, -1 with
 * diag set on timeout or fiber cancellation.
 */
static int
lua_iostream_wait(struct iostream *io, ssize_t status, double deadline)
{
	double timeout = deadline - fiber_clock();
	if (timeout <= 0) {
		diag_set(TimedOut);
		return -1;
	}
	coio_wait(io->fd, iostream_status_to_events(status), timeout);
	if (fiber_is_cancelled()) {
		diag_set(FiberIsCancelled);
		return -1;
	}
	return 0;
}

/**
 * stream:read(timeout) -> string | nil, error
 *
 * Reads what has come, up to 8 KiB. An empty string means that the peer
 * has closed the connection.
 */
static int
lbox_iostream_read(struct lua_State *L)
{
	struct iostream *io = luaL_checkudata(L, 1, iostream_typename);
	double deadline = fiber_clock() + luaL_checknumber(L, 2);
	char buf[8192];
	ssize_t rc;
	while ((rc = iostream_read(io, buf, sizeof(buf))) < 0) {
		if (rc == IOSTREAM_ERROR ||
		    lua_iostream_wait(io, rc, deadline) != 0)
			return luaT_push_nil_and_error(L);
	}
	lua_pushlstring(L, buf, rc);
	return 1;
}

/** stream:write(data, timeout) -> true | nil, error */
static int
lbox_iostream_write(struct lua_State *L)
{
	struct iostream *io = luaL_checkudata(L, 1, iostream_typename);
	size_t len;
	const char *data = luaL_checklstring(L, 2, &len);
	double deadline = fiber_clock() + luaL_checknumber(L, 3);
	while (len > 0) {
		ssize_t rc = iostream_write(io, data, len);
		if (rc >= 0) {
			data += rc;
			len -= rc;
		} else if (rc == IOSTREAM_ERROR ||
			   lua_iostream_wait(io, rc, deadline) != 0) {
			return luaT_push_nil_and_error(L);
		}
	}
	lua_pushboolean(L, true);
	return 1;
}

/** Frees a stream collected by Lua. The socket isn't touched. */
static int
lbox_iostream_gc(struct lua_State *L)
{
	struct iostream *io = luaL_checkudata(L, 1, iostream_typename);
	if (iostream_is_initialized(io))
		iostream_destroy(io);
	return 0;
}

void
tarantool_lua_iostream_init(struct lua_State *L)
{
	static const struct luaL_Reg iostream_methods[] = {
		{"read", lbox_iostream_read},
		{"write", lbox_iostream_write},
		{"__gc", lbox_iostream_gc},
		{NULL, NULL},
	};
	luaL_register_type(L, iostream_typename, iostream_methods);

	static const struct luaL_Reg module_funcs[] = {
		{"new", lbox_iostream_new},
		{NULL, NULL},
	};
	luaT_newmodule(L, "internal.iostream", module_funcs);
	lua_pop(L, 1);
}
