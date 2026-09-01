/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "http_server.h"

#include <math.h>
#include <lua.h>
#include <lauxlib.h>
#include "lib/uri/uri.h"
#include "lib/http_server/http_server.h"
#include "lua/utils.h"
#include "lua/uri.h"

#define EXP2_63 9223372036854775808.0   /* 2.0 ^ 63 */

static size_t
luaT_check_thread_count(struct lua_State *L, int idx)
{
	if (lua_type(L, idx) != LUA_TNUMBER) {
		const char *fmt = "expected a number as the %d argument "
				  "(thread_count), got %s";
		return luaL_error(L, fmt, idx, luaL_typename(L, 1));
	}
	double arg = lua_tonumber(L, idx);
	if (!isfinite(arg)) {
		const char *fmt = "expected a finite number (not NaN/inf/-inf) "
				  "as the %d argument (thread_count), got %f";
		return luaL_error(L, fmt, idx, arg);
	}
	if (arg < 0) {
		const char *fmt = "expected a positive value as the %d "
				  "argument (thread_count), got %f";
		return luaL_error(L, fmt, idx, arg);

	}
	if (arg >= EXP2_63) {
		const char *fmt = "expected a number less than 2^63 as the "
				  "%d argument (thread_count), got %f";
		return luaL_error(L, fmt, idx, arg);
	}
	if (arg != (double)(size_t)arg) {
		const char *fmt = "expected an integral number as the %d "
				  "argument (thread_count), got %f";
		return luaL_error(L, fmt, idx, arg);
	}

	return (size_t)arg;
}

static int
lbox_listen_uri(struct lua_State *L)
{
	struct uri listen_uri;
	if (luaT_uri_create(L, 1, &listen_uri) != 0) {
		return luaT_error(L);
	}
	if (http_server_config_listen_uri(&listen_uri) != 0) {
		uri_destroy(&listen_uri);
		return luaT_error(L);
	}
	uri_destroy(&listen_uri);
	return 0;
}

static int
lbox_thread_count(struct lua_State *L)
{
	size_t thread_count = luaT_check_thread_count(L, 1);
	if (http_server_config_thread_count(thread_count) != 0) {
		return luaT_error(L);
	}
	return 0;
}

void
luaopen_http_server_lib(struct lua_State *L)
{
	/* Module methods. */
	static const struct luaL_Reg methods[] = {
		{"listen_uri",		lbox_listen_uri,	},
		{"thread_count",	lbox_thread_count,	},
		{NULL, NULL},
	};
	luaT_newmodule(L, "http.server.lib", methods);
	lua_pop(L, 1);
}
