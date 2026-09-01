/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

struct lua_State;

void
luaopen_http_server_lib(struct lua_State *L);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
