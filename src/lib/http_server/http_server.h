/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include "uri/uri.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

int
http_server_config_thread_count(size_t thread_count);

int
http_server_config_listen_uri(const struct uri *listen_uri);

/* Initialize the http server subsystem. */
void
http_server_init(void);

/* Close sockets, stop threads. */
void
http_server_shutdown(void);

/* Deinitialize the http server subsystem. */
void
http_server_free(void);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
