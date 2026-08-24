/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

void
http_server_config_enabled(bool enabled);

void
http_server_config_listen_uri(const struct uri *listen_uri);

/* Initialize the http server subsystem. */
void
http_server_init(void);

/* Deinitialize the http server subsystem. */
void
http_server_free(void);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
