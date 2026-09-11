/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

struct iostream;
struct http_connection;

/**
 * Create an http connection object, but don't attach it to any thread.
 *
 * Can be called from any thread.
 */
struct http_connection *
http_connection_new(void);

/**
 * XXX
 */
void
http_connection_delete(struct http_connection *con);

/**
 * XXX
 */
void
http_connection_io_start_in_thread(struct http_connection *con,
				   struct iostream *io);

/**
 * XXX
 */
void
http_connection_io_stop_in_thread(struct http_connection *con);

/**
 * Initialize thread-local structures for tracking http connections.
 *
 * To be called from an http thread.
 */
void
http_connection_init_in_thread(void);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
