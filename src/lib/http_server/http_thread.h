/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

struct evio_service;
struct uri_set;

/** To be called from tx. */
void
http_thread_start(size_t thread_id);

/** To be called from tx. */
void
http_thread_stop(size_t thread_id);

/**
 * Returns the listen service via the output argument.
 *
 * To be called from tx.
 */
int
http_thread_listen_start(size_t thread_id, const struct uri_set *listen_uris,
			 struct evio_service **listen_service);

/** To be called from tx. */
void
http_thread_listen_stop(size_t thread_id);

/** To be called from tx. */
void
http_thread_accept_start(size_t thread_id, struct evio_service *listen_service);

/** To be called from tx. */
void
http_thread_accept_stop(size_t thread_id);

/**
 * Transfer connections owned by the given thread to other threads.
 *
 * To be called from tx.
 */
void
http_thread_connections_transfer(size_t thread_id, size_t max_dest_thread_id);

/**
 * Gracefully shutdown connections owned by the given thread.
 *
 * To be called from tx.
 */
void
http_thread_connections_stop(size_t thread_id);

/**
 * Initialize the subsystem.
 *
 * To be called from tx.
 */
void
http_thread_init(void);

/**
 * Shutdown the subsystem.
 *
 * Call it before http_thread_free() to perform work that needs a fiber.
 *
 * To be called from tx.
 */
void
http_thread_shutdown(void);

/**
 * Destroy the subsystem.
 *
 * To be called from tx.
 */
void
http_thread_free(void);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
