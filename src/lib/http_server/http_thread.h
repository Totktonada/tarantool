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

struct evio_service;

/* To be called from tx. */
void
http_thread_start(size_t thread_id);

/* To be called from tx. */
void
http_thread_stop(size_t thread_id);

/*
 * Writes listen fd to the output argument.
 *
 * To be called from tx.
 */
int
http_thread_listen_start(size_t thread_id, const struct uri *listen_uri,
			 struct evio_service **listen_service);

/* To be called from tx. */
void
http_thread_listen_stop(size_t thread_id);

/* To be called from tx. */
void
http_thread_accept_start(size_t thread_id, struct evio_service *listen_service);

/* To be called from tx. */
void
http_thread_accept_stop(size_t thread_id);

/* To be called from tx. */
void
http_thread_init(void);

/* To be called from tx. */
void
http_thread_free(void);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
