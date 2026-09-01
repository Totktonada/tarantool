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

/* To be called from tx. */
void
http_thread_start(size_t thread_id);

/* To be called from tx. */
void
http_thread_stop(size_t thread_id);

/* To be called from tx.
 *
 * Returns listen fd.
 */
int
http_thread_listen_uri(size_t thread_id, const struct uri *listen_uri);

/* To be called from tx. */
void
http_thread_accept(size_t thread_id, int listen_fd);

/* To be called from tx. */
void
http_thread_init(void);

/* To be called from tx. */
void
http_thread_free(void);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
