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

struct http_thread_config {
	struct uri listen_uri;
};

// To be called from tx.
void
http_thread_start(size_t thread_id);

// To be called from tx.
void
http_thread_stop(size_t thread_id);

// To be called from tx.
void
http_thread_push_config(size_t thread_id,
			const struct http_thread_config *config);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
