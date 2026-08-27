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

struct http_config {
	size_t thread_count;
	struct uri listen_uri;
};

#if 0
/**
 * Access policy:
 *
 * tx thread: take reconfiguration lock
 * tx thread: modify config values as needed
 * XXX
 *
 * 1. Take reconfiguration lock from the tx thread.
 * 2. Modify config values as needed.
 * 3. Notify http threads using cbus.
 * 4. Read config from http threads.
 * 5. Wait until http threads reconfigured.
 * 6. XXX
 */
extern struct http_config config;

void
http_config_init(void);

void
http_config_free(void);
#endif

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
