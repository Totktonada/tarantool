/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "http_server.h"

#include <stddef.h>
#include "trivia/util.h"
#include "uri/uri.h"

/* Globals. */
struct http_config config;

void
http_config_init(void)
{
	config->thread_count = 0;
	uri_create(&config->listen_uri, NULL);
}

void
http_config_free(void)
{
	TRASH(&config);
}
