/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "http_server.h"

#include <stddef.h>
#include "uri/uri.h"

struct http_thread_shared_state {
	int listen_socket_id;
};

/* Globals. */
// XXX: lock with mutex
static struct http_thread_shared_state shared_state;

/* {{{ Wrappers to call from tx */

void
http_thread_start(size_t thread_id)
{
	cord_start();
}

void
http_thread_stop(size_t thread_id)
{
	// XXX: stop
	// XXX: join
}

void
http_thread_push_config(size_t thread_id,
			const struct http_thread_config *config)
{
}

/* }}} Wrappers to call from tx */

/* {{{ Functions that work in the http thread */

void
thread_start()
{
	// XXX: bind and listen if thread_id == 0
	// save listen socket id
}

/* }}} Functions that work in the http thread */
