/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "http_thread.h"

#include <stddef.h>
#include "trivia/util.h"
#include "uri/uri.h"
#include "core/fiber.h"
#include "core/say.h"
#include "tarantool_ev.h"
#include "managed_thread.h"

/* Constants. */
enum {
	URI_BUFFER_SIZE = 1024,
};

static int
thread_listen_uri(void *arg_1, void *arg_2)
{
	const struct uri *listen_uri = (const struct uri *)arg_1;
	int *listen_fd = (int *)arg_2;

	// XXX: do listen
	char uri_str[URI_BUFFER_SIZE];
	uri_format(uri_str, sizeof(uri_str), listen_uri, false);
	say_debug("listen_uri is set to %s\n", uri_str);

	// XXX: Return listen_fd.
	*listen_fd = -1;
	return 0;
}

static int
thread_accept(void *arg_1, void *arg_2)
{
	int listen_fd = (int)(intptr_t)arg_1;
	(void)arg_2;

	// XXX: do accept
	(void)listen_fd;
	say_debug("accept is called\n");

	return 0;
}

void
http_thread_start(size_t thread_id)
{
	managed_thread_start(thread_id);
}

void
http_thread_stop(size_t thread_id)
{
	managed_thread_stop(thread_id);
}

int
http_thread_listen_uri(size_t thread_id, const struct uri *listen_uri,
		       int *listen_fd)
{
	assert(thread_id == 0);

	void *arg_1 = (void *)listen_uri;
	void *arg_2 = (void *)listen_fd;
	return managed_thread_call(thread_id, thread_listen_uri, arg_1, arg_2);
}

int
http_thread_accept(size_t thread_id, int listen_fd)
{
	void *arg_1 = (void *)(intptr_t)listen_fd;
	void *arg_2 = NULL;
	return managed_thread_call(thread_id, thread_accept, arg_1, arg_2);
}

void
http_thread_init(void)
{
	managed_thread_init("http_");
}

void
http_thread_free(void)
{
	managed_thread_free();
}
