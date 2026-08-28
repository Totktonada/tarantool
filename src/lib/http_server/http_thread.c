/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "http_server.h"

#include <assert.h>
#include <stddef.h>
#include "trivia/util.h"
#include "uri/uri.h"
#include "core/fiber.h"
#include "core/say.h"
#include "tarantool_ev.h"

/* Constants. */
enum {
	INITIAL_CORDS_CAPACITY = 16,
};

/* Globals. */
static struct cord **cords;
static size_t cords_capacity;

/* Thread-local state. */
static __thread int listen_fd;

/* Forward declarations. */
static void *
thread_start(void *arg);

/* {{{ Wrappers to call from tx */

/* Extend cords array if needed. */
static void
cords_resize(size_t thread_id)
{
	if (thread_id < cords_capacity) {
		return;
	}

	size_t old_capacity = cords_capacity;
	struct cord **old_cords = cords;
	cords_capacity *= 2;
	cords = xcalloc(cords_capacity,	sizeof(*cords));
	for (size_t i = 0; i < old_capacity; ++i) {
		cords[i] = old_cords[i];
	}
	free(old_cords);
}

void
http_thread_start(size_t thread_id)
{
	assert(cord_is_main());
	assert(thread_id >= cords_capacity || cords[thread_id] == NULL);

	cords_resize(thread_id);
	assert(cords[thread_id] == NULL);
	cords[thread_id] = xmalloc(sizeof(**cords));
	char name[16];
	snprintf(name, sizeof(name), "http_%ld", thread_id);
	int rc = cord_start(cords[thread_id], name, thread_start,
			    (void *)thread_id);
	if (rc != 0) {
		panic_syserror("failed to start http thread");
	}
}

void
http_thread_stop(size_t thread_id)
{
	assert(cord_is_main());
	assert(thread_id < cords_capacity && cords[thread_id] != NULL);
	// XXX: stop
	// XXX: join

	free(cords[thread_id]);
	cords[thread_id] = NULL;
}

int
http_thread_listen_uri(size_t thread_id, const struct uri *listen_uri)
{
	assert(cord_is_main());
	assert(thread_id == 0);

	// XXX: do listen in thread
	(void)listen_uri;

	// XXX: return listen_fd
	return -1;
}

void
http_thread_accept(size_t thread_id, int listen_fd)
{
	assert(cord_is_main());

	(void)thread_id;

	// XXX: do accept
	(void)listen_fd;
}

void
http_thread_init(void)
{
	cords_capacity = INITIAL_CORDS_CAPACITY;
	cords = xcalloc(cords_capacity, sizeof(*cords));
}

void
http_thread_free(void)
{
	for (size_t i = 0; i < cords_capacity; ++i) {
		assert(cords[i] == NULL);
	}
	free(cords);
	cords = NULL;
	cords_capacity = 0;
}

/* }}} Wrappers to call from tx */

/* {{{ Functions that work in the http thread */

static void *
thread_start(void *arg)
{
	size_t thread_id = (size_t)arg;
	(void)thread_id;
	listen_fd = -1;
	ev_run(loop(), 0);
	// XXX: init cbus endpoint
	return NULL;
}

/* }}} Functions that work in the http thread */
