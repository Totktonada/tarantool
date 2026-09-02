/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "http_thread.h"

#include <stddef.h>
#include <stdio.h>
#include "trivia/util.h"
#include "uri/uri.h"
#include "core/fiber.h"
#include "core/say.h"
#include "tarantool_ev.h"
#include "core/evio.h"
#include "managed_thread.h"

/* Constants. */
enum {
	URI_BUFFER_SIZE = 1024,
};

/*
 * The listen service exists only in the zero thread. The accept service works
 * in all the threads.
 */
static __thread struct evio_service listen_service;
// static __thread struct evio_service accept_service;

static int
thread_start(void *arg_1, void *arg_2)
{
	(void)arg_1;
	(void)arg_2;

	char service_name[SERVICE_NAME_MAXLEN];
	snprintf(service_name, sizeof(service_name), "%s_listen",
		 managed_thread_name());
	evio_service_create(loop(), &listen_service, service_name, NULL, NULL);

	return 0;
}

static int
thread_listen_start(void *arg_1, void *arg_2)
{
	const struct uri *listen_uri = (const struct uri *)arg_1;

	/* No previous listening socket. */
	assert(evio_service_count(&listen_service) == 0);

	/*
	 * uri -> uri_set
	 *
	 * NB: uri_set_add() moves the uri, so we have to copy it.
	 */
	struct uri_set uri_set;
	struct uri uri_temp;
	uri_copy(&uri_temp, listen_uri);
	uri_set_create(&uri_set, NULL);
	uri_set_add(&uri_set, &uri_temp);
	uri_destroy(&uri_temp);

	/* Start listening. */
	if (evio_service_start(&listen_service, &uri_set) != 0) {
		uri_set_destroy(&uri_set);
		return -1;
	}
	uri_set_destroy(&uri_set);
	say_debug("listening is started");

	/*
	 * Return pointer to the listening service to attach to it in other
	 * threads.
	 */
	*(struct evio_service **)arg_2 = &listen_service;
	return 0;
}

static int
thread_listen_stop(void *arg_1, void *arg_2)
{
	(void)arg_1;
	(void)arg_2;

	evio_service_stop(&listen_service);
	say_debug("listening is stopped");

	return 0;
}

static int
thread_accept_start(void *arg_1, void *arg_2)
{
	struct evio_service *listen_service = (struct evio_service *)arg_1;
	(void)arg_2;

	// XXX: attach
	(void)listen_service;
	say_debug("accepting is started");

	return 0;
}

static int
thread_accept_stop(void *arg_1, void *arg_2)
{
	(void)arg_1;
	(void)arg_2;

	// XXX: detach
	say_debug("accepting is stopped");

	return 0;
}

void
http_thread_start(size_t thread_id)
{
	managed_thread_start(thread_id);
	managed_thread_call(thread_id, thread_start, NULL, NULL);
}

void
http_thread_stop(size_t thread_id)
{
	managed_thread_stop(thread_id);
}

/* {{{ Wrappers to call the functions above from tx */

int
http_thread_listen_start(size_t thread_id, const struct uri *listen_uri,
			 struct evio_service **listen_service)
{
	assert(thread_id == 0);

	void *arg_1 = (void *)listen_uri;
	void *arg_2 = (void *)listen_service;
	return managed_thread_call(thread_id, thread_listen_start, arg_1,
				   arg_2);
}

void
http_thread_listen_stop(size_t thread_id)
{
	assert(thread_id == 0);

	managed_thread_call(thread_id, thread_listen_stop, NULL, NULL);
}

void
http_thread_accept_start(size_t thread_id, struct evio_service *listen_service)
{
	void *arg_1 = (void *)listen_service;
	void *arg_2 = NULL;
	managed_thread_call(thread_id, thread_accept_start, arg_1, arg_2);
}

void
http_thread_accept_stop(size_t thread_id)
{
	managed_thread_call(thread_id, thread_accept_stop, NULL, NULL);
}

/* }}} Wrappers to call the functions above from tx */

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
