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
#include "core/sio.h"
#include "core/evio.h"
#include "core/iostream.h"
#include "managed_thread.h"
#include "http_connection.h"

/*
 * The listen service exists only in the zero thread. The accept service works
 * in all the threads.
 */
static __thread struct evio_service listen_service;
static __thread struct evio_service accept_service;

static void
on_accept(struct evio_service *accept_service, struct iostream *io,
	  struct sockaddr *addr, socklen_t addrlen)
{
	(void)accept_service;

	/*
	 * TODO: If a disbalance threshold (by connections per thread or by CPU
	 * consumption per thread) is reached, we have to pass this new
	 * connection to another http thread to perform IO.
	 *
	 * First, we need to collect statistics and it should be accessible from
	 * each thread directly (however, it is OK if it falls behind the most
	 * actual state a bit). It is necessary to decide if we keep the
	 * connection in this thread to perform further IO or pass it to another
	 * thread. We want to decide without extra round trips between threads.
	 *
	 * It is important to don't loss a connection by passing it to a
	 * stopping thread (or at least re-transfer such connections before an
	 * actual thread termination).
	 */

	/*
	 * Start the read-write-close loop.
	 *
	 * NB: The io object is moved.
	 *
	 * The connection object is owned by the http_connection module and
	 * will be freed when its state machine reaches a terminal state.
	 */
	struct http_connection *con = http_connection_new();
	http_connection_io_start_in_thread(con, io);
	say_verbose("accepted connection from %s", sio_strfaddr(addr, addrlen));
}

static int
thread_start(void *arg_1, void *arg_2)
{
	(void)arg_1;
	(void)arg_2;

	/* Create listen service. */
	char service_name[SERVICE_NAME_MAXLEN];
	snprintf(service_name, sizeof(service_name), "%s_listen",
		 managed_thread_name());
	evio_service_create(loop(), &listen_service, service_name, NULL, NULL);

	/* Create accept service. */
	snprintf(service_name, sizeof(service_name), "%s_accept",
		 managed_thread_name());
	evio_service_create(loop(), &accept_service, service_name, on_accept,
			    NULL);

	/* Initialize http connection subsustem within the thread. */
	http_connection_init_in_thread();

	return 0;
}

static int
thread_listen_start(void *arg_1, void *arg_2)
{
	const struct uri_set *listen_uris = (const struct uri_set *)arg_1;

	/* No previous listening socket. */
	assert(evio_service_count(&listen_service) == 0);

	/* Start listening. */
	if (evio_service_start(&listen_service, listen_uris) != 0) {
		return -1;
	}
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

	evio_service_attach(&accept_service, listen_service);
	say_debug("accepting is started");

	return 0;
}

static int
thread_accept_stop(void *arg_1, void *arg_2)
{
	(void)arg_1;
	(void)arg_2;

	evio_service_detach(&accept_service);
	say_debug("accepting is stopped");

	return 0;
}

static int
thread_connections_transfer(void *arg_1, void *arg_2)
{
	size_t max_dest_thread_id = (size_t)arg_1;
	(void)arg_2;

	/* TODO: Implement the connections transfer. */
	(void)max_dest_thread_id;
	return 0;
}

static int
thread_connections_stop(void *arg_1, void *arg_2)
{
	(void)arg_1;
	(void)arg_2;

	/* TODO: Implement the connections stop. */
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
http_thread_listen_start(size_t thread_id, const struct uri_set *listen_uris,
			 struct evio_service **listen_service)
{
	assert(thread_id == 0);

	void *arg_1 = (void *)listen_uris;
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

void
http_thread_connections_transfer(size_t thread_id, size_t max_dest_thread_id)
{
	void *arg_1 = (void *)max_dest_thread_id;
	void *arg_2 = NULL;
	managed_thread_call(thread_id, thread_connections_transfer, arg_1,
			    arg_2);
}

void
http_thread_connections_stop(size_t thread_id)
{
	managed_thread_call(thread_id, thread_connections_stop, NULL, NULL);
}

/* }}} Wrappers to call the functions above from tx */

void
http_thread_init(void)
{
	managed_thread_init("http_");
}

void
http_thread_shutdown(void)
{
	managed_thread_shutdown();
}

void
http_thread_free(void)
{
	managed_thread_free();
}
