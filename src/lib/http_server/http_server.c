/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "http_server.h"

#include <stddef.h>
#include "trivia/util.h"
#include "core/latch.h"
#include "uri/uri.h"
#include "http_thread.h"

struct http_server_config {
	size_t thread_count;
	struct uri listen_uri;
};

struct http_server_state {
	struct evio_service *listen_service;
};

/* Globals. */
static struct latch reconfiguration_latch;
static struct http_server_config config;
static struct http_server_state state;

static void
http_server_config_init(void)
{
	config.thread_count = 0;
	uri_create(&config.listen_uri, NULL);
}

static void
http_server_config_free(void)
{
	uri_destroy(&config.listen_uri);
}

static void
http_server_state_init(void)
{
	state.listen_service = NULL;
}

static int
setup_thread(size_t thread_id, bool do_start)
{
	if (do_start) {
		http_thread_start(thread_id);
	}

	if (uri_is_nil(&config.listen_uri)) {
		return 0;
	}

	/* Zero thread is special: it creates a listening socket. */
	if (thread_id == 0) {
		// XXX: Error handling.
		http_thread_listen_start(thread_id, &config.listen_uri,
					 &state.listen_service);
	}

	assert(state.listen_service != NULL);
	http_thread_accept_start(thread_id, state.listen_service);

	return 0;
}

static void
teardown_thread(size_t thread_id, bool do_stop)
{
	http_thread_accept_stop(thread_id);

	if (thread_id == 0) {
		http_thread_listen_stop(thread_id);
		state.listen_service = NULL;
	}

	if (do_stop) {
		http_thread_stop(thread_id);
	}
}

int
http_server_config_thread_count(size_t thread_count)
{
	// Do not enter into configuration changing functions
	// simultaneously.
	latch_lock(&reconfiguration_latch);

	if (config.thread_count == thread_count) {
		latch_unlock(&reconfiguration_latch);
		return 0;
	}

	if (config.thread_count < thread_count) {
		/* Add more threads. */
		size_t from = config.thread_count;
		size_t to = thread_count;
		for (size_t i = from; i < to; ++i) {
			// XXX: Error handling.
			setup_thread(i, true);
		}
	} else {
		/* Stop some threads. */
		size_t from = config.thread_count - 1;
		size_t to = thread_count - 1;
		for (size_t i = from; i != to; --i) {
			teardown_thread(i, true);
		}
	}

	config.thread_count = thread_count;

	latch_unlock(&reconfiguration_latch);
	return 0;
}

int
http_server_config_listen_uri(const struct uri *listen_uri)
{
	// Do not enter into configuration changing functions
	// simultaneously.
	latch_lock(&reconfiguration_latch);

	// TODO: Re-read TLS keys and certificates even if the URI and
	// key/certs paths remain unchanged. Possibly is is better to do
	// via some separate http_server_config_uri_reload() call.
	if (uri_is_equal(&config.listen_uri, listen_uri)) {
		latch_unlock(&reconfiguration_latch);
		return 0;
	}

	uri_destroy(&config.listen_uri);
	uri_copy(&config.listen_uri, listen_uri);

	if (config.thread_count == 0) {
		latch_unlock(&reconfiguration_latch);
		return 0;
	}

	for (size_t i = config.thread_count - 1; i != (size_t)-1; --i) {
		teardown_thread(i, false);
	}

	if (uri_is_nil(&config.listen_uri)) {
		latch_unlock(&reconfiguration_latch);
		return 0;
	}

	for (size_t i = 0; i < config.thread_count; ++i) {
		// XXX: Error handling.
		setup_thread(i, false);
	}

	latch_unlock(&reconfiguration_latch);
	return 0;
}

void
http_server_init(void)
{
	http_thread_init();
	latch_create(&reconfiguration_latch);
	http_server_config_init();
	http_server_state_init();
}

void
http_server_shutdown(void)
{
	latch_lock(&reconfiguration_latch);
	for (size_t i = config.thread_count - 1; i != (size_t)-1; --i) {
		teardown_thread(i, true);
	}
	latch_unlock(&reconfiguration_latch);

	http_thread_shutdown();
}

void
http_server_free(void)
{
	http_server_config_free();
	TRASH(&config);
	TRASH(&state);
	TRASH(&reconfiguration_latch);

	http_thread_free();
}
