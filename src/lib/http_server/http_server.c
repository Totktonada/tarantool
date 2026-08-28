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
#include "http_config.h"
#include "http_thread.h"

struct http_server_state {
	int listen_fd;
};

/* Globals. */
static struct latch reconfiguration_latch;
static struct http_config config;
static struct http_server_state state;

static void
http_server_config_init(void)
{
	config.thread_count = 0;
	uri_create(&config.listen_uri, NULL);
}

static void
http_server_state_init(void)
{
	state.listen_fd = -1;
}

void
http_server_config_thread_count(size_t thread_count)
{
	// Do not enter into configuration changing functions
	// simultaneously.
	latch_lock(&reconfiguration_latch);

	if (config.thread_count == thread_count) {
		latch_unlock(&reconfiguration_latch);
		return;
	}

	if (config.thread_count < thread_count) {
		/* Add more threads. */
		size_t from = config.thread_count;
		size_t to = thread_count;

		/* Zero thread is special: it creates a listening socket. */
		if (from == 0) {
			state.listen_fd =
				http_thread_listen_uri(0, &config.listen_uri);
		}

		for (size_t i = from; i < to; ++i) {
			http_thread_start(i);
			http_thread_accept(i, state.listen_fd);
		}
	} else {
		/* Stop some threads. */
		size_t from = config.thread_count - 1;
		size_t to = thread_count - 1;
		for (size_t i = from; i != to; --i) {
			http_thread_stop(i);
		}
	}

	config.thread_count = thread_count;

	latch_unlock(&reconfiguration_latch);
}

void
http_server_config_listen_uri(const struct uri *listen_uri)
{
	// Do not enter into configuration changing functions
	// simultaneously.
	latch_lock(&reconfiguration_latch);

	// TODO: Re-read TLS keys and certificates even if the URI and
	// key/certs paths remain unchanged.
	if (uri_is_equal(&config.listen_uri, listen_uri)) {
		latch_unlock(&reconfiguration_latch);
		return;
	}

	uri_copy(&config.listen_uri, listen_uri);

	/* Renew a listening socket in the zero thread. */
	state.listen_fd = http_thread_listen_uri(0, &config.listen_uri);

	for (size_t i = 0; i < config.thread_count; ++i) {
		http_thread_accept(i, state.listen_fd);
	}

	latch_unlock(&reconfiguration_latch);
}

#if 0
// XXX: move to thread
void
http_server_start(void)
{
	// TODO: Propagate errors upward instead of assertions?
	assert(!uri_is_nil(&config.listen_uri));
	// XXX: bind + listen
}

// XXX: move to thread
void
http_server_stop(void)
{
	// XXX: unbind
	// XXX: shutdown
	// XXX: close socket
}
#endif

void
http_server_init(void)
{
	http_thread_init();
	latch_create(&reconfiguration_latch);
	http_server_config_init();
	http_server_state_init();
}

void
http_server_free(void)
{
	latch_lock(&reconfiguration_latch);
	for (size_t i = config.thread_count - 1; i != (size_t)-1; --i) {
		http_thread_stop(i);
	}
	TRASH(&config);
	TRASH(&state);
	latch_unlock(&reconfiguration_latch);

	TRASH(&reconfiguration_latch);

	http_thread_free();
}
