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

/* Globals. */
static struct latch config_latch;
static struct http_server_config config;

void
http_server_config_create(struct http_server_config *config)
{
	config->thread_count = 0;
	uri_create(&config->listen_uri, NULL);
}

void
http_server_config_thread_count(size_t thread_count)
{
	// Do not enter into configuration changing functions
	// simultaneously.
	latch_lock(&config_latch);

	if (config.thread_count == thread_count) {
		latch_unlock(&config_latch);
		return;
	}

	if (config.thread_count < thread_count) {
		/* Add more threads. */
		size_t from = config.thread_count;
		size_t to = thread_count;
		for (size_t i = from; i < to; ++i) {
			// XXX: we've to get listening socket fd and pass it
			// to threads to make accepts
			http_thread_start(i);
			// http_thread_push_config(i, &config);
		}
	} else {
		/* Stop some threads. */
		size_t from = config.thread_count - 1;
		size_t to = thread_count;
		for (size_t i = from; i >= to; --i) {
			http_thread_start(i);
		}
	}

	config.thread_count = thread_count;

	latch_unlock(&config_latch);
}

void
http_server_config_listen_uri(const struct uri *listen_uri)
{
	// Do not enter into configuration changing functions
	// simultaneously.
	latch_lock(&config_latch);

	// TODO: Re-read TLS keys and certificates even if the URI and
	// key/certs paths remain unchanged.
	if (uri_is_equal(&config.listen_uri, listen_uri)) {
		latch_unlock(&config_latch);
		return;
	}

	uri_copy(&config.listen_uri, listen_uri);

	for (size_t i = 0; i < config.thread_count; ++i) {
		// http_thread_push_config(i, &config);
	}

	latch_unlock(&config_latch);
}

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

void
http_server_init(void)
{
	latch_create(&config_latch);
	http_server_config_create(&config);
}

void
http_server_free(void)
{
	latch_lock(&config_latch);
	for (size_t i = 0; i < config.thread_count; ++i) {
		http_thread_stop(i);
	}
	latch_unlock(&config_latch);

	TRASH(&config);
	TRASH(&config_latch);
}
