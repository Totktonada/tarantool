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

/* Globals. */
static struct latch reconfiguration_latch;

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
		for (size_t i = from; i < to; ++i) {
			int fd = -1;
			if (i == 0) {
				fd = http_thread_listen_uri(i);
			}
			// XXX: if zero thread is to be started, then
			// push listen uri to first thread, receive fd
			//
			// otherwise just receive fd?
			//
			// push fd to other threads

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

	for (size_t i = 0; i < config.thread_count; ++i) {
		// http_thread_push_config(i, &config);

		// XXX: push uri to first thread, receive fd
		//
		// push fd to other threads
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
	latch_create(&reconfiguration_latch);
	http_config_init();
}

void
http_server_free(void)
{
	latch_lock(&reconfiguration_latch);
	for (size_t i = 0; i < config.thread_count; ++i) {
		http_thread_stop(i);
	}
	http_config_free();
	latch_unlock(&reconfiguration_latch);

	TRASH(&reconfiguration_latch);
}
