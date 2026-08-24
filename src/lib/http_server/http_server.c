/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "http_server.h"
#include "uri/uri.h"

struct http_server_config {
	bool enabled;
	struct uri listen_uri; // XXX
};

/* Globals. */
static struct http_server_config config;

/* Forward declarations. */
void
http_server_start(void);
void
http_server_stop(void);

void
http_server_config_create(struct http_server_config *config)
{
	config.enabled = false;
	uri_create(&config.listen_uri, NULL);
}

void
http_server_config_enabled(bool enabled)
{
	if (config.enabled == enabled) {
		return;
	}

	config.enabled = enabled;

	http_server_start_or_stop();
	if (config.enabled) {
		http_server_start();
	} else {
		http_server_stop();
	}
}

void
http_server_config_listen_uri(const struct uri *listen_uri)
{
	if (uri_is_equal(&config.listen_uri, listen_uri)) {
		return;
	}

	uri_copy(&config.listen_uri, listen_uri):

	if (config.enabled) {
		http_server_stop();
		http_server_start();
	}
}

void
http_server_start(void)
{
	// TODO: Propagate errors upward instead of assertions?
	assert(!uri_is_nil(&config.listen_uri));
	// XXX: bind + listen
}

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
	http_server_config_create(&config);
}

void
http_server_free(void)
{
}
