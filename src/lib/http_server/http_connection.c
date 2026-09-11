/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "http_connection.h"
#include "core/iostream.h"
#include "small/rlist.h"
#include "xbuf.h"
#include "proto_sm.h"
#include "proto_sm_http2.h"

static const size_t READAHEAD = 16384;

static __thread struct rlist connections;

struct http_connection {
	/**
	 * Connection socket wrapped into an iostream object
	 * (which adds TLS encoding/decoding support).
	 *
	 * Supports read and write operations.
	 */
	struct iostream io;
	/**
	 * EOF or an error on input, an error on output. Possible
	 * values:
	 *
	 * - 0
	 * - PROTO_SM_INPUT_EOF
	 * - PROTO_SM_INPUT_ERROR
	 * - PROTO_SM_OUTPUT_ERROR
	 */
	int io_flags;
	/**
	 * libev watcher that notifies us when there are new data
	 * to read.
	 *
	 * However, the watcher is set to wait for the socket
	 * writability if the iostream's read operation want to
	 * perform a write (during a TLS negotiation). See enum
	 * iostream_status comments for details.
	 *
	 * Next, the watcher also reacts to the EV_CUSTOM event:
	 * this way a newly created or sreviously stopped watcher
	 * is started.
	 *
	 * The watcher calls http_connection_on_input().
	 */
	struct ev_io iw;
	/**
	 * libev watcher that notifies us when more data can be
	 * written to the socket (i.e. when there is a space in
	 * socket's output buffer).
	 *
	 * Similarly to the input watcher, it may be set to wait
	 * for readability and reacts to EV_CUSTOM to start.
	 *
	 * The watcher calls http_connection_on_output().
	 */
	struct ev_io ow;
	/** Bufferized input. */
	struct xbuf ibuf;
	/** Bufferized output. */
	struct xbuf obuf;
	/** Protocol state-machine. */
	struct proto_sm sm;
	/** Link in the `connections` thread local list. */
	struct rlist in_connections;
};

/**
 * Change events to watch and start the watcher (if not already started).
 *
 * Usually called after EWOULDBLOCK on attempt to read/write in the watcher's
 * callback, and the reasons are the following:
 *
 * - The watcher may be not started and has no fd initially, just waken up with
 *   EV_CUSTOM event. This way we attempt to perform IO first and if it succeeds
 *   we don't ever touch any epoll machinery.
 * - ssl iostream uses SSL_read_ex/SSL_write_ex from OpenSSL and they perform
 *   client-server negotiation, so read may want to write and vice versa (see
 *   enum iostream_status comment for details). So in order to proceed further
 *   with reading new data we may need to write and so we need to wait for
 *   writability. Same for writing and waiting for readability.
 */
static void
ev_io_restart(ev_loop *loop, struct ev_io *watcher, int fd, int events_to_watch)
{
	if (watcher->events != events_to_watch) {
		ev_io_stop(loop, watcher);
		ev_io_set(watcher, fd, events_to_watch);
	}
	ev_io_start(loop, watcher);
}

static void
http_connection_feed_input(struct http_connection *con)
{
	if (ev_is_active(&con->iw)) {
		return;
	}

	ev_feed_event(loop(), &con->iw, EV_CUSTOM);
}

static void
http_connection_feed_output(struct http_connection *con)
{
	if (ev_is_active(&con->ow)) {
		return;
	}

	ev_feed_event(loop(), &con->ow, EV_CUSTOM);
}

// XXX: Hide this function, not called from outside.
void
http_connection_io_stop_in_thread(struct http_connection *con);

/**
 * Perform a SM step, adjust buffers, stop/start watchers as needed.
 *
 * Returns whether an immediate next call to the function can make a SM
 * progress.
 */
static bool
http_connection_perform_step(struct http_connection *con)
{
	/* Last known wishes from the protocol SM. */
	struct proto_sm_result *result = &con->sm.result;

	/*
	 * Perform a protocol SM step.
	 *
	 * NB: It updates `result`.
	 */
	if (!result->finished) {
		/*
		 * Reserve output buffer space as requested by SM.
		 *
		 * TODO: Implement draining the output buffer before reserving
		 * more if the buffer size reaches a limit.
		 */
		char *out = xbuf_reserve(&con->obuf, result->needed_out_size);
		size_t out_size = xbuf_unused(&con->obuf);

		struct proto_sm_event event = {
			.io_flags = con->io_flags,
			.in = con->ibuf.rpos,
			.in_size = xbuf_used(&con->ibuf),
			.out = out,
			.out_size = out_size,
			.now = ev_monotonic_now(loop()),
		};
		int rc = proto_sm_perform(&con->sm, &event);
		/* TODO: Error handling. */
		assert(rc == 0);

		/* Skip consumed bytes from the input buffer. */
		xbuf_consume(&con->ibuf, result->consumed_bytes);

		/* Bless written bytes in the output buffer. */
		xbuf_alloc(&con->obuf, result->written_bytes);
	}

	/*
	 * Once the SM reaches a terminal state and the output buffer is empty,
	 * we can shutdown and close the socket and unregister the connection.
	 */
	if (result->finished && xbuf_used(&con->obuf) == 0) {
		http_connection_io_stop_in_thread(con);
		http_connection_delete(con);
		return false;
	}

	/*
	 * If we don't need more input, if we got EOF or an error on input, then
	 * stop the input watcher.
	 *
	 * If we need more input than we currently have in the buffer, try read
	 * more (and start the watcher on EWOULDBLOCK).
	 *
	 * If we want to consume more input and already have enough in the
	 * buffer, go to the next http_connection_perform() loop iteration.
	 */
	if (result->needed_in_size == 0 ||
	    (con->io_flags & PROTO_SM_INPUT_EOF) != 0 ||
	    (con->io_flags & PROTO_SM_INPUT_ERROR) != 0) {
		ev_io_stop(loop(), &con->iw);
	} else if (result->needed_in_size > xbuf_used(&con->ibuf)) {
		http_connection_feed_input(con);
	}

	/*
	 * If we have nothing to write or if we got an error on output, then
	 * stop the output watcher.
	 *
	 * Otherwise, try to write (and start the watcher on EWOULDBLOCK).
	 */
	if (xbuf_used(&con->obuf) == 0) {
		ev_io_stop(loop(), &con->ow);
	} else if ((con->io_flags & PROTO_SM_OUTPUT_ERROR) != 0) {
		ev_io_stop(loop(), &con->ow);

		/*
		 * Free the output buffer data and re-initialize it to let
		 * xbuf_used() report zero (so once the SM in finished we don't
		 * do vain attempts to drain the output buffer before close the
		 * socket).
		 */
		xbuf_destroy(&con->obuf);
		xbuf_create(&con->obuf);
	} else {
		http_connection_feed_output(con);
	}

	/* TODO: Setup a timer. */
	(void)result->needed_wake_up_at;

	/*
	 * Ask the outer loop to call us again if we have enough data to consume
	 * in the input buffer or the SM wants to write data.
	 *
	 * TODO: Once we implement the output buffer size limit, we should take
	 * it into account in this condition. If we have to write some output
	 * first into the socket before we can reserve enough space to make a
	 * progress in SM, there is no sense to try to enter into the SM right
	 * now. The output watcher will do.
	 */
	bool sm_can_consume = result->needed_in_size > 0 &&
		result->needed_in_size <= xbuf_used(&con->ibuf);
	bool sm_needed_write = result->needed_out_size > 0;
	return sm_can_consume || sm_needed_write;
}

/** Make as much progress in SM as possible. */
static void
http_connection_perform(struct http_connection *con)
{
	while (http_connection_perform_step(con)) { }
}

static void
http_connection_on_input(ev_loop *loop, struct ev_io *iw, int events)
{
	(void)events;
	struct http_connection *con = (struct http_connection *)iw->data;
	assert(&con->iw == iw);

	/* Reserve space in the input buffer. */
	size_t readahead = con->sm.result.needed_in_size;
	if (readahead < READAHEAD) {
		readahead = READAHEAD;
	}
	char *wpos = xbuf_reserve(&con->ibuf, readahead);
	size_t unused = xbuf_unused(&con->ibuf);

	/* Read the socket. */
	ssize_t nrd = iostream_read(&con->io, wpos, unused);

	/* Handle error, EWOULDBLOCK, EOF. */
	if (nrd == IOSTREAM_ERROR) {
		/* Socket error. */
		con->io_flags |= PROTO_SM_INPUT_ERROR;
		/* TODO: It possibly worth to log the error right here. */
		http_connection_perform(con);
	} else if (nrd < 0) {
		/* Want read/want write (would block). */
		assert(nrd == IOSTREAM_WANT_READ || nrd == IOSTREAM_WANT_WRITE);
		int events_to_watch = iostream_status_to_events(nrd);
		ev_io_restart(loop, &con->iw, con->io.fd, events_to_watch);
		/* Nothing more to do: just wait for next events. */
	} else if (nrd == 0) {
		/* EOF. */
		con->io_flags |= PROTO_SM_INPUT_EOF;
		http_connection_perform(con);
	} else {
		/* Received some new data. */
		xbuf_alloc(&con->ibuf, nrd);
		http_connection_perform(con);
	}
}

static void
http_connection_on_output(ev_loop *loop, struct ev_io *ow, int events)
{
	(void)events;
	struct http_connection *con = (struct http_connection *)ow->data;
	assert(&con->ow == ow);

	char *rpos = con->obuf.rpos;
	size_t used = xbuf_used(&con->obuf);

	/* Write to the socket. */
	ssize_t nwr = 0;
	if (used > 0) {
		nwr = iostream_write(&con->io, rpos, used);
	}

	/* Handle error, EWOULDBLOCK. */
	if (nwr == IOSTREAM_ERROR) {
		/* Socket error. */
		con->io_flags |= PROTO_SM_OUTPUT_ERROR;
		/* TODO: It possibly worth to log the error right here. */
		http_connection_perform(con);
	} else if (nwr < 0) {
		/* Want read/want write (would block). */
		assert(nwr == IOSTREAM_WANT_READ || nwr == IOSTREAM_WANT_WRITE);
		int events_to_watch = iostream_status_to_events(nwr);
		ev_io_restart(loop, &con->ow, con->io.fd, events_to_watch);
		/* Nothing more to do: just wait for next events. */
	} else if (nwr == 0) {
		/*
		 * We had nothing to write, so nothing happens. In this case we
		 * have nothing to do.
		 */
	} else {
		/* Some data is written. */
		xbuf_consume(&con->obuf, nwr);
		http_connection_perform(con);
	}
}

struct http_connection *
http_connection_new(void)
{
	struct http_connection *con = malloc(sizeof(struct http_connection));

	iostream_clear(&con->io);
	con->io_flags = 0;

	/*
	 * Don't start the watchers yet. Instead, send EV_CUSTOM to run the
	 * watcher callback and perform non-blocking read/write first. This
	 * way we avoid setting the watcher (epoll calls and so on) if input
	 * data is already there (or if we have ability to write output data).
	 * The watcher callback performs the read/write and only if the socket
	 * actually non-readable/non-writable yet, sets the watcher.
	 */
	ev_io_init(&con->iw, http_connection_on_input, -1, EV_NONE);
	ev_io_init(&con->ow, http_connection_on_output, -1, EV_NONE);

	con->iw.data = con;
	con->ow.data = con;

	xbuf_create(&con->ibuf);
	xbuf_create(&con->obuf);

	proto_sm_http2_create(&con->sm);

	/*
	 * Makes the rlist_empty() assertion in http_connection_delete() valid.
	 */
	rlist_create(&con->in_connections);

	return con;
}

void
http_connection_delete(struct http_connection *con)
{
	assert(!iostream_is_initialized(&con->io));
	assert(!ev_is_active(&con->iw));
	assert(!ev_is_active(&con->ow));
	assert(con->iw.fd == -1);
	assert(con->ow.fd == -1);
	assert(rlist_empty(&con->in_connections));
	proto_sm_destroy(&con->sm);
	TRASH(con);
	free(con);
}

void
http_connection_io_start_in_thread(struct http_connection *con,
				   struct iostream *io)
{
	iostream_move(&con->io, io);
	rlist_add_entry(&connections, con, in_connections);

	http_connection_perform(con);
}

void
http_connection_io_stop_in_thread(struct http_connection *con)
{
	ev_io_stop(loop(), &con->iw);
	ev_io_stop(loop(), &con->ow);

	/*
	 * Invalidate fd, so any further attempt to use it after this point
	 * will fail. The same fd may be used by other socket or file
	 * descriptor.
	 */
	con->iw.fd = -1;
	con->ow.fd = -1;

	iostream_close(&con->io);

	xbuf_destroy(&con->ibuf);
	xbuf_destroy(&con->obuf);

	rlist_del_entry(con, in_connections);
}

void
http_connection_init_in_thread(void)
{
	rlist_create(&connections);
}
