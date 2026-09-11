/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "trivia/util.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

enum {
	XBUF_INITIAL_CAPACITY = 16384,
};

/**
 * Continuous buffer plus read/write positions.
 *
 *  |.| <- buf
 *  |.|
 *  |.|
 *  |x| <- rpos
 *  |x|
 *  |x|
 *  |x|
 *  |x|
 *  |.| <- wpos
 *  |.|
 *  |.|
 *      <- end
 *
 * This is very similar to struct ibuf, but uses malloc() to ease inter-thread
 * buffer transfer.
 */
struct xbuf {
	/** The memory region that holds the data. */
	char *buf;
	/** Position where a meaningful data starts. */
	char *rpos;
	/** Position where a meaningful data ends. */
	char *wpos;
	/** The end of the allocated memory region. */
	char *end;
};

/**
 * Initialize the xbuf object.
 *
 * Doesn't allocate the buffer. It is allocated at first xbuf_reserve() or
 * xbuf_alloc().
 */
static inline void
xbuf_create(struct xbuf *xbuf)
{
	memset(xbuf, 0, sizeof(*xbuf));
}

/** Free the underneath buffer. */
static inline void
xbuf_destroy(struct xbuf *xbuf)
{
	free(xbuf->buf);
	TRASH(xbuf);
}

/** How much data is read and is not consumed yet. */
static inline size_t
xbuf_used(struct xbuf *xbuf)
{
	assert(xbuf->wpos >= xbuf->rpos);
	return xbuf->wpos - xbuf->rpos;
}

/** How much data can we fit beyond wpos. */
static inline size_t
xbuf_unused(struct xbuf *xbuf)
{
	assert(xbuf->wpos <= xbuf->end);
	return xbuf->end - xbuf->wpos;
}

/** How much memory is allocated. */
static inline size_t
xbuf_capacity(struct xbuf *xbuf)
{
	return xbuf->end - xbuf->buf;
}

/**
 * Ensure the buffer is able to store the given amount of new data.
 */
static inline char *
xbuf_reserve(struct xbuf *xbuf, size_t wsize)
{
	/* Enough space after wpos: just return wpos. */
	if (xbuf->wpos + wsize <= xbuf->end) {
		return xbuf->wpos;
	}

	/*
	 * Enough space after move the data to the beginning: move and return
	 * wpos.
	 */
	size_t used = xbuf_used(xbuf);
	size_t capacity = xbuf_capacity(xbuf);
	if (used + wsize <= capacity) {
		memmove(xbuf->buf, xbuf->rpos, used);
		xbuf->rpos = xbuf->buf;
		xbuf->wpos = xbuf->buf + used;
		return xbuf->wpos;
	}

	/*
	 * Not enough space: reallocate the buffer, move the data to the
	 * beginning of the new buffer and return the new wpos.
	 */

	size_t new_capacity = capacity * 2;
	if (new_capacity < XBUF_INITIAL_CAPACITY) {
		new_capacity = XBUF_INITIAL_CAPACITY;
	}
	while (new_capacity < used + wsize) {
		new_capacity *= 2;
	}

	char *new_buf = xmalloc(new_capacity);
	free(xbuf->buf);

	xbuf->buf = new_buf;
	xbuf->rpos = xbuf->buf;
	xbuf->wpos = xbuf->buf + used;
	xbuf->end = xbuf->buf + new_capacity;
	return xbuf->wpos;
}

/** Advance wpos, return old wpos. */
static inline char *
xbuf_alloc(struct xbuf *xbuf, size_t wsize)
{
	char *wpos = xbuf_reserve(xbuf, wsize);
	xbuf->wpos += wsize;
	return wpos;
}

/** Advance rpos. */
static inline void
xbuf_consume(struct xbuf *xbuf, size_t rsize)
{
	assert(xbuf->rpos + rsize <= xbuf->wpos);
	xbuf->rpos += rsize;
}

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
