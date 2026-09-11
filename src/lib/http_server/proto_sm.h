/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include <assert.h>
#include <stddef.h>
#include <stdbool.h>
#include "core/say.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/**
 * The protocol state-machine API allows to implement IO layer separately from
 * protocol layer and vice versa.
 *
 * The IO layer is supposed to watch for new input, bufferize it, provide output
 * buffer and write the output. Also, it provides a timer.
 *
 * The SM layer implements a particular protocol and supposed to be free of
 * bufferization/queueing problems (but likely have some message/frame sized
 * buffers).
 *
 * The declared API provides particular guarantees to each side.
 */

/* proto_sm_event.io_flags */
enum {
	PROTO_SM_INPUT_EOF = 1 << 0,
	PROTO_SM_INPUT_ERROR = 1 << 1,
	PROTO_SM_OUTPUT_ERROR = 1 << 2,
};

/**
 * Feed events to the SM.
 *
 * - More input arrives or EOF/error happens on input.
 * - The output buffer shrinks to a requested size.
 * - A requested deadline is now.
 *
 * TODO: Represent a wish to start a graceful shutdown.
 *
 * A caller has to assign all the fields.
 */
struct proto_sm_event {
	/**
	 * EOF or an error on input, an error on output.
	 *
	 * Since a new input is not the only event we can feed with this
	 * structure, just in_size == 0 is not sufficient and we need the
	 * flags to notify SM about EOF.
	 */
	int io_flags;
	/** Currently read (and now yet consumed by SM) input. */
	char *in;
	/** The size of the input. */
	size_t in_size;
	/** The output buffer. */
	char *out;
	/** The capacity of the outbuf buffer. */
	size_t out_size;
	/**
	 * Current monotonic time in seconds, same scale as
	 * proto_sm_result.needs_wake_up.
	 */
	double now;
};

/**
 * A result of one proto_sm_perform() call.
 *
 * SM has to assign all the fields on each proto_sm_perform() call.
 *
 * TODO: Consider moving consumed_bytes and written_bytes to struct
 * proto_sm_event (these are related to just one perform call and shouldn't
 * be stored in sm -- double accounting is harmful).
 *
 * TODO: Rename it to proto_sm_state or event include {needed_*,finished} into
 * struct proto_sm directly.
 */
struct proto_sm_result {
	/** Amount of bytes consumed from the input buffer. */
	size_t consumed_bytes;

	/** Amount of bytes written into the output buffer. */
	size_t written_bytes;

	/**
	 * If the protocol SM needs more input to make progress.
	 *
	 * 0 here DOESN'T mean that the read direction of the socket can be shut
	 * down. It only means that more input is not needed for now.
	 *
	 * 1 means that any amount of input is sufficient.
	 *
	 * >1 means that the SM want to consume a frame/message of the given
	 * size and would reject less bytes of input. It allows SM be free of
	 * internal buffering or queueing.
	 */
	size_t needed_in_size;

	/**
	 * If the protocol SM needs an output buffer to make progress and the
	 * size of the output buffer.
	 *
	 * An implementation of a message/frame based protocol is supposed to
	 * expose the maximum output message/frame size via this field in order
	 * to avoid buffering output tail internally or backtracking the SM.
	 *
	 * The SM doesn't have to write this amount of bytes to the output
	 * buffer, but the caller is supposed to adhere the requested buffer
	 * size to avoid lockout (when a large message can't be written to a
	 * smaller output buffer).
	 *
	 * Zero means that no output buffer is needed to make progress.
	 */
	size_t needed_out_size;

	/**
	 * If the protocol SM needs to be waken up if no other events happen and
	 * the time when to wake up.
	 *
	 * It is a monotonic time in seconds, same scale as proto_sm_event.now.
	 * Set to -1 if the timer is not needed.
	 */
	double needed_wake_up_at;

	/**
	 * If a terminal state is reached.
	 *
	 * The terminal here means that mo more input is needed and no more
	 * output will be written. The SM effectively completes its job.
	 *
	 * A caller is supposed to write remaining bytes from the output buffer
	 * to the client before shutdown/close the socket.
	 */
	bool finished;
};

static inline void
proto_sm_result_create(struct proto_sm_result *result)
{
	result->consumed_bytes = 0;
	result->written_bytes = 0;
	result->needed_in_size = false;
	result->needed_out_size = 0;
	result->needed_wake_up_at = -1;
	result->finished = false;
}

struct proto_sm;

struct proto_sm_vtab {
	int
	(*perform)(struct proto_sm *sm, struct proto_sm_event *event);
	void
	(*destroy)(struct proto_sm *sm);
};

/*
 * TODO: We should also provide some way to notify the io side about new data in
 * SM.
 */
struct proto_sm {
	/** Particular protocol SM implementation. */
	const struct proto_sm_vtab *vtab;
	/** Result of last proto_sm_perform() call. */
	struct proto_sm_result result;
};

/**
 * Consume input events, do a step of the SM, fill sm.result.
 *
 * Returns 0 on success, -1 on error (diag is set).
 */
static inline int
proto_sm_perform(struct proto_sm *sm, struct proto_sm_event *event)
{
	assert(!sm->result.finished);
	int rc = sm->vtab->perform(sm, event);
	say_debug("proto_sm_perform: "
		  "event.io_flags: %d; "
		  "event.in_size: %ld; "
		  "event.out_size: %ld; "
		  "event.now: %f; "
		  "sm.consumed_bytes: %ld; "
		  "sm.written_bytes: %ld; "
		  "sm.needed_in_size: %ld; "
		  "sm.needed_out_size: %ld; "
		  "sm.needed_wake_up_at: %f; "
		  "sm.finished: %d; "
		  "rc: %d",
		  event->io_flags,
		  event->in_size,
		  event->out_size,
		  event->now,
		  sm->result.consumed_bytes,
		  sm->result.written_bytes,
		  sm->result.needed_in_size,
		  sm->result.needed_out_size,
		  sm->result.needed_wake_up_at,
		  sm->result.finished,
		  rc);
	/*
	 * Consistency checks: if the SM is finished, it is supposed that the SM
	 * doesn't want to consume more input or write output.
	 */
	assert(!sm->result.finished || sm->result.needed_in_size == 0);
	assert(!sm->result.finished || sm->result.needed_out_size == 0);
	return rc;
}

/**
 * Free resources hold by the SM.
 */
static inline void
proto_sm_destroy(struct proto_sm *sm)
{
	sm->vtab->destroy(sm);
}

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
