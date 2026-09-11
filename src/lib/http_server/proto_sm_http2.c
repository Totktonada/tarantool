/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "proto_sm_http2.h"

#include <assert.h>
#include "proto_sm.h"

/* Forward declarations. */
static int
proto_sm_http2_perform(struct proto_sm *sm, struct proto_sm_event *event);
static void
proto_sm_http2_destroy(struct proto_sm *sm);

/* Globals. */
static const struct proto_sm_vtab proto_sm_http2_vtab = {
	.perform = proto_sm_http2_perform,
	.destroy = proto_sm_http2_destroy,
};

void
proto_sm_http2_create(struct proto_sm *sm)
{
	sm->vtab = &proto_sm_http2_vtab;
	proto_sm_result_create(&sm->result);
}


static int
proto_sm_http2_perform(struct proto_sm *sm, struct proto_sm_event *event)
{
	assert(!sm->result.finished);

	if ((event->io_flags & PROTO_SM_INPUT_EOF) != 0 ||
	    (event->io_flags & PROTO_SM_INPUT_ERROR) != 0 ||
	    (event->io_flags & PROTO_SM_OUTPUT_ERROR) != 0) {
		sm->result.finished = true;
		return 0;
	}

	sm->result.consumed_bytes = event->in_size;

	if (event->out_size >= 2) {
		event->out[0] = 'H';
		event->out[1] = 'i';

		sm->result.written_bytes = 2;
		sm->result.needed_in_size = 0;
		sm->result.needed_out_size = 0;
		sm->result.needed_wake_up_at = -1;
		sm->result.finished = true;
	} else {
		sm->result.written_bytes = 0;
		sm->result.needed_in_size = 0;
		sm->result.needed_out_size = 2;
		sm->result.needed_wake_up_at = -1;
		sm->result.finished = false;
	}

	return 0;
}

static void
proto_sm_http2_destroy(struct proto_sm *sm)
{
	/* Nothing to do. */
	(void)sm;
}
