/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "managed_thread.h"

#include <assert.h>
#include <stddef.h>
#include "trivia/util.h"
#include "core/cbus.h"
#include "core/fiber.h"
#include "core/say.h"
#include "tarantool_ev.h"

/* Constants. */
enum {
	THREAD_NAME_PREFIX_MAX = 16,
	THREAD_NAME_BUFFER_MAX = 32,
	INITIAL_THREADS_CAPACITY = 16,
};

struct managed_thread {
	struct cord cord;
	/* tx -> managed thread. */
	struct cpipe call_pipe;
	/* managed thread -> tx. */
	struct cpipe call_ret_pipe;
};

struct managed_thread_call_msg {
	struct cbus_call_msg base;
	managed_thread_call_f func;
	void *arg_1;
	void *arg_2;
};

/* Globals. */
static char thread_name_prefix[THREAD_NAME_PREFIX_MAX];
static struct managed_thread **threads;
static size_t threads_capacity;

/* Extend the threads array if needed. */
static void
threads_resize(size_t thread_id)
{
	if (thread_id < threads_capacity) {
		return;
	}

	size_t old_capacity = threads_capacity;
	struct managed_thread **old_threads = threads;
	threads_capacity *= 2;
	threads = xcalloc(threads_capacity, sizeof(*threads));
	for (size_t i = 0; i < old_capacity; ++i) {
		threads[i] = old_threads[i];
	}
	free(old_threads);
}

static void
thread_endpoint_cb(ev_loop *loop, struct ev_watcher *watcher, int events)
{
	(void)events;
	struct cbus_endpoint *endpoint = watcher->data;
	cbus_process(endpoint);
	/*
	 * The last producer is gone -- cpipe_destroy() delivered its poison
	 * message just now, and nothing can arrive any more.
	 */
	if (endpoint->n_pipes == 0) {
		ev_break(loop, EVBREAK_ALL);
	}
}

static void
set_thread_name(char *buf, size_t buf_size, size_t thread_id)
{
	snprintf(buf, buf_size, "%s%ld", thread_name_prefix, thread_id);
}

static void *
managed_thread_f(void *arg)
{
	size_t thread_id = (size_t)arg;
	struct managed_thread *thread = threads[thread_id];

	cpipe_create(&thread->call_ret_pipe, "tx_prio");

	/* Cbus endpoint for messages from thread->call_pipe. */
	struct cbus_endpoint endpoint;
	char name[THREAD_NAME_BUFFER_MAX];
	set_thread_name(name, sizeof(name), thread_id);
	cbus_endpoint_create(&endpoint, name, thread_endpoint_cb, &endpoint);

	say_debug("thread is started\n");
	ev_run(loop(), 0);

	cbus_endpoint_destroy(&endpoint, cbus_process);

	cpipe_destroy(&thread->call_ret_pipe);
	say_debug("thread is stopped\n");
	return NULL;
}

/*
 * Hide the cmsg structure behind the implementation and pass the argument to
 * the function directly.
 */
static int
call_adaptor(struct cbus_call_msg *base)
{
	struct managed_thread_call_msg *msg =
		container_of(base, struct managed_thread_call_msg, base);
	return msg->func(msg->arg_1, msg->arg_2);
}

int
managed_thread_call(size_t thread_id, managed_thread_call_f func,
		    void *arg_1, void *arg_2)
{
	assert(cord_is_main());
	assert(thread_id < threads_capacity && threads[thread_id] != NULL);

	struct managed_thread *thread = threads[thread_id];

	// Yields until the function returns.
	struct managed_thread_call_msg msg;
	msg.func = func;
	msg.arg_1 = arg_1;
	msg.arg_2 = arg_2;
	return cbus_call(&thread->call_pipe, &thread->call_ret_pipe, &msg.base,
			 call_adaptor);
}

void
managed_thread_start(size_t thread_id)
{
	assert(cord_is_main());
	assert(thread_id >= threads_capacity ||	threads[thread_id] == NULL);

	threads_resize(thread_id);
	assert(threads[thread_id] == NULL);
	struct managed_thread *thread = xmalloc(sizeof(struct managed_thread));
	threads[thread_id] = thread;

	char name[THREAD_NAME_BUFFER_MAX];
	set_thread_name(name, sizeof(name), thread_id);
	struct cord *cord = &thread->cord;
	if (cord_start(cord, name, managed_thread_f, (void *)thread_id) != 0) {
		panic_syserror("failed to start managed thread %ld", thread_id);
	}
	cpipe_create(&thread->call_pipe, name);
}

void
managed_thread_stop(size_t thread_id)
{
	assert(cord_is_main());
	assert(thread_id < threads_capacity && threads[thread_id] != NULL);

	struct managed_thread *thread = threads[thread_id];
	cpipe_destroy(&thread->call_pipe);
	if (cord_join(&thread->cord) != 0) {
		panic_syserror("failed to join managed thread %ld", thread_id);
	}

	free(threads[thread_id]);
	threads[thread_id] = NULL;
}

void
managed_thread_init(const char *name_prefix)
{
	strlcpy(thread_name_prefix, name_prefix, sizeof(thread_name_prefix));

	threads_capacity = INITIAL_THREADS_CAPACITY;
	threads = xcalloc(threads_capacity, sizeof(*threads));
}

void
managed_thread_free(void)
{
	for (size_t i = 0; i < threads_capacity; ++i) {
		assert(threads[i] == NULL);
	}
	free(threads);
	threads = NULL;
	threads_capacity = 0;
}
