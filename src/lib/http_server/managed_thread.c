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
	THREAD_NAME_BUFFER_SIZE = 32,
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
static char call_ret_endpoint_name[THREAD_NAME_BUFFER_SIZE];
static struct cbus_endpoint call_ret_endpoint;
static char thread_name_prefix[THREAD_NAME_PREFIX_MAX];
static struct managed_thread **threads;
static size_t threads_capacity;

/* Thread locals. */
static __thread char thread_name[THREAD_NAME_BUFFER_SIZE];

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

/**
 * Process all the messages right in the callback.
 *
 * It works, because the last hop of cbus_call()'s route does not
 * yield (it just schedules the caller fiber) and because we don't
 * use this endpoint only for cbus_call().
 */
static void
call_ret_endpoint_cb(ev_loop *loop, struct ev_watcher *watcher, int events)
{
	(void)loop;
	(void)events;
	struct cbus_endpoint *endpoint = (struct cbus_endpoint *)watcher->data;
	cbus_process(endpoint);
}

static void
thread_name_copy(char *buf, size_t buf_size, size_t thread_id)
{
	snprintf(buf, buf_size, "%s%ld", thread_name_prefix, thread_id);
}

char *
managed_thread_name(void)
{
	return thread_name;
}

static void *
managed_thread_f(void *arg)
{
	size_t thread_id = (size_t)arg;
	struct managed_thread *thread = threads[thread_id];

	/* Save the thread name into a thread local variable. */
	thread_name_copy(thread_name, sizeof(thread_name), thread_id);

	cpipe_create(&thread->call_ret_pipe, call_ret_endpoint_name);

	/* Cbus endpoint for messages from thread->call_pipe. */
	struct cbus_endpoint endpoint;
	cbus_endpoint_create(&endpoint, thread_name, thread_endpoint_cb,
			     &endpoint);

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

	char name[THREAD_NAME_BUFFER_SIZE];
	thread_name_copy(name, sizeof(name), thread_id);
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
	/*
	 * Use our own tx side endpoint, because tx/tx_prio are created on box
	 * configuration and not available beforehand.
	 */
	snprintf(call_ret_endpoint_name, sizeof(call_ret_endpoint_name),
		"%scall_ret", name_prefix);
	cbus_endpoint_create(&call_ret_endpoint, call_ret_endpoint_name,
			     call_ret_endpoint_cb, &call_ret_endpoint);

	strlcpy(thread_name_prefix, name_prefix, sizeof(thread_name_prefix));

	threads_capacity = INITIAL_THREADS_CAPACITY;
	threads = xcalloc(threads_capacity, sizeof(*threads));
}

void
managed_thread_shutdown(void)
{
	/*
	 * At this point all the managed threads must be stopped, however poison
	 * messages may still be in fly. They likely read all at once in
	 * cbus_process() without yielding, but let's stay on the safe side and
	 * destroy the endpoint from the ..._shutdown() function (where we have
	 * a running event loop and a fiber), not from the ..._free() function.
	 */
	cbus_endpoint_destroy(&call_ret_endpoint, cbus_process);
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
