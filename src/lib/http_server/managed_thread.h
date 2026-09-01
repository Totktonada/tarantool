/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

/**
 * Implements a thread interconnected with the tx thread using cbus, so the tx
 * thread may execute a function in the managed thread and wait for its result.
 *
 * The threads have initialized cord and libev, but have no fiber.
 */

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

typedef int (*managed_thread_call_f)(void *, void *);

/* To be called from tx. */
int
managed_thread_call(size_t thread_id, managed_thread_call_f func,
		    void *arg_1, void *arg_2);

/* To be called from tx. */
void
managed_thread_start(size_t thread_id);

/* To be called from tx. */
void
managed_thread_stop(size_t thread_id);

/* To be called from tx. */
void
managed_thread_init(const char *name_prefix);

/* To be called from tx. */
void
managed_thread_free(void);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
