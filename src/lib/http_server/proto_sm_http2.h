/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

struct proto_sm;

void
proto_sm_http2_create(struct proto_sm *sm);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
