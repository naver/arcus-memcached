/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015-2021 JaM2in Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _ARCUS_HB_
#define _ARCUS_HB_

#include "memcached/extension_loggers.h"

#ifdef ENABLE_ZK_INTEGRATION
typedef struct {
    uint64_t count;      /* mc heartbeat accumulated count */
    uint64_t latency;    /* mc heartbeat accumulated latency (unit: ms) */
} arcus_hb_stats;

typedef struct {
    uint32_t timeout;    /* mc heartbeat timeout (unit: ms) */
    uint32_t failstop;   /* mc heartbeat failstop (unit: ms) */
} arcus_hb_confs;

int  arcus_hb_init(int port, EXTENSION_LOGGER_DESCRIPTOR *logger,
                   void (*cb_shutdown_server)(void));
void arcus_hb_final(void);

int  arcus_hb_get_timeout(void);
int  arcus_hb_set_timeout(int timeout);
int  arcus_hb_get_failstop(void);
int  arcus_hb_set_failstop(int failstop);

void arcus_hb_get_stats(arcus_hb_stats *stats);
void arcus_hb_get_confs(arcus_hb_confs *confs);
#endif

#endif
