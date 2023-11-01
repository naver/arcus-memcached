/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015 JaM2in Co., Ltd.
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
#ifndef _ARCUS_H_
#define _ARCUS_H_

#include "config.h"
#include "memcached/extension_loggers.h"
#include "memcached/engine.h"

#ifdef ENABLE_ZK_INTEGRATION

typedef struct {
    const char  *zk_libversion; // Zookeeper client version
    uint32_t    zk_timeout;     // Zookeeper session timeout (unit: ms)
    bool        zk_failstop;    // memcached automatic failstop
} arcus_zk_confs;

typedef struct {
    bool     zk_connected;  // ZooKeeper-memcached connection state
    bool     zk_ready;      // cache-list znode created?
#ifdef ENABLE_ZK_RECONFIG
    bool     zk_reconfig_needed;  // ZK dynamic reconfig is needed?
    bool     zk_reconfig_enabled; // ZK dynamic reconfig enabled in ZK?
    int64_t  zk_reconfig_version; // ZK dynamic reconfig version
#endif
} arcus_zk_stats;

/* Interface between memcached.c and arcus_zk.c */

extern volatile sig_atomic_t arcus_zk_shutdown;

void arcus_zk_init(char *ensemble_list, int zk_to,
                   EXTENSION_LOGGER_DESCRIPTOR *logger,
                   int verbose, size_t maxbytes, int port,
#ifdef PROXY_SUPPORT
                   char *proxy,
#endif
                   ENGINE_HANDLE_V1 *engine);
void arcus_zk_final(const char *msg);
void arcus_zk_destroy(void);

int  arcus_zk_set_ensemble(char *ensemble_list);
int  arcus_zk_get_ensemble(char *buf, int size);
int  arcus_zk_rejoin_ensemble(void);

void arcus_zk_set_failstop(bool failstop);
bool arcus_zk_get_failstop(void);
void arcus_zk_get_confs(arcus_zk_confs *confs);
void arcus_zk_get_stats(arcus_zk_stats *stats);

#ifdef ENABLE_CLUSTER_AWARE
int  arcus_key_is_mine(const char *key, size_t nkey, bool *mine);
#endif

#endif /* ENABLE_ZK_INTEGRATION */

#endif /* !defined(_ARCUS_H_) */
