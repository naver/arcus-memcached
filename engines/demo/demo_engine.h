/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2016 JaM2in Co., Ltd.
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
/*
 * Summary: Specification of the storage engine interface.
 *
 * Copy: See Copyright for the status of this software.
 *
 * Author: Trond Norbye <trond.norbye@sun.com>
 */
#ifndef MEMCACHED_DEMO_ENGINE_H
#define MEMCACHED_DEMO_ENGINE_H

#include "config.h"
#include <pthread.h>
#include <stdbool.h>
#include <memcached/engine.h>
#include <memcached/util.h>
#include <memcached/visibility.h>

/* Forward decl */
struct demo_engine;

#include "trace.h"
#include "dm_items.h"
#include "dm_assoc.h"

/**
 * engine configuration
 */
struct engine_config {
   bool   use_cas;
   size_t verbose;
   rel_time_t oldest_live;
   bool   evict_to_free;
   size_t num_threads;
   size_t maxbytes;
   size_t sticky_limit;
   bool   preallocate;
   float  factor;
   size_t chunk_size;
   size_t item_size_max;
   size_t max_list_size;
   size_t max_set_size;
   size_t max_map_size;
   size_t max_btree_size;
   bool   ignore_vbucket;
   char   prefix_delimiter;
   bool   vb0;
};

/**
 * Statistic information collected by engine
 */
struct engine_stats {
   pthread_mutex_t lock;
   uint64_t evictions;
   uint64_t reclaimed;
   uint64_t sticky_bytes;
   uint64_t sticky_items;
   uint64_t curr_bytes;
   uint64_t curr_items;
   uint64_t total_items;
};

/**
 * Definition of the private instance data used by the demo engine.
 *
 * This is currently "work in progress" so it is not as clean as it should be.
 */
struct demo_engine {
   ENGINE_HANDLE_V1 engine;
   SERVER_HANDLE_V1 server;
   GET_SERVER_API get_server_api;

   /* Is the engine initialized or not */
   volatile bool initialized;

   struct dm_assoc assoc;

   /**
    * The cache layer (item_* and assoc_*) is currently protected by
    * this single mutex
    */
   pthread_mutex_t cache_lock;

   struct engine_config config;
   struct engine_stats stats;
   union {
       engine_info engine_info;
       char buffer[sizeof(engine_info) + (sizeof(feature_info)*LAST_REGISTERED_ENGINE_FEATURE)];
   } info;
};

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API get_server_api,
                                  ENGINE_HANDLE **handle);

#endif
