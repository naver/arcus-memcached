/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2016 JaM2in Co., Ltd.
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
#ifndef MEMCACHED_DEFAULT_ENGINE_H
#define MEMCACHED_DEFAULT_ENGINE_H

#include "config.h"
#include <pthread.h>
#include <stdbool.h>
#include <memcached/engine.h>
#include <memcached/util.h>
#include <memcached/visibility.h>

/* Forward decl */
struct default_engine;

#include "trace.h"
#include "items.h"
#include "assoc.h"
#include "slabs.h"

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
 * scrubber
 */
enum scrub_mode {
    SCRUB_MODE_STOP   = 0,
    SCRUB_MODE_NORMAL = 1,
    SCRUB_MODE_STALE  = 2
};

struct engine_scrubber {
   pthread_mutex_t lock;
   volatile bool   enabled;
   volatile bool   running;
   enum scrub_mode runmode;
   uint64_t        visited;
   uint64_t        cleaned;
   time_t          started;
   time_t          stopped;
};

/**
 * cache item dumper
 */
#define MAX_FILEPATH_LENGTH 256
struct engine_dumper {
   pthread_mutex_t lock;
   bool            running;
   bool            success; /* dump final status: success or fail */
   bool            stop;    /* request to stop dump */
   enum dump_mode  mode;    /* dump mode: key dump is only supported. */
   uint64_t        visited; /* # of cache item visited */
   uint64_t        dumpped; /* # of cache item dumped */
   time_t          started; /* dump start time */
   time_t          stopped; /* dump stop time */
   char            filepath[MAX_FILEPATH_LENGTH]; /* dump file path */
   char           *prefix;  /* prefix of keys for dumping */
   int             nprefix;
};

/**
 * Definition of the private instance data used by the default engine.
 *
 * This is currently "work in progress" so it is not as clean as it should be.
 */
struct default_engine {
   ENGINE_HANDLE_V1 engine;
   SERVER_HANDLE_V1 server;
   GET_SERVER_API get_server_api;

   /**
    * Is the engine initialized or not
    */
   volatile bool initialized;

   struct assoc assoc;
   struct slabs slabs;
   struct items items;

   /**
    * The cache layer (item_* and assoc_*) is currently protected by
    * this single mutex
    */
   pthread_mutex_t cache_lock;

   struct engine_config config;
   struct engine_stats stats;
   struct engine_scrubber scrubber;
   struct engine_dumper dumper;
   union {
       engine_info engine_info;
       char buffer[sizeof(engine_info) + (sizeof(feature_info)*LAST_REGISTERED_ENGINE_FEATURE)];
   } info;
   char vbucket_infos[NUM_VBUCKETS];
};

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API get_server_api,
                                  ENGINE_HANDLE **handle);

#endif
