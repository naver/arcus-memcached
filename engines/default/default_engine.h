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
#include "prefix.h"
#include "assoc.h"
#include "slabs.h"

#define MAX_FILEPATH_LENGTH 4096
#define MAX_FILENAME_LENGTH 256
#ifdef ENABLE_LARGE_ITEM
#define MAX_ITEM_VALUE_LENGTH   10 * 1024 * 1024
#define LARGE_ITEM_VALUE_LENGTH 128 * 1024
#endif

/**
 * engine configuration
 */
struct engine_config {
   size_t     verbose;
   rel_time_t oldest_live;
   bool       use_cas;
   bool       evict_to_free;
   char       prefix_delimiter;
   bool       preallocate;
   float      factor;
   size_t     chunk_size;
   size_t     num_threads;
   size_t     maxbytes;
   size_t     sticky_limit;
   size_t     item_size_max;
   uint32_t   max_list_size;
   uint32_t   max_set_size;
   uint32_t   max_map_size;
   uint32_t   max_btree_size;
   uint32_t   max_element_bytes;
   uint32_t   scrub_count;
#ifdef ENABLE_PERSISTENCE
   bool       use_persistence;
   bool       async_logging;
   char       *data_path;
   char       *logs_path;
   size_t     chkpt_interval_pct_snapshot;
   size_t     chkpt_interval_min_logsize;
#endif
   bool       ignore_vbucket;
   bool       vb0;
};

/**
 * Statistic information collected by engine
 */
struct engine_stats {
   pthread_mutex_t lock;
   uint64_t evictions;
   uint64_t reclaimed;
   uint64_t outofmemorys;
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
   volatile bool   restart;
   enum scrub_mode runmode;
   uint64_t        visited;
   uint64_t        cleaned;
   time_t          started;
   time_t          stopped;
};

/**
 * cache item dumper
 */
struct engine_dumper {
   pthread_mutex_t lock;
   volatile bool   running;
   bool            success; /* dump final status: success or fail */
   bool            stop;    /* request to stop dump */
   enum dump_mode  mode;    /* dump mode: key dump is only supported. */
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

   struct prefix prefix;
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
