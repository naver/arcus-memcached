/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
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
 * Author: JunHyun Park <junhyun.park@nhn.com>
 */
#ifndef MEMCACHED_SQUALL_ENGINE_H
#define MEMCACHED_SQUALL_ENGINE_H

#include "config.h"
#include <pthread.h>
#include <stdbool.h>

#include <memcached/engine.h>
#include <memcached/util.h>
#include <memcached/visibility.h>
#include "squall_config.h"

/* Forward decl */
struct squall_engine;

#ifdef __cplusplus
extern "C" {
#endif

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API get_server_api,
                                  ENGINE_HANDLE **handle);

enum vbucket_state {
    VBUCKET_STATE_DEAD    = 0,
    VBUCKET_STATE_ACTIVE  = 1,
    VBUCKET_STATE_REPLICA = 2,
    VBUCKET_STATE_PENDING = 3
};

struct vbucket_info {
    int state : 2;
};

#define NUM_VBUCKETS 65536

/**
 * Definition of the private instance data used by the squall engine.
 *
 * This is currently "work in progress" so it is not as clean as it should be.
 */
struct squall_engine {
   ENGINE_HANDLE_V1 engine;
   SERVER_HANDLE_V1 server;
   GET_SERVER_API get_server_api;

   volatile bool initialized; /* Is the engine initalized or not */

   struct config config;
   union {
       engine_info engine_info;
       char buffer[sizeof(engine_info) +
                   (sizeof(feature_info) * LAST_REGISTERED_ENGINE_FEATURE)];
   } info;

   char vbucket_infos[NUM_VBUCKETS];
};
#endif
