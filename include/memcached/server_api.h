/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#ifndef MEMCACHED_SERVER_API_H
#define MEMCACHED_SERVER_API_H
#include <inttypes.h>

#include <memcached/types.h>
#include <memcached/config_parser.h>

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct {
        /**
         * The current time.
         */
        rel_time_t (*get_current_time)(void);

        /**
         * Get the relative time for the given time_t value.
         */
        rel_time_t (*realtime)(const time_t exptime);


        /**
         * Get the server's version number.
         *
         * @return the server's version number
         */
        const char* (*server_version)(void);

        /**
         * Generate a simple hash value of a piece of data.
         *
         * @param data pointer to data to hash
         * @param size size of the data to generate the hash value of
         * @param seed an extra seed value for the hash function
         * @return hash value of the data.
         */
        uint32_t (*hash)(const void *data, size_t size, uint32_t seed);

        /**
         * parser config options
         */
        int (*parse_config)(const char *str, struct config_item items[], FILE *error);

        /**
         * Get the auth data for the connection associated with the
         * given cookie.
         *
         * @param cookie The cookie provided by the frontend
         * @param data Pointer to auth_data_t structure for returning the values
         *
         */
        void (*get_auth_data)(const void *cookie, auth_data_t *data);

        /**
         * Store engine-specific session data on the given cookie.
         *
         * The engine interface allows for a single item to be
         * attached to the connection that it can use to track
         * connection-specific data throughout duration of the
         * connection.
         *
         * @param cookie The cookie provided by the frontend
         * @param engine_data pointer to opaque data
         */
        void (*store_engine_specific)(const void *cookie, void *engine_data);

        /**
         * Retrieve engine-specific session data for the given cookie.
         *
         * @param cookie The cookie provided by the frontend
         *
         * @return the data provied by store_engine_specific or NULL
         *         if none was provided
         */
        void *(*get_engine_specific)(const void *cookie);

        /**
         * Retrieve socket file descriptor of the session for the given cookie.
         *
         * @param cookie The cookie provided by the frontend
         *
         * @return the socket file descriptor of the session for the given cookie.
         */
        int (*get_socket_fd)(const void *cookie);

        /**
         * Retrieve client ip address of the session for the given cookie.
         *
         * @param cookie The cookie provided by the frontend
         *
         * @return the client ip address of the session for the given cookie.
         */
        const char* (*get_client_ip)(const void *cookie);

        /**
         * Retrieve thread index for the given cookie.
         *
         * @param cookie The cookie provided by the frontend
         *
         * @return the thread index from 0 to num_threads-1.
         */
        int (*get_thread_index)(const void *cookie);

        /**
         * Retrieve noreply value of the session for the given cookie.
         *
         * @param cookie The cookie provided by the frontend
         *
         * @return the noreply value of the session for the given cookie.
         */
        bool (*get_noreply)(const void *cookie);

#ifdef MULTI_NOTIFY_IO_COMPLETE
        /**
         * Let a connection know that it must wait for IO completion.
         * @param cookie cookie representing the connection
         */
        void (*waitfor_io_complete)(const void *cookie);

#endif
        /**
         * Let a connection know that IO has completed.
         * @param cookie cookie representing the connection
         * @param status the status for the io operation
         */
        void (*notify_io_complete)(const void *cookie,
                                   ENGINE_ERROR_CODE status);

#ifdef NEW_PREFIX_STATS_MANAGEMENT
        /**
         * Insert prefix operation stats entry for the given given prefix.
         * @param prefix prefix name to delete
         * @param nprefix the length of prefix name
         */
        int (*prefix_stats_insert)(const char *prefix, const size_t nprefix);

        /**
         * Delete the given prefix from prefix operation stats table.
         * @param prefix prefix name to delete
         * @param nprefix the length of prefix name
         */
        int (*prefix_stats_delete)(const char *prefix, const size_t nprefix);
#endif

#ifdef ENABLE_CLUSTER_AWARE
        /**
         * Check if current cache node is started with zk integration.
         */
        bool (*is_zk_integrated)(void);

        /**
         * Check if the given key is in the right server based on
         * the cluster's key hashing policy.
         *
         * @param key The key to check for
         * @param nkey The key's length
         *
         * @return true if the key is in the right server.
         */
        bool (*is_my_key)(const char *key, size_t nkey);
#endif

        /**
         * Request the server to start a shutdown sequence.
         */
        void (*shutdown)(void);

    } SERVER_CORE_API;

    typedef struct {
        /**
         * Allocate and deallocate thread-specific stats arrays for
         * engine-maintained separate stats.
         */
        void *(*new_stats)(void);
        void (*release_stats)(void*);

        /**
         * Tell the server we've evicted an item.
         */
        void (*evicting)(const void *cookie,
                         const void *key,
                         int nkey);
    } SERVER_STAT_API;

#ifdef __WIN32__
#undef interface
#endif

    typedef SERVER_HANDLE_V1* (*GET_SERVER_API)(void);

#ifdef __cplusplus
}
#endif

#endif
