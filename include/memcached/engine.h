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
#ifndef MEMCACHED_ENGINE_H
#define MEMCACHED_ENGINE_H

#include <sys/types.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "memcached/types.h"
#include "memcached/protocol_binary.h"
#include "memcached/config_parser.h"
#include "memcached/server_api.h"
#include "memcached/callback.h"
#include "memcached/extension.h"
#include "memcached/vbucket.h"
#include "memcached/engine_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/*! \mainpage memcached public API
 *
 * \section intro_sec Introduction
 *
 * The memcached project provides an API for providing engines as well
 * as data definitions for those implementing the protocol in C.  This
 * documentation will explain both to you.
 *
 * \section docs_sec API Documentation
 *
 * Jump right into <a href="modules.html">the modules docs</a> to get started.
 *
 * \example default_engine.c
 */

/**
 * \defgroup Engine Storage Engine API
 * \defgroup Protex Protocol Extension API
 * \defgroup Protocol Binary Protocol Structures
 *
 * \addtogroup Engine
 * @{
 *
 * Most interesting here is to implement engine_interface_v1 for your
 * engine.
 */

#define ENGINE_INTERFACE_VERSION 1

    /**
     * Callback for any function producing stats.
     *
     * @param key the stat's key
     * @param klen length of the key
     * @param val the stat's value in an ascii form (e.g. text form of a number)
     * @param vlen length of the value
     * @param cookie magic callback cookie
     */
    typedef void (*ADD_STAT)(const char *key, const uint16_t klen,
                             const char *val, const uint32_t vlen,
                             const void *cookie);

    /**
     * Callback for adding a response backet
     * @param key The key to put in the response
     * @param keylen The length of the key
     * @param ext The data to put in the extended field in the response
     * @param extlen The number of bytes in the ext field
     * @param body The data body
     * @param bodylen The number of bytes in the body
     * @param datatype This is currently not used and should be set to 0
     * @param status The status code of the return packet (see in protocol_binary
     *               for the legal values)
     * @param cas The cas to put in the return packet
     * @param cookie The cookie provided by the frontend
     * @return true if return message was successfully created, false if an
     *              error occured that prevented the message from being sent
     */
    typedef bool (*ADD_RESPONSE)(const void *key, uint16_t keylen,
                                 const void *ext, uint8_t extlen,
                                 const void *body, uint32_t bodylen,
                                 uint8_t datatype, uint16_t status,
                                 uint64_t cas, const void *cookie);


    /**
     * Abstract interface to an engine.
     */
#ifdef __WIN32__
#undef interface
#endif
    struct item_observer_cb_data {
        const void *key; /* THis isn't going to work from a memory management perspective */
        size_t nkey;
    };

    /* This is typedefed in types.h */
    struct server_handle_v1_t {
        uint64_t interface; /**< The version number on the server structure */
        SERVER_CORE_API *core;
        SERVER_STAT_API *stat;
        SERVER_EXTENSION_API *extension;
        SERVER_CALLBACK_API *callback;
        ENGINE_HANDLE *engine;
        SERVER_LOG_API *log;
    };

    /**
     * The signature for the "create_instance" function exported from the module.
     *
     * This function should fill out an engine inteface structure according to
     * the interface parameter (Note: it is possible to return a lower version
     * number).
     *
     * @param interface The highest interface level the server supports
     * @param get_server_api function to get the server API from
     * @param Where to store the interface handle
     * @return See description of ENGINE_ERROR_CODE
     */
    typedef ENGINE_ERROR_CODE (*CREATE_INSTANCE)(uint64_t interface,
                                                 GET_SERVER_API get_server_api,
                                                 ENGINE_HANDLE** handle);

    typedef enum {
        ENGINE_FEATURE_CAS, /**< has compare-and-set operation */
        ENGINE_FEATURE_PERSISTENT_STORAGE, /**< has persistent storage support*/
        ENGINE_FEATURE_SECONDARY_ENGINE, /**< performs as pseudo engine */
        ENGINE_FEATURE_ACCESS_CONTROL, /**< has access control feature */
        ENGINE_FEATURE_MULTI_TENANCY,
        ENGINE_FEATURE_LRU, /* Cache implements an LRU */
        ENGINE_FEATURE_VBUCKET /* Cache implements virtual buckets */

#define LAST_REGISTERED_ENGINE_FEATURE ENGINE_FEATURE_VBUCKET
    } engine_feature_t;

    typedef struct {
        /**
         * The identifier of this feature. All values with the most significant bit cleared is reserved
         * for "registered" features.
         */
        uint32_t feature;
        /**
         * A textual description of the feature. (null will print the registered name for the feature
         * (or "Unknown feature"))
         */
        const char *description;
    } feature_info;

    typedef struct {
        /**
         * Textual description of this engine
         */
        const char *description;
        /**
         * The number of features the server provides
         */
        uint32_t num_features;
        /**
         * An array containing all of the features the engine supports
         */
        feature_info features[1];
    } engine_info;

    /**
     * Definition of the first version of the engine interface
     */
    typedef struct engine_interface_v1 {
        /**
         * Engine info.
         */
        struct engine_interface interface;

        /**
         * Get a description of this engine.
         *
         * @param handle the engine handle
         * @return a stringz description of this engine
         */
        const engine_info* (*get_info)(ENGINE_HANDLE* handle);

        /**
         * Initialize an engine instance.
         * This is called *after* creation, but before the engine may be used.
         *
         * @param handle the engine handle
         * @param config_str configuration this engine needs to initialize itself.
         */
        ENGINE_ERROR_CODE (*initialize)(ENGINE_HANDLE* handle, const char* config_str);

        /**
         * Tear down this engine.
         *
         * @param handle the engine handle
         */
        void (*destroy)(ENGINE_HANDLE* handle);

#ifdef SCAN_COMMAND
        /**
         * Indicate that a caller who received a prefix no longer needs it.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param item the prefix to be released
         */
        void (*prefix_release)(ENGINE_HANDLE* handle, const void *cookie, item* item);
#endif

        /*
         * Item operations.
         */

        /**
         * Allocate an item.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param output variable that will receive the item
         * @param key the item's key (can be NULL)
         * @param nkey the length of the key
         * @param nbytes the number of bytes that will make up the
         *        value of this item.
         * @param flags the item's flags
         * @param exptime the maximum lifetime of this item
         * @param cas the cas value to set in this item
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*allocate)(ENGINE_HANDLE* handle, const void* cookie,
                                      item **item,
                                      const void* key, const size_t nkey,
                                      const size_t nbytes,
                                      const int flags, const rel_time_t exptime,
                                      const uint64_t cas);

        /**
         * Remove an item.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param key the key identifying the item to be removed
         * @param nkey the length of the key
         * @param vbucket the virtual bucket id
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*remove)(ENGINE_HANDLE* handle, const void* cookie,
                                    const void* key, const size_t nkey,
                                    uint64_t cas, uint16_t vbucket);

        /**
         * Indicate that a caller who received an item no longer needs
         * it.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param item the item to be released
         */
        void (*release)(ENGINE_HANDLE* handle, const void *cookie, item* item);

        /**
         * Retrieve an item.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param item output variable that will receive the located item
         * @param key the key to look up
         * @param nkey the length of the key
         * @param vbucket the virtual bucket id
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*get)(ENGINE_HANDLE* handle, const void* cookie,
                                 item** item,
                                 const void* key, const int nkey,
                                 uint16_t vbucket);

        /**
         * Store an item.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param item the item to store
         * @param cas the CAS value for conditional sets
         * @param operation the type of store operation to perform.
         * @param vbucket the virtual bucket id
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*store)(ENGINE_HANDLE* handle, const void *cookie,
                                   item* item, uint64_t *cas,
                                   ENGINE_STORE_OPERATION operation,
                                   uint16_t vbucket);

        /**
         * Perform an increment or decrement operation on an item.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param key the key to look up
         * @param nkey the length of the key
         * @param increment if true, increment the value, else decrement
         * @param create if true, create the item if it's missing
         * @param delta the amount to increment or decrement.
         * @param initial when creating, specifies the initial value
         * @param exptime when creating, specifies the expiration time
         * @param cas output CAS value
         * @param result output arithmetic value
         * @param vbucket the virtual bucket id
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*arithmetic)(ENGINE_HANDLE* handle, const void* cookie,
                                        const void* key, const int nkey,
                                        const bool increment, const bool create,
                                        const uint64_t delta, const uint64_t initial,
                                        const int flags, const rel_time_t exptime,
                                        uint64_t *cas, uint64_t *result,
                                        uint16_t vbucket);

        /**
         * Flush the cache.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @prefix prefix string
         * @nprefix prefix string length: -1(all prefixes), 0(null prefix)
         * @param when time at which the flush should take effect
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*flush)(ENGINE_HANDLE* handle, const void* cookie,
                                   const void* prefix, const int nprefix,
                                   rel_time_t when);

        /*
         * LIST Interface
         */
        ENGINE_ERROR_CODE (*list_struct_create)(ENGINE_HANDLE* handle, const void* cookie,
                                                const void* key, const int nkey,
                                                item_attr *attrp, uint16_t vbucket);

        ENGINE_ERROR_CODE (*list_elem_alloc)(ENGINE_HANDLE* handle, const void* cookie,
                                             const void* key, const int nkey,
                                             const size_t nbytes, eitem** eitem);

        void (*list_elem_free)(ENGINE_HANDLE* handle, const void* cookie, eitem *eitem);

        void (*list_elem_release)(ENGINE_HANDLE* handle, const void *cookie,
                                  eitem **eitem_array, const int eitem_count);

        ENGINE_ERROR_CODE (*list_elem_insert)(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* key, const int nkey,
                                              int index, eitem *eitem,
                                              item_attr *attrp, bool *created,
                                              uint16_t vbucket);

        ENGINE_ERROR_CODE (*list_elem_delete)(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* key, const int nkey,
                                              int from_index, int to_index,
                                              const bool drop_if_empty,
                                              uint32_t* del_count, bool* dropped,
                                              uint16_t vbucket);

        ENGINE_ERROR_CODE (*list_elem_get)(ENGINE_HANDLE* handle, const void* cookie,
                                           const void* key, const int nkey,
                                           int from_index, int to_index,
                                           const bool delete, const bool drop_if_empty,
                                           struct elems_result *eresult, uint16_t vbucket);

        /*
         * SET Interface
         */
        ENGINE_ERROR_CODE (*set_struct_create)(ENGINE_HANDLE* handle, const void* cookie,
                                               const void* key, const int nkey,
                                               item_attr *attrp, uint16_t vbucket);

        ENGINE_ERROR_CODE (*set_elem_alloc)(ENGINE_HANDLE* handle, const void* cookie,
                                            const void* key, const int nkey,
                                            const size_t nbytes, eitem** eitem);

        void (*set_elem_free)(ENGINE_HANDLE* handle, const void* cookie, eitem *eitem);

        void (*set_elem_release)(ENGINE_HANDLE* handle, const void *cookie,
                                 eitem **eitem_array, const int eitem_count);

        ENGINE_ERROR_CODE (*set_elem_insert)(ENGINE_HANDLE* handle, const void* cookie,
                                             const void* key, const int nkey, eitem *eitem,
                                             item_attr *attrp, bool *created,
                                             uint16_t vbucket);

        ENGINE_ERROR_CODE (*set_elem_delete)(ENGINE_HANDLE* handle, const void* cookie,
                                             const void* key, const int nkey,
                                             const void* value, const int nbytes,
                                             const bool drop_if_empty, bool *dropped,
                                             uint16_t vbucket);

        ENGINE_ERROR_CODE (*set_elem_exist)(ENGINE_HANDLE* handle, const void* cookie,
                                            const void* key, const int nkey,
                                            const void* value, const int nbytes,
                                            bool *exist, uint16_t vbucket);

        ENGINE_ERROR_CODE (*set_elem_get)(ENGINE_HANDLE* handle, const void* cookie,
                                          const void* key, const int nkey,
                                          const uint32_t count,
                                          const bool delete, const bool drop_if_empty,
                                          struct elems_result *eresult, uint16_t vbucket);

        /*
         * MAP Interface
         */
        ENGINE_ERROR_CODE (*map_struct_create)(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               const void* key,
                                               const int nkey,
                                               item_attr *attrp,
                                               uint16_t vbucket);
        ENGINE_ERROR_CODE (*map_elem_alloc)(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            const void* key,
                                            const int nkey,
                                            const size_t nfield,
                                            const size_t nbytes,
                                            eitem** eitem);

        void (*map_elem_free)(ENGINE_HANDLE* handle, const void* cookie, eitem *eitem);

        void (*map_elem_release)(ENGINE_HANDLE* handle,
                                 const void *cookie,
                                 eitem **eitem_array,
                                 const int eitem_count);
        ENGINE_ERROR_CODE (*map_elem_insert)(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const void* key,
                                             const int nkey,
                                             eitem *eitem,
                                             const bool replace_if_exist,
                                             item_attr *attrp,
                                             bool *replaced,
                                             bool *created,
                                             uint16_t vbucket);
        ENGINE_ERROR_CODE (*map_elem_update)(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const void* key,
                                             const int nkey,
                                             const field_t *field,
                                             const void* value,
                                             const int nbytes,
                                             uint16_t vbucket);
        ENGINE_ERROR_CODE (*map_elem_delete)(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const void* key,
                                             const int nkey,
                                             const int numfields,
                                             const field_t *flist,
                                             const bool drop_if_empty,
                                             uint32_t* del_count,
                                             bool *dropped,
                                             uint16_t vbucket);

        ENGINE_ERROR_CODE (*map_elem_get)(ENGINE_HANDLE* handle, const void* cookie,
                                          const void* key, const int nkey,
                                          const int numfields, const field_t *flist,
                                          const bool delete, const bool drop_if_empty,
                                          struct elems_result *eresult, uint16_t vbucket);

        /*
         * B+Tree Interface
         */
        ENGINE_ERROR_CODE (*btree_struct_create)(ENGINE_HANDLE* handle, const void* cookie,
                                                 const void* key, const int nkey,
                                                 item_attr *attrp, uint16_t vbucket);

        ENGINE_ERROR_CODE (*btree_elem_alloc)(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* key, const int nkey,
                                              const size_t nbkey, const size_t neflag,
                                              const size_t nbytes, eitem** eitem);

        void (*btree_elem_free)(ENGINE_HANDLE* handle, const void *cookie, eitem *eitem);

        void (*btree_elem_release)(ENGINE_HANDLE* handle, const void *cookie,
                                   eitem **eitem_array, const int eitem_count);

        ENGINE_ERROR_CODE (*btree_elem_insert)(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* key, const int nkey,
                                              eitem *eitem, const bool replace_if_exist,
                                              item_attr *attrp,
                                              bool *replaced, bool *created,
                                              eitem_result *trimmed,
                                              uint16_t vbucket);

        ENGINE_ERROR_CODE (*btree_elem_update)(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* key, const int nkey,
                                              const bkey_range *bkrange,
                                              const eflag_update *eupdate,
                                              const void* value, const int nbytes,
                                              uint16_t vbucket);

        ENGINE_ERROR_CODE (*btree_elem_delete)(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* key, const int nkey,
                                              const bkey_range *bkrange,
                                              const eflag_filter *efilter,
                                              const uint32_t req_count,
                                              const bool drop_if_empty,
                                              uint32_t* del_count, uint32_t* opcost,
                                              bool* dropped, uint16_t vbucket);

        ENGINE_ERROR_CODE (*btree_elem_arithmetic)(ENGINE_HANDLE* handle, const void* cookie,
                                                   const void* key, const int nkey,
                                                   const bkey_range *bkrange,
                                                   const bool increment, const bool create,
                                                   const uint64_t delta, const uint64_t initial,
                                                   const eflag_t *eflagp, uint64_t *result,
                                                   uint16_t vbucket);

        ENGINE_ERROR_CODE (*btree_elem_get)(ENGINE_HANDLE* handle, const void* cookie,
                                            const void* key, const int nkey,
                                            const bkey_range *bkrange, const eflag_filter *efilter,
                                            const uint32_t offset, const uint32_t req_count,
                                            const bool delete, const bool drop_if_empty,
                                            struct elems_result *eresult, uint16_t vbucket);

        ENGINE_ERROR_CODE (*btree_elem_count)(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* key, const int nkey,
                                              const bkey_range *bkrange,
                                              const eflag_filter *efilter,
                                              uint32_t* eitem_count, uint32_t* opcost,
                                              uint16_t vbucket);

        ENGINE_ERROR_CODE (*btree_posi_find)(ENGINE_HANDLE *handle, const void* cookie,
                                             const char *key, const size_t nkey,
                                             const bkey_range *bkrange,
                                             ENGINE_BTREE_ORDER order,
                                             int *position, uint16_t vbucket);

        ENGINE_ERROR_CODE (*btree_posi_find_with_get)(ENGINE_HANDLE *handle, const void* cookie,
                                             const char *key, const size_t nkey,
                                             const bkey_range *bkrange,
                                             ENGINE_BTREE_ORDER order, const uint32_t count,
                                             int *position, struct elems_result *eresult,
                                             uint16_t vbucket);

        ENGINE_ERROR_CODE (*btree_elem_get_by_posi)(ENGINE_HANDLE *handle, const void* cookie,
                                             const char *key, const size_t nkey,
                                             ENGINE_BTREE_ORDER order, int from_posi, int to_posi,
                                             struct elems_result *eresult, uint16_t vbucket);

#ifdef SUPPORT_BOP_SMGET
#ifdef JHPARK_OLD_SMGET_INTERFACE
        /* smget old interface */
        ENGINE_ERROR_CODE (*btree_elem_smget_old)(ENGINE_HANDLE* handle, const void* cookie,
                                              token_t *karray, const int kcount,
                                              const bkey_range *bkrange,
                                              const eflag_filter *efilter,
                                              const uint32_t offset, const uint32_t count,
                                              eitem** eitem_array,
                                              uint32_t* kfnd_array,
                                              uint32_t* flag_array,
                                              uint32_t* eitem_count,
                                              uint32_t* missed_key_array,
                                              uint32_t* missed_key_count,
                                              bool *trimmed, bool *duplicated,
                                              uint16_t vbucket);
#endif

        /* smget new interface */
        ENGINE_ERROR_CODE (*btree_elem_smget)(ENGINE_HANDLE* handle, const void* cookie,
                                              token_t *karray, const int kcount,
                                              const bkey_range *bkrange,
                                              const eflag_filter *efilter,
                                              const uint32_t offset, const uint32_t count,
                                              const bool unique,
                                              smget_result_t *result,
                                              uint16_t vbucket);
#endif
        /*
         * ATTR Interface
         */
        ENGINE_ERROR_CODE (*getattr)(ENGINE_HANDLE* handle, const void* cookie,
                                     const void* key, const int nkey,
                                     ENGINE_ITEM_ATTR *attr_ids,
                                     const uint32_t attr_count,
                                     item_attr *attr_data,
                                     uint16_t vbucket);

        ENGINE_ERROR_CODE (*setattr)(ENGINE_HANDLE* handle, const void* cookie,
                                     const void* key, const int nkey,
                                     ENGINE_ITEM_ATTR *attr_ids,
                                     const uint32_t attr_count,
                                     item_attr *attr_data,
                                     uint16_t vbucket);

        /*
         * Statistics
         */

        /**
         * Get statistics from the engine.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param stat_key optional argument to stats
         * @param nkey the length of the stat_key
         * @param add_stat callback to feed results to the output
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*get_stats)(ENGINE_HANDLE* handle, const void* cookie,
                                       const char* stat_key, int nkey,
                                       ADD_STAT add_stat);

        /**
         * Reset the stats.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         */
        void (*reset_stats)(ENGINE_HANDLE* handle, const void *cookie);

        /**
         * Get an array of per-thread stats. Set to NULL if you don't need it.
         */
        void *(*get_stats_struct)(ENGINE_HANDLE* handle, const void* cookie);

        /**
         * Aggregate stats among all per-connection stats. Set to NULL if you don't need it.
         */
        ENGINE_ERROR_CODE (*aggregate_stats)(ENGINE_HANDLE* handle, const void* cookie,
                                             void (*callback)(void*, void*),
                                             void*);

        /**
         * Statistical information for each prefix
         */
        char *(*prefix_dump_stats)(ENGINE_HANDLE* handle, const void* cookie,
            token_t *tokenes, const size_t ntokens, int *length);

        /**
         * Set engine config.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param config_key config key
         * @param config_value config value
         *
         * @return ENGINE_SUCCESS if all goes well.
         *         ENGINE_EBADVALUE if given value is not valid.
         *         ENGINE_ENOTSUP if given config is not supported.
         */
        ENGINE_ERROR_CODE (*set_config)(ENGINE_HANDLE* handle, const void* cookie,
                                        const char* config_key, void* config_value);

        /**
         * Get engine config.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param config_key config key
         * @param config_value config value returned
         *
         * @return ENGINE_SUCCESS if all goes well.
         *         ENGINE_ENOTSUP if given config is not supported.
         */
        ENGINE_ERROR_CODE (*get_config)(ENGINE_HANDLE* handle, const void* cookie,
                                        const char* config_key, void* config_value);

        /**
         * Dump cache items from LRU list of slab class.
         */
        char *(*cachedump)(ENGINE_HANDLE* handle, const void *cookie,
                          const unsigned int slabs_clsid,
                          const unsigned int limit,
                          const bool forward,
                          const bool sticky,
                          unsigned int *bytes);

        /**
         * Dump all cache items.
         */
        ENGINE_ERROR_CODE (*dump)(ENGINE_HANDLE* handle, const void *cookie,
                                  const char *opstr, const char *modestr,
                                  const char *prefix, const int nprefix,
                                  const char *filepath);

#ifdef SCAN_COMMAND
        /**
         * Scan a certain number of prefixes that meet the given conditions.
         *
         * @param cursor     scan start point
         * @param count      the number of prefixes to scan
         * @param pattern    prefix string glob pattern
         * @param item_array where to store prefix
         * @param item_arrsz the size of item_array
         * @param item_count the number of matched prefixes
         */
        ENGINE_ERROR_CODE (*prefixscan)(const char cursor[], const uint32_t count, const char *pattern,
                                        item **item_array, int item_arrsz, int *item_count);

        /**
         * Scan a certain number of items that meet the given conditions.
         *
         * @param cursor     scan start point
         * @param count      the number of items to scan
         * @param pattern    key string glob pattern
         * @param type       item's type
         * @param item_array where to store item
         * @param item_arrsz the size of item_array
         * @param item_count the number of matched items
         */
        ENGINE_ERROR_CODE (*keyscan)(const char cursor[], const uint32_t count, const char *pattern,
                                     ENGINE_ITEM_TYPE type, item **item_array, int item_arrsz, int *item_count);
#endif

        /**
         * Any unknown command will be considered engine specific.
         *
         * @param handle the engine handle
         * @param cookie The cookie provided by the frontend
         * @param request pointer to request header to be filled in
         * @param response function to transmit data
         *
         * @return ENGINE_SUCCESS if all goes well
         */
        ENGINE_ERROR_CODE (*unknown_command)(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             protocol_binary_request_header *request,
                                             ADD_RESPONSE response);

        /**
         * Scrub stale items.
         */
        ENGINE_ERROR_CODE (*scrub_stale)(ENGINE_HANDLE* handle);

#ifdef SCAN_COMMAND
        /**
         * Get information about a prefix.
         *
         * The loader of the module may need the pointers to the actual data within
         * a prefix. Instead of having to create multiple functions to get each
         * individual prefix, this function will get all of them.
         *
         * @param handle the engine that owns the object
         * @param cookie connection cookie for this prefix
         * @param item the prefix to request information about
         * @param prefix_info
         * @return true if successful
         */
        bool (*get_prefix_info)(ENGINE_HANDLE *handle, const void *cookie,
                                const item* item, prefix_info *prefix_info);
#endif
        /**
         * Get information about an item.
         *
         * The loader of the module may need the pointers to the actual data within
         * an item. Instead of having to create multiple functions to get each
         * individual item, this function will get all of them.
         *
         * @param handle the engine that owns the object
         * @param cookie connection cookie for this item
         * @param item the item to request information about
         * @param item_info
         * @return true if successful
         */
        bool (*get_item_info)(ENGINE_HANDLE *handle, const void *cookie,
                              const item* item, item_info *item_info);
        /*
         * Get information about a collection element.
         */
        void (*get_elem_info)(ENGINE_HANDLE *handle, const void *cookie,
                              const int type, /* collection type */
                              const eitem* eitem, eitem_info *elem_info);

        /**
         * Get extra error information for an operation.
         *
         * @param handle the engine handle
         * @param cookie The connection cookie
         * @param buffer Where to store the info
         * @param buffsz The size of the buffer
         * @return the number of bytes written to the buffer
         */
        size_t (*errinfo)(ENGINE_HANDLE *handle, const void* cookie,
                          char *buffer, size_t buffsz);

    } ENGINE_HANDLE_V1;

    /**
     * @}
     */

#ifdef __cplusplus
}
#endif

#endif
