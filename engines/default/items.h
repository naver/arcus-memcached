/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2020 JaM2in Co., Ltd.
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
#ifndef ITEMS_H
#define ITEMS_H

#include "item_base.h"
#include "coll_list.h"
#include "coll_set.h"
#include "coll_map.h"
#include "coll_btree.h"

/*
 * You should not try to aquire any of the item locks before calling these
 * functions.
 */

/**
 * Allocate and initialize a new item structure
 * @param key the key for the new item
 * @param nkey the number of bytes in the key
 * @param flags the flags in the new item
 * @param exptime when the object should expire
 * @param nbytes the number of bytes in the body for the item
 * @return a pointer to an item on success NULL otherwise
 */
hash_item *item_alloc(const void *key, const uint32_t nkey,
                      const uint32_t flags, rel_time_t exptime,
                      const uint32_t nbytes, const void *cookie);

/**
 * Frees and adds an item to the freelist
 * @param it the item to free
 */
void item_free(hash_item *it);

/**
 * Get an item from the cache
 *
 * @param key the key for the item to get
 * @param nkey the number of bytes in the key
 * @return pointer to the item if it exists or NULL otherwise
 */
hash_item *item_get(const void *key, const uint32_t nkey);

/**
 * Get item global statitistics
 * @param add_stat callback provided by the core used to
 *                 push statistics into the response
 * @param cookie cookie provided by the core to identify the client
 */
void item_stats_global(ADD_STAT add_stat, const void *cookie);

/**
 * Get item statitistics
 * @param add_stat callback provided by the core used to
 *                 push statistics into the response
 * @param cookie cookie provided by the core to identify the client
 */
void item_stats(ADD_STAT add_stat, const void *cookie);

/**
 * Get detaild item statitistics
 * @param add_stat callback provided by the core used to
 *                 push statistics into the response
 * @param cookie cookie provided by the core to identify the client
 */
void item_stats_sizes(ADD_STAT add_stat, const void *cookie);

/**
 * Reset the item statistics
 */
void item_stats_reset(void);

/**
 * Dump items from the cache
 * @param slabs_clsid the slab class to get items from
 * @param limit the maximum number of items to receive
 * @param bytes the number of bytes in the return message (OUT)
 * @return pointer to a string containint the data
 *
 * @todo we need to rewrite this to use callbacks!!!! currently disabled
 */
char *item_cachedump(const unsigned int slabs_clsid,
                     const unsigned int limit, const bool forward, const bool sticky,
                     unsigned int *bytes);

/**
 * Flush expired items from the cache
 * @prefix prefix string
 * @nprefix prefix string length: -1(all prefixes), 0(null prefix)
 * @param when when the items should be flushed
 */
ENGINE_ERROR_CODE item_flush_expired(const char *prefix, const int nprefix,
                                     rel_time_t when, const void *cookie);

/**
 * Release our reference to the current item
 * @param it the item to release
 */
void item_release(hash_item *it);

/**
 * Store an item in the cache
 * @param item the item to store
 * @param cas the cas value (OUT)
 * @param operation what kind of store operation is this (ADD/SET etc)
 * @return ENGINE_SUCCESS on success
 *
 * @todo should we refactor this into hash_item ** and remove the cas
 *       there so that we can get it from the item instead?
 */
ENGINE_ERROR_CODE item_store(hash_item *item,
                             uint64_t *cas, ENGINE_STORE_OPERATION operation,
                             const void *cookie);

ENGINE_ERROR_CODE item_arithmetic(const void *key, const uint32_t nkey,
                                  const bool increment,
                                  const bool create, const uint64_t delta, const uint64_t initial,
                                  const uint32_t flags, const rel_time_t exptime, uint64_t *cas,
                                  uint64_t *result, const void *cookie);

/**
 * Delete an item of the given key.
 * @param key the key to delete
 * @param nkey the number of bytes in the key
 * @param cas the cas value
 */
ENGINE_ERROR_CODE item_delete(const void *key, const uint32_t nkey,
                              uint64_t cas, const void *cookie);

ENGINE_ERROR_CODE item_init(struct default_engine *engine);

void              item_final(struct default_engine *engine);

ENGINE_ERROR_CODE item_getattr(const void *key, const uint32_t nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data);

ENGINE_ERROR_CODE item_setattr(const void *key, const uint32_t nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data, const void *cookie);

/* Get all elements from collection hash item
 */
int               coll_elem_result_init(elems_result_t *eresult, uint32_t size);
void              coll_elem_result_free(elems_result_t *eresult);
ENGINE_ERROR_CODE coll_elem_get_all(hash_item *it, elems_result_t *eresult, bool lock_hold);
void              coll_elem_result_release(elems_result_t *eresult, int type);

/*
 * Item config functions
 */
bool item_conf_get_evict_to_free(void);
void item_conf_set_evict_to_free(bool value);

/*
 * Apply functions by recovery.
 */
ENGINE_ERROR_CODE item_apply_kv_link(void *engine, const char *key, const uint32_t nkey,
                                     const uint32_t flags, const rel_time_t exptime,
                                     const uint32_t nbytes, const char *value,
                                     const uint64_t cas);
ENGINE_ERROR_CODE item_apply_unlink(void *engine, const char *key, const uint32_t nkey);
ENGINE_ERROR_CODE item_apply_setattr_exptime(void *engine, const char *key, const uint32_t nkey,
                                             rel_time_t exptime);
ENGINE_ERROR_CODE item_apply_setattr_collinfo(void *engine, hash_item *it,
                                              rel_time_t exptime, const int32_t maxcount,
                                              const uint8_t ovflact, const uint8_t mflags,
                                              bkey_t *maxbkeyrange);
ENGINE_ERROR_CODE item_apply_lru_update(void *engine, const char *key, const uint32_t nkey);
ENGINE_ERROR_CODE item_apply_flush(void *engine, const char *prefix, const int nprefix);


/**
 * Item Scan Facility
 */
#include "assoc.h"

/* item scan structure */
typedef struct _item_scan {
    struct assoc_scan asscan; /* assoc scan */
    const char *prefix;
    int        nprefix;
    bool       is_used;
    struct _item_scan *next;
} item_scan;

/* callback functions */
typedef void (*CB_SCAN_OPEN)(void *scanp);
typedef void (*CB_SCAN_CLOSE)(bool success);

/* item scan functions */
void item_scan_open(item_scan *sp, const char *prefix, const int nprefix, CB_SCAN_OPEN cb_scan_open);
int  item_scan_getnext(item_scan *sp, void **item_array, elems_result_t *erst_array, int item_arrsz);
void item_scan_release(item_scan *sp, void **item_array, elems_result_t *erst_array, int item_count);
void item_scan_close(item_scan *sp, CB_SCAN_CLOSE cb_scan_close, bool success);

/**
 * Item scrubber
 */
bool item_start_scrub(struct default_engine *engine, int mode, bool autorun);
bool item_onoff_scrub(struct default_engine *engine, bool val);
void item_stats_scrub(struct default_engine *engine,
                      ADD_STAT add_stat, const void *cookie);

/**
 * Item dumpper
 */
enum dump_mode {
    DUMP_MODE_KEY = 0,  /* key string only */
    DUMP_MODE_ITEM,     /* key string & item value */
    DUMP_MODE_RCOUNT,   /* key string & rcount */
    DUMP_MODE_SNAPSHOT, /* item snapshot */
    DUMP_MODE_MAX
};

ENGINE_ERROR_CODE item_dump_start(struct default_engine *engine,
                                  const char *modestr,
                                  const char *prefix, const int nprefix,
                                  const char *filepath);
void item_dump_stop(struct default_engine *engine);
void item_dump_stats(struct default_engine *engine,
                     ADD_STAT add_stat, const void *cookie);

#endif
