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
#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <stddef.h>
#include <inttypes.h>

#include "default_engine.h"
#include "memcached/util.h"
#include "memcached/config_parser.h"

#define CMD_SET_VBUCKET 0x83
#define CMD_GET_VBUCKET 0x84
#define CMD_DEL_VBUCKET 0x85

static const engine_info* default_get_info(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE  default_initialize(ENGINE_HANDLE* handle, const char* config_str);
static void               default_destroy(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE  default_item_allocate(ENGINE_HANDLE* handle, const void* cookie,
                                                item **item,
                                                const void* key, const size_t nkey,
                                                const size_t nbytes,
                                                const int flags, const rel_time_t exptime);
static ENGINE_ERROR_CODE  default_item_delete(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* key, const size_t nkey,
                                              uint64_t cas, uint16_t vbucket);
static void               default_item_release(ENGINE_HANDLE* handle, const void *cookie,
                                               item* item);
static ENGINE_ERROR_CODE  default_get(ENGINE_HANDLE* handle, const void* cookie,
                                      item** item, const void* key, const int nkey,
                                      uint16_t vbucket);
static ENGINE_ERROR_CODE  default_get_stats(ENGINE_HANDLE* handle, const void *cookie,
                                            const char *stat_key, int nkey, ADD_STAT add_stat);
static void               default_reset_stats(ENGINE_HANDLE* handle, const void *cookie);
static ENGINE_ERROR_CODE  default_get_prefix_stats(ENGINE_HANDLE* handle, const void* cookie,
                                                   const void* key, const int nkey, void *prefix_data);
static ENGINE_ERROR_CODE  default_store(ENGINE_HANDLE* handle, const void *cookie,
                                        item* item, uint64_t *cas, ENGINE_STORE_OPERATION operation,
                                        uint16_t vbucket);
static ENGINE_ERROR_CODE  default_arithmetic(ENGINE_HANDLE* handle, const void* cookie,
                                             const void* key, const int nkey,
                                             const bool increment,
                                             const bool create, const uint64_t delta, const uint64_t initial,
                                             const int flags, const rel_time_t exptime,
                                             uint64_t *cas, uint64_t *result, uint16_t vbucket);
static ENGINE_ERROR_CODE  default_flush(ENGINE_HANDLE* handle, const void* cookie, time_t when);
static ENGINE_ERROR_CODE  default_flush_prefix(ENGINE_HANDLE* handle, const void* cookie,
                                               const void* prefix, const int nprefix, time_t when);
static ENGINE_ERROR_CODE  default_list_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                                                     const void* key, const int nkey, item_attr *attrp,
                                                     uint16_t vbucket);
static ENGINE_ERROR_CODE  default_list_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                                                  eitem** eitem, const size_t nbytes);
static void               default_list_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                                                    eitem **eitem_array, const int eitem_count);
static ENGINE_ERROR_CODE  default_list_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                                                   const void* key, const int nkey,
                                                   int index, eitem *eitem,
                                                   item_attr *attrp, bool *created,
                                                   uint16_t vbucket);
static ENGINE_ERROR_CODE  default_list_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                                                   const void* key, const int nkey,
                                                   int from_index, int to_index,
                                                   const bool drop_if_empty,
                                                   uint32_t* del_count, bool* dropped,
                                                   uint16_t vbucket);
static ENGINE_ERROR_CODE  default_list_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                                                const void* key, const int nkey,
                                                int from_index, int to_index,
                                                const bool delete, const bool drop_if_empty,
                                                eitem** eitem_array, uint32_t* eitem_count,
                                                uint32_t* flags, bool *dropped,
                                                uint16_t vbucket);
static ENGINE_ERROR_CODE  default_set_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                                                    const void* key, const int nkey, item_attr *attrp,
                                                    uint16_t vbucket);
static ENGINE_ERROR_CODE  default_set_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                                                 eitem** eitem, const size_t nbytes);
static void               default_set_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                                                   eitem **eitem_array, const int eitem_count);
static ENGINE_ERROR_CODE  default_set_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                                                  const void* key, const int nkey, eitem *eitem,
                                                  item_attr *attrp, bool *created, uint16_t vbucket);
static ENGINE_ERROR_CODE  default_set_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                                                  const void* key, const int nkey,
                                                  const void* value, const int nbytes,
                                                  const bool drop_if_empty, bool *dropped,
                                                  uint16_t vbucket);
static ENGINE_ERROR_CODE  default_set_elem_exist(ENGINE_HANDLE* handle, const void* cookie,
                                                 const void* key, const int nkey,
                                                 const void* value, const int nbytes,
                                                 bool *exist, uint16_t vbucket);
static ENGINE_ERROR_CODE  default_set_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                                               const void* key, const int nkey, const uint32_t count,
                                               const bool delete, const bool drop_if_empty,
                                               eitem** eitem, uint32_t* eitem_count,
                                               uint32_t* flags, bool* dropped, uint16_t vbucket);
static ENGINE_ERROR_CODE  default_btree_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                                                      const void* key, const int nkey, item_attr *attrp,
                                                      uint16_t vbucket);
static ENGINE_ERROR_CODE  default_btree_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                                                   eitem** eitem,
                                                   const size_t nbkey, const size_t neflag, const size_t nbytes);
static void               default_btree_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                                                     eitem **eitem_array, const int eitem_count);
static ENGINE_ERROR_CODE  default_btree_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                                                    const void* key, const int nkey,
                                                    eitem *eitem, const bool replace_if_exist, item_attr *attrp,
                                                    bool *replaced, bool *created, eitem_result *trimmed,
                                                    uint16_t vbucket);
static ENGINE_ERROR_CODE  default_btree_elem_update(ENGINE_HANDLE* handle, const void* cookie,
                                                    const void* key, const int nkey,
                                                    const bkey_range *bkrange,
                                                    const eflag_update *eupdate,
                                                    const void* value, const int nbytes,
                                                    uint16_t vbucket);
static ENGINE_ERROR_CODE  default_btree_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                                                    const void* key, const int nkey,
                                                    const bkey_range *bkrange,
                                                    const eflag_filter *efilter,
                                                    const uint32_t req_count,
                                                    const bool drop_if_empty,
                                                    uint32_t* del_count, bool* dropped,
                                                    uint16_t vbucket);
static ENGINE_ERROR_CODE  default_btree_elem_arithmetic(ENGINE_HANDLE* handle, const void* cookie,
                                                        const void* key, const int nkey,
                                                        const bkey_range *bkrange,
                                                        const bool increment, const bool create,
                                                        const uint64_t delta, const uint64_t initial,
                                                        const eflag_t *eflagp,
                                                        uint64_t *result, uint16_t vbucket);
static ENGINE_ERROR_CODE  default_btree_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                                                 const void* key, const int nkey,
                                                 const bkey_range *bkrange,
                                                 const eflag_filter *efilter,
                                                 const uint32_t offset, const uint32_t req_count,
                                                 const bool delete, const bool drop_if_empty,
                                                 eitem** eitem_array, uint32_t* eitem_count,
                                                 uint32_t* flags, bool *dropped_trimmed,
                                                 uint16_t vbucket);
static ENGINE_ERROR_CODE  default_btree_elem_count(ENGINE_HANDLE* handle, const void* cookie,
                                                   const void* key, const int nkey,
                                                   const bkey_range *bkrange,
                                                   const eflag_filter *efilter,
                                                   uint32_t* eitem_count, uint32_t* flags,
                                                   uint16_t vbucket);
static ENGINE_ERROR_CODE default_btree_posi_find(ENGINE_HANDLE* handle, const void* cookie,
                                                 const char *key, const size_t nkey, const bkey_range *bkrange,
                                                 ENGINE_BTREE_ORDER order, int *position, uint16_t vbucket);
static ENGINE_ERROR_CODE default_btree_posi_find_with_get(ENGINE_HANDLE* handle, const void* cookie,
                                                 const char *key, const size_t nkey, const bkey_range *bkrange,
                                                 ENGINE_BTREE_ORDER order, const uint32_t count, int *position,
                                                 eitem **eitem_array, uint32_t *eitem_count, uint32_t *eitem_index,
                                                 uint32_t *flags, uint16_t vbucket);
static ENGINE_ERROR_CODE default_btree_elem_get_by_posi(ENGINE_HANDLE* handle, const void* cookie,
                                                 const char *key, const size_t nkey,
                                                 ENGINE_BTREE_ORDER order, int from_posi, int to_posi,
                                                 eitem **eitem_array, uint32_t *eitem_count, uint32_t *flags,
                                                 uint16_t vbucket);
#ifdef SUPPORT_BOP_SMGET
static ENGINE_ERROR_CODE  default_btree_elem_smget(ENGINE_HANDLE* handle, const void* cookie,
                                                   token_t *karray, const int kcount,
                                                   const bkey_range *bkrange,
                                                   const eflag_filter *efilter,
                                                   const uint32_t offset, const uint32_t count,
                                                   eitem** eitem_array,
                                                   uint32_t* kfnd_array, uint32_t* flag_array,
                                                   uint32_t* eitem_count,
                                                   uint32_t* missed_key_array,
                                                   uint32_t* missed_key_count,
                                                   bool *trimmed, bool *duplicated,
                                                   uint16_t vbucket);
#endif
static ENGINE_ERROR_CODE  default_getattr(ENGINE_HANDLE* handle, const void* cookie,
                                          const void* key, const int nkey,
                                          ENGINE_ITEM_ATTR *attr_ids,
                                          const uint32_t attr_count, item_attr *attr_data,
                                          uint16_t vbucket);
static ENGINE_ERROR_CODE  default_setattr(ENGINE_HANDLE* handle, const void* cookie,
                                          const void* key, const int nkey,
                                          ENGINE_ITEM_ATTR *attr_ids,
                                          const uint32_t attr_count, item_attr *attr_data,
                                          uint16_t vbucket);
static ENGINE_ERROR_CODE  initalize_configuration(struct default_engine *se, const char *cfg_str);
#if 0 // ENABLE_TAP_PROTOCOL
static TAP_ITERATOR       get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                           const void* client, size_t nclient, uint32_t flags,
                                           const void* userdata, size_t nuserdata);
#endif
static ENGINE_ERROR_CODE  default_set_memlimit(ENGINE_HANDLE* handle, const void* cookie,
                                               const size_t memlimit, const int sticky_ratio);
static void               default_set_junktime(ENGINE_HANDLE* handle, const void* cookie,
                                               const size_t junktime);
static void               default_set_verbose(ENGINE_HANDLE* handle, const void* cookie,
                                              const size_t verbose);
static char*              default_cachedump(ENGINE_HANDLE* handle, const void* cookie,
                                            const unsigned int slabs_clsid, const unsigned int limit,
                                            const bool forward, const bool sticky, unsigned int *bytes);
static ENGINE_ERROR_CODE  default_unknown_command(ENGINE_HANDLE* handle, const void* cookie,
                                                  protocol_binary_request_header *request,
                                                  ADD_RESPONSE response);

union vbucket_info_adapter {
    char c;
    struct vbucket_info v;
};

static void set_vbucket_state(struct default_engine *e, uint16_t vbid, enum vbucket_state to)
{
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid];
    vi.v.state = to;
    e->vbucket_infos[vbid] = vi.c;
}

static enum vbucket_state get_vbucket_state(struct default_engine *e, uint16_t vbid)
{
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid];
    return vi.v.state;
}

static bool handled_vbucket(struct default_engine *e, uint16_t vbid)
{
    return e->config.ignore_vbucket
        || (get_vbucket_state(e, vbid) == VBUCKET_STATE_ACTIVE);
}

/* mechanism for handling bad vbucket requests */
#define VBUCKET_GUARD(e, v) if (!handled_vbucket(e, v)) { return ENGINE_NOT_MY_VBUCKET; }

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info);
static void get_list_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                               const eitem* eitem, eitem_info *elem_info);
static void get_set_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                              const eitem* eitem, eitem_info *elem_info);
static void get_btree_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                                const eitem* eitem, eitem_info *elem_info);

static const char * vbucket_state_name(enum vbucket_state s)
{
    static const char * vbucket_states[] = {
        "dead", "active", "replica", "pending"
    };
    return vbucket_states[s];
}

ENGINE_ERROR_CODE create_instance(uint64_t interface, GET_SERVER_API get_server_api, ENGINE_HANDLE **handle)
{
    SERVER_HANDLE_V1 *api = get_server_api();
    if (interface != 1 || api == NULL) {
        return ENGINE_ENOTSUP;
    }

    struct default_engine *engine = malloc(sizeof(*engine));
    if (engine == NULL) {
        return ENGINE_ENOMEM;
    }

    struct default_engine default_engine = {
      .engine = {
         .interface = {
            .interface = 1
         },
         .get_info = default_get_info,
         .initialize = default_initialize,
         .destroy = default_destroy,
         .allocate = default_item_allocate,
         .remove = default_item_delete,
         .release = default_item_release,
         .get = default_get,
         .get_stats = default_get_stats,
         .reset_stats = default_reset_stats,
         .get_prefix_stats = default_get_prefix_stats,
         .store = default_store,
         .arithmetic = default_arithmetic,
         .flush = default_flush,
         .flush_prefix = default_flush_prefix,
         /* LIST functions */
         .list_struct_create = default_list_struct_create,
         .list_elem_alloc   = default_list_elem_alloc,
         .list_elem_release = default_list_elem_release,
         .list_elem_insert  = default_list_elem_insert,
         .list_elem_delete  = default_list_elem_delete,
         .list_elem_get     = default_list_elem_get,
         /* SET functions */
         .set_struct_create = default_set_struct_create,
         .set_elem_alloc    = default_set_elem_alloc,
         .set_elem_release  = default_set_elem_release,
         .set_elem_insert   = default_set_elem_insert,
         .set_elem_delete   = default_set_elem_delete,
         .set_elem_exist    = default_set_elem_exist,
         .set_elem_get      = default_set_elem_get,
         /* B+Tree functions */
         .btree_struct_create = default_btree_struct_create,
         .btree_elem_alloc   = default_btree_elem_alloc,
         .btree_elem_release = default_btree_elem_release,
         .btree_elem_insert  = default_btree_elem_insert,
         .btree_elem_update  = default_btree_elem_update,
         .btree_elem_delete  = default_btree_elem_delete,
         .btree_elem_arithmetic  = default_btree_elem_arithmetic,
         .btree_elem_get     = default_btree_elem_get,
         .btree_elem_count   = default_btree_elem_count,
         .btree_posi_find    = default_btree_posi_find,
         .btree_posi_find_with_get = default_btree_posi_find_with_get,
         .btree_elem_get_by_posi = default_btree_elem_get_by_posi,
#ifdef SUPPORT_BOP_SMGET
         .btree_elem_smget   = default_btree_elem_smget,
#endif
         /* Attribute functions */
         .getattr           = default_getattr,
         .setattr           = default_setattr,

         .set_memlimit    = default_set_memlimit,
         .set_junktime    = default_set_junktime,
         .set_verbose     = default_set_verbose,
         .cachedump       = default_cachedump,
         .unknown_command = default_unknown_command,
         .item_set_cas = item_set_cas,
         .get_item_info = get_item_info,
         .get_list_elem_info = get_list_elem_info,
         .get_set_elem_info = get_set_elem_info,
         .get_btree_elem_info = get_btree_elem_info,
#if 0 // ENABLE_TAP_PROTOCOL
         .get_tap_iterator = get_tap_iterator
#endif
      },
      .server = *api,
      .get_server_api = get_server_api,
      .initialized = true,
      .assoc = {
         .hashpower = 16,
         .tot_prefix_items = 0,
      },
      .slabs = {
         .lock = PTHREAD_MUTEX_INITIALIZER
      },
      .cache_lock = PTHREAD_MUTEX_INITIALIZER,
      .stats = {
         .lock = PTHREAD_MUTEX_INITIALIZER,
      },
      .config = {
         .use_cas = true,
         .verbose = 0,
         .oldest_live = 0,
         .evict_to_free = true,
         .num_threads = 0,
         .maxbytes = 64 * 1024 * 1024,
         .sticky_limit = 0,
         .junk_item_time = 0,
         .preallocate = false,
         .factor = 1.25,
         .chunk_size = 48,
         .item_size_max= 1024 * 1024,
         .max_list_size = 50000,
         .max_set_size = 50000,
         .max_btree_size = 50000,
         .prefix_delimiter = ':',
       },
      .scrubber = {
         .lock = PTHREAD_MUTEX_INITIALIZER,
      },
      .info.engine_info = {
           .description = "Default engine v0.1",
           .num_features = 1,
           .features = {
               [0].feature = ENGINE_FEATURE_LRU
           }
       }
    };

    *engine = default_engine;

    *handle = (ENGINE_HANDLE*)&engine->engine;
    return ENGINE_SUCCESS;
}

static inline struct default_engine* get_handle(ENGINE_HANDLE* handle)
{
    return (struct default_engine*)handle;
}

static inline hash_item* get_real_item(item* item)
{
    return (hash_item*)item;
}

static const engine_info* default_get_info(ENGINE_HANDLE* handle)
{
    return &get_handle(handle)->info.engine_info;
}

static ENGINE_ERROR_CODE default_initialize(ENGINE_HANDLE* handle, const char* config_str)
{
    struct default_engine* se = get_handle(handle);

    ENGINE_ERROR_CODE ret = initalize_configuration(se, config_str);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    /* fixup feature_info */
    if (se->config.use_cas) {
        se->info.engine_info.features[se->info.engine_info.num_features++].feature = ENGINE_FEATURE_CAS;
    }

    ret = assoc_init(se);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    ret = slabs_init(se, se->config.maxbytes, se->config.factor, se->config.preallocate);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    ret = item_init(se);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    return ENGINE_SUCCESS;
}

static void default_destroy(ENGINE_HANDLE* handle)
{
    struct default_engine* se = get_handle(handle);

    if (se->initialized) {
        pthread_mutex_destroy(&se->cache_lock);
        pthread_mutex_destroy(&se->stats.lock);
        pthread_mutex_destroy(&se->slabs.lock);
        se->initialized = false;
        free(se);
    }
}

static ENGINE_ERROR_CODE default_item_allocate(ENGINE_HANDLE* handle, const void* cookie,
                                               item **item,
                                               const void* key, const size_t nkey,
                                               const size_t nbytes,
                                               const int flags, const rel_time_t exptime)
{
    struct default_engine* engine = get_handle(handle);
    size_t ntotal = sizeof(hash_item) + nkey + nbytes;
    if (engine->config.use_cas) {
        ntotal += sizeof(uint64_t);
    }
    unsigned int id = slabs_clsid(engine, ntotal);
    if (id == 0) {
        return ENGINE_E2BIG;
    }

    hash_item *it;
    it = item_alloc(engine, key, nkey, flags, exptime, nbytes, cookie);
    if (it != NULL) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ENOMEM;
    }
}

static ENGINE_ERROR_CODE default_item_delete(ENGINE_HANDLE* handle, const void* cookie,
                                             const void* key, const size_t nkey,
                                             uint64_t cas, uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    hash_item *it = item_get(engine, key, nkey);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (cas == 0 || cas == item_get_cas(it)) {
        item_unlink(engine, it);
        item_release(engine, it);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_KEY_EEXISTS;
    }
}

static void default_item_release(ENGINE_HANDLE* handle, const void *cookie, item* item)
{
    item_release(get_handle(handle), get_real_item(item));
}

static ENGINE_ERROR_CODE default_get(ENGINE_HANDLE* handle, const void* cookie,
                                     item** item, const void* key, const int nkey,
                                     uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    *item = item_get(engine, key, nkey);
    if (*item != NULL) {
        hash_item *it = get_real_item(*item);
        if ((it->iflag & ITEM_IFLAG_COLL) != 0) { /* collection item */
            item_release(engine, it);
            *item = NULL;
            return ENGINE_EBADTYPE;
        }
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

static void stats_vbucket(struct default_engine *e, ADD_STAT add_stat, const void *cookie)
{
    for (int i = 0; i < NUM_VBUCKETS; i++) {
        enum vbucket_state state = get_vbucket_state(e, i);
        if (state != VBUCKET_STATE_DEAD) {
            char buf[16];
            snprintf(buf, sizeof(buf), "vb_%d", i);
            const char * state_name = vbucket_state_name(state);
            add_stat(buf, strlen(buf), state_name, strlen(state_name), cookie);
        }
    }
}

static ENGINE_ERROR_CODE default_get_stats(ENGINE_HANDLE* handle, const void* cookie,
                                           const char* stat_key, int nkey, ADD_STAT add_stat)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (stat_key == NULL) {
        char val[128];
        int len;

        pthread_mutex_lock(&engine->stats.lock);
        len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.evictions);
        add_stat("evictions", 9, val, len, cookie);
        len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.sticky_items);
        add_stat("sticky_items", 12, val, len, cookie);
        len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.curr_items);
        add_stat("curr_items", 10, val, len, cookie);
        len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.total_items);
        add_stat("total_items", 11, val, len, cookie);
        len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.sticky_bytes);
        add_stat("sticky_bytes", 12, val, len, cookie);
        len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.curr_bytes);
        add_stat("bytes", 5, val, len, cookie);
        len = sprintf(val, "%"PRIu64, engine->stats.reclaimed);
        add_stat("reclaimed", 9, val, len, cookie);
        len = sprintf(val, "%"PRIu64, (uint64_t)engine->config.sticky_limit);
        add_stat("sticky_limit", 12, val, len, cookie);
        len = sprintf(val, "%"PRIu64, (uint64_t)engine->config.maxbytes);
        add_stat("engine_maxbytes", 15, val, len, cookie);
        pthread_mutex_unlock(&engine->stats.lock);
    } else if (strncmp(stat_key, "slabs", 5) == 0) {
        slabs_stats(engine, add_stat, cookie);
    } else if (strncmp(stat_key, "items", 5) == 0) {
        item_stats(engine, add_stat, cookie);
    } else if (strncmp(stat_key, "sizes", 5) == 0) {
        item_stats_sizes(engine, add_stat, cookie);
    } else if (strncmp(stat_key, "vbucket", 7) == 0) {
        stats_vbucket(engine, add_stat, cookie);
    } else if (strncmp(stat_key, "scrub", 5) == 0) {
        char val[128];
        int len;

        pthread_mutex_lock(&engine->scrubber.lock);
        if (engine->scrubber.running) {
            add_stat("scrubber:status", 15, "running", 7, cookie);
        } else {
            add_stat("scrubber:status", 15, "stopped", 7, cookie);
        }
        if (engine->scrubber.started != 0) {
            if (engine->scrubber.runmode == SCRUB_MODE_NORMAL) {
                add_stat("scrubber:run_mode", 17, "scrub", 5, cookie);
            } else {
                add_stat("scrubber:run_mode", 17, "scrub stale", 11, cookie);
            }
            if (engine->scrubber.stopped != 0) {
                time_t diff = engine->scrubber.stopped - engine->scrubber.started;
                len = sprintf(val, "%"PRIu64, (uint64_t)diff);
                add_stat("scrubber:last_run", 17, val, len, cookie);
            }
            len = sprintf(val, "%"PRIu64, engine->scrubber.visited);
            add_stat("scrubber:visited", 16, val, len, cookie);
            len = sprintf(val, "%"PRIu64, engine->scrubber.cleaned);
            add_stat("scrubber:cleaned", 16, val, len, cookie);
        }
        pthread_mutex_unlock(&engine->scrubber.lock);
    } else {
        ret = ENGINE_KEY_ENOENT;
    }
    return ret;
}

static ENGINE_ERROR_CODE default_store(ENGINE_HANDLE* handle, const void *cookie,
                                       item* item, uint64_t *cas, ENGINE_STORE_OPERATION operation,
                                       uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);
    return store_item(engine, get_real_item(item), cas, operation, cookie);
}

static ENGINE_ERROR_CODE default_arithmetic(ENGINE_HANDLE* handle, const void* cookie,
                                            const void* key, const int nkey, const bool increment,
                                            const bool create, const uint64_t delta, const uint64_t initial,
                                            const int flags, const rel_time_t exptime,
                                            uint64_t *cas, uint64_t *result, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return arithmetic(engine, cookie, key, nkey, increment,
                      create, delta, initial, flags, engine->server.core->realtime(exptime),
                      cas, result);
}

static ENGINE_ERROR_CODE default_flush(ENGINE_HANDLE* handle, const void* cookie, time_t when)
{
    item_flush_expired(get_handle(handle), when, cookie);
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE default_flush_prefix(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* prefix, const int nprefix, time_t when)
{
    return item_flush_prefix_expired(get_handle(handle), prefix, nprefix, when, cookie);
}

static ENGINE_ERROR_CODE default_list_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                                                    const void* key, const int nkey, item_attr *attrp,
                                                    uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return list_struct_create(engine, key, nkey, attrp, cookie);
}

static ENGINE_ERROR_CODE default_list_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                                                 eitem** eitem, const size_t nbytes)
{
    list_elem_item *elem = list_elem_alloc(get_handle(handle), nbytes, cookie);
    if (elem != NULL) {
        *eitem = elem;
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ENOMEM;
    }
}

static void default_list_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                                      eitem **eitem_array, const int eitem_count)
{
    list_elem_release(get_handle(handle), (list_elem_item**)eitem_array, eitem_count);
}

static ENGINE_ERROR_CODE default_list_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                                                  const void* key, const int nkey,
                                                  int index, eitem *eitem,
                                                  item_attr *attrp, bool *created, uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return list_elem_insert(engine, key, nkey, index, (list_elem_item *)eitem,
                            attrp, created, cookie);
}

static ENGINE_ERROR_CODE default_list_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                                                  const void* key, const int nkey,
                                                  int from_index, int to_index, const bool drop_if_empty,
                                                  uint32_t* del_count, bool* dropped, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return list_elem_delete(engine, key, nkey, from_index, to_index, drop_if_empty,
                            del_count, dropped);
}

static ENGINE_ERROR_CODE default_list_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                                               const void* key, const int nkey,
                                               int from_index, int to_index,
                                               const bool delete, const bool drop_if_empty,
                                               eitem** eitem_array, uint32_t* eitem_count,
                                               uint32_t* flags, bool* dropped, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return list_elem_get(engine, key, nkey, from_index, to_index, delete, drop_if_empty,
                         (list_elem_item**)eitem_array, eitem_count, flags, dropped);
}

static ENGINE_ERROR_CODE default_set_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                                                   const void* key, const int nkey, item_attr *attrp,
                                                   uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return set_struct_create(engine, key, nkey, attrp, cookie);
}

static ENGINE_ERROR_CODE default_set_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                                                eitem** eitem, const size_t nbytes)
{
    set_elem_item *elem = set_elem_alloc(get_handle(handle), nbytes, cookie);
    if (elem != NULL) {
        *eitem = elem;
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ENOMEM;
    }
}

static void default_set_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                                     eitem **eitem_array, const int eitem_count)
{
    set_elem_release(get_handle(handle), (set_elem_item**)eitem_array, eitem_count);
}

static ENGINE_ERROR_CODE default_set_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                                                 const void* key, const int nkey, eitem *eitem,
                                                 item_attr *attrp, bool *created, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return set_elem_insert(engine, key, nkey, (set_elem_item*)eitem, attrp, created, cookie);
}

static ENGINE_ERROR_CODE default_set_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                                                 const void* key, const int nkey,
                                                 const void* value, const int nbytes,
                                                 const bool drop_if_empty, bool *dropped,
                                                 uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return set_elem_delete(engine, key, nkey, value, nbytes, drop_if_empty, dropped);
}

static ENGINE_ERROR_CODE default_set_elem_exist(ENGINE_HANDLE* handle, const void* cookie,
                                                const void* key, const int nkey,
                                                const void* value, const int nbytes,
                                                bool *exist, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return set_elem_exist(engine, key, nkey, value, nbytes, exist);
}

static ENGINE_ERROR_CODE default_set_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* key, const int nkey, const uint32_t count,
                                              const bool delete, const bool drop_if_empty,
                                              eitem** eitem, uint32_t* eitem_count,
                                              uint32_t* flags, bool* dropped, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return set_elem_get(engine, key, nkey, count, delete, drop_if_empty,
                        (set_elem_item**)eitem, eitem_count, flags, dropped);
}

static ENGINE_ERROR_CODE default_btree_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                                                     const void* key, const int nkey, item_attr *attrp,
                                                     uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return btree_struct_create(engine, key, nkey, attrp, cookie);
}

static ENGINE_ERROR_CODE default_btree_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                                                  eitem** eitem,
                                                  const size_t nbkey, const size_t neflag, const size_t nbytes)
{
    btree_elem_item *elem = btree_elem_alloc(get_handle(handle), nbkey, neflag, nbytes, cookie);
    if (elem != NULL) {
        *eitem = elem;
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ENOMEM;
    }
}

static void default_btree_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                                       eitem **eitem_array, const int eitem_count)
{
    btree_elem_release(get_handle(handle), (btree_elem_item**)eitem_array, eitem_count);
}

static ENGINE_ERROR_CODE default_btree_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                                                   const void* key, const int nkey,
                                                   eitem *eitem, const bool replace_if_exist, item_attr *attrp,
                                                   bool *replaced, bool *created, eitem_result *trimmed,
                                                   uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);
    ENGINE_ERROR_CODE ret;

    if (trimmed == NULL) {
        ret = btree_elem_insert(engine, key, nkey, (btree_elem_item *)eitem, replace_if_exist, attrp,
                                replaced, created, NULL, NULL, NULL, cookie);
    } else {
        btree_elem_item *trimmed_elem = NULL;
        ret = btree_elem_insert(engine, key, nkey, (btree_elem_item *)eitem, replace_if_exist, attrp,
                                replaced, created, &trimmed_elem,
                                &trimmed->count, &trimmed->flags, cookie);
        trimmed->elems = trimmed_elem;
    }
    return ret;
}

static ENGINE_ERROR_CODE default_btree_elem_update(ENGINE_HANDLE* handle, const void* cookie,
                                                   const void* key, const int nkey,
                                                   const bkey_range *bkrange,
                                                   const eflag_update *eupdate,
                                                   const void* value, const int nbytes,
                                                   uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return btree_elem_update(engine, key, nkey, bkrange, eupdate, value, nbytes, cookie);
}

static ENGINE_ERROR_CODE default_btree_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                                                   const void* key, const int nkey,
                                                   const bkey_range *bkrange,
                                                   const eflag_filter *efilter,
                                                   const uint32_t req_count,
                                                   const bool drop_if_empty,
                                                   uint32_t* del_count, bool* dropped,
                                                   uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return btree_elem_delete(engine, key, nkey, bkrange, efilter, req_count, drop_if_empty,
                             del_count, dropped);
}

static ENGINE_ERROR_CODE default_btree_elem_arithmetic(ENGINE_HANDLE* handle, const void* cookie,
                                                       const void* key, const int nkey,
                                                       const bkey_range *bkrange,
                                                       const bool increment, const bool create,
                                                       const uint64_t delta, const uint64_t initial,
                                                       const eflag_t *eflagp,
                                                       uint64_t *result, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return btree_elem_arithmetic(engine, key, nkey, bkrange, increment, create,
                                 delta, initial, eflagp, result, cookie);
}

static ENGINE_ERROR_CODE default_btree_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                                                const void* key, const int nkey,
                                                const bkey_range *bkrange,
                                                const eflag_filter *efilter,
                                                const uint32_t offset,
                                                const uint32_t req_count,
                                                const bool delete, const bool drop_if_empty,
                                                eitem** eitem_array, uint32_t* eitem_count,
                                                uint32_t* flags, bool* dropped_trimmed,
                                                uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return btree_elem_get(engine, key, nkey, bkrange, efilter, offset, req_count,
                          delete, drop_if_empty, (btree_elem_item**)eitem_array, eitem_count,
                          flags, dropped_trimmed);
}

static ENGINE_ERROR_CODE default_btree_elem_count(ENGINE_HANDLE* handle, const void* cookie,
                                                  const void* key, const int nkey,
                                                  const bkey_range *bkrange,
                                                  const eflag_filter *efilter,
                                                  uint32_t* eitem_count, uint32_t* flags,
                                                  uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return btree_elem_count(engine, key, nkey, bkrange, efilter, eitem_count, flags);
}

static ENGINE_ERROR_CODE default_btree_posi_find(ENGINE_HANDLE* handle, const void* cookie,
                                                 const char *key, const size_t nkey, const bkey_range *bkrange,
                                                 ENGINE_BTREE_ORDER order, int *position, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return btree_posi_find(engine, key, nkey, bkrange, order, position);
}

static ENGINE_ERROR_CODE default_btree_posi_find_with_get(ENGINE_HANDLE* handle, const void* cookie,
                                                 const char *key, const size_t nkey, const bkey_range *bkrange,
                                                 ENGINE_BTREE_ORDER order, const uint32_t count, int *position,
                                                 eitem **eitem_array, uint32_t *eitem_count, uint32_t *eitem_index,
                                                 uint32_t *flags, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return btree_posi_find_with_get(engine, key, nkey, bkrange, order, count, position,
                                    (btree_elem_item**)eitem_array, eitem_count, eitem_index, flags);
}

static ENGINE_ERROR_CODE default_btree_elem_get_by_posi(ENGINE_HANDLE* handle, const void* cookie,
                                                 const char *key, const size_t nkey,
                                                 ENGINE_BTREE_ORDER order, int from_posi, int to_posi,
                                                 eitem **eitem_array, uint32_t *eitem_count, uint32_t *flags,
                                                 uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return btree_elem_get_by_posi(engine, key, nkey, order, from_posi, to_posi,
                                  (btree_elem_item**)eitem_array, eitem_count, flags);
}

#ifdef SUPPORT_BOP_SMGET
static ENGINE_ERROR_CODE default_btree_elem_smget(ENGINE_HANDLE* handle, const void* cookie,
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
                                                  uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return btree_elem_smget(engine, karray, kcount, bkrange, efilter, offset, count,
                            (btree_elem_item**)eitem_array, kfnd_array, flag_array, eitem_count,
                            missed_key_array, missed_key_count, trimmed, duplicated);
}
#endif

static ENGINE_ERROR_CODE default_getattr(ENGINE_HANDLE* handle, const void* cookie,
                                         const void* key, const int nkey,
                                         ENGINE_ITEM_ATTR *attr_ids,
                                         const uint32_t attr_count,
                                         item_attr *attr_data, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return item_getattr(engine, key, nkey, attr_ids, attr_count, attr_data);
}

static ENGINE_ERROR_CODE default_setattr(ENGINE_HANDLE* handle, const void* cookie,
                                         const void* key, const int nkey,
                                         ENGINE_ITEM_ATTR *attr_ids,
                                         const uint32_t attr_count, item_attr *attr_data,
                                         uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return item_setattr(engine, key, nkey, attr_ids, attr_count, attr_data);
}

static void default_reset_stats(ENGINE_HANDLE* handle, const void *cookie)
{
    struct default_engine *engine = get_handle(handle);
    item_stats_reset(engine);

    pthread_mutex_lock(&engine->stats.lock);
    engine->stats.evictions = 0;
    engine->stats.reclaimed = 0;
    engine->stats.total_items = 0;
    pthread_mutex_unlock(&engine->stats.lock);
}

static ENGINE_ERROR_CODE default_get_prefix_stats(ENGINE_HANDLE* handle, const void* cookie,
                                                  const void* key, const int nkey, void *prefix_data)
{
    struct default_engine *engine = get_handle(handle);
    return assoc_get_prefix_stats(engine, key, nkey, prefix_data);
}

#if 0 // ENABLE_TAP_PROTOCOL
static tap_event_t tap_always_pause(ENGINE_HANDLE *e,
                                    const void *cookie, item **itm, void **es,
                                    uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                                    uint32_t *seqno, uint16_t *vbucket)
{
    return TAP_PAUSE;
}

static tap_event_t tap_always_disconnect(ENGINE_HANDLE *e,
                                         const void *cookie, item **itm, void **es,
                                         uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                                         uint32_t *seqno, uint16_t *vbucket)
{
    return TAP_DISCONNECT;
}

static TAP_ITERATOR get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                     const void* client, size_t nclient, uint32_t flags,
                                     const void* userdata, size_t nuserdata)
{
    TAP_ITERATOR rv = tap_always_pause;
    if ((flags & TAP_CONNECT_FLAG_DUMP)
        || (flags & TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS)) {
        rv = tap_always_disconnect;
    }
    return rv;
}
#endif

static ENGINE_ERROR_CODE initalize_configuration(struct default_engine *se, const char *cfg_str)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    se->config.vb0 = true;

    if (cfg_str != NULL) {
        struct config_item items[] = {
            { .key = "use_cas",
              .datatype = DT_BOOL,
              .value.dt_bool = &se->config.use_cas },
            { .key = "verbose",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.verbose },
            { .key = "eviction",
              .datatype = DT_BOOL,
              .value.dt_bool = &se->config.evict_to_free },
            { .key = "num_threads",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.num_threads },
            { .key = "cache_size",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.maxbytes },
            { .key = "sticky_limit",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.sticky_limit},
            { .key = "junk_item_time",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.junk_item_time },
            { .key = "preallocate",
              .datatype = DT_BOOL,
              .value.dt_bool = &se->config.preallocate },
            { .key = "factor",
              .datatype = DT_FLOAT,
              .value.dt_float = &se->config.factor },
            { .key = "chunk_size",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.chunk_size },
            { .key = "item_size_max",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.item_size_max },
            { .key = "max_list_size",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.max_list_size },
            { .key = "max_set_size",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.max_set_size },
            { .key = "max_btree_size",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.max_btree_size },
            { .key = "ignore_vbucket",
              .datatype = DT_BOOL,
              .value.dt_bool = &se->config.ignore_vbucket },
            { .key = "prefix_delimiter",
              .datatype = DT_CHAR,
              .value.dt_char = &se->config.prefix_delimiter },
            { .key = "vb0",
              .datatype = DT_BOOL,
              .value.dt_bool = &se->config.vb0 },
            { .key = "config_file",
              .datatype = DT_CONFIGFILE },
            { .key = NULL}
        };
        ret = se->server.core->parse_config(cfg_str, items, stderr);
    }

    if (se->config.vb0) {
        set_vbucket_state(se, 0, VBUCKET_STATE_ACTIVE);
    }
    return ret;
}

static protocol_binary_response_status set_vbucket(struct default_engine *e,
                                                   protocol_binary_request_header *request,
                                                   const char **msg)
{
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;
    assert(req);

    char keyz[32];
    char valz[32];

    // Read the key.
    int keylen = ntohs(req->message.header.request.keylen);
    if (keylen >= (int)sizeof(keyz)) {
        *msg = "Key is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
    keyz[keylen] = 0x00;

    // Read the value.
    size_t bodylen = ntohl(req->message.header.request.bodylen)
        - ntohs(req->message.header.request.keylen);
    if (bodylen >= sizeof(valz)) {
        *msg = "Value is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(valz, (char*)request + sizeof(req->message.header)
           + keylen, bodylen);
    valz[bodylen] = 0x00;

    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    *msg = "Configured";

    enum vbucket_state state;
    if (strcmp(valz, "active") == 0) {
        state = VBUCKET_STATE_ACTIVE;
    } else if(strcmp(valz, "replica") == 0) {
        state = VBUCKET_STATE_REPLICA;
    } else if(strcmp(valz, "pending") == 0) {
        state = VBUCKET_STATE_PENDING;
    } else if(strcmp(valz, "dead") == 0) {
        state = VBUCKET_STATE_DEAD;
    } else {
        *msg = "Invalid state.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    uint32_t vbucket = 0;
    if (!safe_strtoul(keyz, &vbucket) || vbucket > NUM_VBUCKETS) {
        *msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    } else {
        set_vbucket_state(e, (uint16_t)vbucket, state);
    }

    return rv;
}

static protocol_binary_response_status get_vbucket(struct default_engine *e,
                                                   protocol_binary_request_header *request,
                                                   const char **msg)
{
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;
    assert(req);

    char keyz[8]; // stringy 2^16 int

    // Read the key.
    int keylen = ntohs(req->message.header.request.keylen);
    if (keylen >= (int)sizeof(keyz)) {
        *msg   = "Key is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
    keyz[keylen] = 0x00;

    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    uint32_t vbucket = 0;
    if (!safe_strtoul(keyz, &vbucket) || vbucket > NUM_VBUCKETS) {
        *msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    } else {
        *msg = vbucket_state_name(get_vbucket_state(e, (uint16_t)vbucket));
    }

    return rv;
}

static protocol_binary_response_status rm_vbucket(struct default_engine *e,
                                                  protocol_binary_request_header *request,
                                                  const char **msg)
{
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;
    assert(req);

    char keyz[8]; // stringy 2^16 int

    // Read the key.
    int keylen = ntohs(req->message.header.request.keylen);
    if (keylen >= (int)sizeof(keyz)) {
        *msg = "Key is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
    keyz[keylen] = 0x00;

    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    uint32_t vbucket = 0;
    if (!safe_strtoul(keyz, &vbucket) || vbucket > NUM_VBUCKETS) {
        *msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    } else {
        set_vbucket_state(e, (uint16_t)vbucket, VBUCKET_STATE_DEAD);
    }

    assert(msg);
    return rv;
}

static protocol_binary_response_status scrub_cmd(struct default_engine *e,
                                                 protocol_binary_request_header *request,
                                                 const char **msg)
{
    bool res; /* true or false */
    if (request->request.opcode == PROTOCOL_BINARY_CMD_SCRUB) {
        res = item_start_scrub(e, SCRUB_MODE_NORMAL);
    } else { /* PROTOCOL_BINARY_CMD_SCRUB_STALE */
#ifdef ENABLE_CLUSTER_AWARE
        if (! e->server.core->is_zk_integrated())
#endif
        {
            return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
        }
        res = item_start_scrub(e, SCRUB_MODE_STALE);
    }
    return (res) ? PROTOCOL_BINARY_RESPONSE_SUCCESS : PROTOCOL_BINARY_RESPONSE_EBUSY;
}

static ENGINE_ERROR_CODE default_set_memlimit(ENGINE_HANDLE* handle, const void* cookie,
                                              const size_t memlimit, const int sticky_ratio)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = slabs_set_memlimit(engine, memlimit);
    if (ret == ENGINE_SUCCESS) {
        engine->config.maxbytes = memlimit;
#ifdef ENABLE_STICKY_ITEM
        if (sticky_ratio > 0) {
            engine->config.sticky_limit = (memlimit / 100) * sticky_ratio;
        }
#endif
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

static void default_set_junktime(ENGINE_HANDLE* handle, const void* cookie, const size_t junktime)
{
    struct default_engine* engine = get_handle(handle);

    pthread_mutex_lock(&engine->cache_lock);
    engine->config.junk_item_time = junktime;
    pthread_mutex_unlock(&engine->cache_lock);
}

static void default_set_verbose(ENGINE_HANDLE* handle, const void* cookie, const size_t verbose)
{
    struct default_engine* engine = get_handle(handle);

    pthread_mutex_lock(&engine->cache_lock);
    engine->config.verbose = verbose;
    pthread_mutex_unlock(&engine->cache_lock);
}

static char *default_cachedump(ENGINE_HANDLE* handle, const void* cookie,
                               const unsigned int slabs_clsid, const unsigned int limit,
                               const bool forward, const bool sticky, unsigned int *bytes)
{
    struct default_engine* engine = get_handle(handle);
    return item_cachedump(engine, slabs_clsid, limit, forward, sticky, bytes);
}

static ENGINE_ERROR_CODE default_unknown_command(ENGINE_HANDLE* handle, const void* cookie,
                                                 protocol_binary_request_header *request,
                                                 ADD_RESPONSE response)
{
    struct default_engine* e = get_handle(handle);

    bool handled = true;
    const char *msg = NULL;
    protocol_binary_response_status res = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;

    switch(request->request.opcode) {
    case PROTOCOL_BINARY_CMD_SCRUB:
    case PROTOCOL_BINARY_CMD_SCRUB_STALE:
        res = scrub_cmd(e, request, &msg);
        break;
    case CMD_DEL_VBUCKET:
        res = rm_vbucket(e, request, &msg);
        break;
    case CMD_SET_VBUCKET:
        res = set_vbucket(e, request, &msg);
        break;
    case CMD_GET_VBUCKET:
        res = get_vbucket(e, request, &msg);
        break;
    default:
        handled = false;
        break;
    }

    bool sent = false;
    if (handled) {
        size_t msg_size = msg ? strlen(msg) : 0;
        sent = response(msg, (uint16_t)msg_size, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        (uint16_t)res, 0, cookie);
    } else {
        sent = response(NULL, 0, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, 0, cookie);
    }

    if (sent) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_FAILED;
    }
}

uint64_t item_get_cas(const hash_item* item)
{
    if (item->iflag & ITEM_WITH_CAS) {
        return *(uint64_t*)(item + 1);
    }
    return 0;
}

void item_set_cas(ENGINE_HANDLE *handle, const void *cookie, item* item, uint64_t val)
{
    hash_item* it = get_real_item(item);
    if (it->iflag & ITEM_WITH_CAS) {
        *(uint64_t*)(it + 1) = val;
    }
}

const void* item_get_key(const hash_item* item)
{
    char *ret = (void*)(item + 1);
    if (item->iflag & ITEM_WITH_CAS) {
        ret += sizeof(uint64_t);
    }
    return ret;
}

char* item_get_data(const hash_item* item)
{
    return ((char*)item_get_key(item)) + item->nkey;
}

char* item_get_meta(const hash_item* item)
{
    return ((char*)item_get_key(item)) + META_OFFSET_IN_ITEM(item->nkey, item->nbytes);
}

uint8_t item_get_clsid(const hash_item* item)
{
    return 0;
}

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info)
{
    hash_item* it = (hash_item*)item;
    if (item_info->nvalue < 1) {
        return false;
    }
    item_info->cas = item_get_cas(it);
    item_info->exptime = it->exptime;
    item_info->nbytes = it->nbytes;
    item_info->flags = it->flags;
    item_info->clsid = it->slabs_clsid;
    item_info->nkey = it->nkey;
    item_info->nvalue = 1;
    item_info->key = item_get_key(it);
    item_info->value[0].iov_base = item_get_data(it);
    item_info->value[0].iov_len = it->nbytes;
    return true;
}

static void get_list_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                               const eitem* eitem, eitem_info *elem_info)
{
    list_elem_item *elem = (list_elem_item*)eitem;
    elem_info->nbytes = elem->nbytes;
    elem_info->value  = elem->value;
}

static void get_set_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                              const eitem* eitem, eitem_info *elem_info)
{
    set_elem_item *elem = (set_elem_item*)eitem;
    elem_info->nbytes = elem->nbytes;
    elem_info->value  = elem->value;
}

static void get_btree_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                                const eitem* eitem, eitem_info *elem_info)
{
    btree_elem_item *elem = (btree_elem_item*)eitem;
    elem_info->nscore = elem->nbkey;
    elem_info->neflag = elem->neflag;
    elem_info->nbytes = elem->nbytes;
    elem_info->score  = elem->data;
    if (elem->neflag > 0) {
        elem_info->eflag = elem->data + (elem->nbkey == 0 ? sizeof(uint64_t) : elem->nbkey);
        elem_info->value = (const char*)elem_info->eflag + elem->neflag;
    } else {
        elem_info->eflag = NULL;
        elem_info->value = (const char*)elem->data + (elem->nbkey == 0 ? sizeof(uint64_t) : elem->nbkey);
    }
}
