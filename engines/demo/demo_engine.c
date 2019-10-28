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

#include "demo_engine.h"
#include "memcached/util.h"
#include "memcached/config_parser.h"

#define ACTION_BEFORE_WRITE(c, k, l)
#define ACTION_AFTER_WRITE(c, r)
#define VBUCKET_GUARD(e, v)

/*
 * common static functions
 */
static inline struct demo_engine*
get_handle(ENGINE_HANDLE* handle)
{
    return (struct demo_engine*)handle;
}

static inline hash_item*
get_real_item(item* item)
{
    return (hash_item*)item;
}

/*
 * DEMO ENGINE API
 */

static const engine_info*
Demo_get_info(ENGINE_HANDLE* handle)
{
    return &get_handle(handle)->info.engine_info;
}

static ENGINE_ERROR_CODE
initialize_configuration(struct demo_engine *se, const char *cfg_str)
{
    se->config.vb0 = false;

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
#ifdef ENABLE_STICKY_ITEM
            { .key = "sticky_limit",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.sticky_limit},
#endif
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
            { .key = "max_map_size",
              .datatype = DT_SIZE,
              .value.dt_size = &se->config.max_map_size },
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
        if (se->server.core->parse_config(cfg_str, items, stderr) != 0) {
            return ENGINE_FAILED;
        }
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
Demo_initialize(ENGINE_HANDLE* handle, const char* config_str)
{
    struct demo_engine* se = get_handle(handle);

    ENGINE_ERROR_CODE ret = initialize_configuration(se, config_str);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    /* fixup feature_info */
    if (se->config.use_cas) {
        se->info.engine_info.features[se->info.engine_info.num_features++].feature = ENGINE_FEATURE_CAS;
    }

    ret = dm_assoc_init(se);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    ret = dm_item_init(se);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    return ENGINE_SUCCESS;
}

static void
Demo_destroy(ENGINE_HANDLE* handle)
{
    struct demo_engine* se = get_handle(handle);

    if (se->initialized) {
        se->initialized = false;
        dm_item_final(se);
        dm_assoc_final(se);
        pthread_mutex_destroy(&se->cache_lock);
        pthread_mutex_destroy(&se->stats.lock);
        free(se);
    }
}

/*
 * Item API
 */

static ENGINE_ERROR_CODE
Demo_item_allocate(ENGINE_HANDLE* handle, const void* cookie,
                      item **item,
                      const void* key, const size_t nkey,
                      const size_t nbytes,
                      const int flags, const rel_time_t exptime,
                      const uint64_t cas)
{
    struct demo_engine* engine = get_handle(handle);
    hash_item *it;
    ENGINE_ERROR_CODE ret = ENGINE_EINVAL;

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    it = dm_item_alloc(engine, key, nkey, flags, exptime, nbytes, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    if (it != NULL) {
        dm_item_set_cas(it, cas);
        *item = it;
        ret = ENGINE_SUCCESS;
    } else {
        ret = ENGINE_ENOMEM;
    }
    return ret;
}

static ENGINE_ERROR_CODE
Demo_item_delete(ENGINE_HANDLE* handle, const void* cookie,
                    const void* key, const size_t nkey,
                    uint64_t cas, uint16_t vbucket)
{
    struct demo_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = dm_item_delete(engine, key, nkey, cas);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static void
Demo_item_release(ENGINE_HANDLE* handle, const void *cookie, item* item)
{
    dm_item_release(get_handle(handle), get_real_item(item));
}

static ENGINE_ERROR_CODE
Demo_get(ENGINE_HANDLE* handle, const void* cookie,
            item** item, const void* key, const int nkey,
            uint16_t vbucket)
{
    struct demo_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    *item = dm_item_get(engine, key, nkey);
    if (*item != NULL) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

static ENGINE_ERROR_CODE
Demo_store(ENGINE_HANDLE* handle, const void *cookie,
              item* item, uint64_t *cas, ENGINE_STORE_OPERATION operation,
              uint16_t vbucket)
{
    struct demo_engine *engine = get_handle(handle);
    hash_item *it = get_real_item(item);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, item_get_key(it), it->nkey);
    ret = dm_item_store(engine, it, cas, operation, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
Demo_arithmetic(ENGINE_HANDLE* handle, const void* cookie,
                   const void* key, const int nkey,
                   const bool increment, const bool create,
                   const uint64_t delta, const uint64_t initial,
                   const int flags, const rel_time_t exptime,
                   uint64_t *cas, uint64_t *result, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_flush(ENGINE_HANDLE* handle, const void* cookie,
           const void* prefix, const int nprefix, rel_time_t when)
{
    return ENGINE_ENOTSUP;
}

/*
 * List Collection API
 */

static ENGINE_ERROR_CODE
Demo_list_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                           const void* key, const int nkey, item_attr *attrp,
                           uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_list_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey,
                        const size_t nbytes, eitem** eitem)
{
    return ENGINE_ENOTSUP;
}

static void
Demo_list_elem_free(ENGINE_HANDLE* handle, const void *cookie, eitem *eitem)
{
    return;
}

static void
Demo_list_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                          eitem **eitem_array, const int eitem_count)
{
    return;
}

static ENGINE_ERROR_CODE
Demo_list_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                         const void* key, const int nkey,
                         int index, eitem *eitem,
                         item_attr *attrp, bool *created, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_list_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                         const void* key, const int nkey,
                         int from_index, int to_index, const bool drop_if_empty,
                         uint32_t* del_count, bool* dropped, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_list_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                      const void* key, const int nkey,
                      int from_index, int to_index,
                      const bool delete, const bool drop_if_empty,
                      struct elems_result *eresult, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

/*
 * Set Collection API
 */

static ENGINE_ERROR_CODE
Demo_set_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                          const void* key, const int nkey, item_attr *attrp,
                          uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_set_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                       const void* key, const int nkey,
                       const size_t nbytes, eitem** eitem)
{
    return ENGINE_ENOTSUP;
}

static void
Demo_set_elem_free(ENGINE_HANDLE* handle, const void *cookie, eitem *eitem)
{
    return;
}

static void
Demo_set_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                         eitem **eitem_array, const int eitem_count)
{
    return;
}

static ENGINE_ERROR_CODE
Demo_set_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey, eitem *eitem,
                        item_attr *attrp, bool *created, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_set_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey,
                        const void* value, const int nbytes,
                        const bool drop_if_empty, bool *dropped,
                        uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_set_elem_exist(ENGINE_HANDLE* handle, const void* cookie,
                       const void* key, const int nkey,
                       const void* value, const int nbytes,
                       bool *exist, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_set_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                     const void* key, const int nkey,
                     const uint32_t count,
                     const bool delete, const bool drop_if_empty,
                     struct elems_result *eresult, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

/*
 * Map Collection API
 */

static ENGINE_ERROR_CODE
Demo_map_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                          const void* key, const int nkey, item_attr *attrp,
                          uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_map_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                       const void* key, const int nkey, const size_t nfield,
                       const size_t nbytes, eitem** eitem)
{
    return ENGINE_ENOTSUP;
}

static void
Demo_map_elem_free(ENGINE_HANDLE* handle, const void *cookie, eitem *eitem)
{
    return;
}

static void
Demo_map_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                         eitem **eitem_array, const int eitem_count)
{
    return;
}

static ENGINE_ERROR_CODE
Demo_map_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey, eitem *eitem,
                        item_attr *attrp, bool *created, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_map_elem_update(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey, const field_t *field,
                        const void* value, const int nbytes, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_map_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey, const int numfields,
                        const field_t *flist, const bool drop_if_empty,
                        uint32_t* del_count, bool *dropped, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_map_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                     const void* key, const int nkey,
                     const int numfields, const field_t *flist,
                     const bool delete, const bool drop_if_empty,
                     struct elems_result *eresult, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

/*
 * B+Tree Collection API
 */

static ENGINE_ERROR_CODE
Demo_btree_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                            const void* key, const int nkey, item_attr *attrp,
                            uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_btree_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                         const void* key, const int nkey,
                         const size_t nbkey, const size_t neflag,
                         const size_t nbytes, eitem** eitem)
{
    return ENGINE_ENOTSUP;
}

static void
Demo_btree_elem_free(ENGINE_HANDLE* handle, const void *cookie, eitem *eitem)
{
    return;
}

static void
Demo_btree_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                           eitem **eitem_array, const int eitem_count)
{
    return;
}

static ENGINE_ERROR_CODE
Demo_btree_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                          const void* key, const int nkey,
                          eitem *eitem, const bool replace_if_exist,
                          item_attr *attrp,
                          bool *replaced, bool *created,
                          eitem_result *trimmed, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_btree_elem_update(ENGINE_HANDLE* handle, const void* cookie,
                          const void* key, const int nkey,
                          const bkey_range *bkrange, const eflag_update *eupdate,
                          const void* value, const int nbytes, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_btree_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                          const void* key, const int nkey,
                          const bkey_range *bkrange, const eflag_filter *efilter,
                          const uint32_t req_count, const bool drop_if_empty,
                          uint32_t* del_count, uint32_t *access_count,
                          bool* dropped, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_btree_elem_arithmetic(ENGINE_HANDLE* handle, const void* cookie,
                              const void* key, const int nkey,
                              const bkey_range *bkrange,
                              const bool increment, const bool create,
                              const uint64_t delta, const uint64_t initial,
                              const eflag_t *eflagp,
                              uint64_t *result, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_btree_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                       const void* key, const int nkey,
                       const bkey_range *bkrange, const eflag_filter *efilter,
                       const uint32_t offset, const uint32_t req_count,
                       const bool delete, const bool drop_if_empty,
                       struct elems_result *eresult, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_btree_elem_count(ENGINE_HANDLE* handle, const void* cookie,
                         const void* key, const int nkey,
                         const bkey_range *bkrange, const eflag_filter *efilter,
                         uint32_t* eitem_count, uint32_t* access_count,
                         uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_btree_posi_find(ENGINE_HANDLE* handle, const void* cookie,
                        const char *key, const size_t nkey,
                        const bkey_range *bkrange,
                        ENGINE_BTREE_ORDER order,
                        int *position, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_btree_posi_find_with_get(ENGINE_HANDLE* handle, const void* cookie,
                                 const char *key, const size_t nkey,
                                 const bkey_range *bkrange,
                                 ENGINE_BTREE_ORDER order, const uint32_t count,
                                 int *position, eitem **eitem_array,
                                 uint32_t *eitem_count, uint32_t *eitem_index,
                                 uint32_t *flags, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_btree_elem_get_by_posi(ENGINE_HANDLE* handle, const void* cookie,
                               const char *key, const size_t nkey,
                               ENGINE_BTREE_ORDER order,
                               int from_posi, int to_posi,
                               eitem **eitem_array, uint32_t *eitem_count,
                               uint32_t *flags, uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

#ifdef SUPPORT_BOP_SMGET
/* smget new interface */
static ENGINE_ERROR_CODE
Demo_btree_elem_smget(ENGINE_HANDLE* handle, const void* cookie,
                         token_t *karray, const int kcount,
                         const bkey_range *bkrange,
                         const eflag_filter *efilter,
                         const uint32_t offset, const uint32_t count,
                         const bool unique, smget_result_t *result,
                         uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}
#endif

/*
 * Item Attribute API
 */

static ENGINE_ERROR_CODE
Demo_getattr(ENGINE_HANDLE* handle, const void* cookie,
                const void* key, const int nkey,
                ENGINE_ITEM_ATTR *attr_ids,
                const uint32_t attr_count, item_attr *attr_data,
                uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE
Demo_setattr(ENGINE_HANDLE* handle, const void* cookie,
                const void* key, const int nkey,
                ENGINE_ITEM_ATTR *attr_ids,
                const uint32_t attr_count, item_attr *attr_data,
                uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

/*
 * Stats API
 */

static void stats_engine(struct demo_engine *engine,
                         ADD_STAT add_stat, const void *cookie)
{
    char val[128];
    int len;

    pthread_mutex_lock(&engine->stats.lock);
    len = sprintf(val, "%"PRIu64, engine->stats.reclaimed);
    add_stat("reclaimed", 9, val, len, cookie);
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
    len = sprintf(val, "%"PRIu64, (uint64_t)engine->config.sticky_limit);
    add_stat("sticky_limit", 12, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)engine->config.maxbytes);
    add_stat("engine_maxbytes", 15, val, len, cookie);
    pthread_mutex_unlock(&engine->stats.lock);
}

static ENGINE_ERROR_CODE
Demo_get_stats(ENGINE_HANDLE* handle, const void* cookie,
                  const char* stat_key, int nkey, ADD_STAT add_stat)
{
    struct demo_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (stat_key == NULL) {
        stats_engine(engine, add_stat, cookie);
    }
    else {
        ret = ENGINE_KEY_ENOENT;
    }
    return ret;
}

static void
Demo_reset_stats(ENGINE_HANDLE* handle, const void *cookie)
{
    struct demo_engine *engine = get_handle(handle);
    dm_item_stats_reset(engine);

    pthread_mutex_lock(&engine->stats.lock);
    engine->stats.evictions = 0;
    engine->stats.reclaimed = 0;
    engine->stats.total_items = 0;
    pthread_mutex_unlock(&engine->stats.lock);
}

static ENGINE_ERROR_CODE
Demo_get_prefix_stats(ENGINE_HANDLE* handle, const void* cookie,
                         const void* key, const int nkey, void *prefix_data)
{
    return ENGINE_ENOTSUP;
}

/*
 * Dump API
 */

/*
 * Config API
 */
static ENGINE_ERROR_CODE
Demo_set_config(ENGINE_HANDLE* handle, const void* cookie,
                const char* config_type, void* config_value)
{
    return ENGINE_ENOTSUP;
}

/*
 * Unknown Command API
 */

/* Item/Elem Info */

static bool
Demo_get_item_info(ENGINE_HANDLE *handle, const void *cookie,
              const item* item, item_info *item_info)
{
    hash_item* it = (hash_item*)item;

    item_info->cas = dm_item_get_cas(it);
    item_info->flags = it->flags;
    item_info->exptime = it->exptime;
    item_info->clsid = it->slabs_clsid;
    item_info->nkey = it->nkey;
    item_info->nbytes = it->nbytes;
    item_info->nvalue = it->nbytes;
    item_info->naddnl = 0;
    item_info->key = dm_item_get_key(it);
    item_info->value = dm_item_get_data(it);
    item_info->addnl = NULL;
    return true;
}

static void
Demo_get_elem_info(ENGINE_HANDLE *handle, const void *cookie,
              const int type, /* collection type */
              const eitem* eitem, eitem_info *elem_info)
{
    return;
}

ENGINE_ERROR_CODE
create_instance(uint64_t interface, GET_SERVER_API get_server_api,
                ENGINE_HANDLE **handle)
{
    SERVER_HANDLE_V1 *api = get_server_api();
    if (interface != 1 || api == NULL) {
        return ENGINE_ENOTSUP;
    }

    struct demo_engine *engine = malloc(sizeof(*engine));
    if (engine == NULL) {
        return ENGINE_ENOMEM;
    }

    struct demo_engine demo_engine = {
      .engine = {
         .interface = {
            .interface = 1
         },
         /* Engine API */
         .get_info          = Demo_get_info,
         .initialize        = Demo_initialize,
         .destroy           = Demo_destroy,
         /* Item API */
         .allocate          = Demo_item_allocate,
         .remove            = Demo_item_delete,
         .release           = Demo_item_release,
         .get               = Demo_get,
         .store             = Demo_store,
         .arithmetic        = Demo_arithmetic,
         .flush             = Demo_flush,
         /* LIST Collection API */
         .list_struct_create = Demo_list_struct_create,
         .list_elem_alloc   = Demo_list_elem_alloc,
         .list_elem_free    = Demo_list_elem_free,
         .list_elem_release = Demo_list_elem_release,
         .list_elem_insert  = Demo_list_elem_insert,
         .list_elem_delete  = Demo_list_elem_delete,
         .list_elem_get     = Demo_list_elem_get,
         /* SET Colleciton API */
         .set_struct_create = Demo_set_struct_create,
         .set_elem_alloc    = Demo_set_elem_alloc,
         .set_elem_free     = Demo_set_elem_free,
         .set_elem_release  = Demo_set_elem_release,
         .set_elem_insert   = Demo_set_elem_insert,
         .set_elem_delete   = Demo_set_elem_delete,
         .set_elem_exist    = Demo_set_elem_exist,
         .set_elem_get      = Demo_set_elem_get,
         /* MAP Collection API */
         .map_struct_create = Demo_map_struct_create,
         .map_elem_alloc    = Demo_map_elem_alloc,
         .map_elem_free     = Demo_map_elem_free,
         .map_elem_release  = Demo_map_elem_release,
         .map_elem_insert   = Demo_map_elem_insert,
         .map_elem_update   = Demo_map_elem_update,
         .map_elem_delete   = Demo_map_elem_delete,
         .map_elem_get      = Demo_map_elem_get,
         /* B+Tree Collection API */
         .btree_struct_create = Demo_btree_struct_create,
         .btree_elem_alloc   = Demo_btree_elem_alloc,
         .btree_elem_free    = Demo_btree_elem_free,
         .btree_elem_release = Demo_btree_elem_release,
         .btree_elem_insert  = Demo_btree_elem_insert,
         .btree_elem_update  = Demo_btree_elem_update,
         .btree_elem_delete  = Demo_btree_elem_delete,
         .btree_elem_arithmetic  = Demo_btree_elem_arithmetic,
         .btree_elem_get     = Demo_btree_elem_get,
         .btree_elem_count   = Demo_btree_elem_count,
         .btree_posi_find    = Demo_btree_posi_find,
         .btree_posi_find_with_get = Demo_btree_posi_find_with_get,
         .btree_elem_get_by_posi = Demo_btree_elem_get_by_posi,
#ifdef SUPPORT_BOP_SMGET
         .btree_elem_smget   = Demo_btree_elem_smget,
#endif
         /* Attributes API */
         .getattr          = Demo_getattr,
         .setattr          = Demo_setattr,
         /* Stats API */
         .get_stats        = Demo_get_stats,
         .reset_stats      = Demo_reset_stats,
         .get_prefix_stats = Demo_get_prefix_stats,
         /* Dump API */
         /* Config API */
         .set_config       = Demo_set_config,
         /* Unknown Command API */
         /* Info API */
         .get_item_info    = Demo_get_item_info,
         .get_elem_info    = Demo_get_elem_info
      },
      .server = *api,
      .get_server_api = get_server_api,
      .initialized = true,
      .assoc = {
         .hashsize = 1024 * 1024, /* (1<<17) => 128K hash size */
         .hash_items = 0,
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
         .preallocate = false,
         .factor = 1.25,
         .chunk_size = 48,
         .item_size_max= 1024 * 1024,
         .max_list_size = 50000,
         .max_set_size = 50000,
         .max_map_size = 50000,
         .max_btree_size = 50000,
         .prefix_delimiter = ':',
       },
      .info.engine_info = {
           .description = "Demo engine v0.1",
           .num_features = 1,
           .features = {
               [0].feature = ENGINE_FEATURE_LRU
           }
       }
    };

    *engine = demo_engine;

    *handle = (ENGINE_HANDLE*)&engine->engine;
    return ENGINE_SUCCESS;
}
