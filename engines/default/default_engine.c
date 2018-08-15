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

#define ACTION_BEFORE_WRITE(c, k, l)
#define ACTION_AFTER_WRITE(c, r)

/*
 * vbucket static functions
 */

static const char*
vbucket_state_name(enum vbucket_state s)
{
    static const char * vbucket_states[] = {
        "dead", "active", "replica", "pending"
    };
    return vbucket_states[s];
}

static void
set_vbucket_state(struct default_engine *e, uint16_t vbid, enum vbucket_state to)
{
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid];
    vi.v.state = to;
    e->vbucket_infos[vbid] = vi.c;
}

static enum vbucket_state
get_vbucket_state(struct default_engine *e, uint16_t vbid)
{
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid];
    return vi.v.state;
}

static bool
handled_vbucket(struct default_engine *e, uint16_t vbid)
{
    return e->config.ignore_vbucket
        || (get_vbucket_state(e, vbid) == VBUCKET_STATE_ACTIVE);
}

/* mechanism for handling bad vbucket requests */
#define VBUCKET_GUARD(e, v) if (!handled_vbucket(e, v)) { return ENGINE_NOT_MY_VBUCKET; }

/*
 * common static functions
 */
static inline struct default_engine*
get_handle(ENGINE_HANDLE* handle)
{
    return (struct default_engine*)handle;
}

static inline hash_item*
get_real_item(item* item)
{
    return (hash_item*)item;
}

/*
 * DEFAULT ENGINE API
 */

static const engine_info*
default_get_info(ENGINE_HANDLE* handle)
{
    return &get_handle(handle)->info.engine_info;
}

static ENGINE_ERROR_CODE
initialize_configuration(struct default_engine *se, const char *cfg_str)
{
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

    if (se->config.vb0) {
        set_vbucket_state(se, 0, VBUCKET_STATE_ACTIVE);
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
default_initialize(ENGINE_HANDLE* handle, const char* config_str)
{
    struct default_engine* se = get_handle(handle);

    ENGINE_ERROR_CODE ret = initialize_configuration(se, config_str);
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

static void
default_destroy(ENGINE_HANDLE* handle)
{
    struct default_engine* se = get_handle(handle);

    if (se->initialized) {
        se->initialized = false;
        item_final(se);
        slabs_final(se);
        assoc_final(se);
        pthread_mutex_destroy(&se->cache_lock);
        pthread_mutex_destroy(&se->stats.lock);
        pthread_mutex_destroy(&se->slabs.lock);
        free(se);
    }
}

/*
 * Item API
 */

static ENGINE_ERROR_CODE
default_item_allocate(ENGINE_HANDLE* handle, const void* cookie,
                      item **item,
                      const void* key, const size_t nkey,
                      const size_t nbytes,
                      const int flags, const rel_time_t exptime,
                      const uint64_t cas)
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
    ENGINE_ERROR_CODE ret = ENGINE_EINVAL;

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    it = item_alloc(engine, key, nkey, flags, exptime, nbytes, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    if (it != NULL) {
        item_set_cas(it, cas);
        *item = it;
        ret = ENGINE_SUCCESS;
    } else {
        ret = ENGINE_ENOMEM;
    }
    return ret;
}

static ENGINE_ERROR_CODE
default_item_delete(ENGINE_HANDLE* handle, const void* cookie,
                    const void* key, const size_t nkey,
                    uint64_t cas, uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = item_delete(engine, key, nkey, cas);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static void
default_item_release(ENGINE_HANDLE* handle, const void *cookie, item* item)
{
    item_release(get_handle(handle), get_real_item(item));
}

static ENGINE_ERROR_CODE
default_get(ENGINE_HANDLE* handle, const void* cookie,
            item** item, const void* key, const int nkey,
            uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    *item = item_get(engine, key, nkey);
    if (*item != NULL) {
        hash_item *it = get_real_item(*item);
        if (IS_COLL_ITEM(it)) { /* collection item */
            item_release(engine, it);
            *item = NULL;
            return ENGINE_EBADTYPE;
        }
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

static ENGINE_ERROR_CODE
default_store(ENGINE_HANDLE* handle, const void *cookie,
              item* item, uint64_t *cas, ENGINE_STORE_OPERATION operation,
              uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    hash_item *it = get_real_item(item);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, item_get_key(it), it->nkey);
    ret = store_item(engine, it, cas, operation, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_arithmetic(ENGINE_HANDLE* handle, const void* cookie,
                   const void* key, const int nkey,
                   const bool increment, const bool create,
                   const uint64_t delta, const uint64_t initial,
                   const int flags, const rel_time_t exptime,
                   uint64_t *cas, uint64_t *result, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = arithmetic(engine, cookie, key, nkey, increment, create,
                     delta, initial, flags, exptime, cas, result);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_flush(ENGINE_HANDLE* handle, const void* cookie,
              const void* prefix, const int nprefix, rel_time_t when)
{
    ENGINE_ERROR_CODE ret;

    ACTION_BEFORE_WRITE(cookie, NULL, 0);
    ret = item_flush_expired(get_handle(handle), prefix, nprefix, when, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

/*
 * List Collection API
 */

static ENGINE_ERROR_CODE
default_list_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                           const void* key, const int nkey, item_attr *attrp,
                           uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = list_struct_create(engine, key, nkey, attrp, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_list_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey,
                        const size_t nbytes, eitem** eitem)
{
    list_elem_item *elem;
    ENGINE_ERROR_CODE ret = ENGINE_EINVAL;

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    elem = list_elem_alloc(get_handle(handle), nbytes, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    if (elem != NULL) {
        *eitem = elem;
        ret = ENGINE_SUCCESS;
    } else {
        ret = ENGINE_ENOMEM;
    }
    return ret;
}

static void
default_list_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                          eitem **eitem_array, const int eitem_count)
{
    struct default_engine *engine = get_handle(handle);
    list_elem_release(engine, (list_elem_item**)eitem_array, eitem_count);
}

static ENGINE_ERROR_CODE
default_list_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                         const void* key, const int nkey,
                         int index, eitem *eitem,
                         item_attr *attrp, bool *created, uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = list_elem_insert(engine, key, nkey, index, (list_elem_item *)eitem,
                           attrp, created, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_list_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                         const void* key, const int nkey,
                         int from_index, int to_index, const bool drop_if_empty,
                         uint32_t* del_count, bool* dropped, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = list_elem_delete(engine, key, nkey, from_index, to_index,
                           drop_if_empty, del_count, dropped);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_list_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                      const void* key, const int nkey,
                      int from_index, int to_index,
                      const bool delete, const bool drop_if_empty,
                      eitem** eitem_array, uint32_t* eitem_count,
                      uint32_t* flags, bool* dropped, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    if (delete) ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = list_elem_get(engine, key, nkey, from_index, to_index,
                        delete, drop_if_empty,
                        (list_elem_item**)eitem_array, eitem_count,
                        flags, dropped);
    if (delete) ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

/*
 * Set Collection API
 */

static ENGINE_ERROR_CODE
default_set_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                          const void* key, const int nkey, item_attr *attrp,
                          uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = set_struct_create(engine, key, nkey, attrp, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_set_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                       const void* key, const int nkey,
                       const size_t nbytes, eitem** eitem)
{
    set_elem_item *elem;
    ENGINE_ERROR_CODE ret = ENGINE_EINVAL;

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    elem = set_elem_alloc(get_handle(handle), nbytes, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    if (elem != NULL) {
        *eitem = elem;
        ret = ENGINE_SUCCESS;
    } else {
        ret = ENGINE_ENOMEM;
    }
    return ret;
}

static void
default_set_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                         eitem **eitem_array, const int eitem_count)
{
    struct default_engine *engine = get_handle(handle);
    set_elem_release(engine, (set_elem_item**)eitem_array, eitem_count);
}

static ENGINE_ERROR_CODE
default_set_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey, eitem *eitem,
                        item_attr *attrp, bool *created, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = set_elem_insert(engine, key, nkey, (set_elem_item*)eitem,
                          attrp, created, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_set_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey,
                        const void* value, const int nbytes,
                        const bool drop_if_empty, bool *dropped,
                        uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = set_elem_delete(engine, key, nkey, value, nbytes,
                          drop_if_empty, dropped);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_set_elem_exist(ENGINE_HANDLE* handle, const void* cookie,
                       const void* key, const int nkey,
                       const void* value, const int nbytes,
                       bool *exist, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ret = set_elem_exist(engine, key, nkey, value, nbytes, exist);
    return ret;
}

static ENGINE_ERROR_CODE
default_set_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                     const void* key, const int nkey, const uint32_t count,
                     const bool delete, const bool drop_if_empty,
                     eitem** eitem, uint32_t* eitem_count,
                     uint32_t* flags, bool* dropped, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    if (delete) ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = set_elem_get(engine, key, nkey, count, delete, drop_if_empty,
                       (set_elem_item**)eitem, eitem_count, flags, dropped);
    if (delete) ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}


/*
 * Map Collection API
 */

static ENGINE_ERROR_CODE
default_map_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                          const void* key, const int nkey, item_attr *attrp,
                          uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = map_struct_create(engine, key, nkey, attrp, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_map_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                       const void* key, const int nkey, const size_t nfield,
                       const size_t nbytes, eitem** eitem)
{
    map_elem_item *elem;
    ENGINE_ERROR_CODE ret = ENGINE_EINVAL;

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    elem = map_elem_alloc(get_handle(handle), nfield, nbytes, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    if (elem != NULL) {
        *eitem = elem;
        ret = ENGINE_SUCCESS;
    } else {
        ret = ENGINE_ENOMEM;
    }
    return ret;
}

static void
default_map_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                         eitem **eitem_array, const int eitem_count)
{
    map_elem_release(get_handle(handle), (map_elem_item**)eitem_array, eitem_count);
}

static ENGINE_ERROR_CODE
default_map_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey, eitem *eitem,
                        item_attr *attrp, bool *created, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = map_elem_insert(engine, key, nkey, (map_elem_item*)eitem, attrp, created, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_map_elem_update(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey, const field_t *field,
                        const void* value, const int nbytes, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = map_elem_update(engine, key, nkey, field, value, nbytes, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_map_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                        const void* key, const int nkey, const int numfields,
                        const field_t *flist, const bool drop_if_empty,
                        uint32_t* del_count, bool *dropped, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = map_elem_delete(engine, key, nkey, numfields, flist, drop_if_empty, del_count, dropped);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_map_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                     const void* key, const int nkey, const int numfields,
                     const field_t *flist, const bool delete, const bool drop_if_empty,
                     eitem** eitem, uint32_t* eitem_count, uint32_t* flags,
                     bool* dropped, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    if (delete) ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = map_elem_get(engine, key, nkey, numfields, flist, delete, drop_if_empty,
                       (map_elem_item**)eitem, eitem_count, flags, dropped);
    if (delete) ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

/*
 * B+Tree Collection API
 */

static ENGINE_ERROR_CODE
default_btree_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                            const void* key, const int nkey, item_attr *attrp,
                            uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = btree_struct_create(engine, key, nkey, attrp, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_btree_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                         const void* key, const int nkey,
                         const size_t nbkey, const size_t neflag,
                         const size_t nbytes, eitem** eitem)
{
    struct default_engine *engine = get_handle(handle);
    btree_elem_item *elem;
    ENGINE_ERROR_CODE ret = ENGINE_EINVAL;

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    elem = btree_elem_alloc(engine, nbkey, neflag, nbytes, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    if (elem != NULL) {
        *eitem = elem;
        ret = ENGINE_SUCCESS;
    } else {
        ret = ENGINE_ENOMEM;
    }
    return ret;
}

static void
default_btree_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                           eitem **eitem_array, const int eitem_count)
{
    struct default_engine *engine = get_handle(handle);
    btree_elem_release(engine, (btree_elem_item**)eitem_array, eitem_count);
}

static ENGINE_ERROR_CODE
default_btree_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                          const void* key, const int nkey,
                          eitem *eitem, const bool replace_if_exist,
                          item_attr *attrp,
                          bool *replaced, bool *created,
                          eitem_result *trimmed, uint16_t vbucket)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    if (trimmed == NULL) {
        ret = btree_elem_insert(engine, key, nkey, (btree_elem_item *)eitem,
                                replace_if_exist, attrp, replaced, created,
                                NULL, NULL, NULL, cookie);
    } else {
        /* We use a separate trimmed_elems variable to fix compile warning of
         * "dereferencing type-punned pointer will break strict-aliasing rules".
         */
        btree_elem_item *trimmed_elems=NULL;
        ret = btree_elem_insert(engine, key, nkey, (btree_elem_item *)eitem,
                                replace_if_exist, attrp, replaced, created,
                                &trimmed_elems, &trimmed->count, &trimmed->flags,
                                cookie);
        trimmed->elems = trimmed_elems;
    }
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_btree_elem_update(ENGINE_HANDLE* handle, const void* cookie,
                          const void* key, const int nkey,
                          const bkey_range *bkrange, const eflag_update *eupdate,
                          const void* value, const int nbytes, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = btree_elem_update(engine, key, nkey, bkrange,
                            eupdate, value, nbytes, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_btree_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                          const void* key, const int nkey,
                          const bkey_range *bkrange, const eflag_filter *efilter,
                          const uint32_t req_count, const bool drop_if_empty,
                          uint32_t* del_count, uint32_t *access_count,
                          bool* dropped, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = btree_elem_delete(engine, key, nkey, bkrange, efilter, req_count,
                            drop_if_empty, del_count, access_count, dropped);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_btree_elem_arithmetic(ENGINE_HANDLE* handle, const void* cookie,
                              const void* key, const int nkey,
                              const bkey_range *bkrange,
                              const bool increment, const bool create,
                              const uint64_t delta, const uint64_t initial,
                              const eflag_t *eflagp,
                              uint64_t *result, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = btree_elem_arithmetic(engine, key, nkey, bkrange, increment, create,
                                delta, initial, eflagp, result, cookie);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_btree_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                       const void* key, const int nkey,
                       const bkey_range *bkrange, const eflag_filter *efilter,
                       const uint32_t offset, const uint32_t req_count,
                       const bool delete, const bool drop_if_empty,
                       eitem** eitem_array, uint32_t* eitem_count,
                       uint32_t *access_count, uint32_t* flags,
                       bool* dropped_trimmed, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    if (delete) ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = btree_elem_get(engine, key, nkey, bkrange, efilter,
                         offset, req_count, delete, drop_if_empty,
                         (btree_elem_item**)eitem_array, eitem_count,
                         access_count, flags, dropped_trimmed);
    if (delete) ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

static ENGINE_ERROR_CODE
default_btree_elem_count(ENGINE_HANDLE* handle, const void* cookie,
                         const void* key, const int nkey,
                         const bkey_range *bkrange, const eflag_filter *efilter,
                         uint32_t* eitem_count, uint32_t* access_count,
                         uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ret = btree_elem_count(engine, key, nkey, bkrange, efilter,
                           eitem_count, access_count);
    return ret;
}

static ENGINE_ERROR_CODE
default_btree_posi_find(ENGINE_HANDLE* handle, const void* cookie,
                        const char *key, const size_t nkey,
                        const bkey_range *bkrange,
                        ENGINE_BTREE_ORDER order,
                        int *position, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ret = btree_posi_find(engine, key, nkey, bkrange, order, position);
    return ret;
}

static ENGINE_ERROR_CODE
default_btree_posi_find_with_get(ENGINE_HANDLE* handle, const void* cookie,
                                 const char *key, const size_t nkey,
                                 const bkey_range *bkrange,
                                 ENGINE_BTREE_ORDER order, const uint32_t count,
                                 int *position, eitem **eitem_array,
                                 uint32_t *eitem_count, uint32_t *eitem_index,
                                 uint32_t *flags, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ret = btree_posi_find_with_get(engine, key, nkey, bkrange, order, count,
                                   position, (btree_elem_item**)eitem_array,
                                   eitem_count, eitem_index, flags);
    return ret;
}

static ENGINE_ERROR_CODE
default_btree_elem_get_by_posi(ENGINE_HANDLE* handle, const void* cookie,
                               const char *key, const size_t nkey,
                               ENGINE_BTREE_ORDER order,
                               int from_posi, int to_posi,
                               eitem **eitem_array, uint32_t *eitem_count,
                               uint32_t *flags, uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ret = btree_elem_get_by_posi(engine, key, nkey, order, from_posi, to_posi,
                                 (btree_elem_item**)eitem_array, eitem_count,
                                 flags);
    return ret;
}

#ifdef SUPPORT_BOP_SMGET
#ifdef JHPARK_OLD_SMGET_INTERFACE
/* smget old interface */
static ENGINE_ERROR_CODE
default_btree_elem_smget_old(ENGINE_HANDLE* handle, const void* cookie,
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
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ret = btree_elem_smget_old(engine, karray, kcount, bkrange, efilter,
                               offset, count, (btree_elem_item**)eitem_array,
                               kfnd_array, flag_array, eitem_count,
                               missed_key_array, missed_key_count,
                               trimmed, duplicated);
    return ret;
}
#endif

/* smget new interface */
static ENGINE_ERROR_CODE
default_btree_elem_smget(ENGINE_HANDLE* handle, const void* cookie,
                         token_t *karray, const int kcount,
                         const bkey_range *bkrange,
                         const eflag_filter *efilter,
                         const uint32_t offset, const uint32_t count,
                         const bool unique, smget_result_t *result,
                         uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ret = btree_elem_smget(engine, karray, kcount, bkrange, efilter,
                           offset, count, unique, result);
    return ret;
}
#endif

/*
 * Item Attribute API
 */

static ENGINE_ERROR_CODE
default_getattr(ENGINE_HANDLE* handle, const void* cookie,
                const void* key, const int nkey,
                ENGINE_ITEM_ATTR *attr_ids,
                const uint32_t attr_count, item_attr *attr_data,
                uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ret = item_getattr(engine, key, nkey, attr_ids, attr_count, attr_data);
    return ret;
}

static ENGINE_ERROR_CODE
default_setattr(ENGINE_HANDLE* handle, const void* cookie,
                const void* key, const int nkey,
                ENGINE_ITEM_ATTR *attr_ids,
                const uint32_t attr_count, item_attr *attr_data,
                uint16_t vbucket)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;
    VBUCKET_GUARD(engine, vbucket);

    ACTION_BEFORE_WRITE(cookie, key, nkey);
    ret = item_setattr(engine, key, nkey, attr_ids, attr_count, attr_data);
    ACTION_AFTER_WRITE(cookie, ret);
    return ret;
}

/*
 * Stats API
 */

static void stats_engine(struct default_engine *engine,
                         ADD_STAT add_stat, const void *cookie)
{
    char val[128];
    int len;

    pthread_mutex_lock(&engine->stats.lock);
    len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.evictions);
    add_stat("evictions", 9, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.outofmemorys);
    add_stat("outofmemorys", 12, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)engine->assoc.tot_prefix_items);
    add_stat("curr_prefixes", 13, val, len, cookie);
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
}

static void stats_vbucket(struct default_engine *engine,
                          ADD_STAT add_stat, const void *cookie)
{
    for (int i = 0; i < NUM_VBUCKETS; i++) {
        enum vbucket_state state = get_vbucket_state(engine, i);
        if (state != VBUCKET_STATE_DEAD) {
            char buf[16];
            snprintf(buf, sizeof(buf), "vb_%d", i);
            const char * state_name = vbucket_state_name(state);
            add_stat(buf, strlen(buf), state_name, strlen(state_name), cookie);
        }
    }
}

static ENGINE_ERROR_CODE
default_get_stats(ENGINE_HANDLE* handle, const void* cookie,
                  const char* stat_key, int nkey, ADD_STAT add_stat)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (stat_key == NULL) {
        stats_engine(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "slabs", 5) == 0) {
        slabs_stats(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "items", 5) == 0) {
        item_stats(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "sizes", 5) == 0) {
        item_stats_sizes(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "vbucket", 7) == 0) {
        stats_vbucket(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "scrub", 5) == 0) {
        item_stats_scrub(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "dump", 4) == 0) {
        item_stats_dump(engine, add_stat, cookie);
    }
    else {
        ret = ENGINE_KEY_ENOENT;
    }
    return ret;
}

static void
default_reset_stats(ENGINE_HANDLE* handle, const void *cookie)
{
    struct default_engine *engine = get_handle(handle);
    item_stats_reset(engine);

    pthread_mutex_lock(&engine->stats.lock);
    engine->stats.evictions = 0;
    engine->stats.reclaimed = 0;
    engine->stats.outofmemorys = 0;
    engine->stats.total_items = 0;
    pthread_mutex_unlock(&engine->stats.lock);
}

static ENGINE_ERROR_CODE
default_get_prefix_stats(ENGINE_HANDLE* handle, const void* cookie,
                         const void* key, const int nkey, void *prefix_data)
{
    struct default_engine *engine = get_handle(handle);
    ENGINE_ERROR_CODE ret;

    ret = assoc_get_prefix_stats(engine, key, nkey, prefix_data);
    return ret;
}

/*
 * Dump API
 */

static char *
default_cachedump(ENGINE_HANDLE* handle, const void* cookie,
                  const unsigned int slabs_clsid, const unsigned int limit,
                  const bool forward, const bool sticky, unsigned int *bytes)
{
    struct default_engine* engine = get_handle(handle);
    return item_cachedump(engine, slabs_clsid, limit, forward, sticky, bytes);
}

static ENGINE_ERROR_CODE
default_dump(ENGINE_HANDLE* handle, const void* cookie,
             const char *opstr, const char *modestr,
             const char *prefix, const int nprefix, const char *filepath)
{
    struct default_engine* engine = get_handle(handle);

    if (memcmp(opstr, "start", 5) == 0) {
        if (memcmp(modestr, "key", 3) != 0) {
            return ENGINE_ENOTSUP;
        }
        if (item_start_dump(engine, DUMP_MODE_KEY, prefix, nprefix, filepath) != 0) {
            return ENGINE_FAILED;
        }
    }
    else if (memcmp(opstr, "stop", 4) == 0) {
        item_stop_dump(engine);
    }
    else {
        return ENGINE_ENOTSUP;
    }
    return ENGINE_SUCCESS;
}

/*
 * Config API
 */
static ENGINE_ERROR_CODE
default_set_config(ENGINE_HANDLE* handle, const void* cookie,
                   const char* config_key, void* config_value)
{
    struct default_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (strcmp(config_key, "memlimit") == 0) {
        size_t new_maxbytes = *(size_t*)config_value;
        pthread_mutex_lock(&engine->cache_lock);
        if (new_maxbytes >= engine->config.sticky_limit) {
            ret = slabs_set_memlimit(engine, new_maxbytes);
            if (ret == ENGINE_SUCCESS) {
                engine->config.maxbytes = new_maxbytes;
            }
        } else {
            ret = ENGINE_EBADVALUE;
        }
        pthread_mutex_unlock(&engine->cache_lock);
    }
#ifdef ENABLE_STICKY_ITEM
    else if (strcmp(config_key, "sticky_limit") == 0) {
        size_t new_sticky_limit = *(size_t*)config_value;
        pthread_mutex_lock(&engine->cache_lock);
        if (new_sticky_limit >= engine->stats.sticky_bytes &&
            new_sticky_limit <= engine->config.maxbytes) {
            engine->config.sticky_limit = new_sticky_limit;
        } else {
            ret = ENGINE_EBADVALUE;
        }
        pthread_mutex_unlock(&engine->cache_lock);
    }
#endif
    else if (strcmp(config_key, "max_list_size") == 0) {
        ret = item_conf_set_maxcollsize(engine, ITEM_TYPE_LIST, (int*)config_value);
    }
    else if (strcmp(config_key, "max_set_size") == 0) {
        ret = item_conf_set_maxcollsize(engine, ITEM_TYPE_SET, (int*)config_value);
    }
    else if (strcmp(config_key, "max_map_size") == 0) {
        ret = item_conf_set_maxcollsize(engine, ITEM_TYPE_MAP, (int*)config_value);
    }
    else if (strcmp(config_key, "max_btree_size") == 0) {
        ret = item_conf_set_maxcollsize(engine, ITEM_TYPE_BTREE, (int*)config_value);
    }
    else if (strcmp(config_key, "verbosity") == 0) {
        pthread_mutex_lock(&engine->cache_lock);
        engine->config.verbose = *(size_t*)config_value;
        pthread_mutex_unlock(&engine->cache_lock);
    }
    else {
        ret = ENGINE_ENOTSUP;
    }
    return ret;
}

/*
 * Unknown Command API
 */

static protocol_binary_response_status
scrub_cmd(struct default_engine *e, protocol_binary_request_header *request,
          const char **msg)
{
    bool res; /* true or false */
    if (request->request.opcode == PROTOCOL_BINARY_CMD_SCRUB) {
        res = item_start_scrub(e, SCRUB_MODE_NORMAL, false);
    } else { /* PROTOCOL_BINARY_CMD_SCRUB_STALE */
#ifdef ENABLE_CLUSTER_AWARE
        if (! e->server.core->is_zk_integrated())
#endif
        {
            return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
        }
        res = item_start_scrub(e, SCRUB_MODE_STALE, false);
    }
    return (res) ? PROTOCOL_BINARY_RESPONSE_SUCCESS : PROTOCOL_BINARY_RESPONSE_EBUSY;
}

static protocol_binary_response_status
set_vbucket(struct default_engine *e, protocol_binary_request_header *request,
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

static protocol_binary_response_status
get_vbucket(struct default_engine *e, protocol_binary_request_header *request,
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

static protocol_binary_response_status
rm_vbucket(struct default_engine *e, protocol_binary_request_header *request,
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

static ENGINE_ERROR_CODE
default_unknown_command(ENGINE_HANDLE* handle, const void* cookie,
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

static ENGINE_ERROR_CODE
default_scrub_stale(ENGINE_HANDLE* handle)
{
    struct default_engine* e = get_handle(handle);
    int res = item_start_scrub(e, SCRUB_MODE_STALE, true);
    return (res) ? ENGINE_SUCCESS : ENGINE_FAILED;
}

/* Item/Elem Info */

static bool
get_item_info(ENGINE_HANDLE *handle, const void *cookie,
              const item* item, item_info *item_info)
{
    hash_item* it = (hash_item*)item;

    item_info->cas = item_get_cas(it);
    item_info->flags = it->flags;
    item_info->exptime = it->exptime;
    item_info->clsid = it->slabs_clsid;
    item_info->nkey = it->nkey;
    item_info->nbytes = it->nbytes;
    item_info->nvalue = it->nbytes;
    item_info->naddnl = 0;
    item_info->key = item_get_key(it);
    item_info->value = item_get_data(it);
    item_info->addnl = NULL;
    return true;
}

static void
get_elem_info(ENGINE_HANDLE *handle, const void *cookie,
              const int type, /* collection type */
              const eitem* eitem, eitem_info *elem_info)
{
    if (type == ITEM_TYPE_LIST) {
        list_elem_item *elem = (list_elem_item*)eitem;
        elem_info->nbytes = elem->nbytes;
        elem_info->nvalue = elem->nbytes;
        elem_info->naddnl = 0;
        elem_info->value = elem->value;
        elem_info->addnl = NULL;
    }
    else if (type == ITEM_TYPE_SET) {
        set_elem_item *elem = (set_elem_item*)eitem;
        elem_info->nbytes = elem->nbytes;
        elem_info->nvalue = elem->nbytes;
        elem_info->naddnl = 0;
        elem_info->value = elem->value;
        elem_info->addnl = NULL;
    }
    else if (type == ITEM_TYPE_MAP) {
        map_elem_item *elem = (map_elem_item*)eitem;
        elem_info->nscore = elem->nfield;
        elem_info->nbytes = elem->nbytes;
        elem_info->nvalue = elem->nbytes;
        elem_info->naddnl = 0;
        elem_info->score = elem->data;
        elem_info->value = (const char*)elem->data + elem->nfield;
        elem_info->addnl = NULL;
    }
    else if (type == ITEM_TYPE_BTREE) {
        btree_elem_item *elem = (btree_elem_item*)eitem;
        elem_info->nscore = elem->nbkey;
        elem_info->neflag = elem->neflag;
        elem_info->nbytes = elem->nbytes;
        elem_info->nvalue = elem->nbytes;
        elem_info->naddnl = 0;
        elem_info->score = elem->data;
        if (elem->neflag > 0) {
            elem_info->eflag = elem->data
                             + (elem->nbkey==0 ? sizeof(uint64_t) : elem->nbkey);
            elem_info->value = (const char*)elem_info->eflag + elem->neflag;
        } else {
            elem_info->eflag = NULL;
            elem_info->value = (const char*)elem->data
                             + (elem->nbkey==0 ? sizeof(uint64_t) : elem->nbkey);
        }
        elem_info->addnl = NULL;
    }
}

ENGINE_ERROR_CODE
create_instance(uint64_t interface, GET_SERVER_API get_server_api,
                ENGINE_HANDLE **handle)
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
         /* Engine API */
         .get_info          = default_get_info,
         .initialize        = default_initialize,
         .destroy           = default_destroy,
         /* Item API */
         .allocate          = default_item_allocate,
         .remove            = default_item_delete,
         .release           = default_item_release,
         .get               = default_get,
         .store             = default_store,
         .arithmetic        = default_arithmetic,
         .flush             = default_flush,
         /* LIST Collection API */
         .list_struct_create = default_list_struct_create,
         .list_elem_alloc   = default_list_elem_alloc,
         .list_elem_release = default_list_elem_release,
         .list_elem_insert  = default_list_elem_insert,
         .list_elem_delete  = default_list_elem_delete,
         .list_elem_get     = default_list_elem_get,
         /* SET Colleciton API */
         .set_struct_create = default_set_struct_create,
         .set_elem_alloc    = default_set_elem_alloc,
         .set_elem_release  = default_set_elem_release,
         .set_elem_insert   = default_set_elem_insert,
         .set_elem_delete   = default_set_elem_delete,
         .set_elem_exist    = default_set_elem_exist,
         .set_elem_get      = default_set_elem_get,
         /* MAP Collection API */
         .map_struct_create = default_map_struct_create,
         .map_elem_alloc    = default_map_elem_alloc,
         .map_elem_release  = default_map_elem_release,
         .map_elem_insert   = default_map_elem_insert,
         .map_elem_update   = default_map_elem_update,
         .map_elem_delete   = default_map_elem_delete,
         .map_elem_get      = default_map_elem_get,
         /* B+Tree Collection API */
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
#ifdef JHPARK_OLD_SMGET_INTERFACE
         .btree_elem_smget_old = default_btree_elem_smget_old,
#endif
         .btree_elem_smget   = default_btree_elem_smget,
#endif
         /* Attributes API */
         .getattr          = default_getattr,
         .setattr          = default_setattr,
         /* Stats API */
         .get_stats        = default_get_stats,
         .reset_stats      = default_reset_stats,
         .get_prefix_stats = default_get_prefix_stats,
         /* Dump API */
         .cachedump        = default_cachedump,
         .dump             = default_dump,
         /* Config API */
         .set_config       = default_set_config,
         /* Unknown Command API */
         .unknown_command  = default_unknown_command,
         /* Scrub stale API */
         .scrub_stale      = default_scrub_stale,
         /* Info API */
         .get_item_info    = get_item_info,
         .get_elem_info    = get_elem_info
      },
      .server = *api,
      .get_server_api = get_server_api,
      .initialized = true,
      .assoc = {
         .hashpower = 17, /* (1<<17) => 128K hash size */
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
      .scrubber = {
         .lock = PTHREAD_MUTEX_INITIALIZER,
         .enabled = true,
         .running = false,
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
