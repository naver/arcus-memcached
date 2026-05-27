/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <sched.h>
#include <inttypes.h>

/* Dummy PERSISTENCE_ACTION Macros */
#define PERSISTENCE_ACTION_BEGIN(a, b)
#define PERSISTENCE_ACTION_END(a)

#include "default_engine.h"
#include "item_clog.h"
#include "hash_tree.h"

static struct default_engine *engine=NULL;
static struct engine_config  *config=NULL; // engine config
static EXTENSION_LOGGER_DESCRIPTOR *logger;

/* used by set and map collection */
extern int genhash_string_hash(const void* p, size_t nkey);

/* Cache Lock */
static inline void LOCK_CACHE(void)
{
    pthread_mutex_lock(&engine->cache_lock);
}

static inline void UNLOCK_CACHE(void)
{
    pthread_mutex_unlock(&engine->cache_lock);
}

static const void *map_get_key(const htree_elem_item *elem, uint16_t *nkey) {
    *nkey = (uint16_t)((const map_elem_item *)elem)->nfield;
    return ((const map_elem_item *)elem)->data;
}

static htree_ops map_htree_ops = {
    .get_key = map_get_key,
};

static inline uint32_t do_map_elem_ntotal(map_elem_item *elem)
{
    return sizeof(map_elem_item) + elem->nfield + elem->nbytes;
}

static ENGINE_ERROR_CODE do_map_item_find(const void *key, const uint32_t nkey,
                                          bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_MAP_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static int32_t do_map_real_maxcount(int32_t maxcount)
{
    int32_t real_maxcount = maxcount;

    if (maxcount < 0) {
        /* It has the max_map_size that can be increased in the future */
        real_maxcount = -1;
    } else if (maxcount == 0) {
        real_maxcount = DEFAULT_MAP_SIZE;
    } else if (maxcount > config->max_map_size) {
        real_maxcount = config->max_map_size;
    }
    return real_maxcount;
}

static hash_item *do_map_item_alloc(const void *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    uint32_t nbytes = 2; /* "\r\n" */
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(map_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_MAP;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), "\r\n", nbytes);

        /* initialize map meta information */
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        info->mcnt = do_map_real_maxcount(attrp->maxcount);
        info->ccnt = 0;
        info->ovflact = OVFL_ERROR;
        info->mflags  = 0;
#ifdef ENABLE_STICKY_ITEM
        if (IS_STICKY_EXPTIME(attrp->exptime)) info->mflags |= COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1)              info->mflags |= COLL_META_FLAG_READABLE;
        info->itdist  = (uint16_t)((size_t*)info-(size_t*)it);
        info->stotal  = 0;
        info->root    = NULL;
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);
    }
    return it;
}

static map_elem_item *do_map_elem_alloc(const int nfield,
                                        const uint32_t nbytes, const void *cookie)
{
    size_t ntotal = sizeof(map_elem_item) + nfield + nbytes;

    map_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        elem->slabs_clsid = slabs_clsid(ntotal);
        assert(elem->slabs_clsid > 0);

        elem->refcount    = 0;
        elem->nfield      = (uint8_t)nfield;
        elem->nbytes      = (uint16_t)nbytes;
        elem->status = ELEM_STATUS_UNLINKED; /* unlinked state */
    }
    return elem;
}

static void do_map_elem_free(map_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = do_map_elem_ntotal(elem);
    do_item_mem_free(elem, ntotal);
}

static void do_map_elem_release(map_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->status == ELEM_STATUS_UNLINKED) {
        do_map_elem_free(elem);
    }
}

static void do_map_elem_replace(map_meta_info *info,
                                htree_elem_pos *pos,
                                map_elem_item *new_elem)
{
    map_elem_item *old_elem = (pos->prev != NULL)
                            ? (map_elem_item *)pos->prev->next
                            : (map_elem_item *)pos->node->htab[pos->hidx];

    ssize_t space_delta = (ssize_t)slabs_space_size(do_map_elem_ntotal(new_elem))
                        - (ssize_t)slabs_space_size(do_map_elem_ntotal(old_elem));

    CLOG_MAP_ELEM_INSERT(info, old_elem, new_elem);

    htree_elem_replace_at(pos, (htree_elem_item *)new_elem);
    new_elem->status = ELEM_STATUS_LINKED;
    old_elem->status = ELEM_STATUS_UNLINKED;

    if (old_elem->refcount == 0)
        do_map_elem_free(old_elem);

    if (space_delta > 0)
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)space_delta);
    else if (space_delta < 0)
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)-space_delta);

}

static ENGINE_ERROR_CODE do_map_elem_link(map_meta_info *info, map_elem_item *elem,
                                          const void *cookie)
{
    assert(info->ovflact == OVFL_ERROR);
    int real_mcnt = (int)(info->mcnt > 0 ? info->mcnt : config->max_map_size);
    ssize_t space_delta;

#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_COLLFLG(info) && do_item_sticky_overflowed())
        return ENGINE_ENOMEM;
#endif
    if (real_mcnt > 0 && (int)info->ccnt >= real_mcnt)
        return ENGINE_EOVERFLOW;

    ENGINE_ERROR_CODE ret = htree_elem_link((htree_node **)&info->root,
                                            (htree_elem_item *)elem,
                                            &map_htree_ops,
                                            &space_delta, cookie);
    if (ret != ENGINE_SUCCESS)
        return ret;

    elem->status = ELEM_STATUS_LINKED;
    space_delta += (ssize_t)slabs_space_size(do_map_elem_ntotal(elem));
    CLOG_MAP_ELEM_INSERT(info, NULL, elem);

    info->ccnt++;
    do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)space_delta);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_map_elem_update(map_meta_info *info,
                                            const field_t *field, const char *value,
                                            const uint32_t nbytes, const void *cookie)
{
    htree_elem_pos pos;
    map_elem_item *old_elem = (map_elem_item *)htree_elem_find((htree_node *)info->root,
                                                               field->value, field->length,
                                                               &map_htree_ops, &pos);
    if (old_elem == NULL)
        return ENGINE_ELEM_ENOENT;

    /* in-place: same size, overwrite value without alloc */
    if (old_elem->refcount == 0 && old_elem->nbytes == (uint16_t)nbytes) {
        memcpy(old_elem->data + field->length, value, nbytes);
        CLOG_MAP_ELEM_INSERT(info, old_elem, old_elem);
        return ENGINE_SUCCESS;
    }

    /* chain-replace: different size or elem in use */
#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_COLLFLG(info) && old_elem->nbytes < (uint16_t)nbytes
        && do_item_sticky_overflowed())
        return ENGINE_ENOMEM;
#endif
    map_elem_item *new_elem = do_map_elem_alloc(field->length, nbytes, cookie);
    if (new_elem == NULL)
        return ENGINE_ENOMEM;
    memcpy(new_elem->data, field->value, field->length);
    memcpy(new_elem->data + field->length, value, nbytes);

    do_map_elem_replace(info, &pos, new_elem);
    return ENGINE_SUCCESS;
}

static inline ssize_t do_map_elem_unlink_process(map_meta_info *info, map_elem_item *e)
{
    e->status = ELEM_STATUS_UNLINKED;
    CLOG_MAP_ELEM_DELETE(info, e, ELEM_DELETE_NORMAL);
    return (ssize_t)slabs_space_size(do_map_elem_ntotal(e));
}

static map_elem_item *do_map_elem_unlink_by_field(map_meta_info *info,
                                                  const char *field, int nfield,
                                                  ssize_t *delta)
{
    htree_elem_item *unlinked = htree_elem_unlink((htree_node **)&info->root, field, nfield,
                                                  &map_htree_ops, delta);
    if (unlinked == NULL)
        return NULL;

    map_elem_item *elem = (map_elem_item *)unlinked;
    *delta -= do_map_elem_unlink_process(info, elem);

    return elem;
}

static ENGINE_ERROR_CODE do_map_elem_delete_by_field(map_meta_info *info,
                                                     const field_t *field)
{
    if (info->root == NULL)
        return ENGINE_ELEM_ENOENT;

    ssize_t delta;
    map_elem_item *elem = do_map_elem_unlink_by_field(info, field->value, field->length, &delta);
    if (elem == NULL)
        return ENGINE_ELEM_ENOENT;

    do_map_elem_release(elem);
    info->ccnt--;
    do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)-delta);

    return ENGINE_SUCCESS;
}

static uint32_t do_map_elem_delete_by_fields(map_meta_info *info,
                                              const int numfields, const field_t *flist)
{
    assert(info->root && numfields > 0);
    uint32_t fcnt = 0;
    ssize_t space_delta = 0;

    CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, numfields, ELEM_DELETE_NORMAL);

    for (int ii = 0; ii < numfields; ii++) {
        ssize_t delta;
        map_elem_item *elem = do_map_elem_unlink_by_field(info, flist[ii].value,
                                                          flist[ii].length,
                                                          &delta);
        if (elem == NULL) continue;
        do_map_elem_release(elem);
        space_delta += delta;
        fcnt++;
    }

    info->ccnt -= fcnt;
    do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)-space_delta);

    CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);

    return fcnt;
}

static uint32_t do_map_elem_delete_all(map_meta_info *info)
{
    assert(info->root);
    ssize_t space_delta;
    uint32_t fcnt = 0;

    CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, 0, ELEM_DELETE_NORMAL);

    htree_elem_item *head = htree_elem_unlink_by_cnt((htree_node **)&info->root, 0, &space_delta);
    for (htree_elem_item *cur = head; cur != NULL; ) {
        htree_elem_item *next = cur->next;
        map_elem_item *e = (map_elem_item *)cur;
        space_delta -= do_map_elem_unlink_process(info, e);
        do_map_elem_release(e);
        fcnt++;
        cur = next;
    }

    info->ccnt -= fcnt;
    do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)-space_delta);

    CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);

    return fcnt;
}

static uint32_t do_map_elem_get_by_fields(map_meta_info *info,
                                          const int numfields, const field_t *flist,
                                          const bool delete, map_elem_item **elem_array)
{
    assert(info->root && numfields > 0 && elem_array != NULL);
    uint32_t fcnt = 0;
    ssize_t space_delta = 0;

    if (delete) {
        CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, numfields, ELEM_DELETE_NORMAL);
    }
    for (int ii = 0; ii < numfields; ii++) {
        if (delete) {
            ssize_t delta;
            map_elem_item *elem = do_map_elem_unlink_by_field(info, flist[ii].value,
                                                              flist[ii].length, &delta);
            if (elem == NULL) continue;
            elem_array[fcnt] = elem;
            space_delta += delta;
        } else {
            map_elem_item *elem = (map_elem_item *)htree_elem_find((htree_node *)info->root,
                                                                   flist[ii].value, flist[ii].length,
                                                                   &map_htree_ops, NULL);
            if (elem == NULL)
                continue;
            elem->refcount++;
            elem_array[fcnt] = elem;
        }
        fcnt++;
    }
    if (delete) {
        info->ccnt -= fcnt;
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)-space_delta);
        CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);
    }
    return fcnt;
}

static uint32_t do_map_elem_get_all(map_meta_info *info,
                                    const bool delete, map_elem_item **elem_array)
{
    assert(info->root && elem_array != NULL);
    ssize_t space_delta;
    uint32_t fcnt;

    if (delete) {
        CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, 0, ELEM_DELETE_NORMAL);

        fcnt = htree_elem_get_by_cnt((htree_node **)&info->root, 0,
                                     (htree_elem_item **)elem_array, true, &space_delta);
        for (uint32_t i = 0; i < fcnt; i++) {
            map_elem_item *e = (map_elem_item *)elem_array[i];
            space_delta -= do_map_elem_unlink_process(info, e);
            elem_array[i] = e;
        }
        info->ccnt -= fcnt;
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (size_t)-space_delta);

        CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);
    } else {
        fcnt = htree_elem_get_by_cnt((htree_node **)&info->root, 0,
                                     (htree_elem_item **)elem_array, false, NULL);
        for (uint32_t i = 0; i < fcnt; i++) {
            map_elem_item *e = (map_elem_item *)elem_array[i];
            e->refcount++;
            elem_array[i] = e;
        }
    }
    return fcnt;
}

static ENGINE_ERROR_CODE do_map_elem_insert(hash_item *it, map_elem_item *elem,
                                            const bool replace_if_exist, bool *replaced,
                                            const void *cookie)
{
    map_meta_info *info = (map_meta_info *)item_get_meta(it);

    htree_elem_pos pos;
    map_elem_item *old_elem = (map_elem_item *)htree_elem_find((htree_node *)info->root,
                                                               elem->data, elem->nfield,
                                                               &map_htree_ops, &pos);

    if (replaced) *replaced = false;

    if (old_elem == NULL)
        return do_map_elem_link(info, elem, cookie);

    if (!replace_if_exist)
        return ENGINE_ELEM_EEXISTS;

#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_COLLFLG(info) && do_map_elem_ntotal(old_elem) < do_map_elem_ntotal(elem)
        && do_item_sticky_overflowed())
        return ENGINE_ENOMEM;
#endif

    do_map_elem_replace(info, &pos, elem);
    if (replaced) *replaced = true;
    return ENGINE_SUCCESS;
}

/*
 * MAP Interface Functions
 */
ENGINE_ERROR_CODE map_struct_create(const char *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_MAP_CREATE);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_map_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            do_item_release(it);
        }
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

map_elem_item *map_elem_alloc(const int nfield, const uint32_t nbytes, const void *cookie)
{
    map_elem_item *elem;
    LOCK_CACHE();
    elem = do_map_elem_alloc(nfield, nbytes, cookie);
    UNLOCK_CACHE();
    return elem;
}

void map_elem_free(map_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->status == ELEM_STATUS_UNLINKED);
    do_map_elem_free(elem);
    UNLOCK_CACHE();
}

void map_elem_release(map_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    LOCK_CACHE();
    while (cnt < elem_count) {
        do_map_elem_release(elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            UNLOCK_CACHE();
            LOCK_CACHE();
        }
    }
    UNLOCK_CACHE();
}

ENGINE_ERROR_CODE map_elem_insert(const char *key, const uint32_t nkey,
                                  map_elem_item *elem, const bool replace_if_exist,
                                  item_attr *attrp, bool *replaced, bool *created,
                                  const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_MAP_ELEM_INSERT);

    *created = false;

    LOCK_CACHE();
    ret = do_map_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_map_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            if (ret == ENGINE_SUCCESS) {
                *created = true;
            }
        }
    }
    if (ret == ENGINE_SUCCESS) {
        ret = do_map_elem_insert(it, elem, replace_if_exist, replaced, cookie);
        if (ret != ENGINE_SUCCESS && *created) {
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
        }
    }
    if (it) {
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE map_elem_update(const char *key, const uint32_t nkey,
                                  const field_t *field, const char *value,
                                  const uint32_t nbytes, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_MAP_ELEM_INSERT);

    LOCK_CACHE();
    ret = do_map_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        ret = do_map_elem_update(info, field, value, nbytes, cookie);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE map_elem_delete(const char *key, const uint32_t nkey,
                                  const int numfields, const field_t *flist,
                                  const bool drop_if_empty,
                                  uint32_t *del_count, bool *dropped,
                                  const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_MAP_ELEM_DELETE_DROP
                                                    : UPD_MAP_ELEM_DELETE));

    *dropped = false;

    LOCK_CACHE();
    ret = do_map_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        if (info->root != NULL) {
            if (numfields == 0)
                *del_count = do_map_elem_delete_all(info);
            else
                *del_count = do_map_elem_delete_by_fields(info, numfields, flist);
        }
        if (*del_count > 0) {
            if (info->ccnt == 0 && drop_if_empty) {
                assert(info->root == NULL);
                do_item_unlink(it, ITEM_UNLINK_NORMAL);
                *dropped = true;
            }
        } else {
            ret = ENGINE_ELEM_ENOENT;
        }
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE map_elem_get(const char *key, const uint32_t nkey,
                               const int numfields, const field_t *flist,
                               const bool delete, const bool drop_if_empty,
                               struct elems_result *eresult,
                               const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    if (delete) {
        PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_MAP_ELEM_DELETE_DROP
                                                        : UPD_MAP_ELEM_DELETE));
    }

    LOCK_CACHE();
    ret = do_map_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt <= 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if (numfields == 0 || info->ccnt < numfields) {
                eresult->elem_array = (eitem **)malloc(info->ccnt * sizeof(eitem*));
            } else {
                eresult->elem_array = (eitem **)malloc(numfields * sizeof(eitem*));
            }
            if (eresult->elem_array == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            if (numfields == 0) {
                eresult->elem_count = do_map_elem_get_all(info, delete,
                                                          (map_elem_item **)eresult->elem_array);
            } else {
                eresult->elem_count = do_map_elem_get_by_fields(info, numfields, flist, delete,
                                                                (map_elem_item **)eresult->elem_array);
            }
            if (eresult->elem_count > 0) {
                if (info->ccnt == 0 && drop_if_empty) {
                    assert(delete == true);
                    do_item_unlink(it, ITEM_UNLINK_NORMAL);
                    eresult->dropped = true;
                } else {
                    eresult->dropped = false;
                }
                eresult->flags = it->flags;
            } else {
                ret = ENGINE_ELEM_ENOENT;
                free(eresult->elem_array);
                eresult->elem_array = NULL;
            }
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    if (delete) {
        PERSISTENCE_ACTION_END(ret);
    }
    return ret;
}

uint32_t map_elem_delete_with_count(map_meta_info *info, const uint32_t count)
{
    uint32_t fcnt = 0;
    if (info->root != NULL) {
        htree_elem_item *head = htree_elem_unlink_by_cnt((htree_node **)&info->root, count, NULL);
        for (htree_elem_item *cur = head; cur != NULL; ) {
            htree_elem_item *next = cur->next;
            map_elem_item *e = (map_elem_item *)cur;
            e->status = ELEM_STATUS_UNLINKED;
            do_map_elem_release(e);
            cur = next;
            fcnt++;
        }
    }
    return fcnt;
}

void map_elem_get_all(map_meta_info *info, elems_result_t *eresult)
{
    assert(eresult->elem_arrsz >= info->ccnt && eresult->elem_count == 0);
    eresult->elem_count = do_map_elem_get_all(info, false,
                                              (map_elem_item **)eresult->elem_array);
    assert(eresult->elem_count == info->ccnt);
}

uint32_t map_elem_ntotal(map_elem_item *elem)
{
    return do_map_elem_ntotal(elem);
}

ENGINE_ERROR_CODE map_coll_getattr(hash_item *it, item_attr *attrp,
                                   ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    map_meta_info *info = (map_meta_info *)item_get_meta(it);

    /* check attribute validation */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXBKEYRANGE || attr_ids[i] == ATTR_TRIMMED) {
            return ENGINE_EBADATTR;
        }
    }

    /* get collection attributes */
    attrp->count = info->ccnt;
    attrp->maxcount = (info->mcnt > 0) ? info->mcnt : (int32_t)config->max_map_size;
    attrp->ovflaction = info->ovflact;
    attrp->readable = ((info->mflags & COLL_META_FLAG_READABLE) != 0) ? 1 : 0;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE map_coll_setattr(hash_item *it, item_attr *attrp,
                                   ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    map_meta_info *info = (map_meta_info *)item_get_meta(it);

    /* check the validity of given attributs */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            attrp->maxcount = do_map_real_maxcount(attrp->maxcount);
            if (attrp->maxcount > 0 && attrp->maxcount < info->ccnt) {
                return ENGINE_EBADVALUE;
            }
        } else if (attr_ids[i] == ATTR_OVFLACTION) {
            if (attrp->ovflaction != OVFL_ERROR) {
                return ENGINE_EBADVALUE;
            }
        } else if (attr_ids[i] == ATTR_READABLE) {
            if (attrp->readable != 1) {
                return ENGINE_EBADVALUE;
            }
        }
    }

    /* set the attributes */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            info->mcnt = attrp->maxcount;
        } else if (attr_ids[i] == ATTR_OVFLACTION) {
            info->ovflact = attrp->ovflaction;
        } else if (attr_ids[i] == ATTR_READABLE) {
            info->mflags |= COLL_META_FLAG_READABLE;
        }
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE map_apply_item_link(void *engine, const char *key, const uint32_t nkey,
                                      item_attr *attrp)
{
    hash_item *old_it;
    hash_item *new_it;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "map_apply_item_link. key=%.*s nkey=%u\n",
                PRINT_NKEY(nkey), key, nkey);

    LOCK_CACHE();
    old_it = do_item_get(key, nkey, DONT_UPDATE);
    if (old_it) {
        /* Remove the old item first. */
        do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
        do_item_release(old_it);
    }
    new_it = do_map_item_alloc(key, nkey, attrp, NULL); /* cookie is NULL */
    if (new_it) {
        /* Link the new item into the hash table */
        ret = do_item_link(new_it);
        do_item_release(new_it);
    } else {
        ret = ENGINE_ENOMEM;
    }
    UNLOCK_CACHE();

    if (ret == ENGINE_SUCCESS) {
        /* The caller wants to know if the old item has been replaced.
         * This code still indicates success.
         */
        if (old_it != NULL) ret = ENGINE_KEY_EEXISTS;
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "map_apply_item_link failed. key=%.*s nkey=%u code=%d\n",
                    PRINT_NKEY(nkey), key, nkey, ret);
    }
    return ret;
}

ENGINE_ERROR_CODE map_apply_elem_insert(void *engine, hash_item *it,
                                        const char *field, const uint32_t nfield,
                                        const uint32_t nbytes)
{
    const char *key = item_get_key(it);
    map_elem_item *elem;
    bool replaced;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL,
                "map_apply_elem_insert. key=%.*s nkey=%u field=%.*s nfield=%u\n",
                PRINT_NKEY(it->nkey), key, it->nkey, nfield, field, nfield);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "map_apply_elem_insert failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        elem = do_map_elem_alloc(nfield, nbytes, NULL);
        if (elem == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "map_apply_elem_insert failed."
                        " element alloc failed. nfield=%d nbytes=%d\n", nfield, nbytes);
            ret = ENGINE_ENOMEM; break;
        }
        memcpy(elem->data, field, nfield + nbytes);

        ret = do_map_elem_insert(it, elem, true /* replace_if_exist */, &replaced, NULL);
        if (ret != ENGINE_SUCCESS) {
            do_map_elem_free(elem);
            logger->log(EXTENSION_LOG_WARNING, NULL, "map_apply_elem_insert failed."
                        " key=%.*s nkey=%u field=%.*s nfield=%u code=%d\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, nfield, field, nfield, ret);
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent has_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE map_apply_elem_delete(void *engine, hash_item *it,
                                        const char *field, const uint32_t nfield,
                                        const bool drop_if_empty)
{
    const char *key = item_get_key(it);
    map_meta_info *info;
    field_t flist;
    ENGINE_ERROR_CODE ret;

    flist.value = (char*)field;
    flist.length = nfield;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL,
                "map_apply_elem_delete. key=%.*s nkey=%u field=%.*s nfield=%u\n",
                PRINT_NKEY(it->nkey), key, it->nkey, nfield, field, nfield);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "map_apply_elem_delete failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        info = (map_meta_info *)item_get_meta(it);
        ret = do_map_elem_delete_by_field(info, &flist);
        if (ret == ENGINE_ELEM_ENOENT) {
            logger->log(EXTENSION_LOG_INFO, NULL, "map_apply_elem_delete failed."
                        " no element deleted. key=%.*s nkey=%u field=%.*s nfield=%u\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, nfield, field, nfield);
            break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS || ret == ENGINE_ELEM_ENOENT) {
        if (drop_if_empty && info->ccnt == 0) {
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
        }
    } else {
        /* Remove inconsistent hash_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
    }
    UNLOCK_CACHE();

    return ret;
}

/*
 * External Functions
 */
ENGINE_ERROR_CODE item_map_coll_init(void *engine_ptr)
{
    /* initialize global variables */
    engine = engine_ptr;
    config = &engine->config;
    logger = engine->server.log->get_logger();

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM map module initialized.\n");
    return ENGINE_SUCCESS;
}

void item_map_coll_final(void *engine_ptr)
{
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM map module destroyed.\n");
}
