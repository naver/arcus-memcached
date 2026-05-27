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

static const void *set_get_key(const htree_elem_item *elem, uint16_t *nkey) {
    *nkey = (uint16_t)((const set_elem_item *)elem)->nbytes;
    return ((const set_elem_item *)elem)->value;
}

static htree_ops set_htree_ops = {
    .get_key = set_get_key,
};

/*
 * SET collection manangement
 */

#define SET_GET_HASHIDX(hval, hdepth) \
        (((hval) & (SET_HASHIDX_MASK << ((hdepth)*4))) >> ((hdepth)*4))

static inline uint32_t do_set_elem_ntotal(set_elem_item *elem)
{
    return sizeof(set_elem_item) + elem->nbytes;
}

static ENGINE_ERROR_CODE do_set_item_find(const void *key, const uint32_t nkey,
                                          bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_SET_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static int32_t do_set_real_maxcount(int32_t maxcount)
{
    int32_t real_maxcount = maxcount;

    if (maxcount < 0) {
        /* It has the max_set_size that can be increased in the future */
        real_maxcount = -1;
    } else if (maxcount == 0) {
        real_maxcount = DEFAULT_SET_SIZE;
    } else if (maxcount > config->max_set_size) {
        real_maxcount = config->max_set_size;
    }
    return real_maxcount;
}

static hash_item *do_set_item_alloc(const void *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    uint32_t nbytes = 2; /* "\r\n" */
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(set_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_SET;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), "\r\n", nbytes);

        /* initialize set meta information */
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        info->mcnt = do_set_real_maxcount(attrp->maxcount);
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

static set_elem_item *do_set_elem_alloc(const uint32_t nbytes, const void *cookie)
{
    size_t ntotal = sizeof(set_elem_item) + nbytes;

    set_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        elem->slabs_clsid = slabs_clsid(ntotal);
        assert(elem->slabs_clsid > 0);

        elem->refcount    = 0;
        elem->nbytes      = (uint16_t)nbytes;
        elem->status = ELEM_STATUS_UNLINKED; /* unlinked state */
    }
    return elem;
}

static void do_set_elem_free(set_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = do_set_elem_ntotal(elem);
    do_item_mem_free(elem, ntotal);
}

static void do_set_elem_release(set_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->status == ELEM_STATUS_UNLINKED) {
        do_set_elem_free(elem);
    }
}

static ENGINE_ERROR_CODE do_set_elem_insert(hash_item *it, set_elem_item *elem,
                                            const void *cookie)
{
    set_meta_info *info = (set_meta_info *)item_get_meta(it);
    assert(info->ovflact == OVFL_ERROR);

    int real_mcnt = (int)(info->mcnt > 0 ? info->mcnt : config->max_set_size);
    ssize_t space_delta;

#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_COLLFLG(info) && do_item_sticky_overflowed())
        return ENGINE_ENOMEM;
#endif
    if (real_mcnt > 0 && (int)info->ccnt >= real_mcnt)
        return ENGINE_EOVERFLOW;

    ENGINE_ERROR_CODE ret = htree_elem_link((htree_node **)&info->root,
                                            (htree_elem_item *)elem,
                                            &set_htree_ops,
                                            &space_delta, cookie);
    if (ret != ENGINE_SUCCESS)
        return ret;

    elem->status = ELEM_STATUS_LINKED;
    space_delta += (ssize_t)slabs_space_size(sizeof(set_elem_item) + elem->nbytes);
    CLOG_SET_ELEM_INSERT(info, elem);

    info->ccnt++;
    do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_SET, (size_t)space_delta);
    return ENGINE_SUCCESS;
}

static inline ssize_t do_set_elem_unlink_process(set_meta_info *info, set_elem_item *e)
{
    e->status = ELEM_STATUS_UNLINKED;
    CLOG_SET_ELEM_DELETE(info, e, ELEM_DELETE_NORMAL);
    return (ssize_t)slabs_space_size(sizeof(set_elem_item) + e->nbytes);
}

static ENGINE_ERROR_CODE do_set_elem_delete_by_value(set_meta_info *info,
                                                     const char *val, const int vlen)
{
    if (info->root == NULL)
        return ENGINE_ELEM_ENOENT;

    ssize_t space_delta;
    htree_elem_item *unlinked = htree_elem_unlink((htree_node **)&info->root, val, vlen,
                                                  &set_htree_ops, &space_delta);
    if (unlinked == NULL)
        return ENGINE_ELEM_ENOENT;

    set_elem_item *elem = (set_elem_item *)unlinked;
    space_delta -= do_set_elem_unlink_process(info, elem);

    do_set_elem_release(elem);
    info->ccnt--;
    do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_SET, (size_t)-space_delta);

    return ENGINE_SUCCESS;
}

/*
 * SET Interface Functions
 */
ENGINE_ERROR_CODE set_struct_create(const char *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_SET_CREATE);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_set_item_alloc(key, nkey, attrp, cookie);
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

set_elem_item *set_elem_alloc(const uint32_t nbytes, const void *cookie)
{
    set_elem_item *elem;
    LOCK_CACHE();
    elem = do_set_elem_alloc(nbytes, cookie);
    UNLOCK_CACHE();
    return elem;
}

void set_elem_free(set_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->status == ELEM_STATUS_UNLINKED);
    do_set_elem_free(elem);
    UNLOCK_CACHE();
}

void set_elem_release(set_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    LOCK_CACHE();
    while (cnt < elem_count) {
        do_set_elem_release(elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            UNLOCK_CACHE();
            LOCK_CACHE();
        }
    }
    UNLOCK_CACHE();
}

ENGINE_ERROR_CODE set_elem_insert(const char *key, const uint32_t nkey,
                                  set_elem_item *elem, item_attr *attrp,
                                  bool *created, const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_SET_ELEM_INSERT);

    *created = false;

    LOCK_CACHE();
    ret = do_set_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_set_item_alloc(key, nkey, attrp, cookie);
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
        ret = do_set_elem_insert(it, elem, cookie);
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

ENGINE_ERROR_CODE set_elem_delete(const char *key, const uint32_t nkey,
                                  const char *value, const uint32_t nbytes,
                                  const bool drop_if_empty, bool *dropped,
                                  const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_SET_ELEM_DELETE_DROP
                                                    : UPD_SET_ELEM_DELETE));


    *dropped = false;

    LOCK_CACHE();
    ret = do_set_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        ret = do_set_elem_delete_by_value(info, value, nbytes);
        if (ret == ENGINE_SUCCESS) {
            if (info->ccnt == 0 && drop_if_empty) {
                do_item_unlink(it, ITEM_UNLINK_NORMAL);
                *dropped = true;
            }
        }
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

static uint32_t do_set_elem_get_rand(set_meta_info *info,
                                     const uint32_t count, const bool delete,
                                     set_elem_item **elem_array)
{
    assert(info->root);

    if (delete) {
        uint32_t fcnt = 0;
        ssize_t space_delta = 0;
        uint32_t total_count = info->ccnt;
        CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, count, ELEM_DELETE_NORMAL);

        while (fcnt < count) {
            uint32_t rand_offset = (uint32_t)(rand() % (int)total_count);
            ssize_t delta;
            htree_elem_item *unlinked = htree_elem_unlink_at_offset((htree_node **)&info->root,
                                                                    rand_offset, &delta);
            set_elem_item *e = (set_elem_item *)unlinked;
            delta -= do_set_elem_unlink_process(info, e);
            elem_array[fcnt++] = e;
            space_delta += delta;
            total_count--;
        }

        info->ccnt -= fcnt;
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_SET, (size_t)-space_delta);

        CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);
        return fcnt;
    }

    uint32_t fcnt = htree_elem_get_rand(info->root, info->ccnt, count,
                                        (htree_elem_item **)elem_array);
    for (uint32_t i = 0; i < fcnt; i++) {
        set_elem_item *e = (set_elem_item *)elem_array[i];
        e->refcount++;
        elem_array[i] = e;
    }
    return fcnt;
}

static uint32_t do_set_elem_get_all(set_meta_info *info,
                                    const bool delete, set_elem_item **elem_array)
{
    assert(info->root && elem_array != NULL);
    ssize_t space_delta;
    uint32_t fcnt;

    if (delete) {
        CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, 0, ELEM_DELETE_NORMAL);

        fcnt = htree_elem_get_by_cnt((htree_node **)&info->root, 0,
                                     (htree_elem_item **)elem_array, true, &space_delta);
        for (uint32_t i = 0; i < fcnt; i++) {
            set_elem_item *e = (set_elem_item *)elem_array[i];
            space_delta -= do_set_elem_unlink_process(info, e);
            elem_array[i] = e;
        }
        info->ccnt -= fcnt;
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_SET, (size_t)-space_delta);

        CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);
    } else {
        fcnt = htree_elem_get_by_cnt((htree_node **)&info->root, 0,
                                     (htree_elem_item **)elem_array, false, NULL);
        for (uint32_t i = 0; i < fcnt; i++) {
            set_elem_item *e = (set_elem_item *)elem_array[i];
            e->refcount++;
            elem_array[i] = e;
        }
    }
    return fcnt;
}

ENGINE_ERROR_CODE set_elem_exist(const char *key, const uint32_t nkey,
                                 const char *value, const uint32_t nbytes,
                                 bool *exist)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;

    LOCK_CACHE();
    ret = do_set_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            *exist = (htree_elem_find((htree_node *)info->root,
                                      value, nbytes, &set_htree_ops, NULL) != NULL);
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE set_elem_get(const char *key, const uint32_t nkey,
                               const uint32_t count,
                               const bool delete, const bool drop_if_empty,
                               struct elems_result *eresult,
                               const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    if (delete) {
        PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_SET_ELEM_DELETE_DROP
                                                        : UPD_SET_ELEM_DELETE));
    }

    LOCK_CACHE();
    ret = do_set_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt <= 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if (count == 0 || info->ccnt < count) {
                eresult->elem_array = (eitem **)malloc(info->ccnt * sizeof(eitem*));
            } else {
                eresult->elem_array = (eitem **)malloc(count * sizeof(eitem*));
            }
            if (eresult->elem_array == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            if (count == 0 || count >= info->ccnt) {
                eresult->elem_count = do_set_elem_get_all(info, delete,
                                                          (set_elem_item**)(eresult->elem_array));
            } else {
                eresult->elem_count = do_set_elem_get_rand(info, count, delete,
                                                           (set_elem_item**)(eresult->elem_array));
            }
            if (eresult->elem_count == 0) {
                free(eresult->elem_array);
                eresult->elem_array = NULL;
                ret = ENGINE_ENOMEM;
                break;
            }
            assert(eresult->elem_count > 0);
            if (info->ccnt == 0 && drop_if_empty) {
                assert(delete == true);
                do_item_unlink(it, ITEM_UNLINK_NORMAL);
                eresult->dropped = true;
            } else {
                eresult->dropped = false;
            }
            eresult->flags = it->flags;
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    if (delete) {
        PERSISTENCE_ACTION_END(ret);
    }
    return ret;
}

uint32_t set_elem_delete_with_count(set_meta_info *info, const uint32_t count)
{
    uint32_t fcnt = 0;
    if (info->root != NULL) {
        htree_elem_item *head = htree_elem_unlink_by_cnt((htree_node **)&info->root, count, NULL);
        for (htree_elem_item *cur = head; cur != NULL; ) {
            htree_elem_item *next = cur->next;
            set_elem_item *e = (set_elem_item *)cur;
            e->status = ELEM_STATUS_UNLINKED;
            do_set_elem_release(e);
            cur = next;
            fcnt++;
        }
    }
    return fcnt;
}

void set_elem_get_all(set_meta_info *info, elems_result_t *eresult)
{
    assert(eresult->elem_arrsz >= info->ccnt && eresult->elem_count == 0);
    eresult->elem_count = do_set_elem_get_all(info, false,
                                              (set_elem_item **)eresult->elem_array);
    assert(eresult->elem_count == info->ccnt);
}

uint32_t set_elem_ntotal(set_elem_item *elem)
{
    return do_set_elem_ntotal(elem);
}

ENGINE_ERROR_CODE set_coll_getattr(hash_item *it, item_attr *attrp,
                                   ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    set_meta_info *info = (set_meta_info *)item_get_meta(it);

    /* check attribute validation */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXBKEYRANGE || attr_ids[i] == ATTR_TRIMMED) {
            return ENGINE_EBADATTR;
        }
    }

    /* get collection attributes */
    attrp->count = info->ccnt;
    attrp->maxcount = (info->mcnt > 0) ? info->mcnt : (int32_t)config->max_set_size;
    attrp->ovflaction = info->ovflact;
    attrp->readable = ((info->mflags & COLL_META_FLAG_READABLE) != 0) ? 1 : 0;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE set_coll_setattr(hash_item *it, item_attr *attrp,
                                   ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    set_meta_info *info = (set_meta_info *)item_get_meta(it);

    /* check the validity of given attributs */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            attrp->maxcount = do_set_real_maxcount(attrp->maxcount);
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

ENGINE_ERROR_CODE set_apply_item_link(void *engine, const char *key, const uint32_t nkey,
                                      item_attr *attrp)
{
    hash_item *old_it;
    hash_item *new_it;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "set_apply_item_link. key=%.*s nkey=%u\n",
                PRINT_NKEY(nkey), key, nkey);

    LOCK_CACHE();
    old_it = do_item_get(key, nkey, DONT_UPDATE);
    if (old_it) {
        /* Remove the old item first. */
        do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
        do_item_release(old_it);
    }
    new_it = do_set_item_alloc(key, nkey, attrp, NULL); /* cookie is NULL */
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
                    "item_apply_set_link failed. key=%.*s nkey=%u code=%d\n",
                    PRINT_NKEY(nkey), key, nkey, ret);
    }
    return ret;
}


ENGINE_ERROR_CODE set_apply_elem_insert(void *engine, hash_item *it,
                                        const char *value, const uint32_t nbytes)
{
    const char *key = item_get_key(it);
    set_elem_item *elem;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "set_apply_elem_insert. key=%.*s nkey=%u\n",
                PRINT_NKEY(it->nkey), key, it->nkey);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "set_apply_elem_insert failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        elem = do_set_elem_alloc(nbytes, NULL);
        if (elem == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "set_apply_elem_insert failed."
                        " element alloc failed. nbytes=%d\n", nbytes);
            ret = ENGINE_ENOMEM; break;
        }
        memcpy(elem->value, value, nbytes);

        ret = do_set_elem_insert(it, elem, NULL);
        if (ret != ENGINE_SUCCESS) {
            do_set_elem_free(elem);
            logger->log(EXTENSION_LOG_WARNING, NULL, "set_apply_elem_insert failed."
                        " key=%.*s nkey=%u code=%d\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, ret);
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent hash_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE set_apply_elem_delete(void *engine, hash_item *it,
                                        const char *value, const uint32_t nbytes,
                                        const bool drop_if_empty)
{
    const char *key = item_get_key(it);
    set_meta_info *info;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "set_apply_elem_delete. key=%.*s nkey=%u\n",
                PRINT_NKEY(it->nkey), key, it->nkey);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "set_apply_elem_delete failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        info = (set_meta_info *)item_get_meta(it);
        ret = do_set_elem_delete_by_value(info, value, nbytes);
        if (ret == ENGINE_ELEM_ENOENT) {
            logger->log(EXTENSION_LOG_INFO, NULL, "set_apply_elem_delete failed."
                        " no element deleted. key=%.*s nkey=%u\n",
                        PRINT_NKEY(it->nkey), key, it->nkey);
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
ENGINE_ERROR_CODE item_set_coll_init(void *engine_ptr)
{
    /* initialize global variables */
    engine = engine_ptr;
    config = &engine->config;
    logger = engine->server.log->get_logger();

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM set module initialized.\n");
    return ENGINE_SUCCESS;
}

void item_set_coll_final(void *engine_ptr)
{
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM set module destroyed.\n");
}
