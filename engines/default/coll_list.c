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
#include <sys/time.h> /* gettimeofday() */

#include "default_engine.h"
#include "item_clog.h"

#ifdef REORGANIZE_ITEM_COLL // LIST
#define PERSISTENCE_ACTION_BEGIN(a, b)
#define PERSISTENCE_ACTION_END(a)

static struct default_engine *engine=NULL;
static struct engine_config  *config=NULL; // engine config
static EXTENSION_LOGGER_DESCRIPTOR *logger;
#endif

#ifdef REORGANIZE_ITEM_COLL // LIST
/*
 * LIST collection management
 */
static inline uint32_t do_list_elem_ntotal(list_elem_item *elem)
{
    return sizeof(list_elem_item) + elem->nbytes;
}

static ENGINE_ERROR_CODE do_list_item_find(const void *key, const uint32_t nkey,
                                           bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_LIST_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static int32_t do_list_real_maxcount(int32_t maxcount)
{
    int32_t real_maxcount = maxcount;

    if (maxcount < 0) {
        /* It has the max_list_size that can be increased in the future */
        real_maxcount = -1;
    } else if (maxcount == 0) {
        real_maxcount = DEFAULT_LIST_SIZE;
    } else if (maxcount > config->max_list_size) {
        real_maxcount = config->max_list_size;
    }
    return real_maxcount;
}

static hash_item *do_list_item_alloc(const void *key, const uint32_t nkey,
                                     item_attr *attrp, const void *cookie)
{
    uint32_t nbytes = 2; /* "\r\n" */
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(list_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_LIST;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), "\r\n", nbytes);

        /* initialize list meta information */
        list_meta_info *info = (list_meta_info *)item_get_meta(it);
        info->mcnt = do_list_real_maxcount(attrp->maxcount);
        info->ccnt = 0;
        info->ovflact = (attrp->ovflaction==0 ? OVFL_TAIL_TRIM : attrp->ovflaction);
        info->mflags  = 0;
#ifdef ENABLE_STICKY_ITEM
        if (IS_STICKY_EXPTIME(attrp->exptime)) info->mflags |= COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1)              info->mflags |= COLL_META_FLAG_READABLE;
        info->itdist  = (uint16_t)((size_t*)info-(size_t*)it);
        info->stotal  = 0;
        info->head = info->tail = NULL;
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);
    }
    return it;
}

static list_elem_item *do_list_elem_alloc(const uint32_t nbytes, const void *cookie)
{
    size_t ntotal = sizeof(list_elem_item) + nbytes;

    list_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        elem->slabs_clsid = slabs_clsid(ntotal);
        assert(elem->slabs_clsid > 0);

        elem->refcount    = 0;
        elem->nbytes      = nbytes;
        elem->prev = elem->next = (list_elem_item *)ADDR_MEANS_UNLINKED; /* Unliked state */
    }
    return elem;
}

static void do_list_elem_free(list_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = do_list_elem_ntotal(elem);
    do_item_mem_free(elem, ntotal);
}

static void do_list_elem_release(list_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->next == (list_elem_item *)ADDR_MEANS_UNLINKED) {
        do_list_elem_free(elem);
    }
}

static list_elem_item *do_list_elem_find(list_meta_info *info, int index)
{
    list_elem_item *elem;

    if (index <= (info->ccnt/2)) {
        assert(index >= 0);
        elem = info->head;
        for (int i = 0; i < index && elem != NULL; i++) {
            elem = elem->next;
        }
    } else {
        assert(index < info->ccnt);
        elem = info->tail;
        for (int i = info->ccnt-1; i > index && elem != NULL; i--) {
            elem = elem->prev;
        }
    }
    return elem;
}

static ENGINE_ERROR_CODE do_list_elem_link(list_meta_info *info, const int index,
                                           list_elem_item *elem)
{
    list_elem_item *prev, *next;

    assert(index >= 0);
    if (index == 0) {
        prev = NULL;
        next = info->head;
    } else if (index >= info->ccnt) {
        prev = info->tail;
        next = NULL;
    } else {
        prev = do_list_elem_find(info, (index-1));
        next = prev->next;
    }

    elem->prev = prev;
    elem->next = next;
    if (prev == NULL) info->head = elem;
    else              prev->next = elem;
    if (next == NULL) info->tail = elem;
    else              next->prev = elem;
    info->ccnt++;

    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(do_list_elem_ntotal(elem));
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_LIST, stotal);
    }
    return ENGINE_SUCCESS;
}

static void do_list_elem_unlink(list_meta_info *info, list_elem_item *elem,
                                enum elem_delete_cause cause)
{
    /* if (elem->next != (list_elem_item *)ADDR_MEANS_UNLINKED) */
    {
        if (elem->prev == NULL) info->head = elem->next;
        else                    elem->prev->next = elem->next;
        if (elem->next == NULL) info->tail = elem->prev;
        else                    elem->next->prev = elem->prev;
        elem->prev = elem->next = (list_elem_item *)ADDR_MEANS_UNLINKED;
        info->ccnt--;

        if (info->stotal > 0) { /* apply memory space */
            size_t stotal = slabs_space_size(do_list_elem_ntotal(elem));
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_LIST, stotal);
        }

        if (elem->refcount == 0) {
            do_list_elem_free(elem);
        }
    }
}

static uint32_t do_list_elem_delete(list_meta_info *info,
                                    const int index, const uint32_t count,
                                    enum elem_delete_cause cause)
{
    list_elem_item *elem;
    list_elem_item *next;
    uint32_t fcnt = 0;

    CLOG_LIST_ELEM_DELETE(info, index, count, true, cause);

    elem = do_list_elem_find(info, index);
    while (elem != NULL) {
        next = elem->next;
        fcnt++;
        do_list_elem_unlink(info, elem, cause);
        if (count > 0 && fcnt >= count) break;
        elem = next;
    }
    return fcnt;
}

static uint32_t do_list_elem_get(list_meta_info *info,
                                 const int index, const uint32_t count,
                                 const bool forward, const bool delete,
                                 list_elem_item **elem_array)
{
    list_elem_item *elem;
    list_elem_item *tobe;
    uint32_t fcnt = 0; /* found count */
    enum elem_delete_cause cause = ELEM_DELETE_NORMAL;

    if (delete) {
        CLOG_LIST_ELEM_DELETE(info, index, count, forward, ELEM_DELETE_NORMAL);
    }

    elem = do_list_elem_find(info, index);
    while (elem != NULL) {
        tobe = (forward ? elem->next : elem->prev);
        elem->refcount++;
        elem_array[fcnt++] = elem;
        if (delete) do_list_elem_unlink(info, elem, cause);
        if (count > 0 && fcnt >= count) break;
        elem = tobe;
    }

    return fcnt;
}

static ENGINE_ERROR_CODE do_list_elem_insert(hash_item *it,
                                             int index, list_elem_item *elem,
                                             const void *cookie)
{
    list_meta_info *info = (list_meta_info *)item_get_meta(it);
    uint32_t real_mcnt = (info->mcnt > 0 ? info->mcnt : config->max_list_size);
    ENGINE_ERROR_CODE ret;

    /* validation check: index value */
    if (index >= 0) {
        if (index > info->ccnt || index > (real_mcnt-1))
            return ENGINE_EINDEXOOR;
    } else {
        if ((-index) > (info->ccnt+1) || (-index) > real_mcnt)
            return ENGINE_EINDEXOOR;
    }

#ifdef ENABLE_STICKY_ITEM
    /* sticky memory limit check */
    if (IS_STICKY_EXPTIME(it->exptime)) {
        if (do_item_sticky_overflowed())
            return ENGINE_ENOMEM;
    }
#endif

    /* overflow check */
    if (info->ovflact == OVFL_ERROR && info->ccnt >= real_mcnt) {
        return ENGINE_EOVERFLOW;
    }

    CLOG_LIST_ELEM_INSERT(info, index, elem);

    if (info->ccnt >= real_mcnt) {
        /* info->ovflact: OVFL_HEAD_TRIM or OVFL_TAIL_TRIM */
        int      delidx;
        uint32_t delcnt;
        if (index == 0 || index == -1) {
            /* delete an element item of opposite side to make room */
            delidx = (index == -1 ? 0 : info->ccnt-1);
            delcnt = do_list_elem_delete(info, delidx, 1, ELEM_DELETE_TRIM);
            assert(delcnt == 1);
        } else {
            /* delete an element item that overflow action indicates */
            delidx = (info->ovflact == OVFL_HEAD_TRIM ? 0 : info->ccnt-1);
            delcnt = do_list_elem_delete(info, delidx, 1, ELEM_DELETE_TRIM);
            assert(delcnt == 1);
            /* adjust list index value */
            if (info->ovflact == OVFL_HEAD_TRIM) {
              if (index > 0) index -= 1;
            } else { /* ovflact == OVFL_TAIL_TRIM */
              if (index < 0) index += 1;
            }
        }
    }

    if (index < 0) {
        /* Change the negative index to a positive index.
         * by adding (current element count + 1).  One more addition
         * is needed since the direction of insertion is reversed.
         */
        index += (info->ccnt+1);
        if (index < 0)
            index = 0;
    }

    ret = do_list_elem_link(info, index, elem);
    assert(ret == ENGINE_SUCCESS);
    return ret;
}
#endif

#ifdef REORGANIZE_ITEM_COLL // LIST
/*
 * LIST Interface Functions
 */
ENGINE_ERROR_CODE list_struct_create(const char *key, const uint32_t nkey,
                                     item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_LIST_CREATE);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_list_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            if (ret != ENGINE_SUCCESS) {
                do_item_free(it);
            }
        }
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

list_elem_item *list_elem_alloc(const uint32_t nbytes, const void *cookie)
{
    list_elem_item *elem;
    LOCK_CACHE();
    elem = do_list_elem_alloc(nbytes, cookie);
    UNLOCK_CACHE();
    return elem;
}

void list_elem_free(list_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->next == (list_elem_item *)ADDR_MEANS_UNLINKED);
    do_list_elem_free(elem);
    UNLOCK_CACHE();
}

void list_elem_release(list_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    LOCK_CACHE();
    while (cnt < elem_count) {
        do_list_elem_release(elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            UNLOCK_CACHE();
            LOCK_CACHE();
        }
    }
    UNLOCK_CACHE();
}

ENGINE_ERROR_CODE list_elem_insert(const char *key, const uint32_t nkey,
                                   int index, list_elem_item *elem,
                                   item_attr *attrp,
                                   bool *created, const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_LIST_ELEM_INSERT);

    *created = false;

    LOCK_CACHE();
    ret = do_list_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_list_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            if (ret == ENGINE_SUCCESS) {
                *created = true;
            } else {
                do_item_free(it);
            }
        }
    }
    if (ret == ENGINE_SUCCESS) {
        ret = do_list_elem_insert(it, index, elem, cookie);
        if (*created) {
            if (ret != ENGINE_SUCCESS) {
                do_item_unlink(it, ITEM_UNLINK_NORMAL);
            }
        } else {
            do_item_release(it);
        }
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

static int adjust_list_range(list_meta_info *info, int *from_index, int *to_index)
{
    if (info->ccnt <= 0) return -1; /* out of range */

    if (*from_index < 0) {
        *from_index += info->ccnt;
    }
    if (*to_index < 0) {
        *to_index += info->ccnt;
    }

    if (*from_index < 0) {
        if (*to_index < 0) return -1; /* out of range */
        *from_index = 0;
    } else if (*from_index >= info->ccnt) {
        if (*to_index >= info->ccnt) return -1; /* out of range */
        *from_index = info->ccnt-1;
    }
    if (*to_index < 0) {
        *to_index = 0;
    } else if (*to_index >= info->ccnt) {
        *to_index = info->ccnt-1;
    }
    return 0;
}

ENGINE_ERROR_CODE list_elem_delete(const char *key, const uint32_t nkey,
                                   int from_index, int to_index,
                                   const bool drop_if_empty,
                                   uint32_t *del_count, bool *dropped,
                                   const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_LIST_ELEM_DELETE_DROP
                                                    : UPD_LIST_ELEM_DELETE));

    LOCK_CACHE();
    ret = do_list_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        int      index;
        uint32_t count;
        list_meta_info *info = (list_meta_info *)item_get_meta(it);
        do {
            if (adjust_list_range(info, &from_index, &to_index) != 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if (from_index <= to_index) {
                index = from_index;
                count = to_index - from_index + 1;
            } else {
                index = to_index;
                count = from_index - to_index + 1;
            }

            *del_count = do_list_elem_delete(info, index, count, ELEM_DELETE_NORMAL);
            if (*del_count > 0) {
                if (info->ccnt == 0 && drop_if_empty) {
                    do_item_unlink(it, ITEM_UNLINK_NORMAL);
                    *dropped = true;
                } else {
                    *dropped = false;
                }
            } else {
                ret = ENGINE_ELEM_ENOENT;
            }
        } while(0);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE list_elem_get(const char *key, const uint32_t nkey,
                                int from_index, int to_index,
                                const bool delete, const bool drop_if_empty,
                                struct elems_result *eresult,
                                const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    if (delete) {
        PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_LIST_ELEM_DELETE_DROP
                                                        : UPD_LIST_ELEM_DELETE));
    }

    eresult->elem_array = NULL;
    eresult->elem_count = 0;

    LOCK_CACHE();
    ret = do_list_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        int      index;
        uint32_t count;
        bool forward;
        list_meta_info *info = (list_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (adjust_list_range(info, &from_index, &to_index) != 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            index = from_index;
            if (from_index <= to_index) {
                count = to_index - from_index + 1;
                forward = true;
            } else {
                count = from_index - to_index + 1;
                forward = false;
            }
            if ((eresult->elem_array = (eitem **)malloc(count * sizeof(eitem*))) == NULL) {
                ret = ENGINE_ENOMEM; break;
            }

            eresult->elem_count = do_list_elem_get(info, index, count, forward, delete,
                                                   (list_elem_item**)(eresult->elem_array));
            assert(eresult->elem_count > 0);
            if (info->ccnt == 0 && drop_if_empty) {
                assert(delete == true);
                do_item_unlink(it, ITEM_UNLINK_NORMAL);
                eresult->dropped = true;
            } else {
                eresult->dropped = false;
            }
            eresult->flags = it->flags;
        } while(0);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    if (delete) {
        PERSISTENCE_ACTION_END(ret);
    }
    return ret;
}

uint32_t list_elem_delete_with_count(list_meta_info *info, const uint32_t count)
{
    return do_list_elem_delete(info, 0, count, ELEM_DELETE_COLL);
}

/* See do_list_elem_delete. */
void list_elem_get_all(list_meta_info *info, elems_result_t *eresult)
{
    assert(eresult->elem_arrsz >= info->ccnt && eresult->elem_count == 0);
    list_elem_item *elem;

    elem = do_list_elem_find(info, 0);
    while (elem != NULL) {
        elem->refcount++;
        eresult->elem_array[eresult->elem_count++] = elem;
        elem = elem->next;
    }
    assert(eresult->elem_count == info->ccnt);
}

uint32_t list_elem_ntotal(list_elem_item *elem)
{
    return do_list_elem_ntotal(elem);
}

ENGINE_ERROR_CODE list_coll_getattr(hash_item *it, item_attr *attrp,
                                    ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    list_meta_info *info = (list_meta_info *)item_get_meta(it);

    /* check attribute validation */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXBKEYRANGE || attr_ids[i] == ATTR_TRIMMED) {
            return ENGINE_EBADATTR;
        }
    }

    /* get collection attributes */
    attrp->count = info->ccnt;
    attrp->maxcount = (info->mcnt > 0) ? info->mcnt : (int32_t)config->max_list_size;
    attrp->ovflaction = info->ovflact;
    attrp->readable = ((info->mflags & COLL_META_FLAG_READABLE) != 0) ? 1 : 0;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE list_coll_setattr(hash_item *it, item_attr *attrp,
                                    ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    list_meta_info *info = (list_meta_info *)item_get_meta(it);

    /* check the validity of given attributs */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            attrp->maxcount = do_list_real_maxcount(attrp->maxcount);
            if (attrp->maxcount > 0 && attrp->maxcount < info->ccnt) {
                return ENGINE_EBADVALUE;
            }
        } else if (attr_ids[i] == ATTR_OVFLACTION) {
            if (attrp->ovflaction != OVFL_ERROR &&
                attrp->ovflaction != OVFL_HEAD_TRIM &&
                attrp->ovflaction != OVFL_TAIL_TRIM) {
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
#endif

#ifdef REORGANIZE_ITEM_COLL // APPLY LIST
ENGINE_ERROR_CODE list_apply_item_link(void *engine, const char *key, const uint32_t nkey,
                                       item_attr *attrp)
{
    hash_item *old_it;
    hash_item *new_it;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "list_apply_item_link. key=%.*s nkey=%u\n",
                PRINT_NKEY(nkey), key, nkey);

    LOCK_CACHE();
    old_it = do_item_get(key, nkey, DONT_UPDATE);
    if (old_it) {
        /* Remove the old item first. */
        do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
        do_item_release(old_it);
    }
    new_it = do_list_item_alloc(key, nkey, attrp, NULL); /* cookie is NULL */
    if (new_it) {
        /* Link the new item into the hash table */
        ret = do_item_link(new_it);
        if (ret != ENGINE_SUCCESS) {
            do_item_free(new_it);
        }
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
                    "list_apply_item_link failed. key=%.*s nkey=%u code=%d\n",
                    PRINT_NKEY(nkey), key, nkey, ret);
    }
    return ret;
}

ENGINE_ERROR_CODE list_apply_elem_insert(void *engine, hash_item *it,
                                         const int nelems, const int index,
                                         const char *value, const uint32_t nbytes)
{
    const char *key = item_get_key(it);
    list_meta_info *info;
    list_elem_item *elem;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL,
                "list_apply_elem_insert. key=%.*s nkey=%u nelems=%d index=%d\n",
                PRINT_NKEY(it->nkey), key, it->nkey, nelems, index);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "list_apply_elem_insert failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        info = (list_meta_info*)item_get_meta(it);
        if (nelems != -1 && info->ccnt != nelems) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "list_apply_elem_insert failed."
                        " element count mismatch. ecnt(%d) != nelems(%d)\n",
                        info->ccnt, nelems);
            ret = ENGINE_EINVAL; break;
        }

        elem = do_list_elem_alloc(nbytes, NULL);
        if (elem == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "list_apply_elem_insert failed."
                        " element alloc failed. nbytes=%d\n", nbytes);
            ret = ENGINE_ENOMEM; break;
        }
        memcpy(elem->value, value, nbytes);

        ret = do_list_elem_insert(it, index, elem, NULL);
        if (ret != ENGINE_SUCCESS) {
            do_list_elem_free(elem);
            logger->log(EXTENSION_LOG_WARNING, NULL, "list_apply_elem_insert failed."
                        " key=%.*s nkey=%u index=%d code=%d\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, index, ret);
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent hash_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE list_apply_elem_delete(void *engine, hash_item *it,
                                         const int nelems, const int index,
                                         const int count, const bool drop_if_empty)
{
    const char *key = item_get_key(it);
    list_meta_info *info;
    uint32_t ndeleted;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL,
                "list_apply_elem_delete. key=%.*s nkey=%u index=%d, count=%d\n",
                PRINT_NKEY(it->nkey), key, it->nkey, index, count);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "list_apply_elem_delete failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        info = (list_meta_info*)item_get_meta(it);
        if (info->ccnt != nelems) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "list_apply_elem_delete failed."
                        "element count mismatch. ecnt(%d) != nelems(%d)",
                        info->ccnt, nelems);
            ret = ENGINE_EINVAL; break;
        }

        ndeleted = do_list_elem_delete(info, index, count, ELEM_DELETE_NORMAL);
        if (ndeleted == 0) {
            logger->log(EXTENSION_LOG_INFO, NULL, "list_apply_elem_delete failed."
                        " no element deleted. key=%.*s nkey=%u index=%d count=%d\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, index, count);
            ret = ENGINE_ELEM_ENOENT; break;
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
#endif

#ifdef REORGANIZE_ITEM_COLL // LIST
/*
 * External Functions
 */
ENGINE_ERROR_CODE item_list_coll_init(void *engine_ptr)
{
    /* initialize global variables */
    engine = engine_ptr;
    config = &engine->config;
    logger = engine->server.log->get_logger();

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM list module initialized.\n");
    return ENGINE_SUCCESS;
}

void item_list_coll_final(void *engine_ptr)
{
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM list module destroyed.\n");
}
#endif
