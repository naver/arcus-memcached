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
#include "bplus_tree.h"
#include "hash_tree.h"

#define ZSET_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))

static struct default_engine *engine=NULL;
static struct engine_config  *config=NULL; // engine config
static EXTENSION_LOGGER_DESCRIPTOR *logger;

/* Cache Lock */
static inline void LOCK_CACHE(void)
{
    pthread_mutex_lock(&engine->cache_lock);
}

static inline void UNLOCK_CACHE(void)
{
    pthread_mutex_unlock(&engine->cache_lock);
}

/*
 * ZSET collection management
 */
static const void *zset_get_bkey(const bplus_elem_item *elem, uint32_t *nbkey);
static const void *zset_get_eflag(const bplus_elem_item *elem, uint32_t *neflag);
static const void *zset_get_value(const bplus_elem_item *elem, uint32_t *nvalue);
static const void *zset_get_key(const htree_elem_item *elem, uint16_t *nkey);

static bplus_ops zset_bplus_ops = {
    .get_bkey    = zset_get_bkey,
    .get_eflag   = zset_get_eflag,
    .get_value   = zset_get_value,
    .delete_post = NULL
};

static htree_ops zset_htree_ops = {
    .get_key = zset_get_key
};

static const void *zset_get_bkey(const bplus_elem_item *elem, uint32_t *nbkey)
{
    const zset_elem_item *e = (const zset_elem_item *)elem;
    *nbkey = e->nbkey;
    return e->data;
}

static const void *zset_get_eflag(const bplus_elem_item *elem, uint32_t *neflag)
{
    const zset_elem_item *e = (const zset_elem_item *)elem;
    *neflag = e->neflag;
    return e->data + ZSET_REAL_NBKEY(e->nbkey);
}

static const void *zset_get_value(const bplus_elem_item *elem, uint32_t *nvalue)
{
    /* used as the duplicate-bkey tiebreak: two elems with the same score
     * are ordered by member.
     */
    const zset_elem_item *e = (const zset_elem_item *)elem;
    *nvalue = e->nbytes;
    return e->data + ZSET_REAL_NBKEY(e->nbkey) + e->neflag;
}

static const void *zset_get_key(const htree_elem_item *elem, uint16_t *nkey)
{
    const zset_elem_item *e = (const zset_elem_item *)elem;
    *nkey = e->nbytes;
    return e->data + ZSET_REAL_NBKEY(e->nbkey) + e->neflag;
}

static inline uint32_t do_zset_elem_ntotal(zset_elem_item *elem)
{
    return sizeof(zset_elem_item) + ZSET_REAL_NBKEY(elem->nbkey)
           + elem->neflag + elem->nbytes;
}

static ENGINE_ERROR_CODE do_zset_item_find(const void *key, const uint32_t nkey,
                                           bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_ZSET_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static int32_t do_zset_real_maxcount(int32_t maxcount)
{
    int32_t real_maxcount = maxcount;

    if (maxcount < 0) {
        /* It has the max_zset_size that can be increased in the future */
        real_maxcount = -1;
    } else if (maxcount == 0) {
        real_maxcount = DEFAULT_ZSET_SIZE;
    } else if (maxcount > config->max_zset_size) {
        real_maxcount = config->max_zset_size;
    }
    return real_maxcount;
}

static hash_item *do_zset_item_alloc(const void *key, const uint32_t nkey,
                                     const void *value, const uint32_t nbytes,
                                     item_attr *attrp)
{
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(zset_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_ZSET;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), value, nbytes);

        /* initialize zset meta information */
        zset_meta_info *info = (zset_meta_info *)item_get_meta(it);
        info->mcnt = do_zset_real_maxcount(attrp->maxcount);
        info->ccnt = 0;
        info->ovflact = OVFL_ERROR;
        info->mflags  = 0;
#ifdef ENABLE_STICKY_ITEM
        if (IS_STICKY_EXPTIME(attrp->exptime)) info->mflags |= COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1)              info->mflags |= COLL_META_FLAG_READABLE;
        info->itdist  = (uint16_t)((size_t*)info-(size_t*)it);
        info->stotal  = 0;
        info->bktype  = BKEY_TYPE_UNKNOWN;
        bplus_init(&info->bplus, &zset_bplus_ops, true);
        htree_init(&info->htree, &zset_htree_ops);
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);
    }
    return it;
}

static zset_elem_item *do_zset_elem_alloc(const uint32_t nbkey, const uint32_t neflag,
                                          const uint32_t nbytes)
{
    size_t ntotal = sizeof(zset_elem_item) + ZSET_REAL_NBKEY(nbkey) + neflag + nbytes;

    zset_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL);
    if (elem != NULL) {
        elem->slabs_clsid = slabs_clsid(ntotal);
        assert(elem->slabs_clsid > 0);

        elem->refcount    = 0;
        elem->linked      = 0;
        elem->nbkey       = (uint8_t)nbkey;
        elem->neflag      = (uint8_t)neflag;
        elem->nbytes      = (uint16_t)nbytes;
    }
    return elem;
}

static void do_zset_elem_free(zset_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = do_zset_elem_ntotal(elem);
    do_item_mem_free(elem, ntotal);
}

static ENGINE_ERROR_CODE do_zset_elem_insert(hash_item *it, zset_elem_item *elem)
{
    zset_meta_info *info = (zset_meta_info *)item_get_meta(it);
    htree_prev_info pinfo;
    bplus_elem_posi path[BPLUS_MAX_DEPTH];
    const void *member = elem->data + ZSET_REAL_NBKEY(elem->nbkey) + elem->neflag;

    /* validation check: score(bkey) type */
    if (info->ccnt > 0) {
        if ((info->bktype == BKEY_TYPE_UINT64 && elem->nbkey >  0) ||
            (info->bktype == BKEY_TYPE_BINARY && elem->nbkey == 0)) {
            return ENGINE_EBADBKEY;
        }
    }

    /* check uniqueness + insert position (htree) */
    elem->hval = genhash_string_hash(member, elem->nbytes);
    if (htree_elem_find(&info->htree, elem->hval, member, elem->nbytes, &pinfo) != NULL) {
        return ENGINE_ELEM_EEXISTS;
    }

#ifdef ENABLE_STICKY_ITEM
    /* sticky memory limit check */
    if (IS_STICKY_COLLFLG(info)) {
        if (do_item_sticky_overflowed())
            return ENGINE_ENOMEM;
    }
#endif

    /* overflow check */
    assert(info->ovflact == OVFL_ERROR);
    uint32_t real_mcnt = (info->mcnt > 0 ? info->mcnt : config->max_zset_size);
    if (info->ccnt >= real_mcnt) {
        return ENGINE_EOVERFLOW;
    }

    size_t space_increased = 0;
    ENGINE_ERROR_CODE ret = htree_elem_add(&info->htree, (htree_elem_item *)elem,
                                           &pinfo, &space_increased);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    /* insert position (bplus) only; member already unique, so never found */
    bplus_elem_item *find = bplus_elem_find(&info->bplus, elem->data, elem->nbkey,
                                            member, elem->nbytes, path);
    assert(find == NULL);

    ret = bplus_elem_add(&info->bplus, path, (bplus_elem_item *)elem, &space_increased);
    if (ret != ENGINE_SUCCESS) {
        size_t space_decreased = 0;
        htree_elem_delete(&info->htree, elem->hval, member, elem->nbytes, &space_decreased);
        assert(elem->linked == 0);

        assert(space_increased >= space_decreased);
        if (space_increased > space_decreased) {
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_ZSET,
                               space_increased - space_decreased);
        }

        return ret;
    }

    if (info->ccnt == 0) {
        /* set bkey type */
        if (elem->nbkey == 0)
            info->bktype = BKEY_TYPE_UINT64;
        else
            info->bktype = BKEY_TYPE_BINARY;
    }
    info->ccnt++;
    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(do_zset_elem_ntotal(elem)) + space_increased;
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_ZSET, stotal);
    }

    return ENGINE_SUCCESS;
}

/*
 * ZSET Interface Functions
 */
ENGINE_ERROR_CODE zset_struct_create(const char *key, const uint32_t nkey,
                                     const void *value, const uint32_t nbytes,
                                     item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_ZSET_CREATE);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_zset_item_alloc(key, nkey, value, nbytes, attrp);
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

zset_elem_item *zset_elem_alloc(const uint32_t nbkey, const uint32_t neflag, const uint32_t nbytes)
{
    zset_elem_item *elem;
    LOCK_CACHE();
    elem = do_zset_elem_alloc(nbkey, neflag, nbytes);
    UNLOCK_CACHE();
    return elem;
}

void zset_elem_free(zset_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->linked == 0);
    do_zset_elem_free(elem);
    UNLOCK_CACHE();
}

ENGINE_ERROR_CODE zset_elem_insert(const char *key, const uint32_t nkey,
                                   zset_elem_item *elem, item_attr *attrp,
                                   bool *created, const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_ZSET_ELEM_INSERT);

    *created = false;

    LOCK_CACHE();
    ret = do_zset_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_zset_item_alloc(key, nkey, "\r\n", 2, attrp);
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
        ret = do_zset_elem_insert(it, elem);
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

/*
 * External Functions
 */
ENGINE_ERROR_CODE item_zset_coll_init(void *engine_ptr)
{
    /* initialize global variables */
    engine = engine_ptr;
    config = &engine->config;
    logger = engine->server.log->get_logger();

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM zset module initialized.\n");
    return ENGINE_SUCCESS;
}

void item_zset_coll_final(void *engine_ptr)
{
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM zset module destroyed.\n");
}
