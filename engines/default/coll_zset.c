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

/* zset delete context for bplus delete_post (see zset_elem_delete_post) */
typedef struct {
    zset_meta_info *info;
    bool            htree_delete; /* delete in hash tree */
} zset_delete_ctx;

/*
 * ZSET collection management
 */
static const void *zset_get_bkey(const bplus_elem_item *elem, uint32_t *nbkey);
static const void *zset_get_eflag(const bplus_elem_item *elem, uint32_t *neflag);
static const void *zset_get_value(const bplus_elem_item *elem, uint32_t *nvalue);
static const void *zset_get_key(const htree_elem_item *elem, uint16_t *nkey);
static void        zset_elem_delete_post(bplus_elem_item *elem, void *arg);

static bplus_ops zset_bplus_ops = {
    .get_bkey    = zset_get_bkey,
    .get_eflag   = zset_get_eflag,
    .get_value   = zset_get_value,
    .delete_post = zset_elem_delete_post
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

static void zset_elem_delete_post(bplus_elem_item *elem, void *arg)
{
    if (arg == NULL) {
        /* the caller handles the elem */
        return;
    }

    zset_elem_item *e = (zset_elem_item *)elem;
    zset_delete_ctx *ctx = (zset_delete_ctx *)arg;
    zset_meta_info *info = ctx->info;
    size_t space_decreased = 0;

    if (ctx->htree_delete) {
        const char *member = (const char *)e->data + ZSET_REAL_NBKEY(e->nbkey) + e->neflag;
        zset_elem_item *deleted = (zset_elem_item *)htree_elem_delete(&info->htree, e->hval,
                                                                      member, e->nbytes,
                                                                      &space_decreased);
        assert(deleted == e);
    }

    info->ccnt--;
    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(do_zset_elem_ntotal(e)) + space_decreased;
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_ZSET, stotal);
    }
    assert(e->linked == 0);
    if (e->refcount == 0) {
        do_zset_elem_free(e);
    }
}

static ENGINE_ERROR_CODE do_zset_elem_replace(zset_meta_info *info, htree_prev_info *pinfo,
                                              zset_elem_item *new_elem)
{
    zset_elem_item *old_elem = (pinfo->prev != NULL) ? (zset_elem_item *)pinfo->prev->next
                                                     : (zset_elem_item *)pinfo->node->htab[pinfo->hidx];
    const void *member = new_elem->data + ZSET_REAL_NBKEY(new_elem->nbkey) + new_elem->neflag;
    bplus_elem_posi path[BPLUS_MAX_DEPTH];
    zset_elem_item *replaced;
    size_t space_increased = 0;
    size_t space_decreased = 0;

    /* same score and eflag: nothing to replace */
    if (old_elem->nbkey == new_elem->nbkey && old_elem->neflag == new_elem->neflag &&
        memcmp(old_elem->data, new_elem->data,
               ZSET_REAL_NBKEY(new_elem->nbkey) + new_elem->neflag) == 0) {
        do_zset_elem_free(new_elem);
        return ENGINE_SUCCESS;
    }

    /* replace in bplus tree */
    if (bplus_elem_find(&info->bplus, new_elem->data, new_elem->nbkey,
                        member, new_elem->nbytes, path) != NULL) {
        /* same score: replace in place */
        replaced = (zset_elem_item *)bplus_elem_replace(&path[0], (bplus_elem_item *)new_elem);
        assert(replaced == old_elem);
    } else {
        /* score changed: add at the new position, then remove the old one */
        ENGINE_ERROR_CODE ret = bplus_elem_add(&info->bplus, path, (bplus_elem_item *)new_elem,
                                               &space_increased);
        if (ret != ENGINE_SUCCESS) {
            return ret;
        }
        uint32_t deleted = bplus_elem_delete(&info->bplus, old_elem->data, old_elem->nbkey,
                                             member, old_elem->nbytes, NULL,
                                             NULL, NULL, &space_decreased);
        assert(deleted == 1);
    }

    /* replace in hash tree */
    replaced = (zset_elem_item *)htree_elem_replace(pinfo, (htree_elem_item *)new_elem);
    assert(replaced == old_elem);
    assert(old_elem->linked == 0);

    /* apply memory space */
    size_t old_stotal = slabs_space_size(do_zset_elem_ntotal(old_elem)) + space_decreased;
    size_t new_stotal = slabs_space_size(do_zset_elem_ntotal(new_elem)) + space_increased;

    if (new_stotal != old_stotal) {
        assert(info->stotal > 0);
        if (new_stotal > old_stotal)
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_ZSET, (new_stotal-old_stotal));
        else
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_ZSET, (old_stotal-new_stotal));
    }

    if (old_elem->refcount == 0) {
        do_zset_elem_free(old_elem);
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_zset_elem_add(zset_meta_info *info, htree_prev_info *pinfo,
                                          zset_elem_item *elem)
{
    ENGINE_ERROR_CODE ret;
    bplus_elem_posi path[BPLUS_MAX_DEPTH];
    const void *member = elem->data + ZSET_REAL_NBKEY(elem->nbkey) + elem->neflag;

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
    ret = htree_elem_add(&info->htree, (htree_elem_item *)elem, pinfo, &space_increased);
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

static ENGINE_ERROR_CODE do_zset_elem_insert(hash_item *it, zset_elem_item *elem,
                                             const bool replace_if_exist,
                                             ENGINE_UPDATE_COND cond, bool *replaced)
{
    zset_meta_info *info = (zset_meta_info *)item_get_meta(it);
    ENGINE_ERROR_CODE ret;
    htree_prev_info pinfo;
    zset_elem_item *find;
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
    find = (zset_elem_item *)htree_elem_find(&info->htree, elem->hval,
                                             member, elem->nbytes, &pinfo);

    if (find != NULL) {
        if (!replace_if_exist) {
            return ENGINE_ELEM_EEXISTS;
        }
        /* update condition check: score */
        if ((cond == UPDATE_COND_GT && !BKEY_ISGT(elem->data, elem->nbkey, find->data, find->nbkey)) ||
            (cond == UPDATE_COND_LT && !BKEY_ISLT(elem->data, elem->nbkey, find->data, find->nbkey))) {
            return ENGINE_NOT_STORED;
        }
#ifdef ENABLE_STICKY_ITEM
        /* Do not check sticky memory limit.
         * Because, only score and eflag can be changed,
         * so the space difference is negligible.
         */
#endif
        ret = do_zset_elem_replace(info, &pinfo, elem);
        if (ret != ENGINE_SUCCESS) {
            return ret;
        }
        if (replaced) *replaced = true;
        return ENGINE_SUCCESS;
    }

    return do_zset_elem_add(info, &pinfo, elem);
}

static ENGINE_ERROR_CODE do_zset_elem_update(zset_meta_info *info,
                                             const char *member, const uint32_t nbytes,
                                             const void *score, const uint32_t nscore,
                                             ENGINE_UPDATE_COND cond, const eflag_update *eupdate)
{
    htree_prev_info pinfo;
    zset_elem_item *elem;
    unsigned char *ptr;
    uint32_t real_nbkey;
    uint32_t new_nbkey;
    uint32_t new_neflag;

    int hval = genhash_string_hash(member, nbytes);
    elem = (zset_elem_item *)htree_elem_find(&info->htree, hval, member, nbytes, &pinfo);
    if (elem == NULL) {
        return ENGINE_ELEM_ENOENT;
    }

    /* validation check: eflag update */
    if (eupdate != NULL && eupdate->neflag > 0 && eupdate->bitwop < BITWISE_OP_MAX) {
        if (eupdate->offset >= elem->neflag || eupdate->neflag > (elem->neflag-eupdate->offset)) {
            return ENGINE_EBADEFLAG;
        }
    }

    /* update condition check: score */
    if ((cond == UPDATE_COND_GT && !BKEY_ISGT(score, nscore, elem->data, elem->nbkey)) ||
        (cond == UPDATE_COND_LT && !BKEY_ISLT(score, nscore, elem->data, elem->nbkey))) {
        return ENGINE_NOT_STORED;
    }

    real_nbkey = ZSET_REAL_NBKEY(elem->nbkey);
    new_nbkey = (score == NULL ? elem->nbkey : nscore);
    new_neflag = (eupdate == NULL || eupdate->bitwop < BITWISE_OP_MAX ? elem->neflag : eupdate->neflag);

    if (elem->refcount == 0 &&
        (score == NULL || BKEY_ISEQ(score, nscore, elem->data, elem->nbkey)) &&
        elem->neflag == new_neflag) {
        /* same score and eflag size */
        /* do in-place update */
        if (eupdate != NULL) {
            if (eupdate->bitwop < BITWISE_OP_MAX) {
                ptr = elem->data + real_nbkey + eupdate->offset;
                (*BINARY_BITWISE_OP[eupdate->bitwop])(ptr, eupdate->eflag, eupdate->neflag, ptr);
            } else {
                if (eupdate->neflag > 0) {
                    memcpy(elem->data + real_nbkey, eupdate->eflag, eupdate->neflag);
                }
            }
        }
    } else {
        /* different score or eflag size */
#ifdef ENABLE_STICKY_ITEM
        /* Do not check sticky memory limit.
         * Because, only score and eflag can be changed,
         * so the space difference is negligible.
         */
#endif

        zset_elem_item *new_elem = do_zset_elem_alloc(new_nbkey, new_neflag, elem->nbytes);
        if (new_elem == NULL) {
            return ENGINE_ENOMEM;
        }

        /* build the new element */
        if (score != NULL) {
            memcpy(new_elem->data, score, ZSET_REAL_NBKEY(new_nbkey));
        } else {
            memcpy(new_elem->data, elem->data, real_nbkey);
        }
        ptr = new_elem->data + ZSET_REAL_NBKEY(new_nbkey);

        if (eupdate == NULL || eupdate->bitwop < BITWISE_OP_MAX) {
            if (elem->neflag > 0) {
                memcpy(ptr, elem->data + real_nbkey, elem->neflag);
            }
            if (eupdate != NULL) {
                (*BINARY_BITWISE_OP[eupdate->bitwop])(ptr + eupdate->offset, eupdate->eflag,
                                                      eupdate->neflag, ptr + eupdate->offset);
            }
        } else {
            if (eupdate->neflag > 0) {
                memcpy(ptr, eupdate->eflag, eupdate->neflag);
            }
        }

        memcpy(ptr + new_neflag, elem->data + real_nbkey + elem->neflag, elem->nbytes);
        new_elem->hval = elem->hval;

        /* replace the element */
        ENGINE_ERROR_CODE ret = do_zset_elem_replace(info, &pinfo, new_elem);
        if (ret != ENGINE_SUCCESS) {
            do_zset_elem_free(new_elem);
            return ret;
        }
    }

    return ENGINE_SUCCESS;
}

static uint32_t do_zset_elem_delete(zset_meta_info *info, const int nummembers,
                                    const field_t *mlist)
{
    bplus_meta *bplus = &info->bplus;
    uint32_t delcnt = 0;
    zset_delete_ctx delete_ctx = {info, false};
    size_t space_decreased = 0;

    if (bplus->root != NULL) {
        if (nummembers == 0) {
            uint32_t hdelcnt = htree_elem_delete_bulk(&info->htree, 0 /* all */,
                                                      NULL, &space_decreased);
            delcnt = bplus_elem_delete_bulk(bplus, true, NULL, NULL, 0, 0 /* all */,
                                            &delete_ctx, NULL, &space_decreased);
            assert(delcnt == hdelcnt);
        } else {
            for (int ii = 0; ii < nummembers; ii++) {
                int hval = genhash_string_hash(mlist[ii].value, mlist[ii].length);
                zset_elem_item *elem = (zset_elem_item *)htree_elem_delete(&info->htree, hval,
                                                                           mlist[ii].value,
                                                                           mlist[ii].length,
                                                                           &space_decreased);
                if (elem != NULL) {
                    uint32_t deleted = bplus_elem_delete(bplus, elem->data, elem->nbkey,
                                                         mlist[ii].value, mlist[ii].length, NULL,
                                                         &delete_ctx, NULL, &space_decreased);
                    assert(deleted == 1);
                    delcnt++;
                }
            }
        }

        if (info->stotal > 0 && space_decreased > 0) { /* apply memory space */
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_ZSET, space_decreased);
        }
    }
    return delcnt;
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
                                   zset_elem_item *elem, const bool replace_if_exist,
                                   ENGINE_UPDATE_COND cond, item_attr *attrp,
                                   bool *replaced, bool *created, const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_ZSET_ELEM_INSERT);

    assert(replace_if_exist || cond == UPDATE_COND_NONE);

    *created = false;
    *replaced = false;

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
        ret = do_zset_elem_insert(it, elem, replace_if_exist, cond, replaced);
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

ENGINE_ERROR_CODE zset_elem_update(const char *key, const uint32_t nkey,
                                   const char *member, const uint32_t nbytes,
                                   const void *score, const uint32_t nscore,
                                   ENGINE_UPDATE_COND cond, const eflag_update *eupdate,
                                   const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_ZSET_ELEM_INSERT);

    assert(score != NULL || eupdate != NULL);
    assert(score != NULL || cond == UPDATE_COND_NONE);

    LOCK_CACHE();
    ret = do_zset_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        zset_meta_info *info = (zset_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if (score != NULL &&
                ((info->bktype == BKEY_TYPE_UINT64 && nscore >  0) ||
                 (info->bktype == BKEY_TYPE_BINARY && nscore == 0))) {
                ret = ENGINE_EBADBKEY; break;
            }
            ret = do_zset_elem_update(info, member, nbytes, score, nscore, cond, eupdate);
        } while(0);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE zset_elem_delete(const char *key, const uint32_t nkey,
                                   const int nummembers, const field_t *mlist,
                                   const bool drop_if_empty,
                                   uint32_t *del_count, bool *dropped,
                                   const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_ZSET_ELEM_DELETE_DROP
                                                    : UPD_ZSET_ELEM_DELETE));

    *dropped = false;

    LOCK_CACHE();
    ret = do_zset_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        zset_meta_info *info = (zset_meta_info *)item_get_meta(it);
        *del_count = do_zset_elem_delete(info, nummembers, mlist);
        if (*del_count > 0) {
            if (info->ccnt == 0 && drop_if_empty) {
                assert(info->bplus.root == NULL && info->htree.root == NULL);
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

uint32_t zset_elem_delete_with_count(zset_meta_info *info, const uint32_t count)
{
    bplus_meta *bplus = &info->bplus;
    uint32_t delcnt = 0;
    size_t space_decreased = 0;

    // cache_lock is held by caller.
    if (bplus->root != NULL) {
        zset_delete_ctx delete_ctx = {info, true};
        /* hash tree delete is done in delete_post */
        delcnt = bplus_elem_delete_bulk(bplus, true, NULL, NULL, 0, count,
                                        &delete_ctx, NULL, &space_decreased);
        if (info->stotal > 0 && space_decreased > 0) { /* apply memory space */
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_ZSET, space_decreased);
        }
    }
    return delcnt;
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
