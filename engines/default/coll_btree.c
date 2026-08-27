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

/* bkey type */
#define BKEY_TYPE_UNKNOWN 0
#define BKEY_TYPE_UINT64  1
#define BKEY_TYPE_BINARY  2

/* get bkey real size */
#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))

/* overflow type */
#define OVFL_TYPE_NONE  0
#define OVFL_TYPE_COUNT 1
#define OVFL_TYPE_RANGE 2

/* btree scan structure */
typedef struct _btree_scan_info {
    hash_item          *it;
    ds_btree_elem_posi  posi;
    uint32_t            kidx; /* An index in the given key array as a parameter */
    int32_t             next; /* for free scan link */
} btree_scan_info;

/* btree delete context */
typedef struct {
    btree_meta_info        *info;
    enum elem_delete_cause  cause;
} btree_delete_ctx;

/* bkey min & max value */
static uint64_t      bkey_uint64_min;
static uint64_t      bkey_uint64_max;
static unsigned char bkey_binary_min[MIN_BKEY_LENG];
static unsigned char bkey_binary_max[MAX_BKEY_LENG];

/* Temporary Facility
 * forced btree overflow action
 */
static char    forced_action_prefix[256];
static int32_t forced_action_pfxlen = 0;
static uint8_t forced_btree_ovflact = 0;

static void _check_forced_btree_overflow_action(void)
{
    char *envstr;
    char *envval;

    envstr = getenv("ARCUS_FORCED_BTREE_OVERFLOW_ACTION");
    if (envstr != NULL) {
        char *delimiter = memchr(envstr, ':', strlen(envstr));
        if (delimiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ARCUS_FORCED_BTREE_OVERFLOW_ACTION: NO prefix delimiter\n");
            return;
        }
        envval = delimiter + 1;

        forced_action_pfxlen = envval - envstr;
        if (forced_action_pfxlen >= 256) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ARCUS_FORCED_BTREE_OVERFLOW_ACTION: Too long prefix name\n");
            return;
        }
        memcpy(forced_action_prefix, envstr, forced_action_pfxlen);
        forced_action_prefix[forced_action_pfxlen] = '\0';

        if (strcmp(envval, "smallest_trim") == 0)
            forced_btree_ovflact = OVFL_SMALLEST_TRIM;
        else if (strcmp(envval, "smallest_silent_trim") == 0)
            forced_btree_ovflact = OVFL_SMALLEST_SILENT_TRIM;
        else if (strcmp(envval, "largest_trim") == 0)
            forced_btree_ovflact = OVFL_LARGEST_TRIM;
        else if (strcmp(envval, "largest_silent_trim") == 0)
            forced_btree_ovflact = OVFL_LARGEST_SILENT_TRIM;
        else if (strcmp(envval, "error") == 0)
            forced_btree_ovflact = OVFL_ERROR;
        else {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ARCUS_FORCED_BTREE_OVERFLOW_ACTION: Invalid overflow action\n");
            forced_action_prefix[0] = '\0';
            return;
        }

        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "ARCUS_FORCED_BTREE_OVERFLOW_ACTION: prefix=%s action=%s\n",
                    forced_action_prefix, envval);
    }
}

static void _setif_forced_btree_overflow_action(btree_meta_info *info,
                                                const void *key, const uint32_t nkey)
{
    if (forced_btree_ovflact != 0 && forced_action_pfxlen < nkey &&
        memcmp(forced_action_prefix, key, forced_action_pfxlen) == 0) {
        info->ovflact = forced_btree_ovflact;
    }
}

/*
 * B+TREE collection management
 */
//#define BTREE_DELETE_NO_MERGE

static const void *do_btree_get_bkey(const ds_btree_elem_item *elem, uint32_t *nbkey);
static const void *do_btree_get_eflag(const ds_btree_elem_item *elem, uint32_t *neflag);
static int         do_btree_bkey_cmp(const void *bkey1, uint32_t nbkey1, const void *bkey2, uint32_t nbkey2);
static void        do_btree_elem_delete_post(ds_btree_elem_item *elem, void *arg);

static ds_btree_ops btree_ops = {
    .get_bkey    = do_btree_get_bkey,
    .get_eflag   = do_btree_get_eflag,
    .bkey_cmp    = do_btree_bkey_cmp,
    .tiebreak    = NULL,
    .delete_post = do_btree_elem_delete_post
};

static inline uint32_t do_btree_elem_ntotal(btree_elem_item *elem)
{
    return sizeof(btree_elem_item) + BTREE_REAL_NBKEY(elem->nbkey)
           + elem->neflag + elem->nbytes;
}

static ENGINE_ERROR_CODE do_btree_item_find(const void *key, const uint32_t nkey,
                                            bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_BTREE_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static int32_t do_btree_real_maxcount(int32_t maxcount)
{
    int32_t real_maxcount = maxcount;

    if (maxcount < 0) {
        /* It has the max_btree_size that can be increased in the future */
        real_maxcount = -1;
    } else if (maxcount == 0) {
        real_maxcount = DEFAULT_BTREE_SIZE;
    } else if (maxcount > config->max_btree_size) {
        real_maxcount = config->max_btree_size;
    }
    return real_maxcount;
}

static hash_item *do_btree_item_alloc(const void *key, const uint32_t nkey,
                                      item_attr *attrp)
{
    uint32_t nbytes = 2; /* "\r\n" */
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(btree_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_BTREE;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), "\r\n", nbytes);

        /* initialize b+tree meta information */
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        info->mcnt = do_btree_real_maxcount(attrp->maxcount);
        info->ccnt = 0;
        info->ovflact = (attrp->ovflaction==0 ? OVFL_SMALLEST_TRIM : attrp->ovflaction);
        info->mflags  = 0;
#ifdef ENABLE_STICKY_ITEM
        if (IS_STICKY_EXPTIME(attrp->exptime)) info->mflags |= COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1)              info->mflags |= COLL_META_FLAG_READABLE;
        info->itdist  = (uint16_t)((size_t*)info-(size_t*)it);
        info->stotal  = 0;
        info->bktype  = BKEY_TYPE_UNKNOWN;
        info->maxbkeyrange.len = BKEY_NULL;
        ds_btree_init(&info->btree, &btree_ops);
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);

        /* set if forced_btree_overflow_actions is given */
        _setif_forced_btree_overflow_action(info, key, nkey);
    }
    return it;
}

static btree_elem_item *do_btree_elem_alloc(const uint32_t nbkey, const uint32_t neflag,
                                            const uint32_t nbytes)
{
    size_t ntotal = sizeof(btree_elem_item) + BTREE_REAL_NBKEY(nbkey) + neflag + nbytes;

    btree_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL);
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

static void do_btree_elem_free(btree_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = do_btree_elem_ntotal(elem);
    do_item_mem_free(elem, ntotal);
}

static void do_btree_elem_release(btree_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->linked == 0) {
        do_btree_elem_free(elem);
    }
}

static inline void do_btree_copy_bkey(btree_elem_item *elem, bkey_t *bkey)
{
    if (elem->nbkey > 0) {
        bkey->len = elem->nbkey;
        memcpy(bkey->val, elem->data, elem->nbkey);
    } else {
        bkey->len = 0;
        memcpy(bkey->val, elem->data, sizeof(uint64_t));
    }
}

/******************* BKEY COMPARISION CODE *************************/
static inline int UINT64_COMP(const uint64_t *v1, const uint64_t *v2)
{
    if (*v1 == *v2) return  0;
    if (*v1 <  *v2) return -1;
    else            return  1;
}

static inline bool UINT64_ISEQ(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 == *v2) ? true : false);
}

static inline bool UINT64_ISNE(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 != *v2) ? true : false);
}

static inline bool UINT64_ISLT(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 <  *v2) ? true : false);
}

static inline bool UINT64_ISLE(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 <= *v2) ? true : false);
}

static inline bool UINT64_ISGT(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 >  *v2) ? true : false);
}

static inline bool UINT64_ISGE(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 >= *v2) ? true : false);
}

static bool (*UINT64_COMPARE_OP[COMPARE_OP_MAX]) (const uint64_t *v1, const uint64_t *v2)
    = { UINT64_ISEQ, UINT64_ISNE, UINT64_ISLT, UINT64_ISLE, UINT64_ISGT, UINT64_ISGE };

#define BKEY_COMP(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_COMP((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_COMP((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISEQ(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISEQ((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISEQ((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISNE(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISNE((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISNE((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISLT(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISLT((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISLT((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISLE(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISLE((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISLE((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISGT(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISGT((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISGT((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISGE(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISGE((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISGE((bk1),(nbk1),(bk2),(nbk2)))
/******************* BKEY COMPARISION CODE *************************/

/**************** MAX BKEY RANGE MANIPULATION **********************/
static inline void UINT64_COPY(const uint64_t *v, uint64_t *result)
{
    *result = *v;
}

static inline void UINT64_DIFF(const uint64_t *v1, const uint64_t *v2, uint64_t *result)
{
    assert(*v1 >= *v2);
    *result = *v1 - *v2;
}

#if 0 // OLD_CODE
static inline void UINT64_INCR(uint64_t *v)
{
    assert(*v < UINT64_MAX);
    *v += 1;
}
#endif

static inline void UINT64_DECR(uint64_t *v)
{
    assert(*v > 0);
    *v -= 1;
}

static inline void BINARY_COPY(const unsigned char *v, const int length,
                               unsigned char *result)
{
    if (length > 0)
        memcpy(result, v, length);
}

static inline void BINARY_DIFF(unsigned char *v1, const uint8_t nv1,
                               unsigned char *v2, const uint8_t nv2,
                               const int length, unsigned char *result)
{
    assert(length > 0);
    unsigned char bkey1_space[MAX_BKEY_LENG];
    unsigned char bkey2_space[MAX_BKEY_LENG];
    int i, subtraction;

    if (nv1 < length) {
        memcpy(bkey1_space, v1, nv1);
        for (i=nv1; i<length; i++)
            bkey1_space[i] = 0x00;
        v1 = bkey1_space;
    }
    if (nv2 < length) {
        memcpy(bkey2_space, v2, nv2);
        for (i=nv2; i<length; i++)
            bkey2_space[i] = 0x00;
        v2 = bkey2_space;
    }

    /* assume that the value of v1 >= the value of v2 */
    subtraction = 0;
    for (i = (length-1); i >= 0; i--) {
        if (subtraction == 0) {
            if (v1[i] >= v2[i]) {
                result[i] = v1[i] - v2[i];
            } else {
                result[i] = 0xFF - v2[i] + v1[i] + 1;
                subtraction = 1;
            }
        } else {
            if (v1[i] > v2[i]) {
                result[i] = v1[i] - v2[i] - 1;
                subtraction = 0;
            } else {
                result[i] = 0xFF - v2[i] + v1[i];
            }
        }
    }
}

#if 0 // OLD_CODE
static inline void BINARY_INCR(unsigned char *v, const int length)
{
    assert(length > 0);
    int i;
    for (i = (length-1); i >= 0; i--) {
        if (v[i] < 0xFF) {
            v[i] += 1;
            break;
        }
        v[i] = 0x00;
    }
    assert(i >= 0);
}
#endif

static inline void BINARY_DECR(unsigned char *v, const int length)
{
    assert(length > 0);
    int i;
    for (i = (length-1); i >= 0; i--) {
        if (v[i] > 0x00) {
            v[i] -= 1;
            break;
        }
        v[i] = 0xFF;
    }
    assert(i >= 0);
}

#define BKEY_COPY(bk, nbk, res) \
        ((nbk)==0 ? UINT64_COPY((const uint64_t*)(bk), (uint64_t*)(res)) \
                  : BINARY_COPY((bk), (nbk), (res)))
#define BKEY_DIFF(bk1, nbk1, bk2, nbk2, len, res) \
        ((len)==0 ? UINT64_DIFF((const uint64_t*)(bk1), (const uint64_t*)(bk2), (uint64_t*)(res)) \
                  : BINARY_DIFF((bk1), (nbk1), (bk2), (nbk2), (len), (res)))
#if 0 // OLD_CODE
#define BKEY_INCR(bk, nbk) \
        ((nbk)==0 ? UINT64_INCR((uint64_t*)(bk)) : BINARY_INCR((bk), (nbk)))
#endif
#define BKEY_DECR(bk, nbk) \
        ((nbk)==0 ? UINT64_DECR((uint64_t*)(bk)) : BINARY_DECR((bk), (nbk)))

/**************** MAX BKEY RANGE MANIPULATION **********************/

static const void *do_btree_get_bkey(const ds_btree_elem_item *elem, uint32_t *nbkey)
{
    btree_elem_item *e = (btree_elem_item *)elem;
    *nbkey = e->nbkey;
    return e->data;
}

static const void *do_btree_get_eflag(const ds_btree_elem_item *elem, uint32_t *neflag)
{
    btree_elem_item *e = (btree_elem_item *)elem;
    *neflag = e->neflag;
    return e->data + BTREE_REAL_NBKEY(e->nbkey);
}

static int do_btree_bkey_cmp(const void *bkey1, uint32_t nbkey1, const void *bkey2, uint32_t nbkey2)
{
    return BKEY_COMP(bkey1, nbkey1, bkey2, nbkey2);
}

static void do_btree_elem_delete_post(ds_btree_elem_item *elem, void *arg)
{
    btree_elem_item *e = (btree_elem_item *)elem;
    btree_delete_ctx *ctx = (btree_delete_ctx *)arg;
    btree_meta_info *info = ctx->info;
    enum elem_delete_cause cause = ctx->cause;

    CLOG_BTREE_ELEM_DELETE(info, e, cause);
    info->ccnt--;
    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(do_btree_elem_ntotal(e));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
    }
    assert(e->linked == 0);
    if (e->refcount == 0) {
        do_btree_elem_free(e);
    }
}

static int do_btree_bkey_range_type(const bkey_range *bkrange)
{
    if (bkrange->to_nbkey == BKEY_NULL) {
        return BKEY_RANGE_TYPE_SIN;
    } else {
        int comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey,
                             bkrange->to_bkey,   bkrange->to_nbkey);
        if (comp == 0)      return BKEY_RANGE_TYPE_SIN; /* single bkey */
        else if (comp < 0)  return BKEY_RANGE_TYPE_ASC; /* ascending */
        else                return BKEY_RANGE_TYPE_DSC; /* descending */
    }
}

static void do_btree_elem_replace(btree_meta_info *info,
                                  ds_btree_elem_posi *posi, btree_elem_item *new_elem)
{
    btree_elem_item *old_elem = (btree_elem_item *)ds_btree_elem_replace(posi, (ds_btree_elem_item *)new_elem);

    CLOG_BTREE_ELEM_INSERT(info, old_elem, new_elem);

    size_t old_stotal = slabs_space_size(do_btree_elem_ntotal(old_elem));
    size_t new_stotal = slabs_space_size(do_btree_elem_ntotal(new_elem));

    if (new_stotal != old_stotal) { /* apply memory space */
        assert(info->stotal > 0);
        if (new_stotal > old_stotal)
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_BTREE, (new_stotal-old_stotal));
        else
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, (old_stotal-new_stotal));
    }

    if (old_elem->refcount == 0) {
        do_btree_elem_free(old_elem);
    }
}

static ENGINE_ERROR_CODE do_btree_elem_update(btree_meta_info *info,
                                              const int bkrtype, const bkey_range *bkrange,
                                              const eflag_update *eupdate,
                                              const char *value, const uint32_t nbytes)
{
    btree_elem_item *elem;
    unsigned char *ptr;
    uint32_t real_nbkey;
    uint32_t new_neflag;
    uint32_t new_nbytes;
    ds_btree_elem_posi posi;
    ds_btree_meta *btree = &info->btree;

    if (btree->root == NULL) {
        return ENGINE_ELEM_ENOENT;
    }

    elem = (btree_elem_item *)ds_btree_find_first(btree, bkrtype, bkrange, &posi, false);
    if (elem == NULL) {
        return ENGINE_ELEM_ENOENT;
    }

    assert(posi.bkeq == true);

    /* check eflag update validation check */
    if (eupdate != NULL && eupdate->neflag > 0 && eupdate->bitwop < BITWISE_OP_MAX) {
        if (eupdate->offset >= elem->neflag || eupdate->neflag > (elem->neflag-eupdate->offset)) {
            return ENGINE_EBADEFLAG;
        }
    }

    real_nbkey = BTREE_REAL_NBKEY(elem->nbkey);
    new_neflag = (eupdate == NULL || eupdate->bitwop < BITWISE_OP_MAX ? elem->neflag : eupdate->neflag);
    new_nbytes = (value == NULL ? elem->nbytes : nbytes);

    if (elem->refcount == 0 && (elem->neflag+elem->nbytes) == (new_neflag+new_nbytes)) {
        /* old body size == new body size */
        /* do in-place update */
        if (eupdate != NULL) {
            if (eupdate->bitwop < BITWISE_OP_MAX) {
                ptr = elem->data + real_nbkey + eupdate->offset;
                (*BINARY_BITWISE_OP[eupdate->bitwop])(ptr, eupdate->eflag, eupdate->neflag, ptr);
            } else {
                if (eupdate->neflag > 0) {
                    memcpy(elem->data + real_nbkey, eupdate->eflag, eupdate->neflag);
                }
                elem->neflag = eupdate->neflag;
            }
        }
        if (value != NULL) {
            memcpy(elem->data + real_nbkey + elem->neflag, value, nbytes);
            elem->nbytes = nbytes;
        }
        CLOG_BTREE_ELEM_INSERT(info, elem, elem);
    } else {
        /* old body size != new body size */
#ifdef ENABLE_STICKY_ITEM
         /* sticky memory limit check */
         if (IS_STICKY_COLLFLG(info)) {
             if ((elem->neflag + elem->nbytes) < (new_neflag + new_nbytes)) {
                 if (do_item_sticky_overflowed())
                     return ENGINE_ENOMEM;
             }
         }
#endif

        btree_elem_item *new_elem = do_btree_elem_alloc(elem->nbkey, new_neflag, new_nbytes);
        if (new_elem == NULL) {
            return ENGINE_ENOMEM;
        }

        /* build the new element */
        memcpy(new_elem->data, elem->data, real_nbkey);

        if (eupdate == NULL || eupdate->bitwop < BITWISE_OP_MAX) {
            if (elem->neflag > 0) {
                memcpy(new_elem->data + real_nbkey, elem->data + real_nbkey, elem->neflag);
            }
            if (eupdate != NULL) {
                ptr = new_elem->data + real_nbkey + eupdate->offset;
                (*BINARY_BITWISE_OP[eupdate->bitwop])(ptr, eupdate->eflag, eupdate->neflag, ptr);
            }
        } else {
            if (eupdate->neflag > 0) {
                memcpy(new_elem->data + real_nbkey, eupdate->eflag, eupdate->neflag);
            }
        }

        ptr = new_elem->data + real_nbkey + new_elem->neflag;
        if (value != NULL) {
            memcpy(ptr, value, nbytes);
        } else {
            memcpy(ptr, elem->data + real_nbkey + elem->neflag, elem->nbytes);
        }

        do_btree_elem_replace(info, &posi, new_elem);
    }

    return ENGINE_SUCCESS;
}

#ifdef BTREE_DELETE_NO_MERGE
static int do_btree_elem_delete_fast(btree_meta_info *info,
                                     ds_btree_elem_posi *path, const uint32_t count)
{
    ds_btree_indx_node *node;
    btree_elem_item *elem;
    int i, delcnt=0;
    int cur_depth;

    if (info->root == NULL) {
        return 0;
    }
    assert(info->root->ndepth < DS_BTREE_MAX_DEPTH);

    if (path[0].node == NULL) {
        path[0].node = ds_btree_get_first_leaf(info->root, path);
        cur_depth = 0;
    } else {
        cur_depth = path[0].indx; /* it's used to keep btree depth on delete */
    }

    node = path[cur_depth].node;
    while (node != NULL) {
        /* delete element items or lower nodes */
        if (node->ndepth == 0) { /* leaf node */
            for (i = 0; i < node->used_count; i++) {
                elem = (btree_elem_item *)node->item[i];
                elem->linked--;
                assert(elem->linked == 0);
                if (elem->refcount == 0) {
                    do_btree_elem_free(elem);
                }
            }
        } else {
            for (i = 0; i < node->used_count; i++) {
                ds_btree_node_free(node->item[i]);
            }
        }
        delcnt += node->used_count;

        /* get the next node */
        node = node->next;
        if (node == NULL && cur_depth < info->root->ndepth) {
            cur_depth += 1;
            node = path[cur_depth].node;
        }

        /* check if current deletion should be stopped */
        if (count > 0 && delcnt >= count) {
            path[cur_depth].node = node;
            path[0].indx = cur_depth;
            break;
        }
    }
    if (node == NULL) {
        info->root = NULL;
        info->ccnt = 0;
        if (info->stotal > 0) {
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, info->stotal);
        }
    }
    return delcnt;
}
#endif

static uint32_t do_btree_elem_delete(btree_meta_info *info,
                                     const int bkrtype, const bkey_range *bkrange,
                                     const eflag_filter *efilter, const uint32_t offset,
                                     const uint32_t count, uint32_t *opcost,
                                     enum elem_delete_cause cause)
{
    ds_btree_meta *btree = &info->btree;
    btree_delete_ctx delete_ctx = {info, cause};
    uint32_t tot_found = 0;
    size_t space_decreased = 0;

    if (opcost) *opcost = 0;
    if (btree->root == NULL) return 0;
    assert(btree->root->ndepth < DS_BTREE_MAX_DEPTH);

    if (bkrtype == BKEY_RANGE_TYPE_SIN) {
        assert(offset == 0);
        btree_elem_item *deleted = (btree_elem_item *)ds_btree_elem_delete(btree, bkrtype, bkrange, efilter,
                                                                           &delete_ctx,
                                                                           opcost, &space_decreased);
        if (deleted != NULL) {
            tot_found++;
            if (info->stotal > 0 && space_decreased > 0) {
                do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, space_decreased);
            }
        }
    } else {
        CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, count, cause);

        tot_found = ds_btree_elem_delete_bulk(btree, bkrtype, bkrange, efilter, offset, count,
                                              &delete_ctx, opcost, &space_decreased);
        if (info->stotal > 0 && space_decreased > 0) {
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, space_decreased);
        }
        CLOG_ELEM_DELETE_END((coll_meta_info*)info, cause);
    }
    if (tot_found > 0) {
        CLOG_BTREE_ELEM_DELETE_LOGICAL(info, bkrange, efilter, offset, count, cause);
    }
    return tot_found;
}

#if 0 /* Might be used later */
static inline void get_bkey_full_range(const int bktype, const bool ascend, bkey_range *bkrange)
{
    if (bktype == BKEY_TYPE_BINARY) {
        if (ascend) {
            memcpy(bkrange->from_bkey, bkey_binary_min, MIN_BKEY_LENG);
            memcpy(bkrange->to_bkey,   bkey_binary_max, MAX_BKEY_LENG);
            bkrange->from_nbkey = MIN_BKEY_LENG;
            bkrange->to_nbkey   = MAX_BKEY_LENG;
        } else {
            memcpy(bkrange->from_bkey, bkey_binary_max, MAX_BKEY_LENG);
            memcpy(bkrange->to_bkey,   bkey_binary_min, MIN_BKEY_LENG);
            bkrange->from_nbkey = MAX_BKEY_LENG;
            bkrange->to_nbkey   = MIN_BKEY_LENG;
        }
    } else { /* bktype == BKEY_TYPE_UINT64 or BKEY_TYPE_UNKNOWN */
        if (ascend) {
            memcpy(bkrange->from_bkey, (void*)&bkey_uint64_min, sizeof(uint64_t));
            memcpy(bkrange->to_bkey,   (void*)&bkey_uint64_max, sizeof(uint64_t));
        } else {
            memcpy(bkrange->from_bkey, (void*)&bkey_uint64_max, sizeof(uint64_t));
            memcpy(bkrange->to_bkey,   (void*)&bkey_uint64_min, sizeof(uint64_t));
        }
        bkrange->from_nbkey = bkrange->to_nbkey = 0;
    }
}
#endif

static ENGINE_ERROR_CODE do_btree_overflow_check(btree_meta_info *info, btree_elem_item *elem,
                                                 int *overflow_type)
{
    /* info->ccnt >= 1 */
    ds_btree_meta *btree = &info->btree;
    btree_elem_item *min_bkey_elem = NULL;
    btree_elem_item *max_bkey_elem = NULL;
    uint32_t real_mcnt = (info->mcnt > 0 ? info->mcnt : config->max_btree_size);

    /* step 1: overflow check on max bkey range */
    if (info->maxbkeyrange.len != BKEY_NULL) {
        bkey_t newbkeyrange;

        min_bkey_elem = (btree_elem_item *)ds_btree_get_first_elem(btree->root);
        max_bkey_elem = (btree_elem_item *)ds_btree_get_last_elem(btree->root);

        if (BKEY_ISLT(elem->data, elem->nbkey, min_bkey_elem->data, min_bkey_elem->nbkey))
        {
            newbkeyrange.len = info->maxbkeyrange.len;
            BKEY_DIFF(max_bkey_elem->data, max_bkey_elem->nbkey, elem->data, elem->nbkey,
                      newbkeyrange.len, newbkeyrange.val);
            if (BKEY_ISGT(newbkeyrange.val, newbkeyrange.len, info->maxbkeyrange.val, info->maxbkeyrange.len))
            {
                if (info->ovflact == OVFL_LARGEST_TRIM || info->ovflact == OVFL_LARGEST_SILENT_TRIM)
                    *overflow_type = OVFL_TYPE_RANGE;
                else /* OVFL_SMALLEST_TRIM || OVFL_SMALLEST_SILENT_TRIM || OVFL_ERROR */
                    return ENGINE_EBKEYOOR;
            }
        }
        else if (BKEY_ISGT(elem->data, elem->nbkey, max_bkey_elem->data, max_bkey_elem->nbkey))
        {
            newbkeyrange.len = info->maxbkeyrange.len;
            BKEY_DIFF(elem->data, elem->nbkey, min_bkey_elem->data, min_bkey_elem->nbkey,
                      newbkeyrange.len, newbkeyrange.val);
            if (BKEY_ISGT(newbkeyrange.val, newbkeyrange.len, info->maxbkeyrange.val, info->maxbkeyrange.len))
            {
                if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM)
                    *overflow_type = OVFL_TYPE_RANGE;
                else /* OVFL_LARGEST_TRIM || OVFL_LARGEST_SILENT_TRIM || OVFL_ERROR */
                    return ENGINE_EBKEYOOR;
            }
        }
    }

    /* step 2: overflow check on max element count */
    if (info->ccnt >= real_mcnt && *overflow_type == OVFL_TYPE_NONE) {
        if (info->ovflact == OVFL_ERROR) {
            return ENGINE_EOVERFLOW;
        }
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM) {
            if (min_bkey_elem == NULL)
                min_bkey_elem = (btree_elem_item *)ds_btree_get_first_elem(btree->root);
            if (BKEY_ISLT(elem->data, elem->nbkey, min_bkey_elem->data, min_bkey_elem->nbkey)) {
                if (info->ovflact == OVFL_SMALLEST_TRIM) {
                    /* It means the implicit trim. */
                    info->mflags |= COLL_META_FLAG_TRIMMED; // set trimmed
                }
                return ENGINE_EBKEYOOR;
            }
        } else { /* OVFL_LARGEST_TRIM || OVFL_LARGEST_SILENT_TRIM */
            if (max_bkey_elem == NULL)
                max_bkey_elem = (btree_elem_item *)ds_btree_get_last_elem(btree->root);
            if (BKEY_ISGT(elem->data, elem->nbkey, max_bkey_elem->data, max_bkey_elem->nbkey)) {
                if (info->ovflact == OVFL_LARGEST_TRIM) {
                    /* It means the implicit trim. */
                    info->mflags |= COLL_META_FLAG_TRIMMED; // set trimmed
                }
                return ENGINE_EBKEYOOR;
            }
        }
        *overflow_type = OVFL_TYPE_COUNT;
    }

    return ENGINE_SUCCESS;
}

static void do_btree_build_smallest_trim_range(btree_meta_info *info,
                                               btree_elem_item *elem, bkey_range *bkrange)
{
    /* bkey range that must be trimmed.
     * => min bkey ~ (new max bkey - maxbkeyrange - 1)
     */
    /* from bkey */
    btree_elem_item *edge_elem = (btree_elem_item *)ds_btree_get_first_elem(info->btree.root);
    bkrange->from_nbkey = edge_elem->nbkey;
    BKEY_COPY(edge_elem->data, edge_elem->nbkey, bkrange->from_bkey);
    /* to bkey */
    bkrange->to_nbkey = info->maxbkeyrange.len;
    BKEY_DIFF(elem->data, elem->nbkey,
              info->maxbkeyrange.val, info->maxbkeyrange.len,
              bkrange->to_nbkey, bkrange->to_bkey);
    BKEY_DECR(bkrange->to_bkey, bkrange->to_nbkey);
}

static void do_btree_build_largest_trim_range(btree_meta_info *info,
                                              btree_elem_item *elem, bkey_range *bkrange)
{
    /* bkey range that must be trimmed.
     * => (new min bkey + maxbkeyrange + 1) ~ max bkey
     * => max bkey - (max bkey - new min bkey - maxbkeyrange - 1) ~ max bkey
     */
    /* from bkey */
    btree_elem_item *edge_elem = (btree_elem_item *)ds_btree_get_last_elem(info->btree.root);
    bkrange->from_nbkey = info->maxbkeyrange.len;
    BKEY_DIFF(edge_elem->data, edge_elem->nbkey, elem->data, elem->nbkey,
              bkrange->from_nbkey, bkrange->from_bkey);
    BKEY_DIFF(bkrange->from_bkey, bkrange->from_nbkey,
              info->maxbkeyrange.val, info->maxbkeyrange.len,
              bkrange->from_nbkey, bkrange->from_bkey);
    BKEY_DECR(bkrange->from_bkey, bkrange->from_nbkey);
    BKEY_DIFF(edge_elem->data, edge_elem->nbkey,
              bkrange->from_bkey, bkrange->from_nbkey,
              bkrange->from_nbkey, bkrange->from_bkey);
    /* to bkey */
    bkrange->to_nbkey = edge_elem->nbkey;
    BKEY_COPY(edge_elem->data, edge_elem->nbkey, bkrange->to_bkey);
}

static void do_btree_overflow_trim(btree_meta_info *info,
                                   btree_elem_item *elem, const int overflow_type,
                                   btree_elem_item **trimmed_elems, uint32_t *trimmed_count)
{
    assert(info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM ||
           info->ovflact == OVFL_LARGEST_TRIM  || info->ovflact == OVFL_LARGEST_SILENT_TRIM);

    if (overflow_type == OVFL_TYPE_RANGE) {
        bkey_range bkrange;
        uint32_t del_count;

        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM)
            do_btree_build_smallest_trim_range(info, elem, &bkrange);
        else
            do_btree_build_largest_trim_range(info, elem, &bkrange);

        int bkrtype = do_btree_bkey_range_type(&bkrange);
        del_count = do_btree_elem_delete(info, bkrtype, &bkrange,
                                         NULL, 0, 0, NULL, ELEM_DELETE_TRIM);
        assert(del_count > 0);
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_LARGEST_TRIM)
            info->mflags &= ~COLL_META_FLAG_TRIMMED; // clear trimmed
    } else { /* overflow_type == OVFL_TYPE_COUNT */
        assert(overflow_type == OVFL_TYPE_COUNT);
        ds_btree_elem_item *edge_elem;
        size_t space_decreased = 0;
        btree_delete_ctx delete_ctx = {info, ELEM_DELETE_TRIM};

        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM)
            edge_elem = ds_btree_delete_first_elem(&info->btree, &space_decreased);
        else
            edge_elem = ds_btree_delete_last_elem(&info->btree, &space_decreased);

        if (trimmed_elems != NULL) {
            edge_elem->refcount++;
            *trimmed_elems = (btree_elem_item *)edge_elem;
            *trimmed_count = 1;
        }
        do_btree_elem_delete_post(edge_elem, &delete_ctx);
        if (info->stotal > 0 && space_decreased > 0) { /* apply memory space */
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, space_decreased);
        }

        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_LARGEST_TRIM)
            info->mflags |= COLL_META_FLAG_TRIMMED; // set trimmed
    }
}

#define BTREE_NEED_TRIM_NOTIFICATION(info) \
    (((info)->mflags & COLL_META_FLAG_TRIMMED) != 0 && \
     ((info)->ovflact == OVFL_SMALLEST_TRIM || (info)->ovflact == OVFL_LARGEST_TRIM))

static bool do_btree_overlapped_with_trimmed_space(btree_meta_info *info, uint32_t outside)
{
    return ((info->ovflact == OVFL_SMALLEST_TRIM && (outside & DS_BTREE_OUTSIDE_LEFT)) ||
            (info->ovflact == OVFL_LARGEST_TRIM  && (outside & DS_BTREE_OUTSIDE_RIGHT)));
}

static uint32_t do_btree_elem_get(btree_meta_info *info,
                                  const int bkrtype, const bkey_range *bkrange,
                                  const eflag_filter *efilter,
                                  const uint32_t offset, const uint32_t count, const bool delete,
                                  btree_elem_item **elem_array,
                                  uint32_t *opcost, bool *potentialbkeytrim)
{
    ds_btree_meta *btree = &info->btree;
    assert(btree->root);
    btree_delete_ctx delete_ctx = {info, ELEM_DELETE_NORMAL};
    uint32_t tot_found = 0;
    uint32_t outside;
    uint32_t *outside_ptr = BTREE_NEED_TRIM_NOTIFICATION(info) ? &outside : NULL;
    size_t space_decreased = 0;

    if (opcost) *opcost = 0;
    *potentialbkeytrim = false;

    if (bkrtype == BKEY_RANGE_TYPE_SIN) {
        assert(offset == 0);
        bool ret = ds_btree_elem_get(btree, bkrtype, bkrange, efilter,
                                     delete, &delete_ctx,
                                     (ds_btree_elem_item **)elem_array, opcost, outside_ptr, &space_decreased);
        if (ret) {
            tot_found++;
            if (delete && info->stotal > 0 && space_decreased > 0) {
                do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, space_decreased);
            }
        }
    } else {
        if (delete) {
            CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, count, ELEM_DELETE_NORMAL);
        }

        tot_found = ds_btree_elem_get_bulk(btree, bkrtype, bkrange, efilter,
                                           offset, count, delete, &delete_ctx,
                                           (ds_btree_elem_item **)elem_array, opcost, outside_ptr, &space_decreased);

        if (delete) {
            if (info->stotal > 0 && space_decreased > 0) {
                do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, space_decreased);
            }
            CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);
        }
    }

    if (outside_ptr != NULL) {
       *potentialbkeytrim = do_btree_overlapped_with_trimmed_space(info, outside);
    }

    if (delete && tot_found > 0) {
        CLOG_BTREE_ELEM_DELETE_LOGICAL(info, bkrange, efilter, offset, count,
                                       ELEM_DELETE_NORMAL);
    }
    return tot_found;
}

static ENGINE_ERROR_CODE do_btree_elem_add(btree_meta_info *info, ds_btree_elem_posi *path,
                                           btree_elem_item *elem,
                                           btree_elem_item **trimmed_elems, uint32_t *trimmed_count)
{

    ENGINE_ERROR_CODE ret;
    int ovfl_type = OVFL_TYPE_NONE;

#ifdef ENABLE_STICKY_ITEM
    /* sticky memory limit check */
    if (IS_STICKY_COLLFLG(info)) {
        if (do_item_sticky_overflowed())
            return ENGINE_ENOMEM;
    }
#endif

    if (info->ccnt > 0) {
        /* overflow check */
        ret = do_btree_overflow_check(info, elem, &ovfl_type);
        if (ret != ENGINE_SUCCESS) {
            return ret;
        }
    }

    size_t space_increased = 0;
    ret = ds_btree_elem_add(&info->btree, path, (ds_btree_elem_item *)elem, &space_increased);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    CLOG_BTREE_ELEM_INSERT(info, NULL, elem);
    if (info->ccnt == 0) {
        /* set bkey type */
        if (elem->nbkey == 0)
            info->bktype = BKEY_TYPE_UINT64;
        else
            info->bktype = BKEY_TYPE_BINARY;
    }
    info->ccnt++;
    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(do_btree_elem_ntotal(elem)) + space_increased;
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
    }

    if (ovfl_type != OVFL_TYPE_NONE) {
        do_btree_overflow_trim(info, elem, ovfl_type, trimmed_elems, trimmed_count);
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_btree_elem_insert(hash_item *it, btree_elem_item *elem,
                                              const bool replace_if_exist, bool *replaced,
                                              btree_elem_item **trimmed_elems,
                                              uint32_t *trimmed_count)
{
    btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
    ds_btree_meta *btree = &info->btree;
    ds_btree_elem_posi path[DS_BTREE_MAX_DEPTH];
    btree_elem_item *find;

    /* validation check: bkey type */
    if (info->ccnt > 0 || info->maxbkeyrange.len != BKEY_NULL) {
        if ((info->bktype == BKEY_TYPE_UINT64 && elem->nbkey >  0) ||
            (info->bktype == BKEY_TYPE_BINARY && elem->nbkey == 0)) {
            return ENGINE_EBADBKEY;
        }
    }

    /* insert the element */
    find = (btree_elem_item *)ds_btree_elem_find(btree, elem->data, elem->nbkey, path);

    if (find != NULL) {
        if (!replace_if_exist) {
            return ENGINE_ELEM_EEXISTS;
        }
#ifdef ENABLE_STICKY_ITEM
        /* sticky memory limit check */
        if (IS_STICKY_COLLFLG(info)) {
            if ((find->neflag + find->nbytes) < (elem->neflag + elem->nbytes)) {
                if (do_item_sticky_overflowed())
                    return ENGINE_ENOMEM;
            }
        }
#endif
        do_btree_elem_replace(info, &path[0], elem);
        if (replaced) *replaced = true;
        return ENGINE_SUCCESS;
    }

    return do_btree_elem_add(info, path, elem, trimmed_elems, trimmed_count);
}

static ENGINE_ERROR_CODE do_btree_elem_arithmetic(btree_meta_info *info,
                                                  const void *bkey, uint32_t nbkey,
                                                  const bool increment, const bool create,
                                                  const uint64_t delta, const uint64_t initial,
                                                  const eflag_t *eflagp,
                                                  uint64_t *result)
{
    ENGINE_ERROR_CODE ret;
    btree_elem_item *elem;
    ds_btree_elem_posi path[DS_BTREE_MAX_DEPTH];
    uint64_t value;
    char     nbuf[128];
    int      nlen;
    uint32_t real_nbkey;

    /* validation check: bkey type */
    if (info->ccnt > 0 || info->maxbkeyrange.len != BKEY_NULL) {
        if ((info->bktype == BKEY_TYPE_UINT64 && nbkey >  0) ||
            (info->bktype == BKEY_TYPE_BINARY && nbkey == 0)) {
            return ENGINE_EBADBKEY;
        }
    }

    elem = (btree_elem_item *)ds_btree_elem_find(&info->btree, bkey, nbkey, path);

    if (elem != NULL) {
        real_nbkey = BTREE_REAL_NBKEY(elem->nbkey);
        if (!safe_strtoull((const char*)elem->data + real_nbkey + elem->neflag, &value) || elem->nbytes == 2) {
            return ENGINE_EINVAL;
        }

        if (increment) {
            value += delta;
        } else {
            if (delta >= value) {
                value = 0;
            } else {
                value -= delta;
            }
        }
        if ((nlen = snprintf(nbuf, sizeof(nbuf), "%"PRIu64"\r\n", value)) == -1) {
            return ENGINE_EINVAL;
        }

        if (elem->refcount == 0 && elem->nbytes == nlen) {
            memcpy(elem->data + real_nbkey + elem->neflag, nbuf, elem->nbytes);
            CLOG_BTREE_ELEM_INSERT(info, elem, elem);
        } else {
#ifdef ENABLE_STICKY_ITEM
            /* Do not check sticky memory limit.
             * Because, the space difference is negligible.
             */
#endif
            btree_elem_item *new_elem = do_btree_elem_alloc(elem->nbkey, elem->neflag, nlen);
            if (new_elem == NULL) {
                return ENGINE_ENOMEM;
            }
            memcpy(new_elem->data, elem->data, real_nbkey + elem->neflag);
            memcpy(new_elem->data + real_nbkey + new_elem->neflag, nbuf, nlen);

            do_btree_elem_replace(info, &path[0], new_elem);
        }
        *result = value;
        return ENGINE_SUCCESS;
    }

    if (!create) return ENGINE_ELEM_ENOENT;

    if ((nlen = snprintf(nbuf, sizeof(nbuf), "%"PRIu64"\r\n", initial)) == -1) {
        return ENGINE_EINVAL;
    }

    elem = do_btree_elem_alloc(nbkey,
                               (eflagp == NULL || eflagp->len == EFLAG_NULL ? 0 : eflagp->len),
                               nlen);
    if (elem == NULL) {
        return ENGINE_ENOMEM;
    }

    real_nbkey = BTREE_REAL_NBKEY(nbkey);
    memcpy(elem->data, bkey, real_nbkey);
    if (eflagp == NULL || eflagp->len == EFLAG_NULL) {
        memcpy(elem->data + real_nbkey, nbuf, nlen);
    } else {
        memcpy(elem->data + real_nbkey, eflagp->val, eflagp->len);
        memcpy(elem->data + real_nbkey + eflagp->len, nbuf, nlen);
    }

    ret = do_btree_elem_add(info, path, elem, NULL, NULL);
    if (ret != ENGINE_SUCCESS) {
        do_btree_elem_free(elem);
        return ret;
    }

    *result = initial;
    return ENGINE_SUCCESS;
}

#ifdef SUPPORT_BOP_SMGET
static inline int do_btree_comp_hkey(hash_item *it1, hash_item *it2)
{
    int cmp_res;
    if (it1->nkey == it2->nkey) {
        cmp_res = strncmp(item_get_key(it1), item_get_key(it2), it1->nkey);
    } else {
        if (it1->nkey < it2->nkey) {
            cmp_res = strncmp(item_get_key(it1), item_get_key(it2), it1->nkey);
            if (cmp_res == 0) cmp_res = -1;
        } else {
            cmp_res = strncmp(item_get_key(it1), item_get_key(it2), it2->nkey);
            if (cmp_res == 0) cmp_res =  1;
        }
    }
    return cmp_res;
}

/*** NOT USED CODE ***
static inline int do_comp_key_string(const char *key1, const int len1,
                                     const char *key2, const int len2)
{
    int res;
    if (len1 == len2) {
        retrun strncmp(key1, key2, len1);
    }
    if (len1 < len2) {
        res = strncmp(key1, key2, len1);
        if (res == 0) res = -1;
    } else {
        res = strncmp(key1, key2, len2);
        if (res == 0) res =  1;
    }
    return res;
}
**********************/

static btree_elem_item *do_btree_scan_next(ds_btree_meta *btree, ds_btree_elem_posi *posi,
                                           const int bkrtype, const bkey_range *bkrange)
{
    if (posi->bkeq == true)
        return NULL;

    if (bkrtype != BKEY_RANGE_TYPE_DSC) // ascending
        return (btree_elem_item *)ds_btree_find_next(btree, posi, bkrange);
    else // descending
        return (btree_elem_item *)ds_btree_find_prev(btree, posi, bkrange);
}

static void do_btree_smget_add_miss(smget_result_t *smres,
                                    uint16_t kidx, uint16_t erid)
{
    /* miss_kinfo: forward array */
    smres->miss_kinfo[smres->miss_count].kidx = kidx;
    smres->miss_kinfo[smres->miss_count].code = erid; /* error id */
    smres->miss_count++;
}

static void do_btree_smget_add_trim(smget_result_t *smres,
                                    uint16_t kidx, btree_elem_item *elem)
{
    /* trim_elems & trim_kinfo: backward array */
    assert(smres->trim_count < smres->keys_arrsz);
    smres->trim_elems[smres->keys_arrsz-1-smres->trim_count] = elem;
    smres->trim_kinfo[smres->keys_arrsz-1-smres->trim_count].kidx = kidx;
    //smres->trim_kinfo[smres->keys_arrsz-1-smres->trim_count].code = 0;
    smres->trim_count++;
}

#if 0 // JHPARK_SMGET_OFFSET_HANDLING
static bool do_btree_smget_check_trim(smget_result_t *smres)
{
    btree_elem_item *head_elem = smres->elem_array[0];
    btree_elem_item *trim_elem;
    bool valid = true;

    /* Check if all the trimmed elements(actually the last element before trim)
     * are behind the first found element of smget.
     */
    if (smres->ascending) {
        for (int i = 0; i < smres->trim_count; i++) {
            trim_elem = smres->trim_elems[smres->keys_arrsz-1-i];
            if (BKEY_COMP(trim_elem->data, trim_elem->nbkey,
                          head_elem->data, head_elem->nbkey) < 0) {
                valid = false; break;
            }
        }
    } else {
        for (int i = 0; i < smres->trim_count; i++) {
            trim_elem = smres->trim_elems[smres->keys_arrsz-1-i];
            if (BKEY_COMP(trim_elem->data, trim_elem->nbkey,
                          head_elem->data, head_elem->nbkey) > 0) {
                valid = false; break;
            }
        }
    }
    return valid;
}
#endif

static void do_btree_smget_adjust_trim(smget_result_t *smres)
{
    eitem       **new_trim_elems = &smres->elem_array[smres->elem_count];
    smget_emis_t *new_trim_kinfo = &smres->miss_kinfo[smres->miss_count];
    uint32_t      new_trim_count = 0;
    btree_elem_item *tail_elem = NULL;
    btree_elem_item *comp_elem;
    btree_elem_item *trim_elem;
    uint16_t         trim_kidx;
    int idx, res, pos, i;
    int left, right, mid;

    if (smres->elem_count == smres->elem_arrsz) {
        /* We found the elements as many as the requested count. In this case,
         * we might trim the trimmed keys if the bkey-before-trim is behind
         * the bkey of the last found element.
         */
        tail_elem = smres->elem_array[smres->elem_count-1];
    }

    for (idx = smres->trim_count-1; idx >= 0; idx--)
    {
        trim_elem = smres->trim_elems[smres->keys_arrsz-1-idx];
        trim_kidx = smres->trim_kinfo[smres->keys_arrsz-1-idx].kidx;
        /* check if the trim elem is valid */
        if (tail_elem != NULL) {
            res = BKEY_COMP(trim_elem->data, trim_elem->nbkey,
                            tail_elem->data, tail_elem->nbkey);
            if ((smres->ascending == true && res >= 0) ||
                (smres->ascending != true && res <= 0)) {
                continue; /* invalid trim */
            }
        }
        /* add the valid trim info in sorted arry */
        if (new_trim_count == 0) {
            pos = 0;
        } else {
            left  = 0;
            right = new_trim_count-1;
            while (left <= right) {
                mid = (left + right) / 2;
                comp_elem = new_trim_elems[mid];
                res = BKEY_COMP(trim_elem->data, trim_elem->nbkey,
                                comp_elem->data, comp_elem->nbkey);
                if (res == 0) {
                    right = mid; left = mid+1;
                    break;
                }
                if (smres->ascending) {
                    if (res < 0) right = mid-1;
                    else         left  = mid+1;
                } else {
                    if (res > 0) right = mid-1;
                    else         left  = mid+1;
                }
            }
            /* left: insertion position */
            for (i = new_trim_count-1; i >= left; i--) {
                new_trim_elems[i+1] = new_trim_elems[i];
                new_trim_kinfo[i+1] = new_trim_kinfo[i];
            }
            pos = left;
        }
        trim_elem->refcount++;
        new_trim_elems[pos] = trim_elem;
        new_trim_kinfo[pos].kidx = trim_kidx;
        new_trim_count++;
    }
    smres->trim_elems = new_trim_elems;
    smres->trim_kinfo = new_trim_kinfo;
    smres->trim_count = new_trim_count;
}

static ENGINE_ERROR_CODE
do_btree_smget_scan_sort(token_t *key_array, const int key_count,
                         const int bkrtype, const bkey_range *bkrange,
                         const eflag_filter *efilter,
                         const uint32_t req_count, const bool unique,
                         btree_scan_info *btree_scan_buf,
                         uint16_t *sort_sindx_buf, uint32_t *sort_sindx_cnt,
                         smget_result_t *smres)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    hash_item *it;
    btree_meta_info *info;
    btree_elem_item *elem, *comp;
    ds_btree_meta *btree;
    ds_btree_elem_posi posi;
    uint32_t outside;
    int comp_idx;
    int curr_idx = -1; /* curr scan index */
    int free_idx = 0;  /* free scan list */
    int sort_count = 0; /* sorted scan count */
    int k, i, kidx, cmp_res;
    int mid, left, right;
    bool ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);
    bool is_first;

    for (k = 0; k < key_count; k++) {
        kidx = k;
        ret = do_btree_item_find(key_array[k].value, key_array[k].length, DO_UPDATE, &it);
        if (ret != ENGINE_SUCCESS) {
            if (ret == ENGINE_KEY_ENOENT) { /* key missed */
                do_btree_smget_add_miss(smres, kidx, ENGINE_KEY_ENOENT);
                ret = ENGINE_SUCCESS; continue;
            }
            break; /* ret == ENGINE_EBADTYPE */
        }

        info = (btree_meta_info *)item_get_meta(it);
        btree = &info->btree;
        if ((info->mflags & COLL_META_FLAG_READABLE) == 0) { /* unreadable collection */
            do_btree_smget_add_miss(smres, kidx, ENGINE_UNREADABLE);
            do_item_release(it); continue;
        }
        if (info->ccnt == 0) { /* empty collection */
            do_item_release(it); continue;
        }
        if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
            (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
            do_item_release(it);
            ret = ENGINE_EBADBKEY; break;
        }
        assert(btree->root != NULL);

        elem = (btree_elem_item *)ds_btree_find_first(btree, bkrtype, bkrange, &posi, false);
        if (elem == NULL) { /* No elements within the bkey range */
            if (BTREE_NEED_TRIM_NOTIFICATION(info)) {
                outside = ds_btree_posi_outside(&posi, bkrtype);
                if (do_btree_overlapped_with_trimmed_space(info, outside)) {
                    /* Some elements weren't cached because of overflow trim */
                    do_btree_smget_add_miss(smres, kidx, ENGINE_EBKEYOOR);
                }
            }
            do_item_release(it); continue;
        }

        if (posi.bkeq == false) {
            if (BTREE_NEED_TRIM_NOTIFICATION(info)) {
                outside = ds_btree_posi_outside(&posi, bkrtype);
                if (do_btree_overlapped_with_trimmed_space(info, outside)) {
                    /* Some elements weren't cached because of overflow trim */
                    do_btree_smget_add_miss(smres, kidx, ENGINE_EBKEYOOR);
                    do_item_release(it); continue;
                }
            }
        }

        /* initialize for the next scan */
        is_first = true;
        posi.bkeq = false;

scan_next:
        if (is_first != true) {
            assert(elem != NULL);
            btree_elem_item *prev = elem;
            elem = do_btree_scan_next(btree, &posi, bkrtype, bkrange);
            if (elem == NULL) {
                if (posi.node == NULL) {
                    if (BTREE_NEED_TRIM_NOTIFICATION(info)) {
                        outside = ds_btree_posi_outside(&posi, bkrtype);
                        if (do_btree_overlapped_with_trimmed_space(info, outside)) {
                            /* Some elements weren't cached because of overflow trim */
                            do_btree_smget_add_trim(smres, kidx, prev);
                        }
                    }
                }
                do_item_release(it); continue;
            }
        }
        is_first = false;

        if (efilter != NULL && !ds_btree_elem_filter(btree, (ds_btree_elem_item *)elem, efilter)) {
            goto scan_next;
        }

        /* found the item */
        if (curr_idx == -1) {
            /* allocate free scan */
            assert(free_idx != -1);
            curr_idx = free_idx;
            free_idx = btree_scan_buf[free_idx].next;
        }
        btree_scan_buf[curr_idx].it   = it;
        btree_scan_buf[curr_idx].posi = posi;
        btree_scan_buf[curr_idx].kidx = kidx;

        /* add the current scan into the scan sort buffer */
        if (sort_count == 0) {
            sort_sindx_buf[sort_count++] = curr_idx;
            curr_idx = -1;
            continue;
        }

        if (sort_count >= req_count) {
            /* compare with the element of the last scan */
            comp_idx = sort_sindx_buf[sort_count-1];
            comp = (btree_elem_item *)DS_BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
                                                             btree_scan_buf[comp_idx].posi.indx);
            cmp_res = BKEY_COMP(elem->data, elem->nbkey, comp->data, comp->nbkey);
            if (cmp_res == 0) {
                cmp_res = do_btree_comp_hkey(btree_scan_buf[curr_idx].it,
                                             btree_scan_buf[comp_idx].it);
                if (cmp_res == 0) {
                    do_item_release(btree_scan_buf[curr_idx].it);
                    btree_scan_buf[curr_idx].it = NULL;
                    ret = ENGINE_EBADVALUE; break;
                }
            }
            if ((ascending ==  true && cmp_res > 0) ||
                (ascending == false && cmp_res < 0)) {
                /* do not need to proceed the current scan */
                do_item_release(btree_scan_buf[curr_idx].it);
                btree_scan_buf[curr_idx].it = NULL;
                continue;
            }
        }

        left = 0;
        right = sort_count-1;
        while (left <= right) {
            mid  = (left + right) / 2;
            comp_idx = sort_sindx_buf[mid];
            comp = (btree_elem_item *)DS_BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
                                                             btree_scan_buf[comp_idx].posi.indx);
            cmp_res = BKEY_COMP(elem->data, elem->nbkey, comp->data, comp->nbkey);
            if (cmp_res == 0) {
                cmp_res = do_btree_comp_hkey(btree_scan_buf[curr_idx].it,
                                             btree_scan_buf[comp_idx].it);
                if (cmp_res == 0) {
                    ret = ENGINE_EBADVALUE; break;
                }
                if (unique) break;
            }
            if (ascending) {
                if (cmp_res < 0) right = mid-1;
                else             left  = mid+1;
            } else {
                if (cmp_res > 0) right = mid-1;
                else             left  = mid+1;
            }
        }
        if (ret == ENGINE_EBADVALUE) {
            do_item_release(btree_scan_buf[curr_idx].it);
            btree_scan_buf[curr_idx].it = NULL;
            break;
        }
        if (left <= right) {
            assert(unique == true);
            if ((ascending ==  true && cmp_res < 0) ||
                (ascending == false && cmp_res > 0)) {
                assert(sort_sindx_buf[mid] == comp_idx);
                sort_sindx_buf[mid] = curr_idx;
                it = btree_scan_buf[comp_idx].it;
                posi = btree_scan_buf[comp_idx].posi;
                kidx = btree_scan_buf[comp_idx].kidx;
                info = (btree_meta_info *)item_get_meta(it);
                btree_scan_buf[comp_idx].it = NULL;
                curr_idx = comp_idx;
            }
            goto scan_next;
        }

        if (sort_count >= req_count) {
            /* free the last scan */
            comp_idx = sort_sindx_buf[sort_count-1];
            do_item_release(btree_scan_buf[comp_idx].it);
            btree_scan_buf[comp_idx].it = NULL;
            sort_count--;
            btree_scan_buf[comp_idx].next = free_idx;
            free_idx = comp_idx;
        }
        for (i = sort_count-1; i >= left; i--) {
            sort_sindx_buf[i+1] = sort_sindx_buf[i];
        }
        sort_sindx_buf[left] = curr_idx;
        sort_count++;
        curr_idx = -1;
    }

    if (ret == ENGINE_SUCCESS) {
        *sort_sindx_cnt = sort_count;
    } else {
        for (i = 0; i < sort_count; i++) {
            curr_idx = sort_sindx_buf[i];
            do_item_release(btree_scan_buf[curr_idx].it);
            btree_scan_buf[curr_idx].it = NULL;
        }
    }
    return ret;
}
#endif

#ifdef SUPPORT_BOP_SMGET
static ENGINE_ERROR_CODE
do_btree_smget_elem_sort(btree_scan_info *btree_scan_buf,
                         uint16_t *sort_sindx_buf, const int sort_sindx_cnt,
                         const int bkrtype, const bkey_range *bkrange,
                         const eflag_filter *efilter,
                         const uint32_t offset, const uint32_t count,
                         const bool unique,
                         smget_result_t *smres)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    btree_meta_info *info;
    btree_elem_item *elem, *comp;
    btree_elem_item *last;
    btree_elem_item *prev = NULL;
    uint16_t first_idx = 0;
    uint16_t curr_idx;
    uint16_t comp_idx;
    uint32_t outside;
    int i, cmp_res;
    int mid, left, right;
    int skip_count = 0;
    int sort_count = sort_sindx_cnt;
    bool ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);
    bool dup_bkey_found;

    while (sort_count > 0) {
        curr_idx = sort_sindx_buf[first_idx];
        elem = (btree_elem_item *)DS_BTREE_GET_ELEM_ITEM(btree_scan_buf[curr_idx].posi.node,
                                                         btree_scan_buf[curr_idx].posi.indx);
        dup_bkey_found = false;
        if (prev != NULL) { /* check duplicate bkeys */
            if (BKEY_COMP(prev->data, prev->nbkey, elem->data, elem->nbkey) == 0) {
                dup_bkey_found = true;
            }
        }
        prev = elem;

        if (unique && dup_bkey_found) {
            /* give up current duplicate bkey */
            goto scan_next;
        }

        if (skip_count < offset) {
            skip_count++;
        } else { /* skip_count == offset */
            if (smres->elem_count > 0 && dup_bkey_found) {
                smres->duplicated = true;
            }
            smres->elem_array[smres->elem_count] = elem;
            smres->elem_kinfo[smres->elem_count].kidx = btree_scan_buf[curr_idx].kidx;
            smres->elem_kinfo[smres->elem_count].flag = btree_scan_buf[curr_idx].it->flags;
            smres->elem_count += 1;
#if 0 // JHPARK_SMGET_OFFSET_HANDLING
            if (smres->elem_count == 1) { /* the first element is found */
                if (offset > 0 && smres->trim_count > 0 &&
                    do_btree_smget_check_trim(smres) != true) {
                    /* Some elements are trimmed in 0 ~ offset range.
                     * So, we cannot make the correct smget result.
                     */
                    ret = ENGINE_EBKEYOOR; break;
                }
            }
#endif
            elem->refcount++;
            if (smres->elem_count >= count) break;
        }

scan_next:
        info = (btree_meta_info *)item_get_meta(btree_scan_buf[curr_idx].it);
        last = elem;
        elem = do_btree_scan_next(&info->btree, &btree_scan_buf[curr_idx].posi, bkrtype, bkrange);
        if (elem == NULL) {
            if (btree_scan_buf[curr_idx].posi.node == NULL) {
                /* reached to the end of b+tree scan */
                if (BTREE_NEED_TRIM_NOTIFICATION(info)) {
                    outside = ds_btree_posi_outside(&btree_scan_buf[curr_idx].posi, bkrtype);
                    if (do_btree_overlapped_with_trimmed_space(info, outside)) {
#if 0 // JHPARK_SMGET_OFFSET_HANDLING
                        if (skip_count < offset) {
                            /* Some elements are trimmed in 0 ~ offset range.
                            * So, we cannot make correct smget result.
                            */
                            assert(smres->elem_count == 0);
                            ret = ENGINE_EBKEYOOR; break;
                        }
#endif
                        do_btree_smget_add_trim(smres, btree_scan_buf[curr_idx].kidx, last);
                    }
                }
            }
            first_idx++; sort_count--;
            continue;
        }

        if (efilter != NULL && !ds_btree_elem_filter(&info->btree, (ds_btree_elem_item *)elem, efilter)) {
            goto scan_next;
        }

        if (sort_count == 1) {
            continue; /* sorting is not needed */
        }

        left  = first_idx + 1;
        right = first_idx + sort_count - 1;
        while (left <= right) {
            mid  = (left + right) / 2;
            comp_idx = sort_sindx_buf[mid];
            comp = (btree_elem_item *)DS_BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
                                                             btree_scan_buf[comp_idx].posi.indx);

            cmp_res = BKEY_COMP(elem->data, elem->nbkey, comp->data, comp->nbkey);
            if (cmp_res == 0) {
                cmp_res = do_btree_comp_hkey(btree_scan_buf[curr_idx].it,
                                             btree_scan_buf[comp_idx].it);
                assert(cmp_res != 0);
            }
            if (ascending) {
                if (cmp_res < 0) right = mid-1;
                else             left  = mid+1;
            } else {
                if (cmp_res > 0) right = mid-1;
                else             left  = mid+1;
            }
        }
        if (left <= right) { /* Duplicate bkey is found */
            goto scan_next;
        }

        /* right : insertion position */
        for (i = first_idx+1; i <= right; i++) {
            sort_sindx_buf[i-1] = sort_sindx_buf[i];
        }
        sort_sindx_buf[right] = curr_idx;
    }
    if (ret == ENGINE_SUCCESS) {
        if (smres->trim_count > 0) {
            do_btree_smget_adjust_trim(smres);
        }
    }
    return ret;
}
#endif

/*
 * B+TREE Interface Functions
 */
ENGINE_ERROR_CODE btree_struct_create(const char *key, const uint32_t nkey,
                                      item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_BT_CREATE);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_btree_item_alloc(key, nkey, attrp);
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

btree_elem_item *btree_elem_alloc(const uint32_t nbkey, const uint32_t neflag, const uint32_t nbytes)
{
    btree_elem_item *elem;
    LOCK_CACHE();
    elem = do_btree_elem_alloc(nbkey, neflag, nbytes);
    UNLOCK_CACHE();
    return elem;
}

void btree_elem_free(btree_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->linked == 0);
    do_btree_elem_free(elem);
    UNLOCK_CACHE();
}

void btree_elem_release(btree_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    LOCK_CACHE();
    while (cnt < elem_count) {
        do_btree_elem_release(elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            UNLOCK_CACHE();
            LOCK_CACHE();
        }
    }
    UNLOCK_CACHE();
}

ENGINE_ERROR_CODE btree_elem_insert(const char *key, const uint32_t nkey,
                                    btree_elem_item *elem, const bool replace_if_exist, item_attr *attrp,
                                    bool *replaced, bool *created, btree_elem_item **trimmed_elems,
                                    uint32_t *trimmed_count, uint32_t *trimmed_flags, const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_BT_ELEM_INSERT);

    *created = false;
    *replaced = false;

    if (trimmed_elems != NULL) {
        /* initialize as no trimmed element */
        *trimmed_elems = NULL;
        *trimmed_count = 0;
    }

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_btree_item_alloc(key, nkey, attrp);
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
        ret = do_btree_elem_insert(it, elem, replace_if_exist, replaced,
                                   trimmed_elems, trimmed_count);
        if (ret != ENGINE_SUCCESS && *created) {
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
        }
        if (trimmed_elems != NULL && *trimmed_elems != NULL) {
            *trimmed_flags = it->flags;
        }
    }
    if (it) {
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_update(const char *key, const uint32_t nkey, const bkey_range *bkrange,
                                    const eflag_update *eupdate, const char *value, const uint32_t nbytes,
                                    const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    assert(bkrtype == BKEY_RANGE_TYPE_SIN); /* single bkey */
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_BT_ELEM_INSERT);

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            ret = do_btree_elem_update(info, bkrtype, bkrange, eupdate, value, nbytes);
        } while(0);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_delete(const char *key, const uint32_t nkey,
                                    const bkey_range *bkrange, const eflag_filter *efilter,
                                    const uint32_t req_count, const bool drop_if_empty,
                                    uint32_t *del_count, uint32_t *opcost, bool *dropped,
                                    const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_BT_ELEM_DELETE_DROP
                                                    : UPD_BT_ELEM_DELETE));

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *del_count = do_btree_elem_delete(info, bkrtype, bkrange, efilter, 0, req_count,
                                              opcost, ELEM_DELETE_NORMAL);
            if (*del_count > 0) {
                if (info->ccnt == 0 && drop_if_empty) {
                    assert(info->btree.root == NULL);
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

ENGINE_ERROR_CODE btree_elem_arithmetic(const char *key, const uint32_t nkey,
                                        const bkey_range *bkrange,
                                        const bool increment, const bool create,
                                        const uint64_t delta, const uint64_t initial,
                                        const eflag_t *eflagp,
                                        uint64_t *result, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    assert(bkrtype == BKEY_RANGE_TYPE_SIN); /* single bkey */
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_BT_ELEM_INSERT);

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        ret = do_btree_elem_arithmetic(info, bkrange->from_bkey, bkrange->from_nbkey,
                                       increment, create, delta, initial, eflagp, result);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_get(const char *key, const uint32_t nkey,
                                 const bkey_range *bkrange, const eflag_filter *efilter,
                                 const uint32_t offset, const uint32_t req_count,
                                 const bool delete, const bool drop_if_empty,
                                 struct elems_result *eresult,
                                 const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    bool potentialbkeytrim;
    if (delete) {
        PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_BT_ELEM_DELETE_DROP
                                                        : UPD_BT_ELEM_DELETE));
    }

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt == 0 || offset >= info->ccnt) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            if (req_count == 0 || info->ccnt < req_count) {
                eresult->elem_array = (eitem **)malloc(info->ccnt * sizeof(eitem*));
            } else {
                eresult->elem_array = (eitem **)malloc(req_count * sizeof(eitem*));
            }
            if (eresult->elem_array == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            eresult->elem_count = do_btree_elem_get(info, bkrtype, bkrange, efilter,
                                                    offset, req_count, delete,
                                                    (btree_elem_item **)(eresult->elem_array),
                                                    &(eresult->opcost_or_eindex), &potentialbkeytrim);
            if (eresult->elem_count > 0) {
                if (delete) {
                    if (info->ccnt == 0 && drop_if_empty) {
                        assert(info->btree.root == NULL);
                        do_item_unlink(it, ITEM_UNLINK_NORMAL);
                        eresult->dropped = true;
                    } else {
                        eresult->dropped = false;
                    }
                } else {
                    eresult->trimmed = potentialbkeytrim;
                }
                eresult->flags = it->flags;
            } else {
                if (potentialbkeytrim == true) {
                    ret = ENGINE_EBKEYOOR;
                } else {
                    ret = ENGINE_ELEM_ENOENT;
                }
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

ENGINE_ERROR_CODE btree_elem_count(const char *key, const uint32_t nkey,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   uint32_t *elem_count, uint32_t *opcost)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *elem_count = ds_btree_elem_count(&info->btree, bkrtype, bkrange, efilter, opcost);
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE btree_posi_find(const char *key, const uint32_t nkey, const bkey_range *bkrange,
                                  ENGINE_BTREE_ORDER order, int *position)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    assert(bkrtype == BKEY_RANGE_TYPE_SIN);

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *position = ds_btree_posi_find(&info->btree, bkrtype, bkrange, order);
            if (*position < 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE btree_posi_find_with_get(const char *key, const uint32_t nkey,
                                           const bkey_range *bkrange, ENGINE_BTREE_ORDER order,
                                           const int count, int *position,
                                           struct elems_result *eresult)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    assert(bkrtype == BKEY_RANGE_TYPE_SIN);

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            if ((eresult->elem_array = (eitem **)malloc((2*count+1)*sizeof(eitem*))) == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            *position = ds_btree_posi_find_with_get(&info->btree, bkrtype, bkrange, order, count,
                                                    (ds_btree_elem_item**)(eresult->elem_array),
                                                    &(eresult->elem_count),
                                                    &(eresult->opcost_or_eindex));
            if (*position >= 0) {
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
    return ret;
}

ENGINE_ERROR_CODE btree_elem_get_by_posi(const char *key, const uint32_t nkey,
                                         ENGINE_BTREE_ORDER order, int from_posi, int to_posi,
                                         struct elems_result *eresult)
{
    assert(from_posi >= 0 && to_posi >= 0);
    hash_item *it;
    ENGINE_ERROR_CODE ret;

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        uint32_t rqcount;
        bool     forward;
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            /* adjust from_posi and to_posi considering given order */
            if (from_posi >= info->ccnt && to_posi >= info->ccnt) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if (from_posi >= info->ccnt) from_posi = info->ccnt-1;
            if (to_posi   >= info->ccnt) to_posi   = info->ccnt-1;
            if (order == BTREE_ORDER_DESC) {
                from_posi = info->ccnt - from_posi - 1;
                to_posi   = info->ccnt - to_posi   - 1;
            }
            if (from_posi <= to_posi) {
                forward = true;
                rqcount = to_posi - from_posi + 1;
            } else {
                forward = false;
                rqcount = from_posi - to_posi + 1;
            }
            if ((eresult->elem_array = (eitem **)malloc(rqcount * sizeof(eitem*))) == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            ret = ds_btree_elem_get_by_posi(&info->btree, from_posi, rqcount, forward,
                                            (ds_btree_elem_item**)(eresult->elem_array),
                                            &(eresult->elem_count));
            if (ret == ENGINE_SUCCESS) {
                eresult->flags = it->flags;
            } else {
                /* ret == ENGINE_ELEM_ENOENT */
                free(eresult->elem_array);
                eresult->elem_array = NULL;
            }
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();
    return ret;
}

#ifdef SUPPORT_BOP_SMGET
ENGINE_ERROR_CODE btree_elem_smget(token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   const bool unique,
                                   smget_result_t *result)
{
    btree_scan_info btree_scan_buf[offset+count+1]; /* one more scan needed */
    uint16_t        sort_sindx_buf[offset+count];   /* sorted scan index buffer */
    uint32_t        sort_sindx_cnt, i;
    int             bkrtype = do_btree_bkey_range_type(bkrange);
    ENGINE_ERROR_CODE ret;

    /* prepare */
    for (i = 0; i <= (offset+count); i++) {
        btree_scan_buf[i].it = NULL;
        btree_scan_buf[i].next = (i < (offset+count)) ? (i+1) : -1;
    }

    /* set the ascending field of smget result */
    result->ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);

    LOCK_CACHE();
    do {
        /* the 1st phase: get the sorted scans */
        ret = do_btree_smget_scan_sort(key_array, key_count,
                                       bkrtype, bkrange, efilter, (offset+count), unique,
                                       btree_scan_buf, sort_sindx_buf, &sort_sindx_cnt,
                                       result);
        if (ret != ENGINE_SUCCESS) {
            break;
        }

        /* the 2nd phase: get the sorted elems */
        ret = do_btree_smget_elem_sort(btree_scan_buf, sort_sindx_buf, sort_sindx_cnt,
                                       bkrtype, bkrange, efilter, offset, count, unique,
                                       result);
        if (ret != ENGINE_SUCCESS) {
            break;
        }

        for (i = 0; i <= (offset+count); i++) {
            if (btree_scan_buf[i].it != NULL)
                do_item_release(btree_scan_buf[i].it);
        }
    } while(0);
    UNLOCK_CACHE();

    return ret;
}
#endif

uint32_t btree_elem_delete_with_count(btree_meta_info *info, const uint32_t count)
{
    return do_btree_elem_delete(info, BKEY_RANGE_TYPE_ASC, NULL, NULL,
                                0, count, NULL, ELEM_DELETE_COLL);
}

/* Scan the whole btree with the cache lock acquired.
 * We only build the table of the current elements.
 * See do_btree_elem_delete and do_btree_multi_elem_unlink.
 * They show how to traverse the btree.
 * FIXME. IS THIS STILL RIGHT AFTER THE MERGE? do_btree_multi_elem_unlink is gone.
 */
void btree_elem_get_all(btree_meta_info *info, elems_result_t *eresult)
{
    assert(eresult->elem_arrsz >= info->ccnt && eresult->elem_count == 0);
    btree_elem_item *elem;
    ds_btree_meta *btree = &info->btree;
    ds_btree_elem_posi  posi;

    elem = (btree_elem_item *)ds_btree_find_first(btree, BKEY_RANGE_TYPE_ASC, NULL, &posi, false);
    while (elem != NULL) {
        elem->refcount++;
        eresult->elem_array[eresult->elem_count++] = elem;
        /* Never have to go backward?  FIXME */
        elem = (btree_elem_item *)ds_btree_find_next(btree, &posi, NULL);
    }
    assert(eresult->elem_count == info->ccnt);
}

uint32_t btree_elem_ntotal(btree_elem_item *elem)
{
    return do_btree_elem_ntotal(elem);
}

uint8_t  btree_real_nbkey(uint8_t nbkey)
{
    return (uint8_t)BTREE_REAL_NBKEY(nbkey);
}

ENGINE_ERROR_CODE btree_coll_getattr(hash_item *it, item_attr *attrp,
                                     ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    btree_meta_info *info = (btree_meta_info *)item_get_meta(it);

    /* get collection attributes */
    attrp->count = info->ccnt;
    attrp->maxcount = (info->mcnt > 0) ? info->mcnt : (int32_t)config->max_btree_size;
    attrp->ovflaction = info->ovflact;
    attrp->readable = ((info->mflags & COLL_META_FLAG_READABLE) != 0) ? 1 : 0;

    attrp->trimmed = ((info->mflags & COLL_META_FLAG_TRIMMED) != 0) ? 1 : 0;
    attrp->maxbkeyrange = info->maxbkeyrange;
    if (info->ccnt > 0) {
        btree_elem_item *min_bkey_elem = (btree_elem_item *)ds_btree_get_first_elem(info->btree.root);
        btree_elem_item *max_bkey_elem = (btree_elem_item *)ds_btree_get_last_elem(info->btree.root);
        do_btree_copy_bkey(min_bkey_elem, &attrp->minbkey);
        do_btree_copy_bkey(max_bkey_elem, &attrp->maxbkey);
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE btree_coll_setattr(hash_item *it, item_attr *attrp,
                                     ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
    ds_btree_meta *btree = &info->btree;

    /* check the validity of given attributs */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            attrp->maxcount = do_btree_real_maxcount(attrp->maxcount);
            if (attrp->maxcount > 0 && attrp->maxcount < info->ccnt) {
                return ENGINE_EBADVALUE;
            }
        } else if (attr_ids[i] == ATTR_OVFLACTION) {
            if (attrp->ovflaction != OVFL_ERROR &&
                attrp->ovflaction != OVFL_SMALLEST_TRIM &&
                attrp->ovflaction != OVFL_LARGEST_TRIM &&
                attrp->ovflaction != OVFL_SMALLEST_SILENT_TRIM &&
                attrp->ovflaction != OVFL_LARGEST_SILENT_TRIM) {
                return ENGINE_EBADVALUE;
            }
        } else if (attr_ids[i] == ATTR_READABLE) {
            if (attrp->readable != 1) {
                return ENGINE_EBADVALUE;
            }
        } else if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
            if (attrp->maxbkeyrange.len != BKEY_NULL && info->ccnt > 0) {
                /* check bkey type of maxbkeyrange */
                if ((info->bktype == BKEY_TYPE_UINT64 && attrp->maxbkeyrange.len >  0) ||
                    (info->bktype == BKEY_TYPE_BINARY && attrp->maxbkeyrange.len == 0)) {
                    return ENGINE_EBADVALUE;
                }
                /* New maxbkeyrange must contain the current bkey range */
                if ((info->ccnt >= 2) && /* current key range exists */
                    (attrp->maxbkeyrange.len != info->maxbkeyrange.len ||
                     BKEY_ISNE(attrp->maxbkeyrange.val, attrp->maxbkeyrange.len,
                               info->maxbkeyrange.val, info->maxbkeyrange.len))) {
                    bkey_t curbkeyrange;
                    btree_elem_item *min_bkey_elem = (btree_elem_item *)ds_btree_get_first_elem(btree->root);
                    btree_elem_item *max_bkey_elem = (btree_elem_item *)ds_btree_get_last_elem(btree->root);
                    curbkeyrange.len = attrp->maxbkeyrange.len;
                    BKEY_DIFF(max_bkey_elem->data, max_bkey_elem->nbkey,
                              min_bkey_elem->data, min_bkey_elem->nbkey,
                              curbkeyrange.len, curbkeyrange.val);
                    if (BKEY_ISGT(curbkeyrange.val, curbkeyrange.len,
                                  attrp->maxbkeyrange.val, attrp->maxbkeyrange.len)) {
                        return ENGINE_EBADVALUE;
                    }
                }
            }
        }
    }

    /* set the attributes */
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            info->mcnt = attrp->maxcount;
        } else if (attr_ids[i] == ATTR_OVFLACTION) {
            if (info->ovflact != attrp->ovflaction) {
                info->mflags &= ~COLL_META_FLAG_TRIMMED; // clear trimmed
            }
            info->ovflact = attrp->ovflaction;
            _setif_forced_btree_overflow_action(info, item_get_key(it), it->nkey);
        } else if (attr_ids[i] == ATTR_READABLE) {
            info->mflags |= COLL_META_FLAG_READABLE;
        } else if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
            if (attrp->maxbkeyrange.len == BKEY_NULL) {
                if (info->maxbkeyrange.len != BKEY_NULL) {
                    info->maxbkeyrange = attrp->maxbkeyrange;
                    if (info->ccnt == 0) {
                        if (info->bktype != BKEY_TYPE_UNKNOWN)
                            info->bktype = BKEY_TYPE_UNKNOWN;
                    }
                }
            } else { /* attrp->maxbkeyrange.len != BKEY_NULL */
                if (info->ccnt == 0) {
                    /* just reset maxbkeyrange with new value */
                    info->maxbkeyrange = attrp->maxbkeyrange;
                    if (attrp->maxbkeyrange.len == 0) {
                        info->bktype = BKEY_TYPE_UINT64;
                    } else {
                        info->bktype = BKEY_TYPE_BINARY;
                    }
                } else { /* info->ccnt > 0 */
                    info->maxbkeyrange = attrp->maxbkeyrange;
                }
            }
        }
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE btree_apply_item_link(void *engine, const char *key, const uint32_t nkey,
                                        item_attr *attrp)
{
    hash_item *old_it;
    hash_item *new_it;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "btree_apply_item_link. key=%.*s nkey=%u\n",
                PRINT_NKEY(nkey), key, nkey);

    LOCK_CACHE();
    old_it = do_item_get(key, nkey, DONT_UPDATE);
    if (old_it) {
        /* Remove the old item first. */
        do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
        do_item_release(old_it);
    }
    new_it = do_btree_item_alloc(key, nkey, attrp);
    if (new_it) {
        /* Copy relevent fields in meta info */
        btree_meta_info *info = (btree_meta_info*)item_get_meta(new_it);
        if (attrp->trimmed) {
            info->mflags |= COLL_META_FLAG_TRIMMED; // set trimmed
        }
        if (attrp->maxbkeyrange.len != BKEY_NULL) {
            info->maxbkeyrange = attrp->maxbkeyrange;
        }
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
                    "btree_apply_item_link failed. key=%.*s nkey=%u code=%d\n",
                    PRINT_NKEY(nkey), key, nkey, ret);
    }
    return ret;
}

ENGINE_ERROR_CODE btree_apply_elem_insert(void *engine, hash_item *it,
                                          const char *bkey, const uint32_t nbkey,
                                          const uint32_t neflag, const uint32_t nbytes)
{
    const char *key = item_get_key(it);
    btree_elem_item *elem;
    bool replaced;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL,
                "btree_apply_elem_insert. key=%.*s nkey=%u bkey=%.*s nbkey=%u\n",
                PRINT_NKEY(it->nkey), key, it->nkey, nbkey, bkey, nbkey);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree_apply_elem_insert failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        elem = do_btree_elem_alloc(nbkey, neflag, nbytes);
        if (elem == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree_apply_elem_insert failed."
                        " element alloc failed. nbkey=%d neflag=%d nbytes=%d\n", nbkey, neflag, nbytes);
            ret = ENGINE_ENOMEM; break;
        }
        memcpy(elem->data, bkey, BTREE_REAL_NBKEY(nbkey) + neflag + nbytes);

        ret = do_btree_elem_insert(it, elem, true /* replace_if_exist */,
                                   &replaced, NULL, NULL);
        if (ret != ENGINE_SUCCESS) {
            do_btree_elem_free(elem);
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree_apply_elem_insert failed."
                        " key=%.*s nkey=%u d bkey=%.*s nbkey=%u code=%d\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, nbkey, bkey, nbkey, ret);
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent has_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE btree_apply_elem_delete(void *engine, hash_item *it,
                                          const char *bkey, const uint32_t nbkey,
                                          const bool drop_if_empty)
{
    const char *key = item_get_key(it);
    btree_meta_info *info;
    bkey_range bkrange;
    uint32_t ndeleted;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL,
                "btree_apply_elem_delete. key=%.*s nkey=%u bkey=%.*s nbkey=%u\n",
                PRINT_NKEY(it->nkey), key, it->nkey, nbkey, bkey, nbkey);

    /* one-element range */
    memcpy(bkrange.from_bkey, bkey, BTREE_REAL_NBKEY(nbkey));
    bkrange.from_nbkey = nbkey;
    /* bkey_range.to_bkey */
    bkrange.to_nbkey = BKEY_NULL;

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree_apply_elem_delete failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        info = (btree_meta_info *)item_get_meta(it);
        if (info->ccnt == 0) {
            logger->log(EXTENSION_LOG_INFO, NULL, "btree_apply_elem_delete failed."
                        " no element.\n");
            ret = ENGINE_ELEM_ENOENT; break;
        }
        if ((info->bktype == BKEY_TYPE_UINT64 && bkrange.from_nbkey > 0) ||
            (info->bktype == BKEY_TYPE_BINARY && bkrange.from_nbkey == 0)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree_apply_elem_delete failed."
                        " bkey mismatch. key=%.*s nkey=%u bkey=%.*s nbkey=%u\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, nbkey, bkey, nbkey);
            ret = ENGINE_EBADBKEY; break;
        }

        ndeleted = do_btree_elem_delete(info, BKEY_RANGE_TYPE_SIN, &bkrange, NULL,
                                         0, 0, NULL, ELEM_DELETE_NORMAL);
        if (ndeleted == 0) {
            logger->log(EXTENSION_LOG_INFO, NULL, "btree_apply_elem_delete failed."
                        " no element deleted. key=%.*s nkey=%u bkey=%.*s nbkey=%u\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, nbkey, bkey, nbkey);
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

ENGINE_ERROR_CODE btree_apply_elem_delete_logical(void *engine, hash_item *it,
                                                  const bkey_range *bkrange,
                                                  const eflag_filter *efilter,
                                                  const uint32_t offset, const uint32_t count,
                                                  const bool drop_if_empty)
{
    const char *key = item_get_key(it);
    btree_meta_info *info;
    uint32_t ndeleted;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL,
                "btree_apply_elem_delete_logical. key=%.*s nkey=%u\n",
                PRINT_NKEY(it->nkey), key, it->nkey);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree_apply_elem_delete_logical failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        info = (btree_meta_info *)item_get_meta(it);
        if (info->ccnt == 0) {
            logger->log(EXTENSION_LOG_INFO, NULL, "btree_apply_elem_delete_logical failed."
                        " no element.\n");
            ret = ENGINE_ELEM_ENOENT; break;
        }
        if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey > 0) ||
            (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree_apply_elem_delete_logical failed."
                        " bkey mismatch. key=%.*s nkey=%u from_bkey=%.*s\n",
                        PRINT_NKEY(it->nkey), key, it->nkey, bkrange->from_nbkey, bkrange->from_bkey);
            ret = ENGINE_EBADBKEY; break;
        }

        ndeleted = do_btree_elem_delete(info, bkrtype, bkrange, efilter, offset, count,
                                         NULL, ELEM_DELETE_NORMAL);
        if (ndeleted == 0) {
            logger->log(EXTENSION_LOG_INFO, NULL, "btree_apply_elem_delete_logical failed."
                        " no element deleted. key=%.*s nkey=%u from_bkey=%.*s to_bkey=%.*s",
                        PRINT_NKEY(it->nkey), key, it->nkey,
                        bkrange->from_nbkey, bkrange->from_bkey, bkrange->to_nbkey, bkrange->to_bkey);
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

void btree_traverse_init(coll_meta_info *info, void *posi)
{
    ds_btree_traverse_init(&((btree_meta_info *)info)->btree, posi);
}

uint32_t btree_traverse_next(void *posi, void **elem_array, uint32_t count)
{
    return ds_btree_traverse_next(posi, elem_array, count);
}

/*
 * External Functions
 */
ENGINE_ERROR_CODE item_btree_coll_init(void *engine_ptr)
{
    /* initialize global variables */
    engine = engine_ptr;
    config = &engine->config;
    logger = engine->server.log->get_logger();

    /* check forced btree overflow action */
    _check_forced_btree_overflow_action();

    /* prepare bkey min & max value */
    bkey_uint64_min = 0;
    bkey_uint64_max = (uint64_t)((int64_t)-1); /* need check */
    bkey_binary_min[0] = 0x00;
    for (int i=0; i < MAX_BKEY_LENG; i++) {
        bkey_binary_max[i] = 0xFF;
    }

    /* remove unused function warnings */
    if (1) {
        uint64_t val1 = 10;
        uint64_t val2 = 20;
        assert(UINT64_COMPARE_OP[COMPARE_OP_LT](&val1, &val2) == true);
    }

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM btree module initialized.\n");
    return ENGINE_SUCCESS;
}

void item_btree_coll_final(void *engine_ptr)
{
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM btree module destroyed.\n");
}
