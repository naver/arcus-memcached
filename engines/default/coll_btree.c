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

#ifdef REORGANIZE_ITEM_COLL // BTREE
#define PERSISTENCE_ACTION_BEGIN(a, b)
#define PERSISTENCE_ACTION_END(a)

static struct default_engine *engine=NULL;
static struct engine_config  *config=NULL; // engine config
static EXTENSION_LOGGER_DESCRIPTOR *logger;
#endif

#ifdef REORGANIZE_ITEM_COLL // BTREE
/* bkey type */
#define BKEY_TYPE_UNKNOWN 0
#define BKEY_TYPE_UINT64  1
#define BKEY_TYPE_BINARY  2

/* get bkey real size */
#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))

/* bkey range type */
#define BKEY_RANGE_TYPE_SIN 1 /* single bkey */
#define BKEY_RANGE_TYPE_ASC 2 /* ascending bkey range */
#define BKEY_RANGE_TYPE_DSC 3 /* descending bkey range */

/* btree item status */
#define BTREE_ITEM_STATUS_USED   2
#define BTREE_ITEM_STATUS_UNLINK 1
#define BTREE_ITEM_STATUS_FREE   0

/* overflow type */
#define OVFL_TYPE_NONE  0
#define OVFL_TYPE_COUNT 1
#define OVFL_TYPE_RANGE 2

/* btree scan direction */
#define BTREE_DIRECTION_PREV 2
#define BTREE_DIRECTION_NEXT 1
#define BTREE_DIRECTION_NONE 0

/* btree element item or btree node item */
#define BTREE_GET_ELEM_ITEM(node, indx) ((btree_elem_item *)((node)->item[indx]))
#define BTREE_GET_NODE_ITEM(node, indx) ((btree_indx_node *)((node)->item[indx]))

/* btree element position */
typedef struct _btree_elem_posi {
    btree_indx_node *node;
    uint16_t         indx;
    /* It is used temporarily in order to check
     * if the found bkey is equal to from_bkey or to_bkey of given bkey range
     * in the do_btree_find_first/next/prev functions.
     */
    bool             bkeq;
} btree_elem_posi;

/* btree scan structure */
typedef struct _btree_scan_info {
    hash_item       *it;
    btree_elem_posi  posi;
    uint32_t         kidx; /* An index in the given key array as a parameter */
    int32_t          next; /* for free scan link */
} btree_scan_info;

/* btree position debugging */
static bool btree_position_debug = false;

/* bkey min & max value */
static uint64_t      bkey_uint64_min;
static uint64_t      bkey_uint64_max;
static unsigned char bkey_binary_min[MIN_BKEY_LENG];
static unsigned char bkey_binary_max[MAX_BKEY_LENG];
#endif

#ifdef REORGANIZE_ITEM_COLL // BTREE
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
#endif

#ifdef REORGANIZE_ITEM_COLL // BTREE
/*
 * B+TREE collection management
 */
//#define BTREE_DELETE_NO_MERGE

static inline uint32_t do_btree_elem_ntotal(btree_elem_item *elem)
{
    return sizeof(btree_elem_item_fixed) + BTREE_REAL_NBKEY(elem->nbkey)
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
                                      item_attr *attrp, const void *cookie)
{
    uint32_t nbytes = 2; /* "\r\n" */
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(btree_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
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
        info->root    = NULL;
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);

        /* set if forced_btree_overflow_actions is given */
        _setif_forced_btree_overflow_action(info, key, nkey);
    }
    return it;
}

static btree_indx_node *do_btree_node_alloc(const uint8_t node_depth, const void *cookie)
{
    size_t ntotal = (node_depth > 0 ? sizeof(btree_indx_node) : sizeof(btree_leaf_node));

    btree_indx_node *node = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (node != NULL) {
        node->slabs_clsid = slabs_clsid(ntotal);
        assert(node->slabs_clsid > 0);

        node->refcount    = 0;
        node->ndepth      = node_depth;
        node->used_count  = 0;
        node->prev = node->next = NULL;
        memset(node->item, 0, BTREE_ITEM_COUNT*sizeof(void*));
        if (node_depth > 0)
            memset(node->ecnt, 0, BTREE_ITEM_COUNT*sizeof(uint16_t));
    }
    return node;
}

static void do_btree_node_free(btree_indx_node *node)
{
    size_t ntotal = (node->ndepth > 0 ? sizeof(btree_indx_node) : sizeof(btree_leaf_node));
    do_item_mem_free(node, ntotal);
}

static btree_elem_item *do_btree_elem_alloc(const uint32_t nbkey, const uint32_t neflag,
                                            const uint32_t nbytes, const void *cookie)
{
    size_t ntotal = sizeof(btree_elem_item_fixed) + BTREE_REAL_NBKEY(nbkey) + neflag + nbytes;

    btree_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        elem->slabs_clsid = slabs_clsid(ntotal);
        assert(elem->slabs_clsid > 0);

        elem->refcount    = 0;
        elem->status      = BTREE_ITEM_STATUS_UNLINK; /* unlinked state */
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
    /* assert(elem->status != BTREE_ITEM_STATUS_FREE); */
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->status == BTREE_ITEM_STATUS_UNLINK) {
        elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(elem);
    }
}

static inline btree_elem_item *do_btree_get_first_elem(btree_indx_node *node)
{
    while (node->ndepth > 0) {
        node = (btree_indx_node *)(node->item[0]);
    }
    assert(node->ndepth == 0);
    return (btree_elem_item *)(node->item[0]);
}

static inline btree_elem_item *do_btree_get_last_elem(btree_indx_node *node)
{
    while (node->ndepth > 0) {
        node = (btree_indx_node *)(node->item[node->used_count-1]);
    }
    assert(node->ndepth == 0);
    return (btree_elem_item *)(node->item[node->used_count-1]);
}

static inline btree_indx_node *do_btree_get_first_leaf(btree_indx_node *node,
                                                       btree_elem_posi *path)
{
    while (node->ndepth > 0) {
        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = 0;
        }
        node = (btree_indx_node *)(node->item[0]);
    }
    assert(node->ndepth == 0);
    return node;
}

static inline btree_indx_node *do_btree_get_last_leaf(btree_indx_node *node,
                                                      btree_elem_posi *path)
{
    while (node->ndepth > 0) {
        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = node->used_count-1;
        }
        node = (btree_indx_node *)(node->item[node->used_count-1]);
    }
    assert(node->ndepth == 0);
    return node;
}

static inline void do_btree_get_bkey(btree_elem_item *elem, bkey_t *bkey)
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

static inline int BINARY_COMP(const unsigned char *v1, const int nv1,
                              const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] <  v2[i]) return -1;
        else                return  1;
    }
    if (nv1 == nv2) return  0;
    if (nv1 <  nv2) return -1;
    else            return  1;
}

static inline bool BINARY_ISEQ(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    if (nv1 != nv2) return false;
    for (int i=0; i < nv1; i++) {
        if (v1[i] != v2[i]) return false;
    }
    return true;
}

static inline bool BINARY_ISNE(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    if (nv1 != nv2) return true;
    for (int i=0; i < nv1; i++) {
        if (v1[i] != v2[i]) return true;
    }
    return false;
}

static inline bool BINARY_ISLT(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] <  v2[i]) return true;
        else                return false;
    }
    if (nv1 < nv2) return true;
    else           return false;
}

static inline bool BINARY_ISLE(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] <  v2[i]) return true;
        else                return false;
    }
    if (nv1 <= nv2) return true;
    else            return false;
}

static inline bool BINARY_ISGT(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] >  v2[i]) return true;
        else                return false;
    }
    if (nv1 > nv2) return true;
    else           return false;
}

static inline bool BINARY_ISGE(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] >  v2[i]) return true;
        else                return false;
    }
    if (nv1 >= nv2) return true;
    else            return false;
}

static inline void BINARY_AND(const unsigned char *v1, const unsigned char *v2,
                              const int length, unsigned char *result)
{
    for (int i=0; i < length; i++) {
        result[i] = v1[i] & v2[i];
    }
}

static inline void BINARY_OR(const unsigned char *v1, const unsigned char *v2,
                             const int length, unsigned char *result)
{
    for (int i=0; i < length; i++) {
        result[i] = v1[i] | v2[i];
    }
}

static inline void BINARY_XOR(const unsigned char *v1, const unsigned char *v2,
                              const int length, unsigned char *result)
{
    for (int i=0; i < length; i++) {
        result[i] = v1[i] ^ v2[i];
    }
}

static bool (*UINT64_COMPARE_OP[COMPARE_OP_MAX]) (const uint64_t *v1, const uint64_t *v2)
    = { UINT64_ISEQ, UINT64_ISNE, UINT64_ISLT, UINT64_ISLE, UINT64_ISGT, UINT64_ISGE };

static bool (*BINARY_COMPARE_OP[COMPARE_OP_MAX]) (const unsigned char *v1, const int nv1,
                                                  const unsigned char *v2, const int nv2)
    = { BINARY_ISEQ, BINARY_ISNE, BINARY_ISLT, BINARY_ISLE, BINARY_ISGT, BINARY_ISGE };

static void (*BINARY_BITWISE_OP[BITWISE_OP_MAX]) (const unsigned char *v1, const unsigned char *v2,
                                                  const int length, unsigned char *result)
    = { BINARY_AND, BINARY_OR, BINARY_XOR };

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

static inline void do_btree_incr_posi(btree_elem_posi *posi)
{
    if (posi->indx < (posi->node->used_count-1)) {
        posi->indx += 1;
    } else {
        posi->node = posi->node->next;
        posi->indx = 0;
    }
}

static inline void do_btree_decr_posi(btree_elem_posi *posi)
{
    if (posi->indx > 0) {
        posi->indx -= 1;
    } else {
        posi->node = posi->node->prev;
        if (posi->node != NULL)
            posi->indx = posi->node->used_count-1;
        else
            posi->indx = BTREE_ITEM_COUNT;
    }
}

static void do_btree_incr_path(btree_elem_posi *path, int depth)
{
    btree_indx_node *saved_node;

    while (depth < BTREE_MAX_DEPTH) {
        saved_node = path[depth].node;
        do_btree_incr_posi(&path[depth]);
        if (path[depth].node == saved_node) break;
        depth += 1;
    }
    assert(depth < BTREE_MAX_DEPTH);
}

static void do_btree_decr_path(btree_elem_posi *path, int depth)
{
    btree_indx_node *saved_node;

    while (depth < BTREE_MAX_DEPTH) {
        saved_node = path[depth].node;
        do_btree_decr_posi(&path[depth]);
        if (path[depth].node == saved_node) break;
        depth += 1;
    }
    assert(depth < BTREE_MAX_DEPTH);
}

static btree_indx_node *do_btree_find_leaf(btree_indx_node *root,
                                           const unsigned char *bkey, const uint32_t nbkey,
                                           btree_elem_posi *path,
                                           btree_elem_item **found_elem)
{
    btree_indx_node *node = root;
    btree_elem_item *elem;
    int mid, left, right, comp;

    *found_elem = NULL; /* the same bkey is not found */

    while (node->ndepth > 0) {
        left  = 1;
        right = node->used_count-1;

        while (left <= right) {
            mid  = (left + right) / 2;
            elem = do_btree_get_first_elem(node->item[mid]); /* separator */
            comp = BKEY_COMP(bkey, nbkey, elem->data, elem->nbkey);
            if (comp == 0) { /* the same bkey is found */
                *found_elem = elem;
                if (path) {
                    path[node->ndepth].node = node;
                    path[node->ndepth].indx = mid;
                }
                node = do_btree_get_first_leaf(node->item[mid], path);
                assert(node->ndepth == 0);
                break;
            }
            if (comp <  0) right = mid-1;
            else           left  = mid+1;
        }
        if (left <= right) { /* found the element */
            break;
        }

        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = right;
        }
        node = (btree_indx_node *)(node->item[right]);
    }
    return node;
}

static ENGINE_ERROR_CODE do_btree_find_insposi(btree_indx_node *root,
                                               const unsigned char *ins_bkey, const uint32_t ins_nbkey,
                                               btree_elem_posi *path)
{
    btree_indx_node *node;
    btree_elem_item *elem;
    int mid, left, right, comp;

    /* find leaf node */
    node = do_btree_find_leaf(root, ins_bkey, ins_nbkey, path, &elem);
    if (elem != NULL) { /* the bkey(ins_bkey) is found */
        /* while traversing to leaf node, the bkey can be found.
         * refer to do_btree_find_leaf() function.
         */
        path[0].node = node;
        path[0].indx = 0;
        return ENGINE_ELEM_EEXISTS;
    }

    /* do search the bkey(ins_bkey) in leaf node */
    left  = 0;
    right = node->used_count-1;

    while (left <= right) {
        mid  = (left + right) / 2;
        elem = BTREE_GET_ELEM_ITEM(node, mid);
        comp = BKEY_COMP(ins_bkey, ins_nbkey, elem->data, elem->nbkey);
        if (comp == 0) break;
        if (comp <  0) right = mid-1;
        else           left  = mid+1;
    }

    if (left <= right) { /* the bkey(ins_bkey) is found */
        path[0].node = node;
        path[0].indx = mid;
        return ENGINE_ELEM_EEXISTS;
    } else {             /* the bkey(ins_bkey) is not found */
        path[0].node = node;
        path[0].indx = left;
        return ENGINE_SUCCESS;
    }
}

static btree_elem_item *do_btree_find_first(btree_indx_node *root,
                                            const int bkrtype, const bkey_range *bkrange,
                                            btree_elem_posi *path, const bool path_flag)
{
    btree_indx_node *node;
    btree_elem_item *elem;
    int mid, left, right, comp;

    if (bkrange == NULL) {
        assert(bkrtype != BKEY_RANGE_TYPE_SIN);
        if (bkrtype == BKEY_RANGE_TYPE_ASC) {
            path[0].node = do_btree_get_first_leaf(root, (path_flag ? path : NULL));
            path[0].indx = 0;
        } else {
            path[0].node = do_btree_get_last_leaf(root, (path_flag ? path : NULL));
            path[0].indx = path[0].node->used_count - 1;
        }
        path[0].bkeq = false;

        elem = BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
        assert(elem != NULL);
        return elem;
    }

    /* find leaf node */
    node = do_btree_find_leaf(root, bkrange->from_bkey, bkrange->from_nbkey,
                              (path_flag ? path : NULL), &elem);
    if (elem != NULL) { /* the bkey(from_bkey) is found */
        /* while traversing to leaf node, the bkey can be found.
         * refer to do_btree_find_leaf() function.
         */
        path[0].bkeq = true;
        path[0].node = node;
        path[0].indx = 0;
        return elem;
    }

    /* do search the bkey(from_bkey) in leaf node */
    left  = 0;
    right = node->used_count-1;

    while (left <= right) {
        mid  = (left + right) / 2;
        elem = BTREE_GET_ELEM_ITEM(node, mid);
        comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey, elem->data, elem->nbkey);
        if (comp == 0) break;
        if (comp <  0) right = mid-1;
        else           left  = mid+1;
    }

    if (left <= right) { /* the bkey(from_bkey) is found. */
        path[0].bkeq = true;
        path[0].node = node;
        path[0].indx = mid;
        /* elem != NULL */
    } else {             /* the bkey(from_bkey) is not found */
        path[0].bkeq = false;
        switch (bkrtype) {
          case BKEY_RANGE_TYPE_SIN: /* single bkey */
            if (left > 0 && left < node->used_count) {
                /* In order to represent the bkey is NOT outside of btree,
                 * set any existent element position.
                 */
                path[0].node = node;
                path[0].indx = left;
            } else {
                if (left >= node->used_count) {
                    path[0].node = node->next;
                    path[0].indx = 0;
                    if (path[0].node != NULL) {
                        if (path_flag) do_btree_incr_path(path, 1);
                    }
                } else { /* left == 0 && right == -1 */
                    path[0].node = node->prev;
                    if (node->prev != NULL) {
                        path[0].indx = node->prev->used_count-1;
                        if (path_flag) do_btree_decr_path(path, 1);
                    } else {
                        path[0].indx = BTREE_ITEM_COUNT;
                    }
                }
            }
            elem = NULL;
            break;
          case BKEY_RANGE_TYPE_ASC: /* ascending bkey range */
            /* find the next element */
            if (left < node->used_count) {
                path[0].node = node;
                path[0].indx = left;
            } else {
                path[0].node = node->next;
                path[0].indx = 0;
                if (path[0].node != NULL) {
                    if (path_flag) do_btree_incr_path(path, 1);
                }
            }
            if (path[0].node == NULL) {
                elem = NULL;
            } else {
                elem = BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
                if (BKEY_ISGT(elem->data, elem->nbkey, bkrange->to_bkey, bkrange->to_nbkey))
                    elem = NULL;
            }
            break;
          case BKEY_RANGE_TYPE_DSC: /* descending bkey range */
            /* find the prev element */
            if (right >= 0) {
                path[0].node = node;
                path[0].indx = right;
            } else {
                path[0].node = node->prev;
                if (node->prev != NULL) {
                    path[0].indx = node->prev->used_count-1;
                    if (path_flag) do_btree_decr_path(path, 1);
                } else {
                    path[0].indx = BTREE_ITEM_COUNT;
                }
            }
            if (path[0].node == NULL) {
                elem = NULL;
            } else {
                elem = BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
                if (BKEY_ISLT(elem->data, elem->nbkey, bkrange->to_bkey, bkrange->to_nbkey))
                    elem = NULL;
            }
            break;
        }
    }
    return elem;
}

static btree_elem_item *do_btree_find_next(btree_elem_posi *posi,
                                           const bkey_range *bkrange)
{
    btree_elem_item *elem;

    do_btree_incr_posi(posi);
    if (posi->node == NULL) {
        posi->bkeq = false;
        return NULL;
    }

    elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    if (bkrange != NULL) {
        int comp = BKEY_COMP(elem->data, elem->nbkey,
                             bkrange->to_bkey, bkrange->to_nbkey);
        if (comp == 0) {
            posi->bkeq = true;
        } else {
            posi->bkeq = false;
            if (comp > 0) elem = NULL;
        }
    } else {
        posi->bkeq = false;
    }
    return elem;
}

static btree_elem_item *do_btree_find_prev(btree_elem_posi *posi,
                                           const bkey_range *bkrange)
{
    btree_elem_item *elem;

    do_btree_decr_posi(posi);
    if (posi->node == NULL) {
        posi->bkeq = false;
        return NULL;
    }

    elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    if (bkrange != NULL) {
        int comp = BKEY_COMP(elem->data, elem->nbkey,
                             bkrange->to_bkey, bkrange->to_nbkey);
        if (comp == 0) {
            posi->bkeq = true;
        } else {
            posi->bkeq = false;
            if (comp < 0) elem = NULL;
        }
    } else {
        posi->bkeq = false;
    }
    return elem;
}

static inline bool do_btree_elem_filter(btree_elem_item *elem, const eflag_filter *efilter)
{
    assert(efilter != NULL);
    if (efilter->fwhere >= elem->neflag || efilter->ncompval > (elem->neflag-efilter->fwhere)) {
        return (efilter->compop == COMPARE_OP_NE ? true : false);
    }

    unsigned char result[MAX_EFLAG_LENG];
    unsigned char *operand = elem->data + BTREE_REAL_NBKEY(elem->nbkey) + efilter->fwhere;

    if (efilter->nbitwval > 0) {
        (*BINARY_BITWISE_OP[efilter->bitwop])(operand, efilter->bitwval, efilter->nbitwval, result);
        operand = &result[0];
    }

    if (efilter->compvcnt > 1) {
        assert(efilter->compop == COMPARE_OP_EQ || efilter->compop == COMPARE_OP_NE);
        for (int i = 0; i < efilter->compvcnt; i++) {
            if (BINARY_ISEQ(operand, efilter->ncompval,
                            &efilter->compval[i*efilter->ncompval], efilter->ncompval)) {
                return (efilter->compop == COMPARE_OP_EQ ? true : false);
            }
        }
        return (efilter->compop == COMPARE_OP_EQ ? false : true);
    } else {
        return (*BINARY_COMPARE_OP[efilter->compop])(operand, efilter->ncompval,
                                                     efilter->compval, efilter->ncompval);
    }
}

static void do_btree_consistency_check(btree_indx_node *node, uint32_t ecount, bool detail)
{
    uint32_t i, tot_ecnt;

    if (node == NULL) { /* root node */
        assert(ecount == 0);
        return;
    }

    if (node->prev != NULL) {
        assert(node->prev->next == node);
    }
    if (node->next != NULL) {
        assert(node->next->prev == node);
    }
    if (node->ndepth > 0) { /* nonleaf page check */
        tot_ecnt = 0;
        for (i = 0; i < node->used_count; i++) {
            assert(node->item[i] != NULL);
            assert(node->ecnt[i] > 0);
            do_btree_consistency_check((btree_indx_node*)node->item[i], node->ecnt[i], detail);
            tot_ecnt += node->ecnt[i];
        }
        assert(tot_ecnt == ecount);
    } else { /* node->ndepth == 0: leaf page check */
        for (i = 0; i < node->used_count; i++) {
            assert(node->item[i] != NULL);
        }
        assert(node->used_count == ecount);
        if (detail) {
            btree_elem_item *p_elem;
            btree_elem_item *c_elem;
            int comp;

            if (node->prev == NULL) {
                p_elem = NULL;
            } else {
                p_elem = BTREE_GET_ELEM_ITEM(node->prev, node->prev->used_count-1);
            }
            for (i = 0; i < node->used_count; i++) {
                c_elem = BTREE_GET_ELEM_ITEM(node, i);
                if (p_elem != NULL) {
                    comp = BKEY_COMP(p_elem->data, p_elem->nbkey, c_elem->data, c_elem->nbkey);
                    assert(comp < 0);
                }
                p_elem = c_elem;
            }
            if (node->next == NULL) {
                c_elem = NULL;
            } else {
                c_elem = BTREE_GET_ELEM_ITEM(node->next, 0);
            }
            if (c_elem != NULL) {
                comp = BKEY_COMP(p_elem->data, p_elem->nbkey, c_elem->data, c_elem->nbkey);
                assert(comp < 0);
            }
        }
    }
}

static void do_btree_node_item_move(btree_indx_node *c_node, /* current node */
                                    btree_indx_node *n_node, /* neighbor node */
                                    int direction, int move_count)
{
    assert(move_count > 0);
    int i;

    if (direction == BTREE_DIRECTION_NEXT) {
        if (c_node->ndepth == 0) { /* leaf node */
            for (i = (n_node->used_count-1); i >= 0; i--) {
                n_node->item[move_count+i] = n_node->item[i];
            }
            for (i = 0; i < move_count; i++) {
                n_node->item[i] = c_node->item[c_node->used_count-move_count+i];
                c_node->item[c_node->used_count-move_count+i] = NULL;
            }
        } else { /* c_node->ndepth > 0: nonleaf node */
            for (i = (n_node->used_count-1); i >= 0; i--) {
                n_node->item[move_count+i] = n_node->item[i];
                n_node->ecnt[move_count+i] = n_node->ecnt[i];
            }
            for (i = 0; i < move_count; i++) {
                n_node->item[i] = c_node->item[c_node->used_count-move_count+i];
                c_node->item[c_node->used_count-move_count+i] = NULL;
                n_node->ecnt[i] = c_node->ecnt[c_node->used_count-move_count+i];
                c_node->ecnt[c_node->used_count-move_count+i] = 0;
            }
        }
    } else { /* BTREE_DIRECTION_PREV */
        if (c_node->ndepth == 0) { /* leaf node */
            for (i = 0; i < move_count; i++) {
                n_node->item[n_node->used_count+i] = c_node->item[i];
            }
            for (i = move_count; i < c_node->used_count; i++) {
                c_node->item[i-move_count] = c_node->item[i];
                c_node->item[i] = NULL;
            }
        } else { /* c_node->ndepth > 0: nonleaf node */
            for (i = 0; i < move_count; i++) {
                n_node->item[n_node->used_count+i] = c_node->item[i];
                n_node->ecnt[n_node->used_count+i] = c_node->ecnt[i];
            }
            for (i = move_count; i < c_node->used_count; i++) {
                c_node->item[i-move_count] = c_node->item[i];
                c_node->item[i] = NULL;
                c_node->ecnt[i-move_count] = c_node->ecnt[i];
                c_node->ecnt[i] = 0;
            }
        }
    }
    n_node->used_count += move_count;
    c_node->used_count -= move_count;
}

static void do_btree_ecnt_move_split(btree_elem_posi *path, int depth, int direction, uint32_t elem_count)
{
    btree_elem_posi  posi;
    btree_indx_node *saved_node;

    while (depth < BTREE_MAX_DEPTH) {
        posi = path[depth];
        posi.node->ecnt[posi.indx] -= elem_count;

        saved_node = posi.node;
        if (direction == BTREE_DIRECTION_NEXT) {
            do_btree_incr_posi(&posi);
        } else {
            do_btree_decr_posi(&posi);
        }
        posi.node->ecnt[posi.indx] += elem_count;
        if (saved_node == posi.node) break;
        depth += 1;
    }
    assert(depth < BTREE_MAX_DEPTH);
}

static void do_btree_ecnt_move_merge(btree_elem_posi *path, int depth, int direction, uint32_t elem_count)
{
    btree_elem_posi  posi;
    btree_indx_node *saved_node;

    while (depth < BTREE_MAX_DEPTH) {
        posi = path[depth];
        posi.node->ecnt[posi.indx] -= elem_count;

        saved_node = posi.node;
        if (direction == BTREE_DIRECTION_NEXT) {
            do {
                do_btree_incr_posi(&posi);
            } while (posi.node->used_count == 0 ||
                     posi.node->ecnt[posi.indx] == 0);
        } else {
            do {
                do_btree_decr_posi(&posi);
            } while (posi.node->used_count == 0 ||
                     posi.node->ecnt[posi.indx] == 0);
        }
        posi.node->ecnt[posi.indx] += elem_count;
        if (saved_node == posi.node) break;
        depth += 1;
    }
    assert(depth < BTREE_MAX_DEPTH);
}

static void do_btree_node_sbalance(btree_indx_node *node, btree_elem_posi *path, int depth)
{
    btree_elem_posi *posi;
    int direction;
    int move_count;
    int elem_count; /* total count of elements moved */
    int i;

    /* balance the number of elements with neighber node */
    if (node->next != NULL && node->prev != NULL) {
        direction = (node->next->used_count < node->prev->used_count ?
                     BTREE_DIRECTION_NEXT : BTREE_DIRECTION_PREV);
    } else {
        direction = (node->next != NULL ?
                     BTREE_DIRECTION_NEXT : BTREE_DIRECTION_PREV);
    }
    if (direction == BTREE_DIRECTION_NEXT) {
        if (node->next->used_count > 0) {
            move_count = (node->used_count - node->next->used_count) / 2;
        } else {
            move_count = (node->next->next == NULL ? (node->used_count / 10)
                                                   : (node->used_count / 2));
        }
        if (move_count == 0) move_count = 1;

        if (depth == 0) {
            elem_count = move_count;
        } else {
            elem_count = 0;
            for (i = 0; i < move_count; i++) {
                elem_count += node->ecnt[node->used_count-move_count+i];
            }
        }

        do_btree_node_item_move(node, node->next, direction, move_count);

        /* move element count in upper btree nodes */
        do_btree_ecnt_move_split(path, depth+1, direction, elem_count);

        /* adjust posi information */
        posi = &path[depth];
        if (posi->indx >= node->used_count) {
            posi->node = node->next;
            posi->indx -= node->used_count;
            /* adjust upper path info */
            do_btree_incr_path(path, depth+1);
        }
    } else {
        if (node->prev->used_count > 0) {
            move_count = (node->used_count - node->prev->used_count) / 2;
        } else {
            move_count = (node->prev->prev == NULL ? (node->used_count / 10)
                                                   : (node->used_count / 2));
        }
        if (move_count == 0) move_count = 1;

        if (depth == 0) {
            elem_count = move_count;
        } else {
            elem_count = 0;
            for (i = 0; i < move_count; i++) {
                elem_count += node->ecnt[i];
            }
        }

        do_btree_node_item_move(node, node->prev, direction, move_count);

        /* move element count in upper btree nodes */
        do_btree_ecnt_move_split(path, depth+1, direction, elem_count);

        /* adjust posi information */
        posi = &path[depth];
        if (posi->indx < move_count) {
            posi->node = node->prev;
            posi->indx += (node->prev->used_count-move_count);
            /* adjust upper path info */
            do_btree_decr_path(path, depth+1);
        } else {
            posi->indx -= move_count;
        }
    }
}

static void do_btree_node_link(btree_meta_info *info, btree_indx_node *node,
                               btree_elem_posi *p_posi)
{
    /*
     * p_posi: the position of to-be-linked node in parent node.
     */
    if (p_posi == NULL) {
        /* No parent node : make a new root node */
        if (info->root == NULL) {
            node->used_count = 0;
        } else {
            node->item[0] = info->root;
            node->ecnt[0] = info->ccnt;
            node->used_count = 1;
        }
        info->root = node;
    } else {
        /* Parent node exists */
        btree_indx_node *p_node = p_posi->node;
        assert(p_node->used_count >= 1);
        assert(p_posi->indx <= p_node->used_count);

        if (p_posi->indx == 0) {
            node->prev = (p_node->prev == NULL ?
                          NULL : p_node->prev->item[p_node->prev->used_count-1]);
            node->next = p_node->item[p_posi->indx];
        } else if (p_posi->indx < p_node->used_count) {
            node->prev = p_node->item[p_posi->indx-1];
            node->next = p_node->item[p_posi->indx];
        } else { /* p_posi->index == p_node->used_count */
            node->prev = p_node->item[p_posi->indx-1];
            node->next = (p_node->next == NULL ?
                          NULL : p_node->next->item[0]);
        }
        if (node->prev != NULL) node->prev->next = node;
        if (node->next != NULL) node->next->prev = node;

        for (int i = (p_node->used_count-1); i >= p_posi->indx; i--) {
            p_node->item[i+1] = p_node->item[i];
            p_node->ecnt[i+1] = p_node->ecnt[i];
        }
        p_node->item[p_posi->indx] = node;
        p_node->ecnt[p_posi->indx] = 0;
        p_node->used_count++;
    }

    if (1) { /* apply memory space */
        size_t stotal;
        if (node->ndepth > 0) stotal = slabs_space_size(sizeof(btree_indx_node));
        else                  stotal = slabs_space_size(sizeof(btree_leaf_node));
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
    }
}

static ENGINE_ERROR_CODE do_btree_node_split(btree_meta_info *info, btree_elem_posi *path,
                                             const void *cookie)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    btree_indx_node *s_node;
    btree_indx_node *n_node[BTREE_MAX_DEPTH]; /* neighber nodes */
    btree_elem_posi  p_posi;
    int     i, direction;
    uint8_t btree_depth = 0;

    s_node = path[btree_depth].node;
    do {
        if ((s_node->next != NULL && s_node->next->used_count < (BTREE_ITEM_COUNT/2)) ||
            (s_node->prev != NULL && s_node->prev->used_count < (BTREE_ITEM_COUNT/2))) {
            do_btree_node_sbalance(s_node, path, btree_depth);
            break;
        }

        n_node[btree_depth] = do_btree_node_alloc(btree_depth, cookie);
        if (n_node[btree_depth] == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        btree_depth += 1;
        assert(btree_depth < BTREE_MAX_DEPTH);
        if (btree_depth > info->root->ndepth) {
            btree_indx_node *r_node = do_btree_node_alloc(btree_depth, cookie);
            if (r_node == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            do_btree_node_link(info, r_node, NULL);
            path[btree_depth].node = r_node;
            path[btree_depth].indx = 0;
            break;
        }
        s_node = path[btree_depth].node;
    }
    while (s_node->used_count >= BTREE_ITEM_COUNT);

    if (ret == ENGINE_SUCCESS) {
        for (i = btree_depth-1; i >= 0; i--) {
            s_node = path[i].node;
            if (s_node->prev == NULL && s_node->next == NULL) {
                direction = (path[i].indx < (BTREE_ITEM_COUNT/2) ?
                             BTREE_DIRECTION_PREV : BTREE_DIRECTION_NEXT);
            } else {
                direction = (s_node->prev == NULL ?
                             BTREE_DIRECTION_PREV : BTREE_DIRECTION_NEXT);
            }
            p_posi = path[i+1];
            if (direction == BTREE_DIRECTION_NEXT) p_posi.indx += 1;
            do_btree_node_link(info, n_node[i], &p_posi);

            if (direction == BTREE_DIRECTION_PREV) {
                /* adjust upper path */
                path[i+1].indx += 1;
                //do_btree_incr_path(path, i+1);
            }
            do_btree_node_sbalance(s_node, path, i);
        }
    } else {
        for (i = 0; i < btree_depth; i++) {
            do_btree_node_free(n_node[i]);
        }
    }
    if (btree_position_debug) {
        do_btree_consistency_check(info->root, info->ccnt, true);
    }
    return ret;
}

/* merge check */
static void do_btree_node_mbalance(btree_indx_node *node, btree_elem_posi *path, int depth)
{
    int direction;

    if (node->prev != NULL && node->next != NULL) {
        direction = (node->next->used_count < node->prev->used_count ?
                     BTREE_DIRECTION_NEXT : BTREE_DIRECTION_PREV);
    } else {
        direction = (node->next != NULL ?
                     BTREE_DIRECTION_NEXT : BTREE_DIRECTION_PREV);
    }
    if (direction == BTREE_DIRECTION_NEXT) {
        do_btree_node_item_move(node, node->next, direction, node->used_count);
    } else {
        do_btree_node_item_move(node, node->prev, direction, node->used_count);
    }

    int elem_count = path[depth+1].node->ecnt[path[depth+1].indx];
    do_btree_ecnt_move_merge(path, depth+1, direction, elem_count);
}

static void do_btree_node_unlink(btree_meta_info *info, btree_indx_node *node,
                                 btree_elem_posi *p_posi)
{
    if (p_posi == NULL) {
        /* No parent node : remove the root node */
        info->root = NULL;
    } else {
        /* unlink the given node from b+tree */
        if (node->prev != NULL) node->prev->next = node->next;
        if (node->next != NULL) node->next->prev = node->prev;
        node->prev = node->next = NULL;

        /* Parent node exists */
        btree_indx_node *p_node = p_posi->node;
        assert(p_node->ecnt[p_posi->indx] == 0);
        for (int i = p_posi->indx+1; i < p_node->used_count; i++) {
            p_node->item[i-1] = p_node->item[i];
            p_node->ecnt[i-1] = p_node->ecnt[i];
        }
        p_node->item[p_node->used_count-1] = NULL;
        p_node->ecnt[p_node->used_count-1] = 0;
        p_node->used_count--;
    }

    if (info->stotal > 0) { /* apply memory space */
        size_t stotal;
        if (node->ndepth > 0) stotal = slabs_space_size(sizeof(btree_indx_node));
        else                  stotal = slabs_space_size(sizeof(btree_leaf_node));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
    }

    /* The amount of space to be decreased become different according to node depth.
     * So, the btree node must be freed after collection space is decreased.
     */
    do_btree_node_free(node);
}

static void do_btree_node_detach(btree_indx_node *node)
{
    /* unlink the given node from b+tree */
    if (node->prev != NULL) node->prev->next = node->next;
    if (node->next != NULL) node->next->prev = node->prev;
    node->prev = node->next = NULL;

    do_btree_node_free(node);
}

static inline void do_btree_node_remove_null_items(btree_elem_posi *posi, const bool forward, const int null_count)
{
    btree_indx_node *node = posi->node;
    assert(null_count <= node->used_count);

    if (null_count < node->used_count) {
        int f, i;
        int rem_count = 0;
        f = (forward ? posi->indx : 0);
        for ( ; f < node->used_count; f++) {
            if (node->item[f] == NULL) {
                rem_count++;
                break;
            }
        }
        for (i = f+1; i < node->used_count; i++) {
            if (node->item[i] != NULL) {
                node->item[f] = node->item[i];
                node->item[i] = NULL;
                if (node->ndepth > 0) {
                    node->ecnt[f] = node->ecnt[i];
                    node->ecnt[i] = 0;
                }
                f++;
            } else {
                rem_count++;
            }
        }
        assert(rem_count == null_count);
    }
    node->used_count -= null_count;
}

static void do_btree_node_merge(btree_meta_info *info, btree_elem_posi *path,
                                const bool forward, const int leaf_node_count)
{
    btree_indx_node *node;
    int cur_node_count = leaf_node_count;
    int par_node_count;
    uint8_t btree_depth = 0;

    /*
     * leaf_node_count : # of leaf nodes to be merged.
     * cur_node_count  : # of current nodes to be merged in the current btree depth.
     * par_node_count  : # of parent nodes that might be merged after the current merge.
     */
    while (cur_node_count > 0)
    {
        par_node_count = 0;
        if (cur_node_count == 1) {
            node = path[btree_depth].node;
            if (node == info->root) {
                if (node->used_count == 0) {
                    do_btree_node_unlink(info, node, NULL);
                } else {
                    btree_indx_node *new_root;
                    while (node->used_count == 1 && node->ndepth > 0) {
                        new_root = BTREE_GET_NODE_ITEM(node, 0);
                        do_btree_node_unlink(info, node, NULL);
                        info->root = new_root;
                        node = new_root;
                    }
                }
            } else {
                if (node->used_count == 0) {
                    do_btree_node_unlink(info, node, &path[btree_depth+1]);
                    par_node_count = 1;
                }
                else if (node->used_count < (BTREE_ITEM_COUNT/2)) {
                    if ((node->prev != NULL && node->prev->used_count < (BTREE_ITEM_COUNT/2)) ||
                        (node->next != NULL && node->next->used_count < (BTREE_ITEM_COUNT/2))) {
                        do_btree_node_mbalance(node, path, btree_depth);
                        do_btree_node_unlink(info, node, &path[btree_depth+1]);
                        par_node_count = 1;
                    }
                }
            }
        } else { /* cur_node_count > 1 */
            btree_elem_posi  upth[BTREE_MAX_DEPTH]; /* upper node path */
            btree_elem_posi  s_posi;
            int tot_unlink_cnt = 0;
            int cur_unlink_cnt = 0;
            int i, upp_depth = btree_depth+1;

            /* prepare upper node path */
            for (i = upp_depth; i <= info->root->ndepth; i++) {
                upth[i] = path[i];
            }

            s_posi = upth[upp_depth];
            for (i = 1; i <= cur_node_count; i++) {
                node = BTREE_GET_NODE_ITEM(s_posi.node, s_posi.indx);
                assert(node != NULL);

                if (node->used_count == 0) {
                    do_btree_node_detach(node);
                    s_posi.node->item[s_posi.indx] = NULL;
                    assert(s_posi.node->ecnt[s_posi.indx] == 0);
                }

                if (i == cur_node_count) break;

                if (forward) do_btree_incr_posi(&s_posi);
                else         do_btree_decr_posi(&s_posi);
            }

            s_posi = upth[upp_depth];
            for (i = 1; i <= cur_node_count; i++) {
                node = BTREE_GET_NODE_ITEM(upth[upp_depth].node, upth[upp_depth].indx);
                if (node == NULL) {
                    cur_unlink_cnt++;
                }
                else if (node->used_count < (BTREE_ITEM_COUNT/2)) {
                    if ((node->prev != NULL && node->prev->used_count < (BTREE_ITEM_COUNT/2)) ||
                        (node->next != NULL && node->next->used_count < (BTREE_ITEM_COUNT/2))) {
                        do_btree_node_mbalance(node, upth, btree_depth);
                        do_btree_node_detach(node);
                        upth[upp_depth].node->item[upth[upp_depth].indx] = NULL;
                        assert(upth[upp_depth].node->ecnt[upth[upp_depth].indx] == 0);
                        cur_unlink_cnt++;
                    }
                }

                if (i == cur_node_count) break;

                if (forward) do_btree_incr_path(upth, upp_depth);
                else         do_btree_decr_path(upth, upp_depth);

                if (s_posi.node != upth[upp_depth].node) {
                    if (cur_unlink_cnt > 0) {
                        do_btree_node_remove_null_items(&s_posi, forward, cur_unlink_cnt);
                        tot_unlink_cnt += cur_unlink_cnt; cur_unlink_cnt = 0;
                    }
                    s_posi = upth[upp_depth];
                    par_node_count += 1;
                }
            }
            if (cur_unlink_cnt > 0) {
                do_btree_node_remove_null_items(&s_posi, forward, cur_unlink_cnt);
                tot_unlink_cnt += cur_unlink_cnt;
                par_node_count += 1;
            }
            if (tot_unlink_cnt > 0 && info->stotal > 0) { /* apply memory space */
                size_t stotal;
                if (btree_depth > 0) stotal = tot_unlink_cnt * slabs_space_size(sizeof(btree_indx_node));
                else                 stotal = tot_unlink_cnt * slabs_space_size(sizeof(btree_leaf_node));
                do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
            }
        }
        btree_depth += 1;
        cur_node_count = par_node_count;
    }
    if (btree_position_debug) {
        do_btree_consistency_check(info->root, info->ccnt, true);
    }
}

static void do_btree_elem_unlink(btree_meta_info *info, btree_elem_posi *path,
                                 enum elem_delete_cause cause)
{
    btree_elem_posi *posi = &path[0];
    btree_elem_item *elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    int i;

    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(do_btree_elem_ntotal(elem));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
    }

    CLOG_BTREE_ELEM_DELETE(info, elem, cause);

    if (elem->refcount > 0) {
        elem->status = BTREE_ITEM_STATUS_UNLINK;
    } else  {
        elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(elem);
    }

    /* remove the element from the leaf node */
    btree_indx_node *node = posi->node;
    for (i = posi->indx+1; i < node->used_count; i++) {
        node->item[i-1] = node->item[i];
    }
    node->item[node->used_count-1] = NULL;
    node->used_count--;
    /* decrement element count in upper nodes */
    for (i = 1; i <= info->root->ndepth; i++) {
        path[i].node->ecnt[path[i].indx]--;
    }
    info->ccnt--;

    if (node->used_count < (BTREE_ITEM_COUNT/2)) {
        do_btree_node_merge(info, path, true, 1);
    }
}

static void do_btree_elem_replace(btree_meta_info *info,
                                  btree_elem_posi *posi, btree_elem_item *new_elem)
{
    btree_elem_item *old_elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    size_t old_stotal;
    size_t new_stotal;

    old_stotal = slabs_space_size(do_btree_elem_ntotal(old_elem));
    new_stotal = slabs_space_size(do_btree_elem_ntotal(new_elem));

    CLOG_BTREE_ELEM_INSERT(info, old_elem, new_elem);

    if (old_elem->refcount > 0) {
        old_elem->status = BTREE_ITEM_STATUS_UNLINK;
    } else  {
        old_elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(old_elem);
    }

    new_elem->status = BTREE_ITEM_STATUS_USED;
    posi->node->item[posi->indx] = new_elem;

    if (new_stotal != old_stotal) { /* apply memory space */
        assert(info->stotal > 0);
        if (new_stotal > old_stotal)
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_BTREE, (new_stotal-old_stotal));
        else
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, (old_stotal-new_stotal));
    }
}

static ENGINE_ERROR_CODE do_btree_elem_update(btree_meta_info *info,
                                              const int bkrtype, const bkey_range *bkrange,
                                              const eflag_update *eupdate,
                                              const char *value, const uint32_t nbytes,
                                              const void *cookie)
{
    btree_elem_posi  posi;
    btree_elem_item *elem;
    unsigned char *ptr;
    uint32_t real_nbkey;
    uint32_t new_neflag;
    uint32_t new_nbytes;

    if (info->root == NULL) {
        return ENGINE_ELEM_ENOENT;
    }

    elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
    if (elem == NULL) {
        return ENGINE_ELEM_ENOENT;
    }

    assert(posi.bkeq == true);

    /* check eflag update validation check */
    if (eupdate != NULL && eupdate->neflag > 0 && eupdate->bitwop < BITWISE_OP_MAX) {
        if (eupdate->fwhere >= elem->neflag || eupdate->neflag > (elem->neflag-eupdate->fwhere)) {
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
                ptr = elem->data + real_nbkey + eupdate->fwhere;
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

        btree_elem_item *new_elem = do_btree_elem_alloc(elem->nbkey, new_neflag, new_nbytes, cookie);
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
                ptr = new_elem->data + real_nbkey + eupdate->fwhere;
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
                                     btree_elem_posi *path, const uint32_t count)
{
    btree_indx_node *node;
    btree_elem_item *elem;
    int i, delcnt=0;
    int cur_depth;

    if (info->root == NULL) {
        return 0;
    }
    assert(info->root->ndepth < BTREE_MAX_DEPTH);

    if (path[0].node == NULL) {
        path[0].node = do_btree_get_first_leaf(info->root, path);
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
                if (elem->refcount > 0) {
                    elem->status = BTREE_ITEM_STATUS_UNLINK;
                } else {
                    elem->status = BTREE_ITEM_STATUS_FREE;
                    do_btree_elem_free(elem);
                }
            }
        } else {
            for (i = 0; i < node->used_count; i++) {
                do_btree_node_free(node->item[i]);
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
    btree_indx_node *root = info->root;
    btree_elem_item *elem;
    btree_elem_posi path[BTREE_MAX_DEPTH];
    uint32_t tot_found = 0; /* found count */

    if (opcost) *opcost = 0;
    if (root == NULL) return 0;

    assert(root->ndepth < BTREE_MAX_DEPTH);
    elem = do_btree_find_first(root, bkrtype, bkrange, path, true);
    if (elem == NULL) return 0;

    if (bkrtype == BKEY_RANGE_TYPE_SIN) {
        assert(path[0].bkeq == true);
        if (opcost) *opcost += 1;
        if (offset == 0 && (efilter == NULL || do_btree_elem_filter(elem, efilter))) {
            /* cause == ELEM_DELETE_NORMAL */
            do_btree_elem_unlink(info, path, cause);
            tot_found = 1;
        }
    } else {
        btree_elem_posi upth[BTREE_MAX_DEPTH]; /* upper node path */
        btree_elem_posi c_posi = path[0];
        btree_elem_posi s_posi = c_posi; /* save the current posi */
        size_t   tot_space = 0;
        uint32_t cur_found = 0;
        uint32_t node_cnt = 1;
        uint32_t skip_cnt = 0;
        int i;
        bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);

        CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, count, cause);
        /* prepare upper node path
         * used to incr/decr element counts in upper nodes.
         */
        for (i = 1; i <= root->ndepth; i++) {
            upth[i] = path[i];
        }
        /* clear the bkeq flag of current posi */
        c_posi.bkeq = false;

        do {
            if (opcost) *opcost += 1;
            if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                if (skip_cnt < offset) {
                    skip_cnt++;
                } else {
                    tot_space += slabs_space_size(do_btree_elem_ntotal(elem));

                    CLOG_BTREE_ELEM_DELETE(info, elem, cause);
                    if (elem->refcount > 0) {
                        elem->status = BTREE_ITEM_STATUS_UNLINK;
                    } else {
                        elem->status = BTREE_ITEM_STATUS_FREE;
                        do_btree_elem_free(elem);
                    }
                    c_posi.node->item[c_posi.indx] = NULL;

                    cur_found++;
                    if (count > 0 && (tot_found+cur_found) >= count) break;
                }
            }

            /* get the next element */
            if (c_posi.bkeq == true) {
                elem = NULL; /* reached to the end of bkey range */
            } else {
                elem = (forward ? do_btree_find_next(&c_posi, bkrange)
                                : do_btree_find_prev(&c_posi, bkrange));
            }
            if (elem == NULL) break;

            if (s_posi.node != c_posi.node) {
                node_cnt += 1;
                if (cur_found > 0) {
                    do_btree_node_remove_null_items(&s_posi, forward, cur_found);
                    /* decrement element count in upper nodes */
                    for (i = 1; i <= root->ndepth; i++) {
                        assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                        upth[i].node->ecnt[upth[i].indx] -= cur_found;
                    }
                    tot_found += cur_found;
                    cur_found = 0;
                }
                if (root->ndepth > 0) {
                    /* adjust upper node path */
                    if (forward) do_btree_incr_path(upth, 1);
                    else         do_btree_decr_path(upth, 1);
                }
                s_posi = c_posi;
            }
        } while (elem != NULL);

        if (cur_found > 0) {
            do_btree_node_remove_null_items(&s_posi, forward, cur_found);
            /* decrement element count in upper nodes */
            for (i = 1; i <= root->ndepth; i++) {
                assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                upth[i].node->ecnt[upth[i].indx] -= cur_found;
            }
            tot_found += cur_found;
        }

        if (tot_found > 0) {
            info->ccnt -= tot_found;
            if (info->stotal > 0) { /* apply memory space */
                /* The btree might have been unlinked from hash table.
                 * If then, the btree doesn't have prefix info and has stotal of 0.
                 * So, do not need to descrese space total info.
                 */
                assert(tot_space <= info->stotal);
                do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, tot_space);
            }
            do_btree_node_merge(info, path, forward, node_cnt);
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
    btree_elem_item *min_bkey_elem = NULL;
    btree_elem_item *max_bkey_elem = NULL;
    uint32_t real_mcnt = (info->mcnt > 0 ? info->mcnt : config->max_btree_size);

    /* step 1: overflow check on max bkey range */
    if (info->maxbkeyrange.len != BKEY_NULL) {
        bkey_t newbkeyrange;

        min_bkey_elem = do_btree_get_first_elem(info->root);
        max_bkey_elem = do_btree_get_last_elem(info->root);

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
                min_bkey_elem = do_btree_get_first_elem(info->root);
            if (BKEY_ISLT(elem->data, elem->nbkey, min_bkey_elem->data, min_bkey_elem->nbkey)) {
                if (info->ovflact == OVFL_SMALLEST_TRIM) {
                    /* It means the implicit trim. */
                    info->mflags |= COLL_META_FLAG_TRIMMED; // set trimmed
                }
                return ENGINE_EBKEYOOR;
            }
        } else { /* OVFL_LARGEST_TRIM || OVFL_LARGEST_SILENT_TRIM */
            if (max_bkey_elem == NULL)
                max_bkey_elem = do_btree_get_last_elem(info->root);
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

static void do_btree_overflow_trim(btree_meta_info *info,
                                   btree_elem_item *elem, const int overflow_type,
                                   btree_elem_item **trimmed_elems, uint32_t *trimmed_count)
{
    assert(info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM ||
           info->ovflact == OVFL_LARGEST_TRIM  || info->ovflact == OVFL_LARGEST_SILENT_TRIM);

    if (overflow_type == OVFL_TYPE_RANGE) {
        btree_elem_item *edge_elem;
        uint32_t del_count;
        int      bkrtype;
        bkey_range bkrange_space;
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM) {
            /* bkey range that must be trimmed.
             * => min bkey ~ (new max bkey - maxbkeyrange - 1)
             */
            /* from bkey */
            edge_elem = do_btree_get_first_elem(info->root); /* min bkey elem */
            bkrange_space.from_nbkey = edge_elem->nbkey;
            BKEY_COPY(edge_elem->data, edge_elem->nbkey, bkrange_space.from_bkey);
            /* to bkey */
            bkrange_space.to_nbkey = info->maxbkeyrange.len;
            BKEY_DIFF(elem->data, elem->nbkey,
                      info->maxbkeyrange.val, info->maxbkeyrange.len,
                      bkrange_space.to_nbkey, bkrange_space.to_bkey);
            BKEY_DECR(bkrange_space.to_bkey, bkrange_space.to_nbkey);
        } else {
            /* bkey range that must be trimmed.
             * => (new min bkey + maxbkeyrange + 1) ~ max bkey
             * => max bkey - (max bkey - new min bkey - maxbkeyrange - 1) ~ max bkey
             */
            /* from bkey */
            edge_elem = do_btree_get_last_elem(info->root);  /* max bkey elem */
            bkrange_space.from_nbkey = info->maxbkeyrange.len;
            BKEY_DIFF(edge_elem->data, edge_elem->nbkey,
                      elem->data, elem->nbkey,
                      bkrange_space.from_nbkey, bkrange_space.from_bkey);
            BKEY_DIFF(bkrange_space.from_bkey, bkrange_space.from_nbkey,
                      info->maxbkeyrange.val, info->maxbkeyrange.len,
                      bkrange_space.from_nbkey, bkrange_space.from_bkey);
            BKEY_DECR(bkrange_space.from_bkey, bkrange_space.from_nbkey);
            BKEY_DIFF(edge_elem->data, edge_elem->nbkey,
                      bkrange_space.from_bkey, bkrange_space.from_nbkey,
                      bkrange_space.from_nbkey, bkrange_space.from_bkey);
            /* to bkey */
            bkrange_space.to_nbkey = edge_elem->nbkey;
            BKEY_COPY(edge_elem->data, edge_elem->nbkey, bkrange_space.to_bkey);
        }
        bkrtype = do_btree_bkey_range_type(&bkrange_space);
        del_count = do_btree_elem_delete(info, bkrtype, &bkrange_space, NULL, 0,
                                         0, NULL, ELEM_DELETE_TRIM);
        assert(del_count > 0);
        assert(info->ccnt > 0);
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_LARGEST_TRIM)
            info->mflags &= ~COLL_META_FLAG_TRIMMED; // clear trimmed
    } else { /* overflow_type == OVFL_TYPE_COUNT */
        assert(overflow_type == OVFL_TYPE_COUNT);

        btree_elem_posi delpath[BTREE_MAX_DEPTH];
        assert(info->root->ndepth < BTREE_MAX_DEPTH);
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM) {
            delpath[0].node = do_btree_get_first_leaf(info->root, delpath);
            delpath[0].indx = 0;
        } else { /* info->ovflact == OVFL_LARGEST_TRIM or OVFL_LARGEST_SILENT_TRIM */
            delpath[0].node = do_btree_get_last_leaf(info->root, delpath);
            delpath[0].indx = delpath[0].node->used_count - 1;
        }
        if (trimmed_elems != NULL) {
            btree_elem_item *edge_elem = BTREE_GET_ELEM_ITEM(delpath[0].node, delpath[0].indx);
            edge_elem->refcount++;
            *trimmed_elems = edge_elem;
            *trimmed_count = 1;
        }
        do_btree_elem_unlink(info, delpath, ELEM_DELETE_TRIM);
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_LARGEST_TRIM)
            info->mflags |= COLL_META_FLAG_TRIMMED; // set trimmed
    }
}

static ENGINE_ERROR_CODE do_btree_elem_link(btree_meta_info *info, btree_elem_item *elem,
                                            const bool replace_if_exist, bool *replaced,
                                            btree_elem_item **trimmed_elems, uint32_t *trimmed_count,
                                            const void *cookie)
{
    btree_elem_posi path[BTREE_MAX_DEPTH];
    int ovfl_type = OVFL_TYPE_NONE;
    ENGINE_ERROR_CODE res;

    if (replaced) *replaced = false;

    assert(info->root->ndepth < BTREE_MAX_DEPTH);
    res = do_btree_find_insposi(info->root, elem->data, elem->nbkey, path);
    if (res == ENGINE_SUCCESS) {
#ifdef ENABLE_STICKY_ITEM
        /* sticky memory limit check */
        if (IS_STICKY_COLLFLG(info)) {
            if (do_item_sticky_overflowed())
                return ENGINE_ENOMEM;
        }
#endif

        /* If the leaf node is full of elements, split it ahead. */
        if (path[0].node->used_count >= BTREE_ITEM_COUNT) {
            res = do_btree_node_split(info, path, cookie);
            if (res != ENGINE_SUCCESS) {
                return res;
            }
        }

        if (info->ccnt > 0) {
            /* overflow check */
            res = do_btree_overflow_check(info, elem, &ovfl_type);
            if (res != ENGINE_SUCCESS) {
                return res;
            }
        } else { /* info->ccnt == 0 */
            /* set bkey type */
            if (elem->nbkey == 0)
                info->bktype = BKEY_TYPE_UINT64;
            else
                info->bktype = BKEY_TYPE_BINARY;
        }

        CLOG_BTREE_ELEM_INSERT(info, NULL, elem);

        /* insert the element into the leaf page */
        elem->status = BTREE_ITEM_STATUS_USED;
        if (path[0].indx < path[0].node->used_count) {
            for (int i = (path[0].node->used_count-1); i >= path[0].indx; i--) {
                path[0].node->item[i+1] = path[0].node->item[i];
            }
        }
        path[0].node->item[path[0].indx] = elem;
        path[0].node->used_count++;
        /* increment element count in upper nodes */
        for (int i = 1; i <= info->root->ndepth; i++) {
            path[i].node->ecnt[path[i].indx]++;
        }
        info->ccnt++;

        if (1) { /* apply memory space */
            size_t stotal = slabs_space_size(do_btree_elem_ntotal(elem));
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
        }

        if (ovfl_type != OVFL_TYPE_NONE) {
            do_btree_overflow_trim(info, elem, ovfl_type, trimmed_elems, trimmed_count);
        }
    }
    else if (res == ENGINE_ELEM_EEXISTS) {
        if (replace_if_exist) {
#ifdef ENABLE_STICKY_ITEM
            /* sticky memory limit check */
            if (IS_STICKY_COLLFLG(info)) {
                btree_elem_item *find = BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
                if ((find->neflag + find->nbytes) < (elem->neflag + elem->nbytes)) {
                    if (do_item_sticky_overflowed())
                        return ENGINE_ENOMEM;
                }
            }
#endif

            do_btree_elem_replace(info, &path[0], elem);
            if (replaced) *replaced = true;
            res = ENGINE_SUCCESS;
        }
    }
    return res;
}

static bool do_btree_overlapped_with_trimmed_space(btree_meta_info *info,
                                                   btree_elem_posi *posi, const int bkrtype)
{
    assert((info->mflags & COLL_META_FLAG_TRIMMED) != 0);
    bool overlapped = false;

    switch (info->ovflact) {
      case OVFL_SMALLEST_TRIM:
           if (posi->node == NULL) {
               if (posi->indx == BTREE_ITEM_COUNT) overlapped = true;
           } else {
               /* the bkey of the found elem isn't same with the from_bkey of bkey range */
               assert(posi->node->ndepth == 0); /* leaf node */
               if (posi->node->prev == NULL && posi->indx == 0 /* the first element */
                   && bkrtype == BKEY_RANGE_TYPE_ASC) overlapped = true;
           }
           break;
      case OVFL_LARGEST_TRIM:
           if (posi->node == NULL) {
               if (posi->indx == 0) overlapped = true;
           } else {
               /* the bkey of the found elem isn't same with the from_bkey of bkey range */
               assert(posi->node->ndepth == 0); /* leaf node */
               if (posi->node->next == NULL && posi->indx == (posi->node->used_count-1) /* the last element */
                   && bkrtype == BKEY_RANGE_TYPE_DSC) overlapped = true;
           }
           break;
    }
    return overlapped;
}

static uint32_t do_btree_elem_get(btree_meta_info *info,
                                  const int bkrtype, const bkey_range *bkrange,
                                  const eflag_filter *efilter,
                                  const uint32_t offset, const uint32_t count, const bool delete,
                                  btree_elem_item **elem_array,
                                  uint32_t *opcost, bool *potentialbkeytrim)
{
    assert(info->root);
    btree_indx_node *root = info->root;
    btree_elem_item *elem;
    btree_elem_posi path[BTREE_MAX_DEPTH];
    uint32_t tot_found = 0; /* found count */

    if (opcost) *opcost = 0;
    *potentialbkeytrim = false;

    assert(root->ndepth < BTREE_MAX_DEPTH);
    elem = do_btree_find_first(root, bkrtype, bkrange, path, delete);
    if (elem == NULL) {
        if ((info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
            if (do_btree_overlapped_with_trimmed_space(info, &path[0], bkrtype))
                *potentialbkeytrim = true;
        }
        return 0;
    }

    if (bkrtype == BKEY_RANGE_TYPE_SIN) {
        assert(path[0].bkeq == true);
        if (opcost) *opcost += 1;
        if (offset == 0 && (efilter == NULL || do_btree_elem_filter(elem, efilter))) {
            elem->refcount++;
            elem_array[tot_found++] = elem;
            if (delete) {
                do_btree_elem_unlink(info, path, ELEM_DELETE_NORMAL);
            }
        }
    } else {
        btree_elem_posi upth[BTREE_MAX_DEPTH]; /* upper node path */
        btree_elem_posi c_posi = path[0];
        btree_elem_posi s_posi = c_posi; /* save the current posi */
        size_t   tot_space = 0;
        uint32_t cur_found = 0;
        uint32_t node_cnt = 1;
        uint32_t skip_cnt = 0;
        int i;
        bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);

        /* check if start position might be trimmed */
        if (c_posi.bkeq == false && (info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
            if (do_btree_overlapped_with_trimmed_space(info, &c_posi, bkrtype))
                *potentialbkeytrim = true;
        }

        if (delete) {
            CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, count, ELEM_DELETE_NORMAL);
            /* prepare upper node path
             * used to incr/decr element counts  in upper nodes.
             */
            for (i = 1; i <= root->ndepth; i++) {
                upth[i] = path[i];
            }
        }
        /* clear the bkeq flag of current posi */
        c_posi.bkeq = false;

        do {
            if (opcost) *opcost += 1;
            if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                if (skip_cnt < offset) {
                    skip_cnt++;
                } else {
                    elem->refcount++;
                    elem_array[tot_found+cur_found] = elem;
                    if (delete) {
                        tot_space += slabs_space_size(do_btree_elem_ntotal(elem));
                        elem->status = BTREE_ITEM_STATUS_UNLINK;
                        c_posi.node->item[c_posi.indx] = NULL;
                        CLOG_BTREE_ELEM_DELETE(info, elem, ELEM_DELETE_NORMAL);
                    }
                    cur_found++;
                    if (count > 0 && (tot_found+cur_found) >= count) break;
                }
            }

            /* get the next element */
            if (c_posi.bkeq == true) {
                elem = NULL; /* reached to the end of bkey range */
            } else {
                elem = (forward ? do_btree_find_next(&c_posi, bkrange)
                                : do_btree_find_prev(&c_posi, bkrange));
            }
            if (elem == NULL) break;

            if (s_posi.node != c_posi.node) {
                node_cnt += 1;
                if (cur_found > 0) {
                    if (delete) {
                        do_btree_node_remove_null_items(&s_posi, forward, cur_found);
                        /* decrement element count in upper nodes */
                        for (i = 1; i <= root->ndepth; i++) {
                            assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                            upth[i].node->ecnt[upth[i].indx] -= cur_found;
                        }
                    }
                    tot_found += cur_found;
                    cur_found = 0;
                }
                if (delete && root->ndepth > 0) {
                    /* adjust upper node path */
                    if (forward) do_btree_incr_path(upth, 1);
                    else         do_btree_decr_path(upth, 1);
                }
                s_posi = c_posi;
            }
        } while (elem != NULL);

        if (cur_found > 0) {
            if (delete) {
                do_btree_node_remove_null_items(&s_posi, forward, cur_found);
                /* decrement element count in upper nodes */
                for (i = 1; i <= root->ndepth; i++) {
                    assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                    upth[i].node->ecnt[upth[i].indx] -= cur_found;
                }
            }
            tot_found += cur_found;
        }

        if (delete && tot_found > 0) { /* apply memory space */
            info->ccnt -= tot_found;
            assert(tot_space <= info->stotal);
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, tot_space);
            do_btree_node_merge(info, path, forward, node_cnt);
        }

        /* check if end position might be trimmed */
        if (c_posi.node == NULL && (info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
            if (do_btree_overlapped_with_trimmed_space(info, &c_posi, bkrtype))
                *potentialbkeytrim = true;
        }

        if (delete) {
            CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);
        }
    }
    if (delete && tot_found > 0) {
        CLOG_BTREE_ELEM_DELETE_LOGICAL(info, bkrange, efilter, offset, count,
                                       ELEM_DELETE_NORMAL);
    }
    return tot_found;
}

static uint32_t do_btree_elem_count(btree_meta_info *info,
                                    const int bkrtype, const bkey_range *bkrange,
                                    const eflag_filter *efilter, uint32_t *opcost)
{
    btree_elem_posi  posi;
    btree_elem_item *elem;
    uint32_t tot_found = 0; /* total found count */
    uint32_t tot_access = 0; /* total access count */

    if (info->root == NULL) {
        if (opcost)
            *opcost = 0;
        return 0;
    }

#ifdef BOP_COUNT_OPTIMIZE
    if (bkrtype != BKEY_RANGE_TYPE_SIN && efilter == NULL) {
        btree_elem_item *min_bkey_elem = do_btree_get_first_elem(info->root);
        btree_elem_item *max_bkey_elem = do_btree_get_last_elem(info->root);
        int min_comp, max_comp;
        if (bkrtype == BKEY_RANGE_TYPE_ASC) {
            min_comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey,
                                 min_bkey_elem->data, min_bkey_elem->nbkey);
            max_comp = BKEY_COMP(bkrange->to_bkey, bkrange->to_nbkey,
                                 max_bkey_elem->data, max_bkey_elem->nbkey);
        } else { /* BKEY_RANGE_TYPE_DSC */
            min_comp = BKEY_COMP(bkrange->to_bkey, bkrange->to_nbkey,
                                 min_bkey_elem->data, min_bkey_elem->nbkey);
            max_comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey,
                                 max_bkey_elem->data, max_bkey_elem->nbkey);
        }
        if (min_comp <= 0 && max_comp >= 0) {
            return info->ccnt;
        }
    }
#endif

    elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
    if (elem != NULL) {
        if (bkrtype == BKEY_RANGE_TYPE_SIN) {
            assert(posi.bkeq == true);
            tot_access++;
            if (efilter == NULL || do_btree_elem_filter(elem, efilter))
                tot_found++;
        } else { /* BKEY_RANGE_TYPE_ASC || BKEY_RANGE_TYPE_DSC */
            bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);
            posi.bkeq = false;
            do {
                tot_access++;
                if (efilter == NULL || do_btree_elem_filter(elem, efilter))
                    tot_found++;

                if (posi.bkeq == true) {
                    elem = NULL; break;
                }
                elem = (forward ? do_btree_find_next(&posi, bkrange)
                                : do_btree_find_prev(&posi, bkrange));
            } while (elem != NULL);
        }
    }
    if (opcost)
        *opcost = tot_access;
    return tot_found;
}

static ENGINE_ERROR_CODE do_btree_elem_insert(hash_item *it, btree_elem_item *elem,
                                              const bool replace_if_exist, bool *replaced,
                                              btree_elem_item **trimmed_elems,
                                              uint32_t *trimmed_count, const void *cookie)
{
    btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
    ENGINE_ERROR_CODE ret;

    /* validation check: bkey type */
    if (info->ccnt > 0 || info->maxbkeyrange.len != BKEY_NULL) {
        if ((info->bktype == BKEY_TYPE_UINT64 && elem->nbkey >  0) ||
            (info->bktype == BKEY_TYPE_BINARY && elem->nbkey == 0)) {
            return ENGINE_EBADBKEY;
        }
    }

    /* Both sticky memory limit check and overflow check
     * are to be performed in the below do_btree_elem_link().
     */

    /* create the root node if it does not exist */
    bool new_root_flag = false;
    if (info->root == NULL) {
        btree_indx_node *r_node = do_btree_node_alloc(0, cookie);
        if (r_node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_btree_node_link(info, r_node, NULL);
        new_root_flag = true;
    }

    /* insert the element */
    ret = do_btree_elem_link(info, elem, replace_if_exist, replaced,
                             trimmed_elems, trimmed_count, cookie);
    if (ret != ENGINE_SUCCESS) {
        if (new_root_flag) {
            do_btree_node_unlink(info, info->root, NULL);
        }
        return ret;
    }

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_btree_elem_arithmetic(btree_meta_info *info,
                                                  const int bkrtype, const bkey_range *bkrange,
                                                  const bool increment, const bool create,
                                                  const uint64_t delta, const uint64_t initial,
                                                  const eflag_t *eflagp,
                                                  uint64_t *result, const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    btree_elem_item *elem;
    btree_elem_posi  posi;
    uint64_t value;
    char     nbuf[128];
    int      nlen;
    uint32_t real_nbkey;

    if (info->root == NULL) {
        assert(create != true);
        return ENGINE_ELEM_ENOENT;
    }

    elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
    if (elem == NULL) {
        if (create != true) return ENGINE_ELEM_ENOENT;

        if ((nlen = snprintf(nbuf, sizeof(nbuf), "%"PRIu64"\r\n", initial)) == -1) {
            return ENGINE_EINVAL;
        }

        elem = do_btree_elem_alloc(bkrange->from_nbkey,
                                   (eflagp == NULL || eflagp->len == EFLAG_NULL ? 0 : eflagp->len),
                                   nlen, cookie);
        if (elem == NULL) {
            return ENGINE_ENOMEM;
        }

        real_nbkey = BTREE_REAL_NBKEY(bkrange->from_nbkey);
        memcpy(elem->data, bkrange->from_bkey, real_nbkey);
        if (eflagp == NULL || eflagp->len == EFLAG_NULL) {
            memcpy(elem->data + real_nbkey, nbuf, nlen);
        } else {
            memcpy(elem->data + real_nbkey, eflagp->val, eflagp->len);
            memcpy(elem->data + real_nbkey + eflagp->len, nbuf, nlen);
        }

        ret = do_btree_elem_link(info, elem, false, NULL, NULL, NULL, cookie);
        if (ret != ENGINE_SUCCESS) {
            assert(ret != ENGINE_ELEM_EEXISTS);
            /* ENGINE_ENOMEM || ENGINE_BKEYOOR || ENGINE_OVERFLOW */
            do_btree_elem_free(elem);
        }
        *result = initial;
    } else {
        real_nbkey = BTREE_REAL_NBKEY(elem->nbkey);
        if (! safe_strtoull((const char*)elem->data + real_nbkey + elem->neflag, &value) || elem->nbytes == 2) {
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
            btree_elem_item *new_elem = do_btree_elem_alloc(elem->nbkey, elem->neflag, nlen, cookie);
            if (new_elem == NULL) {
                return ENGINE_ENOMEM;
            }
            memcpy(new_elem->data, elem->data, real_nbkey + elem->neflag);
            memcpy(new_elem->data + real_nbkey + new_elem->neflag, nbuf, nlen);

            do_btree_elem_replace(info, &posi, new_elem);
        }
        ret = ENGINE_SUCCESS;
        *result = value;
    }
    return ret;
}

static int do_btree_posi_from_path(btree_meta_info *info,
                                   btree_elem_posi *path, ENGINE_BTREE_ORDER order)
{
    int d, i, bpos;

    bpos = path[0].indx;
    for (d = 1; d <= info->root->ndepth; d++) {
        for (i = 0; i < path[d].indx; i++) {
            bpos += path[d].node->ecnt[i];
        }
    }
    if (order == BTREE_ORDER_DESC) {
        bpos = info->ccnt - bpos - 1;
    }
    return bpos; /* btree position */
}

static int do_btree_posi_find(btree_meta_info *info,
                              const int bkrtype, const bkey_range *bkrange,
                              ENGINE_BTREE_ORDER order)
{
    btree_elem_posi  path[BTREE_MAX_DEPTH];
    btree_elem_item *elem;
    int bpos; /* btree position */

    if (info->root == NULL) return -1; /* not found */

    elem = do_btree_find_first(info->root, bkrtype, bkrange, path, true);
    if (elem != NULL) {
        assert(path[0].bkeq == true);
        bpos = do_btree_posi_from_path(info, path, order);
        assert(bpos >= 0);
    } else {
        bpos = -1; /* not found */
    }
    return bpos;
}

static int do_btree_elem_batch_get(btree_elem_posi posi, const int count,
                                   const bool forward, const bool reverse,
                                   btree_elem_item **elem_array)
{
    btree_elem_item *elem;
    int nfound = 0;
    while (nfound < count) {
        if (forward) do_btree_incr_posi(&posi);
        else         do_btree_decr_posi(&posi);
        if (posi.node == NULL) break;

        elem = BTREE_GET_ELEM_ITEM(posi.node, posi.indx);
        elem->refcount++;
        if (reverse) elem_array[count-nfound-1] = elem;
        else         elem_array[nfound] = elem;
        nfound += 1;
    }
    return nfound;
}

static int do_btree_posi_find_with_get(btree_meta_info *info,
                                       const int bkrtype, const bkey_range *bkrange,
                                       ENGINE_BTREE_ORDER order, const int count,
                                       btree_elem_item **elem_array,
                                       uint32_t *elem_count, uint32_t *elem_index)
{
    btree_elem_posi  path[BTREE_MAX_DEPTH];
    btree_elem_item *elem;
    int bpos = -1; /* NOT found */

    if (info->root == NULL) return -1; /* not found */

    elem = do_btree_find_first(info->root, bkrtype, bkrange, path, true);
    if (elem != NULL) {
        int ecnt, eidx;
        assert(path[0].bkeq == true);
        bpos = do_btree_posi_from_path(info, path, order);
        assert(bpos >= 0);

        ecnt = 1;                             /* elem count */
        eidx = (bpos < count) ? bpos : count; /* elem index in elem array */
        elem->refcount++;
        elem_array[eidx] = elem;

        if (order == BTREE_ORDER_ASC) {
            ecnt += do_btree_elem_batch_get(path[0], eidx,  false, true,  &elem_array[0]);
            assert((ecnt-1) == eidx);
            ecnt += do_btree_elem_batch_get(path[0], count, true,  false, &elem_array[eidx+1]);
        } else {
            ecnt += do_btree_elem_batch_get(path[0], eidx,  true,  true,  &elem_array[0]);
            assert((ecnt-1) == eidx);
            ecnt += do_btree_elem_batch_get(path[0], count, false, false, &elem_array[eidx+1]);
        }
        *elem_count = (uint32_t)ecnt;
        *elem_index = (uint32_t)eidx;
    }
    return bpos; /* btree_position */
}

static ENGINE_ERROR_CODE do_btree_elem_get_by_posi(btree_meta_info *info,
                                                   const int index, const uint32_t count, const bool forward,
                                                   btree_elem_item **elem_array, uint32_t *elem_count)
{
    btree_elem_posi  posi;
    btree_indx_node *node;
    btree_elem_item *elem;
    int i, tot_ecnt;
    uint32_t nfound; /* found count */

    if (info->root == NULL) return ENGINE_ELEM_ENOENT;

    node = info->root;
    tot_ecnt = 0;
    while (node->ndepth > 0) {
        for (i = 0; i < node->used_count; i++) {
            assert(node->ecnt[i] > 0);
            if ((tot_ecnt + node->ecnt[i]) > index) break;
            tot_ecnt += node->ecnt[i];
        }
        assert(i < node->used_count);
        node = (btree_indx_node *)node->item[i];
    }
    assert(node->ndepth == 0);
    posi.node = node;
    posi.indx = index-tot_ecnt;
    posi.bkeq = false;

    elem = BTREE_GET_ELEM_ITEM(posi.node, posi.indx);
    elem->refcount++;
    elem_array[0] = elem;
    nfound = 1;
    nfound += do_btree_elem_batch_get(posi, count-1, forward, false, &elem_array[nfound]);

    *elem_count = nfound;
    if (*elem_count > 0) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ELEM_ENOENT;
    }
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

static btree_elem_item *do_btree_scan_next(btree_elem_posi *posi,
                             const int bkrtype, const bkey_range *bkrange)
{
    if (posi->bkeq == true)
        return NULL;

    if (bkrtype != BKEY_RANGE_TYPE_DSC) // ascending
        return do_btree_find_next(posi, bkrange);
    else // descending
        return do_btree_find_prev(posi, bkrange);
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

#ifdef JHPARK_OLD_SMGET_INTERFACE
static ENGINE_ERROR_CODE
do_btree_smget_scan_sort_old(token_t *key_array, const int key_count,
                             const int bkrtype, const bkey_range *bkrange,
                             const eflag_filter *efilter, const uint32_t req_count,
                             btree_scan_info *btree_scan_buf,
                             uint16_t *sort_sindx_buf, uint32_t *sort_sindx_cnt,
                             uint32_t *missed_key_array, uint32_t *missed_key_count)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    hash_item *it;
    btree_meta_info *info;
    btree_elem_item *elem, *comp;
    btree_elem_posi posi;
    uint16_t comp_idx;
    uint16_t curr_idx = 0;
    uint16_t free_idx = req_count;
    int sort_count = 0; /* sorted scan count */
    int k, i, cmp_res;
    int mid, left, right;
    bool ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);
    bool is_first;

    *missed_key_count = 0;

    for (k = 0; k < key_count; k++) {
        ret = do_btree_item_find(key_array[k].value, key_array[k].length, DO_UPDATE, &it);
        if (ret != ENGINE_SUCCESS) {
            if (ret == ENGINE_KEY_ENOENT) { /* key missed */
                missed_key_array[*missed_key_count] = k;
                *missed_key_count += 1;
                ret = ENGINE_SUCCESS; continue;
            }
            break; /* ret == ENGINE_EBADTYPE */
        }

        info = (btree_meta_info *)item_get_meta(it);
        if ((info->mflags & COLL_META_FLAG_READABLE) == 0) { /* unreadable collection */
            missed_key_array[*missed_key_count] = k;
            *missed_key_count += 1;
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
        assert(info->root != NULL);

        elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
        if (elem == NULL) { /* No elements within the bkey range */
            if ((info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
                /* Some elements weren't cached because of overflow trim */
                if (do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                    do_item_release(it);
                    ret = ENGINE_EBKEYOOR; break;
                }
            }
            do_item_release(it);
            continue;
        }

        if (posi.bkeq == false && (info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
            /* Some elements weren't cached because of overflow trim */
            if (do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                do_item_release(it);
                ret = ENGINE_EBKEYOOR; break;
            }
        }

        /* initialize for the next scan */
        is_first = true;
        posi.bkeq = false;

scan_next:
        if (is_first != true) {
            assert(elem != NULL);
            elem = do_btree_scan_next(&posi, bkrtype, bkrange);
            if (elem == NULL) {
                if (posi.node == NULL && (info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
                    if (do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                        do_item_release(it);
                        ret = ENGINE_EBKEYOOR; break;
                    }
                }
                do_item_release(it);
                continue;
            }
        }
        is_first = false;

        if (efilter != NULL && !do_btree_elem_filter(elem, efilter)) {
            goto scan_next;
        }

        /* found the item */
        btree_scan_buf[curr_idx].it   = it;
        btree_scan_buf[curr_idx].posi = posi;
        btree_scan_buf[curr_idx].kidx = k;

        /* add the current scan into the scan sort buffer */
        if (sort_count == 0) {
            sort_sindx_buf[sort_count++] = curr_idx;
            curr_idx += 1;
            continue;
        }

        if (sort_count >= req_count) {
            /* compare with the element of the last scan */
            comp_idx = sort_sindx_buf[sort_count-1];
            comp = BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
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
            comp = BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
                                       btree_scan_buf[comp_idx].posi.indx);
            cmp_res = BKEY_COMP(elem->data, elem->nbkey, comp->data, comp->nbkey);
            if (cmp_res == 0) {
                cmp_res = do_btree_comp_hkey(btree_scan_buf[curr_idx].it,
                                             btree_scan_buf[comp_idx].it);
                if (cmp_res == 0) {
                    ret = ENGINE_EBADVALUE; break;
                }
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

        if (sort_count >= req_count) {
            /* free the last scan */
            comp_idx = sort_sindx_buf[sort_count-1];
            do_item_release(btree_scan_buf[comp_idx].it);
            btree_scan_buf[comp_idx].it = NULL;
            free_idx = comp_idx;
            sort_count--;
        }
        for (i = sort_count-1; i >= left; i--) {
            sort_sindx_buf[i+1] = sort_sindx_buf[i];
        }
        sort_sindx_buf[left] = curr_idx;
        sort_count++;

        if (sort_count < req_count) {
            curr_idx += 1;
        } else {
            curr_idx = free_idx;
        }
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
    btree_elem_posi posi;
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
        assert(info->root != NULL);

        elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
        if (elem == NULL) { /* No elements within the bkey range */
            if ((info->mflags & COLL_META_FLAG_TRIMMED) != 0 &&
                do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                /* Some elements weren't cached because of overflow trim */
                do_btree_smget_add_miss(smres, kidx, ENGINE_EBKEYOOR);
            }
            do_item_release(it); continue;
        }

        if (posi.bkeq == false && (info->mflags & COLL_META_FLAG_TRIMMED) != 0 &&
            do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
            /* Some elements weren't cached because of overflow trim */
            do_btree_smget_add_miss(smres, kidx, ENGINE_EBKEYOOR);
            do_item_release(it); continue;
        }

        /* initialize for the next scan */
        is_first = true;
        posi.bkeq = false;

scan_next:
        if (is_first != true) {
            assert(elem != NULL);
            btree_elem_item *prev = elem;
            elem = do_btree_scan_next(&posi, bkrtype, bkrange);
            if (elem == NULL) {
                if (posi.node == NULL && (info->mflags & COLL_META_FLAG_TRIMMED) != 0 &&
                    do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                    /* Some elements weren't cached because of overflow trim */
                    do_btree_smget_add_trim(smres, kidx, prev);
                }
                do_item_release(it); continue;
            }
        }
        is_first = false;

        if (efilter != NULL && !do_btree_elem_filter(elem, efilter)) {
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
            comp = BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
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
            comp = BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
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
#ifdef JHPARK_OLD_SMGET_INTERFACE
static ENGINE_ERROR_CODE do_btree_smget_elem_sort_old(btree_scan_info *btree_scan_buf,
                                   uint16_t *sort_sindx_buf, const int sort_sindx_cnt,
                                   const int bkrtype, const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count, btree_elem_item **elem_array,
                                   uint32_t *kfnd_array, uint32_t *flag_array, uint32_t *elem_count,
                                   bool *potentialbkeytrim, bool *bkey_duplicated)
{
    btree_meta_info *info;
    btree_elem_item *elem, *comp;
    btree_elem_item *prev = NULL;
    uint16_t first_idx = 0;
    uint16_t curr_idx;
    uint16_t comp_idx;
    int i, cmp_res;
    int mid, left, right;
    int skip_count = 0;
    int sort_count = sort_sindx_cnt;
    bool ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);
    bool key_trim_found = false;
    bool dup_bkey_found;
    *elem_count = 0;

    while (sort_count > 0) {
        curr_idx = sort_sindx_buf[first_idx];
        elem = BTREE_GET_ELEM_ITEM(btree_scan_buf[curr_idx].posi.node,
                                   btree_scan_buf[curr_idx].posi.indx);
        dup_bkey_found = false;
        if (prev != NULL) { /* check duplicate bkeys */
            if (BKEY_COMP(prev->data, prev->nbkey, elem->data, elem->nbkey) == 0) {
                dup_bkey_found = true;
            }
        }
        prev = elem;

        if (key_trim_found && !dup_bkey_found) {
            break; /* stop smget */
        }

        if (skip_count < offset) {
            skip_count++;
        } else { /* skip_count == offset */
            if (*elem_count > 0 && dup_bkey_found) {
                *bkey_duplicated = true;
            }
            elem->refcount++;
            elem_array[*elem_count] = elem;
            kfnd_array[*elem_count] = btree_scan_buf[curr_idx].kidx;
            flag_array[*elem_count] = btree_scan_buf[curr_idx].it->flags;
            *elem_count += 1;
            if (*elem_count >= count) break;
        }

scan_next:
        elem = do_btree_scan_next(&btree_scan_buf[curr_idx].posi, bkrtype, bkrange);
        if (elem == NULL) {
            if (btree_scan_buf[curr_idx].posi.node == NULL) {
                /* reached to the end of b+tree scan */
                info = (btree_meta_info *)item_get_meta(btree_scan_buf[curr_idx].it);
                if ((info->mflags & COLL_META_FLAG_TRIMMED) != 0 &&
                    do_btree_overlapped_with_trimmed_space(info, &btree_scan_buf[curr_idx].posi, bkrtype)) {
                    key_trim_found = true;
                }
            }
            first_idx++; sort_count--;
            continue;
        }

        if (efilter != NULL && !do_btree_elem_filter(elem, efilter)) {
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
            comp = BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
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

        assert(left > right);
        /* right : insertion position */
        for (i = first_idx+1; i <= right; i++) {
            sort_sindx_buf[i-1] = sort_sindx_buf[i];
        }
        sort_sindx_buf[right] = curr_idx;
    }

    if (key_trim_found && *elem_count < count) {
        *potentialbkeytrim = true;
    }
    return ENGINE_SUCCESS;
}
#endif

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
    int i, cmp_res;
    int mid, left, right;
    int skip_count = 0;
    int sort_count = sort_sindx_cnt;
    bool ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);
    bool dup_bkey_found;

    while (sort_count > 0) {
        curr_idx = sort_sindx_buf[first_idx];
        elem = BTREE_GET_ELEM_ITEM(btree_scan_buf[curr_idx].posi.node,
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
        last = elem;
        elem = do_btree_scan_next(&btree_scan_buf[curr_idx].posi, bkrtype, bkrange);
        if (elem == NULL) {
            if (btree_scan_buf[curr_idx].posi.node == NULL) {
                /* reached to the end of b+tree scan */
                info = (btree_meta_info *)item_get_meta(btree_scan_buf[curr_idx].it);
                if ((info->mflags & COLL_META_FLAG_TRIMMED) != 0 &&
                    do_btree_overlapped_with_trimmed_space(info, &btree_scan_buf[curr_idx].posi, bkrtype)) {
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
            first_idx++; sort_count--;
            continue;
        }

        if (efilter != NULL && !do_btree_elem_filter(elem, efilter)) {
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
            comp = BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
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
#endif

#ifdef REORGANIZE_ITEM_COLL // BTREE
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
        it = do_btree_item_alloc(key, nkey, attrp, cookie);
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

btree_elem_item *btree_elem_alloc(const uint32_t nbkey, const uint32_t neflag, const uint32_t nbytes,
                                  const void *cookie)
{
    btree_elem_item *elem;
    LOCK_CACHE();
    elem = do_btree_elem_alloc(nbkey, neflag, nbytes, cookie);
    UNLOCK_CACHE();
    return elem;
}

void btree_elem_free(btree_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->status == BTREE_ITEM_STATUS_UNLINK);
    elem->status = BTREE_ITEM_STATUS_FREE;
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
    if (trimmed_elems != NULL) {
        /* initialize as no trimmed element */
        *trimmed_elems = NULL;
        *trimmed_count = 0;
    }

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_btree_item_alloc(key, nkey, attrp, cookie);
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
        ret = do_btree_elem_insert(it, elem, replace_if_exist, replaced,
                                   trimmed_elems, trimmed_count, cookie);
        if (trimmed_elems != NULL && *trimmed_elems != NULL) {
            *trimmed_flags = it->flags;
        }
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
            ret = do_btree_elem_update(info, bkrtype, bkrange, eupdate, value, nbytes, cookie);
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
                    assert(info->root == NULL);
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
        bool new_root_flag = false;
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt > 0 || info->maxbkeyrange.len != BKEY_NULL) {
                if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                    (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                    ret = ENGINE_EBADBKEY; break;
                }
            }
            if (info->root == NULL && create == true) {
                /* create new root node */
                btree_indx_node *r_node = do_btree_node_alloc(0, cookie);
                if (r_node == NULL) {
                    ret = ENGINE_ENOMEM; break;
                }
                do_btree_node_link(info, r_node, NULL);
                new_root_flag = true;
            }
            ret = do_btree_elem_arithmetic(info, bkrtype, bkrange, increment, create,
                                           delta, initial, eflagp, result, cookie);
            if (ret != ENGINE_SUCCESS) {
                if (new_root_flag) {
                    /* unlink the root node and free it */
                    do_btree_node_unlink(info, info->root, NULL);
                }
            }
        } while(0);
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

    eresult->elem_array = NULL;
    eresult->elem_count = 0;

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
                        assert(info->root == NULL);
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
            *elem_count = do_btree_elem_count(info, bkrtype, bkrange, efilter, opcost);
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
            *position = do_btree_posi_find(info, bkrtype, bkrange, order);
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

    eresult->elem_array = NULL;
    eresult->elem_count = 0;

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
            *position = do_btree_posi_find_with_get(info, bkrtype, bkrange, order, count,
                                                    (btree_elem_item**)(eresult->elem_array),
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

    eresult->elem_array = NULL;
    eresult->elem_count = 0;

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
            ret = do_btree_elem_get_by_posi(info, from_posi, rqcount, forward,
                                            (btree_elem_item**)(eresult->elem_array),
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
#ifdef JHPARK_OLD_SMGET_INTERFACE
ENGINE_ERROR_CODE btree_elem_smget_old(token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   btree_elem_item **elem_array, uint32_t *kfnd_array,
                                   uint32_t *flag_array, uint32_t *elem_count,
                                   uint32_t *missed_key_array, uint32_t *missed_key_count,
                                   bool *trimmed, bool *duplicated)
{
    btree_scan_info btree_scan_buf[offset+count+1]; /* one more scan needed */
    uint16_t        sort_sindx_buf[offset+count];   /* sorted scan index buffer */
    uint32_t        sort_sindx_cnt, i;
    int             bkrtype = do_btree_bkey_range_type(bkrange);
    ENGINE_ERROR_CODE ret;

    /* prepare */
    for (i = 0; i <= (offset+count); i++) {
        btree_scan_buf[i].it = NULL;
    }

    *trimmed = false;
    *duplicated = false;

    LOCK_CACHE();
    /* the 1st phase: get the sorted scans */
    ret = do_btree_smget_scan_sort_old(key_array, key_count,
                                   bkrtype, bkrange, efilter, (offset+count),
                                   btree_scan_buf, sort_sindx_buf, &sort_sindx_cnt,
                                   missed_key_array, missed_key_count);
    if (ret == ENGINE_SUCCESS) {
        /* the 2nd phase: get the sorted elems */
        ret = do_btree_smget_elem_sort_old(btree_scan_buf, sort_sindx_buf, sort_sindx_cnt,
                                           bkrtype, bkrange, efilter, offset, count,
                                           elem_array, kfnd_array, flag_array, elem_count,
                                           trimmed, duplicated);
        for (i = 0; i <= (offset+count); i++) {
            if (btree_scan_buf[i].it != NULL)
                do_item_release(btree_scan_buf[i].it);
        }
    }
    UNLOCK_CACHE();

    return ret;
}
#endif

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

    /* initialize smget result structure */
    assert(result->elem_array != NULL);
    result->trim_elems = (eitem *)&result->elem_array[count];
    result->elem_kinfo = (smget_ehit_t *)&result->elem_array[count + key_count];
    result->miss_kinfo = (smget_emis_t *)&result->elem_kinfo[count];
    result->trim_kinfo = result->miss_kinfo;
    result->elem_count = 0;
    result->miss_count = 0;
    result->trim_count = 0;
    result->elem_arrsz = count;
    result->keys_arrsz = key_count;
    result->duplicated = false;
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
    btree_elem_posi  posi;

    elem = do_btree_find_first(info->root, BKEY_RANGE_TYPE_ASC, NULL, &posi, false);
    while (elem != NULL) {
        elem->refcount++;
        eresult->elem_array[eresult->elem_count++] = elem;
        /* Never have to go backward?  FIXME */
        elem = do_btree_find_next(&posi, NULL);
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
        btree_elem_item *min_bkey_elem = do_btree_get_first_elem(info->root);
        btree_elem_item *max_bkey_elem = do_btree_get_last_elem(info->root);
        do_btree_get_bkey(min_bkey_elem, &attrp->minbkey);
        do_btree_get_bkey(max_bkey_elem, &attrp->maxbkey);
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE btree_coll_setattr(hash_item *it, item_attr *attrp,
                                     ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    btree_meta_info *info = (btree_meta_info *)item_get_meta(it);

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
                    btree_elem_item *min_bkey_elem = do_btree_get_first_elem(info->root);
                    btree_elem_item *max_bkey_elem = do_btree_get_last_elem(info->root);
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
#endif

#ifdef REORGANIZE_ITEM_COLL // BTREE
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
#endif
