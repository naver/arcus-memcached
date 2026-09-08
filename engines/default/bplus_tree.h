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
#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include "memcached/types.h"
#include "typed_ops.h"
#include <assert.h>
#include <string.h>

/* Layout contract for bplus tree elements.
 * Any elem_item used with bplus_tree functions must begin with these
 * fields in this exact order. Fields beyond this point may differ. */
typedef struct _bplus_elem_item {
    uint16_t refcount;
    uint8_t  slabs_clsid;        /* which slab class we're in */
    uint8_t  linked;             /* link count */
} bplus_elem_item;

#define BPLUS_MAX_DEPTH  7
#define BPLUS_ITEM_COUNT 32 /* Recommend BPLUS_ITEM_COUNT >= 8 */

typedef struct _bplus_leaf_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;      /* which slab class we're in */
    uint8_t  ndepth;
    uint16_t used_count;
    uint16_t reserved;
    struct _bplus_indx_node *prev;
    struct _bplus_indx_node *next;
    void    *item[BPLUS_ITEM_COUNT];
} bplus_leaf_node;

typedef struct _bplus_indx_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;      /* which slab class we're in */
    uint8_t  ndepth;
    uint16_t used_count;
    uint16_t reserved;
    struct _bplus_indx_node *prev;
    struct _bplus_indx_node *next;
    void    *item[BPLUS_ITEM_COUNT];
    uint32_t ecnt[BPLUS_ITEM_COUNT];
} bplus_indx_node;

typedef struct {
    const void *(*get_bkey)(const bplus_elem_item *elem, uint32_t *nbkey);
    const void *(*get_eflag)(const bplus_elem_item *elem, uint32_t *neflag);
    int (*tiebreak)(const bplus_elem_item *e1, const bplus_elem_item *e2);
    void (*delete_post)(bplus_elem_item *elem, void *arg);
} bplus_ops;

typedef struct _bplus_meta {
    bplus_indx_node *root;
    bplus_ops       *ops;
    uint32_t         tot_elem_cnt;
} bplus_meta;

/* bplus element position */
typedef struct _bplus_elem_posi {
    bplus_indx_node *node;
    uint16_t         indx;
    /* It is used temporarily in order to check
     * if the found bkey is equal to from_bkey or to_bkey of given bkey range
     * in the bplus_find_first/next/prev functions.
     */
    bool             bkeq;
} bplus_elem_posi;

/* bkey type */
#define BKEY_TYPE_UNKNOWN 0
#define BKEY_TYPE_UINT64  1
#define BKEY_TYPE_BINARY  2

/* bkey range type */
#define BKEY_RANGE_TYPE_SIN 1 /* single bkey */
#define BKEY_RANGE_TYPE_ASC 2 /* ascending bkey range */
#define BKEY_RANGE_TYPE_DSC 3 /* descending bkey range */

/* bplus scan direction */
#define BPLUS_DIRECTION_PREV 2
#define BPLUS_DIRECTION_NEXT 1
#define BPLUS_DIRECTION_NONE 0

#define BPLUS_OUTSIDE_LEFT  1
#define BPLUS_OUTSIDE_RIGHT 2

/* bplus element item or bplus node item */
#define BPLUS_GET_ELEM_ITEM(node, indx) ((bplus_elem_item *)((node)->item[indx]))
#define BPLUS_GET_NODE_ITEM(node, indx) ((bplus_indx_node *)((node)->item[indx]))

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

#define BKEY_COPY(bk, nbk, res) \
        ((nbk)==0 ? UINT64_COPY((const uint64_t*)(bk), (uint64_t*)(res)) \
                  : BINARY_COPY((bk), (nbk), (res)))

#define BKEY_DIFF(bk1, nbk1, bk2, nbk2, len, res) \
        ((len)==0 ? UINT64_DIFF((const uint64_t*)(bk1), (const uint64_t*)(bk2), (uint64_t*)(res)) \
                  : BINARY_DIFF((bk1), (nbk1), (bk2), (nbk2), (len), (res)))

#define BKEY_INCR(bk, nbk) \
        ((nbk)==0 ? UINT64_INCR((uint64_t*)(bk)) : BINARY_INCR((bk), (nbk)))

#define BKEY_DECR(bk, nbk) \
        ((nbk)==0 ? UINT64_DECR((uint64_t*)(bk)) : BINARY_DECR((bk), (nbk)))

static inline bplus_elem_item *bplus_get_first_elem(bplus_indx_node *node)
{
    while (node->ndepth > 0) {
        node = (bplus_indx_node *)(node->item[0]);
    }
    assert(node->ndepth == 0);
    return (bplus_elem_item *)(node->item[0]);
}

static inline bplus_elem_item *bplus_get_last_elem(bplus_indx_node *node)
{
    while (node->ndepth > 0) {
        node = (bplus_indx_node *)(node->item[node->used_count-1]);
    }
    assert(node->ndepth == 0);
    return (bplus_elem_item *)(node->item[node->used_count-1]);
}

static inline bplus_indx_node *bplus_get_first_leaf(bplus_indx_node *node,
                                                    bplus_elem_posi *path)
{
    while (node->ndepth > 0) {
        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = 0;
        }
        node = (bplus_indx_node *)(node->item[0]);
    }
    assert(node->ndepth == 0);
    return node;
}

static inline bplus_indx_node *bplus_get_last_leaf(bplus_indx_node *node,
                                                   bplus_elem_posi *path)
{
    while (node->ndepth > 0) {
        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = node->used_count-1;
        }
        node = (bplus_indx_node *)(node->item[node->used_count-1]);
    }
    assert(node->ndepth == 0);
    return node;
}

static inline bool bplus_elem_filter(bplus_meta *bplus, bplus_elem_item *elem, const eflag_filter *efilter)
{
    assert(efilter != NULL);
    uint32_t neflag;
    unsigned char *operand = (unsigned char *)bplus->ops->get_eflag(elem, &neflag) + efilter->offset;

    if (efilter->offset >= neflag || efilter->ncompval > (neflag - efilter->offset)) {
        return (efilter->compop == COMPARE_OP_NE ? true : false);
    }

    unsigned char result[MAX_EFLAG_LENG];

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

void bplus_init(bplus_meta *bplus, bplus_ops *ops);

bplus_elem_item *bplus_elem_find(bplus_meta *bplus,
                                 const void *bkey, uint32_t nbkey,
                                 bplus_elem_posi *path);
bplus_elem_item *bplus_find_first(bplus_meta *bplus,
                                  const int bkrtype, const bkey_range *bkrange,
                                  bplus_elem_posi *path, const bool path_flag);
bplus_elem_item *bplus_find_next(bplus_meta *bplus,
                                 bplus_elem_posi *posi, const bkey_range *bkrange);
bplus_elem_item *bplus_find_prev(bplus_meta *bplus,
                                 bplus_elem_posi *posi, const bkey_range *bkrange);

ENGINE_ERROR_CODE bplus_elem_add(bplus_meta *bplus,
                                 bplus_elem_posi *path, bplus_elem_item *elem,
                                 size_t *space_increased);
bplus_elem_item *bplus_elem_replace(bplus_elem_posi *posi, bplus_elem_item *new_elem);

bplus_elem_item *bplus_elem_delete(bplus_meta *bplus,
                                   const int bkrtype, const bkey_range *bkrange,
                                   const eflag_filter *efilter,
                                   void *delete_arg,
                                   uint32_t *opcost, size_t *space_decreased);
uint32_t bplus_elem_delete_bulk(bplus_meta *bplus,
                                const int bkrtype, const bkey_range *bkrange,
                                const eflag_filter *efilter,
                                const uint32_t offset, const uint32_t count,
                                void *delete_arg,
                                uint32_t *opcost, size_t *space_decreased);
bplus_elem_item *bplus_delete_first_elem(bplus_meta *bplus, size_t *space_decreased);
bplus_elem_item *bplus_delete_last_elem(bplus_meta *bplus, size_t *space_decreased);

uint32_t bplus_posi_outside(const bplus_elem_posi *posi, const int bkrtype);
bool bplus_elem_get(bplus_meta *bplus,
                    const int bkrtype, const bkey_range *bkrange,
                    const eflag_filter *efilter,
                    const bool delete, void *delete_arg,
                    bplus_elem_item **elem_array,
                    uint32_t *opcost, uint32_t *outside, size_t *space_decreased);
uint32_t bplus_elem_get_bulk(bplus_meta *bplus,
                             const int bkrtype, const bkey_range *bkrange,
                             const eflag_filter *efilter,
                             const uint32_t offset, const uint32_t count,
                             const bool delete, void *delete_arg,
                             bplus_elem_item **elem_array,
                             uint32_t *opcost, uint32_t *outside, size_t *space_decreased);

int bplus_posi_find(bplus_meta *bplus,
                    const int bkrtype, const bkey_range *bkrange,
                    ENGINE_BTREE_ORDER order);
int bplus_posi_find_with_get(bplus_meta *bplus,
                             const int bkrtype, const bkey_range *bkrange,
                             ENGINE_BTREE_ORDER order, const int count,
                             bplus_elem_item **elem_array,
                             uint32_t *elem_count, uint32_t *elem_index);
ENGINE_ERROR_CODE bplus_elem_get_by_posi(bplus_meta *bplus,
                                         const int index, const uint32_t count, const bool forward,
                                         bplus_elem_item **elem_array, uint32_t *elem_count);

uint32_t bplus_elem_count(bplus_meta *bplus,
                          const int bkrtype, const bkey_range *bkrange,
                          const eflag_filter *efilter, uint32_t *opcost);

#endif
