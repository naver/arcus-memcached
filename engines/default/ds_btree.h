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
#ifndef DS_BTREE_H
#define DS_BTREE_H

#include "memcached/types.h"
#include <assert.h>
#include <string.h>

#define DS_BTREE_MAX_DEPTH  7
#define DS_BTREE_ITEM_COUNT 32 /* Recommend DS_BTREE_ITEM_COUNT >= 8 */

#define DS_BTREE_OUTSIDE_LEFT  1
#define DS_BTREE_OUTSIDE_RIGHT 2

/* btree element item or btree node item */
#define DS_BTREE_GET_ELEM_ITEM(node, indx) ((ds_btree_elem_item *)((node)->item[indx]))
#define DS_BTREE_GET_NODE_ITEM(node, indx) ((ds_btree_indx_node *)((node)->item[indx]))

typedef struct _ds_btree_elem_item {
    uint16_t refcount;
    uint8_t  slabs_clsid;        /* which slab class we're in */
    uint8_t  linked;             /* link count */
} ds_btree_elem_item;

typedef struct _ds_btree_leaf_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;      /* which slab class we're in */
    uint8_t  ndepth;
    uint16_t used_count;
    uint16_t reserved;
    struct _ds_btree_indx_node *prev;
    struct _ds_btree_indx_node *next;
    void    *item[DS_BTREE_ITEM_COUNT];
} ds_btree_leaf_node;

typedef struct _ds_btree_indx_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;      /* which slab class we're in */
    uint8_t  ndepth;
    uint16_t used_count;
    uint16_t reserved;
    struct _ds_btree_indx_node *prev;
    struct _ds_btree_indx_node *next;
    void    *item[DS_BTREE_ITEM_COUNT];
    uint32_t ecnt[DS_BTREE_ITEM_COUNT];
} ds_btree_indx_node;

typedef struct _ds_btree_elem_posi {
    ds_btree_indx_node *node;
    uint16_t            indx;
    /* It is used temporarily in order to check
     * if the found bkey is equal to from_bkey or to_bkey of given bkey range
     * in the ds_btree_find_first/next/prev functions.
     */
    bool                bkeq;
} ds_btree_elem_posi;

typedef struct {
    const void *(*get_bkey)(const ds_btree_elem_item *elem, uint32_t *nbkey);
    const void *(*get_eflag)(const ds_btree_elem_item *elem, uint32_t *neflag);
    int (*bkey_cmp)(const void *bkey1, uint32_t nbkey1, const void *bkey2, uint32_t nbkey2);
    int (*tiebreak)(const ds_btree_elem_item *e1, const ds_btree_elem_item *e2);
    void (*delete_post)(ds_btree_elem_item *elem, void *arg);
} ds_btree_ops;

typedef struct _ds_btree_meta {
    ds_btree_indx_node *root;
    ds_btree_ops       *ops;
    uint32_t            tot_elem_cnt;
} ds_btree_meta;

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

extern bool (*BINARY_COMPARE_OP[COMPARE_OP_MAX])(const unsigned char *v1, const int nv1,
                                                 const unsigned char *v2, const int nv2);

extern void (*BINARY_BITWISE_OP[BITWISE_OP_MAX])(const unsigned char *v1, const unsigned char *v2,
                                                 const int length, unsigned char *result);

static inline bool ds_btree_elem_filter(ds_btree_meta *btree,
                                        const ds_btree_elem_item *elem,
                                        const eflag_filter *efilter)
{
    assert(efilter != NULL);
    uint32_t neflag;
    unsigned char *operand = (unsigned char *)btree->ops->get_eflag(elem, &neflag) + efilter->offset;

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

void ds_btree_init(ds_btree_meta *btree, ds_btree_ops *ops);

void ds_btree_node_free(ds_btree_indx_node *node);

ds_btree_elem_item *ds_btree_get_first_elem(ds_btree_indx_node *node);

ds_btree_elem_item *ds_btree_get_last_elem(ds_btree_indx_node *node);

ds_btree_indx_node *ds_btree_get_first_leaf(ds_btree_indx_node *node,
                                            ds_btree_elem_posi *path);

ds_btree_indx_node *ds_btree_get_last_leaf(ds_btree_indx_node *node,
                                           ds_btree_elem_posi *path);

ds_btree_elem_item *ds_btree_elem_find(ds_btree_meta *btree,
                                       const void *bkey, uint32_t nbkey,
                                       ds_btree_elem_posi *path);

ds_btree_elem_item *ds_btree_find_first(ds_btree_meta *btree,
                                        const int bkrtype, const bkey_range *bkrange,
                                        ds_btree_elem_posi *path, const bool path_flag);

ds_btree_elem_item *ds_btree_find_prev(ds_btree_meta *btree,
                                       ds_btree_elem_posi *posi,
                                       const bkey_range *bkrange);

ds_btree_elem_item *ds_btree_find_next(ds_btree_meta *btree,
                                       ds_btree_elem_posi *posi,
                                       const bkey_range *bkrange);

ds_btree_elem_item *ds_btree_elem_replace(ds_btree_elem_posi *posi, ds_btree_elem_item *new_elem);

ds_btree_elem_item *ds_btree_elem_delete(ds_btree_meta *btree,
                                         const int bkrtype, const bkey_range *bkrange,
                                         const eflag_filter *efilter,
                                         void *delete_arg,
                                         uint32_t *opcost, size_t *space_decreased);

uint32_t ds_btree_elem_delete_bulk(ds_btree_meta *btree,
                                   const int bkrtype, const bkey_range *bkrange,
                                   const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   void *delete_arg,
                                   uint32_t *opcost, size_t *space_decreased);

ds_btree_elem_item *ds_btree_delete_first_elem(ds_btree_meta *btree,
                                               size_t *space_decreased);

ds_btree_elem_item *ds_btree_delete_last_elem(ds_btree_meta *btree,
                                              size_t *space_decreased);

ENGINE_ERROR_CODE ds_btree_elem_add(ds_btree_meta *btree,
                                    ds_btree_elem_posi *path, ds_btree_elem_item *elem,
                                    size_t *space_increased);

uint32_t ds_btree_posi_outside(const ds_btree_elem_posi *posi, const int bkrtype);

bool ds_btree_elem_get(ds_btree_meta *btree,
                       const int bkrtype, const bkey_range *bkrange,
                       const eflag_filter *efilter,
                       const bool delete, void *delete_arg,
                       ds_btree_elem_item **elem_array,
                       uint32_t *opcost, uint32_t *outside, size_t *space_decreased);

uint32_t ds_btree_elem_get_bulk(ds_btree_meta *btree,
                                const int bkrtype, const bkey_range *bkrange,
                                const eflag_filter *efilter,
                                const uint32_t offset, const uint32_t count,
                                const bool delete, void *delete_arg,
                                ds_btree_elem_item **elem_array,
                                uint32_t *opcost, uint32_t *outside, size_t *space_decreased);

uint32_t ds_btree_elem_count(ds_btree_meta *btree,
                             const int bkrtype, const bkey_range *bkrange,
                             const eflag_filter *efilter, uint32_t *opcost);


int ds_btree_posi_find(ds_btree_meta *btree,
                       const int bkrtype, const bkey_range *bkrange,
                       ENGINE_BTREE_ORDER order);

int ds_btree_posi_find_with_get(ds_btree_meta *btree,
                                const int bkrtype, const bkey_range *bkrange,
                                ENGINE_BTREE_ORDER order, const int count,
                                ds_btree_elem_item **elem_array,
                                uint32_t *elem_count, uint32_t *elem_index);

ENGINE_ERROR_CODE ds_btree_elem_get_by_posi(ds_btree_meta *btree,
                                            const int index, const uint32_t count, const bool forward,
                                            ds_btree_elem_item **elem_array, uint32_t *elem_count);

void ds_btree_traverse_init(ds_btree_meta *btree, void *posi);
uint32_t ds_btree_traverse_next(void *posi, void **elem_array, uint32_t count);

#endif
