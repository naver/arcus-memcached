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

void ds_btree_init(ds_btree_meta *btree, ds_btree_ops *ops);

#endif
