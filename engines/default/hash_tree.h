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
#ifndef HASH_TREE_H
#define HASH_TREE_H

#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <sys/types.h>
#include <memcached/engine.h>

#define HTREE_HASHTAB_SIZE 16
#define HTREE_HASHIDX_MASK 0x0000000F
#define HTREE_MAX_HASHCHAIN_SIZE 64

#define HTREE_GET_HASHIDX(hval, hdepth) \
        (((hval) & (HTREE_HASHIDX_MASK << ((hdepth)*4))) >> ((hdepth)*4))

static inline int htree_hash_eq(const int h1, const void *k1, size_t nkey1,
                                const int h2, const void *k2, size_t nkey2)
{
    return (h1 == h2 && nkey1 == nkey2 && memcmp(k1, k2, nkey1) == 0);
}

extern int genhash_string_hash(const void* p, size_t nkey);

/* Layout contract for hash-tree elements.
 * Any elem_item used with hash_tree functions must begin with these
 * fields in this exact order. Fields beyond this point may differ. */
typedef struct _htree_elem_item {
    uint16_t refcount;
    uint8_t  slabs_clsid;          /* which slab class we're in */
    uint8_t  linked;               /* link count */
    uint8_t  reserved[4];          /* available for use by the caller's own header fields */
    struct _htree_elem_item *next; /* hash chain next */
    uint32_t hval;                 /* hash value */
} htree_elem_item;

typedef struct _htree_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;
    uint8_t  hdepth;
    uint32_t tot_elem_cnt;
    int16_t  hcnt[HTREE_HASHTAB_SIZE];
    void    *htab[HTREE_HASHTAB_SIZE];
} htree_node;

/* ops: collection-specific key extraction */
typedef struct {
    const void *(*get_key)(const htree_elem_item *elem, uint16_t *nkey);
} htree_ops;

typedef struct _htree_meta {
    htree_node *root;
    htree_ops  *ops;
} htree_meta;

/* Position context for hash-tree mutation operations (add, replace).
 * Filled by htree_elem_find. */
typedef struct {
    htree_node      *node;
    htree_elem_item *prev;
    int              hidx;
} htree_elem_pos;

void htree_init(htree_meta *htree, htree_ops *ops);

htree_elem_item *htree_elem_find(htree_meta *htree,
                                 const int hval,
                                 const void *key, uint16_t nkey,
                                 htree_elem_pos *pos);

ENGINE_ERROR_CODE htree_elem_insert(htree_meta *htree, htree_elem_item *elem,
                                    size_t *space_increased);

ENGINE_ERROR_CODE htree_elem_add(htree_meta *htree, htree_elem_item *elem,
                                 htree_elem_pos *pos, size_t *space_increased);

htree_elem_item *htree_elem_replace(htree_elem_pos *pos, htree_elem_item *new_elem);

htree_elem_item *htree_elem_delete(htree_meta *htree, htree_node *node,
                                   const int hval, const char *key, const int nkey,
                                   size_t *space_decreased);

uint32_t htree_elem_delete_bulk(htree_meta *htree, htree_node *node, const uint32_t count,
                                htree_elem_item **deleted_head,
                                size_t *space_decreased);

uint32_t htree_elem_get_bulk(htree_meta *htree, htree_node *node, const uint32_t count,
                             const bool delete, htree_elem_item **elem_array,
                             size_t *space_decreased);

uint32_t htree_elem_get_rand(htree_meta *htree, const uint32_t ccnt, const uint32_t count,
                             const bool delete, htree_elem_item **elem_array,
                             size_t *space_decreased);

#endif
