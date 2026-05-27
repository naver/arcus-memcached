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
#include <sys/types.h>
#include <memcached/engine.h>

#define HTREE_HASHTAB_SIZE 16

/* Layout contract for hash-tree elements.
 * Any elem_item used with hash_tree functions must begin with these
 * fields in this exact order. Fields beyond this point may differ. */
typedef struct _htree_elem_item {
    uint8_t reserved[8];           /* available for use by the caller's own header fields */
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

/* Position context for hash-tree mutation operations (replace, delete).
 * Filled by htree_elem_find when the caller needs to mutate the found element. */
typedef struct {
    htree_node *node;
    int hidx;
    htree_elem_item *prev;
} htree_elem_pos;

/* Returns the found element, or NULL if not found.
 * pos is filled only when non-NULL and the element is found. */
htree_elem_item *htree_elem_find(htree_node *root,
                                 const void *key, uint16_t nkey,
                                 htree_ops *ops,
                                 htree_elem_pos *pos);

void htree_elem_replace_at(htree_elem_pos *pos, htree_elem_item *new_elem);

ENGINE_ERROR_CODE htree_elem_link(htree_node **root_pptr,
                                  htree_elem_item *elem,
                                  htree_ops *ops,
                                  ssize_t *htree_space_delta,
                                  const void *cookie);

/* unlinks the element matching the given key and returns it.
 * returns NULL if not found. caller is responsible for CLOG and free. */
htree_elem_item *htree_elem_unlink(htree_node **root_pptr,
                                   const void *key, uint16_t nkey,
                                   htree_ops *ops,
                                   ssize_t *htree_space_delta);

/* unlinks the element matching the given key and returns it.
 * returns NULL if not found. caller is responsible for CLOG and free. */
htree_elem_item *htree_elem_unlink_at_offset(htree_node **root_pptr,
                                             uint32_t offset,
                                             ssize_t *htree_space_delta);

htree_elem_item *htree_elem_unlink_by_cnt(htree_node **root_pptr,
                                          uint32_t count,
                                          ssize_t *htree_space_delta);

uint32_t htree_elem_get_by_cnt(htree_node **root_pptr,
                               uint32_t count,
                               htree_elem_item **elem_array,
                               bool unlink,
                               ssize_t *htree_space_delta);

int htree_elem_get_rand(htree_node *node,
                        uint32_t total_count, uint32_t count,
                        htree_elem_item **elem_array);

#endif
