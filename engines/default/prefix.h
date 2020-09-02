/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015-current JaM2in Co., Ltd.
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
#ifndef PREFIX_H
#define PREFIX_H

#include <memcached/types.h>

typedef struct _prefix_t {
    uint8_t  nprefix;  /* the length of prefix name */
    uint8_t  internal; /* is internal prefix ? 1 or 0 */
    uint16_t dummy16;

    rel_time_t oldest_live;
    time_t     create_time;

    struct _prefix_t *h_next;  /* prefix hash chain */
    struct _prefix_t *parent_prefix;

    /* lower prefix count */
    uint32_t prefix_items;

    /* the count and bytes of cache items per item type */
    uint64_t total_count_exclusive;
    uint64_t total_bytes_exclusive;
#ifdef NESTED_PREFIX
    uint64_t total_count_inclusive; /* NOT yet used */
    uint64_t total_bytes_inclusive; /* NOT yet used */

    uint64_t items_count_inclusive[ITEM_TYPE_MAX];
    uint64_t items_bytes_inclusive[ITEM_TYPE_MAX];

    uint64_t items_count_exclusive[ITEM_TYPE_MAX];
    uint64_t items_bytes_exclusive[ITEM_TYPE_MAX];
#else
    uint64_t items_count[ITEM_TYPE_MAX];
    uint64_t items_bytes[ITEM_TYPE_MAX];
#endif
} prefix_t;

#define PREFIX_IS_RSVD(pfx,npfx) ((npfx) == 5 && strncmp((pfx), "arcus", 5) == 0)
#define PREFIX_IS_USER(pfx,npfx) ((npfx) != 5 || strncmp((pfx), "arcus", 5) != 0)

struct prefix {
    prefix_t**  hashtable;
    prefix_t    null_prefix_data;

    /* Number of items in the hash table. */
    unsigned int tot_prefix_items;
};

/* prefix functions */
ENGINE_ERROR_CODE prefix_init(struct default_engine *engine);
void              prefix_final(struct default_engine *engine);

prefix_t *        prefix_find(const char *prefix, const int nprefix);
ENGINE_ERROR_CODE prefix_link(hash_item *it, const uint32_t item_size, bool *internal);
void              prefix_unlink(hash_item *it, const uint32_t item_size, bool drop_if_empty);
bool              prefix_isincluded(prefix_t *pt, const char *prefix, const int nprefix);
bool              prefix_issame(prefix_t *pt, const char *prefix, const int nprefix);
void              prefix_bytes_incr(prefix_t *pt, ENGINE_ITEM_TYPE item_type, const uint32_t bytes);
void              prefix_bytes_decr(prefix_t *pt, ENGINE_ITEM_TYPE item_type, const uint32_t bytes);
bool              prefix_isvalid(hash_item *it, rel_time_t current_time);
uint32_t          prefix_count(void);
ENGINE_ERROR_CODE prefix_get_stats(const char *prefix, const int nprefix, void *prefix_data);

#endif
