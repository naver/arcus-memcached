/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015-2016 JaM2in Co., Ltd.
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
#ifndef ASSOC_H
#define ASSOC_H

#define SLOW_HASH_EXPANSION 1

struct bucket_info {
    uint16_t refcount; /* reference count */
#ifdef SLOW_HASH_EXPANSION
#else
    uint16_t curpower; /* current bucket power: how many hash tables exist in bucket ? */
#endif
};

struct assoc {
    uint32_t hashpower; /* how many hash buckets in a hash table ? (power of 2) */
    uint32_t hashsize;  /* hash table size (constant value) */
    uint32_t hashmask;  /* hash bucket mask (constant value) */
    uint32_t rootpower; /* how many hash tables in a root table ? (power of 2) */
    uint32_t rootsize;  /* hash table count (variable value) */
    uint32_t rootmask;  /* hash table mask (variable value) */
#ifdef SLOW_HASH_EXPANSION
    uint32_t prevsize;  /* previous rootsize */
    uint32_t prevmask;  /* previous rootmask */
#endif
    uint32_t roottabsz; /* root table size */
#ifdef SLOW_HASH_EXPANSION

    /* hash table expansion status */
    bool expanding;
    uint32_t exp_bucket;
    uint32_t exp_tabidx;
#else
    uint32_t redistributed_bucket_cnt;
#endif

    /* cache item hash table : an array of hash tables */
    struct table {
       hash_item** hashtable;
    } *roottable;

    /* bucket info table */
    struct bucket_info *infotable;

    uint64_t hash_items; /* number of items in all hash tables */
    uint64_t hash_expansion_limit;
};

/* assoc scan structure */
struct assoc_scan {
    struct default_engine *engine;
    int        hashsz;    /* hash table size */
    int        bucket;    /* current bucket index */
    int        tabcnt;    /* table count in the bucket */
    int        tabidx;    /* table index in the bucket */
    hash_item  ph_item;   /* placeholder item itself */
    bool       ph_linked; /* placeholder item linked */
    bool       initialized;
};

/* associative array */
ENGINE_ERROR_CODE assoc_init(struct default_engine *engine);
void              assoc_final(struct default_engine *engine);

hash_item *       assoc_find(const char *key, const uint32_t nkey, uint32_t hash);
int               assoc_insert(hash_item *item, uint32_t hash);
void              assoc_replace(hash_item *old_it, hash_item *new_it);
void              assoc_delete(const char *key, const uint32_t nkey, uint32_t hash);

/* assoc scan functions */
void              assoc_scan_init(struct assoc_scan *scan);
int               assoc_scan_next(struct assoc_scan *scan, hash_item **item_array,
                                  int array_size, int elem_limit);
bool              assoc_scan_in_visited_area(struct assoc_scan *scan, hash_item *it);
void              assoc_scan_final(struct assoc_scan *scan);

#endif
