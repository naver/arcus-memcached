/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
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

#define MWJIN_HASHTABLE_EXPANSION_BY_WORKERS

typedef struct _prefix_t prefix_t;

struct _prefix_t {
    uint8_t nprefix;

    uint32_t prefix_items;
    uint64_t list_hash_items;
    uint64_t set_hash_items;
    uint64_t btree_hash_items;
    uint64_t hash_items;
    //uint64_t tot_hash_items;

    prefix_t *h_next;

    rel_time_t oldest_live;
    time_t create_time;

    uint64_t list_hash_items_bytes;
    uint64_t set_hash_items_bytes;
    uint64_t btree_hash_items_bytes;
    uint64_t hash_items_bytes;
    //uint64_t tot_hash_items_bytes;

    prefix_t *parent_prefix;
};

struct assoc {
#ifdef MWJIN_HASHTABLE_EXPANSION_BY_WORKERS
   uint32_t hashpower; /* how many hash buckets in a hash table ? (power of 2) */
   uint32_t hashsize;  /* hash table size */
   uint32_t hashmask;  /* hash bucket mask */
   uint32_t rootpower; /* how many hash tables we use ? (power of 2) */

   /* cache item hash table : an array of hash tables */
   struct table {
      hash_item** hashtable;
   } *roottable;

   /* hash power table: how many hash tables each hash bucket use ? (power of 2) */
   uint32_t* powertable;

   /* prefix hash table : single hash table */
   prefix_t**  prefix_hashtable;
   prefix_t    noprefix_stats;

   /* Number of items in the hash table. */
   unsigned int hash_items;
   unsigned int tot_prefix_items;
#else
   /* how many powers of 2's worth of buckets we use */
   unsigned int hashpower;

   /* Main hash table. This is where we look except during expansion. */
   hash_item** primary_hashtable;
   prefix_t**  prefix_hashtable;
   prefix_t    noprefix_stats;

   /*
    * Previous hash table. During expansion, we look here for keys that haven't
    * been moved over to the primary yet.
    */
   hash_item** old_hashtable;

   /* Number of items in the hash table. */
   unsigned int hash_items;
   unsigned int tot_prefix_items;

   /* Flag: Are we in the middle of expanding now? */
   bool expanding;
   bool threadrun;

   /*
    * During expansion we migrate values with bucket granularity; this is how
    * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
    */
   unsigned int expand_bucket;
#endif
};

/* associative array */
ENGINE_ERROR_CODE assoc_init(struct default_engine *engine);
void              assoc_final(struct default_engine *engine);

hash_item *       assoc_find(struct default_engine *engine,
                             uint32_t hash, const char *key, const size_t nkey);
int               assoc_insert(struct default_engine *engine,
                               uint32_t hash, hash_item *item);
void              assoc_delete(struct default_engine *engine,
                               uint32_t hash, const char *key, const size_t nkey);
prefix_t *        assoc_prefix_find(struct default_engine *engine,
                                    uint32_t hash, const char *prefix, const size_t nprefix);
bool              assoc_prefix_isvalid(struct default_engine *engine, hash_item *it);
void              assoc_prefix_update_size(prefix_t *pt, ENGINE_ITEM_TYPE item_type,
                                    const size_t item_size, const bool increment);
ENGINE_ERROR_CODE assoc_prefix_link(struct default_engine *engine,
                                    hash_item *it, const size_t item_size,
                                    prefix_t **pfx_item);
void              assoc_prefix_unlink(struct default_engine *engine,
                                    hash_item *it, const size_t item_size);
ENGINE_ERROR_CODE assoc_get_prefix_stats(struct default_engine *engine,
                                    const char *prefix, const int nprefix,
                                    void *prefix_data);
#endif
