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

   /*
    * During expansion we migrate values with bucket granularity; this is how
    * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
    */
   unsigned int expand_bucket;

   /* The number of cursors in use?  If there are cursors, do not expand */
   int cursors_in_use;
   bool cursor_blocking_expansion;
};

/* associative array */
ENGINE_ERROR_CODE assoc_init(struct default_engine *engine);
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

/* One-way cursor.  Right now, hash table expansion is disabled while cursors
 * are is use.  Likewise, the user cannot get a new cursor while expansion
 * is in progress.  The user must wait till expansion completes.
 */

struct assoc_cursor {
  struct default_engine *engine;
  int bucket;
  bool init;
  hash_item it; /* A placeholder.  See item_scrubber_main. */
};

/* Initialize the cursor.  Returns false if expansion is in progress and
 * we cannot use cursors.
 */
bool assoc_cursor_begin(struct default_engine *engine, struct assoc_cursor *c);

/* Indicate that this cursor is no longer used. */
void assoc_cursor_end(struct assoc_cursor *c);

/* Return the next item in the hash table and advance the cursor.
 * NULL indicates the cursor has reached the end of the table.
 */
hash_item *assoc_cursor_next(struct assoc_cursor *c);

/* Is the given hash item in the area of the hash table where the cursor
 * has passed through?  The item must be in the hash table.
 */
bool assoc_cursor_in_visited_area(struct assoc_cursor *c, hash_item *it);

#endif
