/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2016 JaM2in Co., Ltd.
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
#ifndef DM_ASSOC_H
#define DM_ASSOC_H

struct dm_assoc {
   uint32_t hashsize;  /* hash table size */
   uint32_t hashmask;  /* hash bucket mask */

   /* cache item hash table : an array of hash tables */
   hash_item** hashtable;

   /* Number of items in the hash table. */
   uint64_t hash_items;
};

/* associative array */
ENGINE_ERROR_CODE dm_assoc_init(struct demo_engine *engine);
void              dm_assoc_final(struct demo_engine *engine);

hash_item *       dm_assoc_find(struct demo_engine *engine, uint32_t hash,
                             const char *key, const size_t nkey);
int               dm_assoc_insert(struct demo_engine *engine, uint32_t hash, hash_item *item);
void              dm_assoc_delete(struct demo_engine *engine, uint32_t hash,
                               const char *key, const size_t nkey);
#endif
