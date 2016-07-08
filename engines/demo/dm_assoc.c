/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "demo_engine.h"

#define GET_HASH_BUCKET(hash, mask) ((hash) & (mask))

static EXTENSION_LOGGER_DESCRIPTOR *logger;

ENGINE_ERROR_CODE dm_assoc_init(struct demo_engine *engine)
{
    struct dm_assoc *assoc = &engine->assoc;

    logger = engine->server.log->get_logger();

    assoc->hashsize = 1024*1024;
    assoc->hashmask = assoc->hashsize-1;

    assoc->hashtable = calloc(assoc->hashsize, sizeof(void *));
    if (assoc->hashtable == NULL) {
        return ENGINE_ENOMEM;
    }

    logger->log(EXTENSION_LOG_INFO, NULL, "DEMO ASSOC module initialized.\n");
    return ENGINE_SUCCESS;
}

void dm_assoc_final(struct demo_engine *engine)
{
    struct dm_assoc *assoc = &engine->assoc;
    hash_item *it;

    for (int ii=0; ii < assoc->hashsize; ++ii) {
         while ((it = assoc->hashtable[ii]) != NULL) {
             assoc->hashtable[ii] = it->h_next;
             free(it);
         }
    }
    free(assoc->hashtable);
    logger->log(EXTENSION_LOG_INFO, NULL, "DEMO ASSOC module destroyed.\n");
}

hash_item *dm_assoc_find(struct demo_engine *engine, uint32_t hash,
                      const char *key, const size_t nkey)
{
    struct dm_assoc *assoc = &engine->assoc;
    hash_item *curr = NULL;
    uint32_t bucket = GET_HASH_BUCKET(hash, assoc->hashmask);

    while ((curr = assoc->hashtable[bucket]) != NULL) {
        if (nkey == curr->nkey && hash == curr->hval &&
            memcmp(key, dm_item_get_key(curr), nkey) == 0)
            break;
        curr = curr->h_next;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return curr;
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int dm_assoc_insert(struct demo_engine *engine, uint32_t hash, hash_item *it)
{
    struct dm_assoc *assoc = &engine->assoc;
    uint32_t bucket = GET_HASH_BUCKET(hash, assoc->hashmask);

    /* shouldn't have duplicately named things defined */
    assert(dm_assoc_find(engine, hash, dm_item_get_key(it), it->nkey) == 0);

    it->h_next = assoc->hashtable[bucket];
    assoc->hashtable[bucket] = it;
    assoc->hash_items++;

    MEMCACHED_ASSOC_INSERT(dm_item_get_key(it), it->nkey, assoc->hash_items);
    return 1;
}

void dm_assoc_delete(struct demo_engine *engine, uint32_t hash,
                  const char *key, const size_t nkey)
{
    hash_item *curr=NULL;
    hash_item *prev=NULL;
    struct dm_assoc *assoc = &engine->assoc;
    uint32_t bucket = GET_HASH_BUCKET(hash, assoc->hashmask);

    while ((curr = assoc->hashtable[bucket]) != NULL) {
        if (nkey == curr->nkey && hash == curr->hval &&
            memcmp(key, dm_item_get_key(curr), nkey) == 0)
            break;
        prev = curr;
        curr = curr->h_next;
    }
    if (curr != NULL) {
        if (prev == NULL)
            assoc->hashtable[bucket] = curr->h_next;
        else
            prev->h_next = curr->h_next;
        assoc->hash_items--;
    }
}
