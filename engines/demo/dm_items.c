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
#include <time.h>
#include <assert.h>
#include <inttypes.h>
#include <sys/time.h> /* gettimeofday() */

#include "demo_engine.h"

//#define SET_DELETE_NO_MERGE
//#define BTREE_DELETE_NO_MERGE

/* item unlink cause */
enum item_unlink_cause {
    ITEM_UNLINK_NORMAL = 1, /* unlink by normal request */
    ITEM_UNLINK_EVICT,      /* unlink by eviction */
    ITEM_UNLINK_INVALID,    /* unlink by invalidation such like expiration/flush */
    ITEM_UNLINK_REPLACE,    /* unlink by replacement of set/replace command,
                             * simple kv type only
                             */
    ITEM_UNLINK_ABORT,      /* unlink by abortion of creating a collection
                             * collection type only
                             */
    ITEM_UNLINK_EMPTY,      /* unlink by empty collection
                             * collection type only
                             */
    ITEM_UNLINK_STALE       /* unlink by staleness */
};

static EXTENSION_LOGGER_DESCRIPTOR *logger;

/*
 * Static functions
 */

#define ITEM_REFCOUNT_FULL 65535
#define ITEM_REFCOUNT_MOVE 32768

static inline void ITEM_REFCOUNT_INCR(hash_item *it)
{
    it->refcount++;
    if (it->refcount == ITEM_REFCOUNT_FULL) {
        it->refchunk += 1;
        it->refcount -= ITEM_REFCOUNT_MOVE;
        assert(it->refchunk != 0); /* overflow */
    }
}

static inline void ITEM_REFCOUNT_DECR(hash_item *it)
{
    it->refcount--;
    if (it->refcount == 0 && it->refchunk > 0) {
        it->refchunk -= 1;
        it->refcount = ITEM_REFCOUNT_MOVE;
    }
}

/* warning: don't use these macros with a function, as it evals its arg twice */
static inline size_t ITEM_ntotal(struct demo_engine *engine, const hash_item *item)
{
    size_t ret;
    ret = sizeof(*item) + item->nkey + item->nbytes;
    if (engine->config.use_cas) {
        ret += sizeof(uint64_t);
    }
    return ret;
}

static inline size_t ITEM_stotal(struct demo_engine *engine, const hash_item *item)
{
    size_t ntotal = ITEM_ntotal(engine, item);
    return ntotal;
}

/* Get the next CAS id for a new item. */
static uint64_t get_cas_id(void)
{
    static uint64_t cas_id = 0;
    return ++cas_id;
}

/* Enable this for reference-count debugging. */
#if 0
# define DEBUG_REFCNT(it,op) \
         fprintf(stderr, "item %x refcnt(%c) %d %c\n", \
                        it, op, it->refcount, \
                        (it->it_flags & ITEM_LINKED) ? 'L' : ' ')
#else
# define DEBUG_REFCNT(it,op) while(0)
#endif

static bool do_item_isvalid(struct demo_engine *engine,
                            hash_item *it, rel_time_t current_time)
{
    /* check if it's expired */
    if (it->exptime != 0 && it->exptime <= current_time) {
        return false; /* expired */
    }
    /* check flushed items as well as expired items */
    if (engine->config.oldest_live != 0) {
        if (engine->config.oldest_live <= current_time &&
            it->time <= engine->config.oldest_live)
            return false; /* flushed by flush_all */
    }
    return true; /* Yes, it's a valid item */
}

/*@null@*/
static hash_item *do_item_alloc(struct demo_engine *engine,
                                const void *key, const size_t nkey,
                                const int flags, const rel_time_t exptime,
                                const int nbytes, const void *cookie)
{
    hash_item *it=NULL;
    size_t ntotal;
    rel_time_t real_exptime = exptime;

#ifdef ENABLE_STICKY_ITEM
    /* sticky memory limit check */
    if (real_exptime == (rel_time_t)(-1)) { /* sticky item */
        real_exptime = 0; // ignore sticky item */
    }
#endif

    ntotal = sizeof(hash_item) + nkey + nbytes;
    if (engine->config.use_cas) {
        ntotal += sizeof(uint64_t);
    }

    it = (void*)malloc(ntotal);
    if (it == NULL)  {
        return NULL;
    }
    //assert(it->slabs_clsid == 0);

    it->slabs_clsid = 1;
    it->next = it->prev = it; /* special meaning: unlinked from LRU */
    it->h_next = 0;
    it->refcount = 1;     /* the caller will have a reference */
    it->refchunk = 0;
    DEBUG_REFCNT(it, '*');
    it->iflag = engine->config.use_cas ? ITEM_WITH_CAS : 0;
    it->nkey = nkey;
    it->nbytes = nbytes;
    it->flags = flags;
    memcpy((void*)dm_item_get_key(it), key, nkey);
    it->exptime = real_exptime;
    it->nprefix = 0;
    return it;
}

static void do_item_free(struct demo_engine *engine, hash_item *it)
{
    assert((it->iflag & ITEM_LINKED) == 0);
    assert(it->refcount == 0);

    it->slabs_clsid = 0;
    DEBUG_REFCNT(it, 'F');
    free(it);
}

static ENGINE_ERROR_CODE do_item_link(struct demo_engine *engine, hash_item *it)
{
    const char *key = dm_item_get_key(it);
    size_t stotal = ITEM_stotal(engine, it);
    assert((it->iflag & ITEM_LINKED) == 0);
    assert(it->nbytes < (1024 * 1024));  /* 1MB max size */

    MEMCACHED_ITEM_LINK(key, it->nkey, it->nbytes);

    /* Allocate a new CAS ID on link. */
    dm_item_set_cas(it, get_cas_id());

    /* link the item to the hash table */
    it->iflag |= ITEM_LINKED;
    it->time = engine->server.core->get_current_time();
    it->hval = engine->server.core->hash(key, it->nkey, 0);
    dm_assoc_insert(engine, it->hval, it);

    /* update item statistics */
    pthread_mutex_lock(&engine->stats.lock);
    engine->stats.curr_bytes += stotal;
    engine->stats.curr_items += 1;
    engine->stats.total_items += 1;
    pthread_mutex_unlock(&engine->stats.lock);

    return ENGINE_SUCCESS;
}

static void do_item_unlink(struct demo_engine *engine, hash_item *it,
                           enum item_unlink_cause cause)
{
    /* cause: item unlink cause will be used, later
    */
    const char *key = dm_item_get_key(it);
    size_t stotal = ITEM_stotal(engine, it);
    MEMCACHED_ITEM_UNLINK(key, it->nkey, it->nbytes);

    if ((it->iflag & ITEM_LINKED) != 0) {
        /* unlink the item from hash table */
        dm_assoc_delete(engine, it->hval, key, it->nkey);
        it->iflag &= ~ITEM_LINKED;

        /* update item statistics */
        pthread_mutex_lock(&engine->stats.lock);
        engine->stats.curr_bytes -= stotal;
        engine->stats.curr_items -= 1;
        pthread_mutex_unlock(&engine->stats.lock);

        /* free the item if no one reference it */
        if (it->refcount == 0) {
            do_item_free(engine, it);
        }
    }
}

static void do_item_release(struct demo_engine *engine, hash_item *it)
{
    MEMCACHED_ITEM_REMOVE(dm_item_get_key(it), it->nkey, it->nbytes);
    if (it->refcount != 0) {
        ITEM_REFCOUNT_DECR(it);
        DEBUG_REFCNT(it, '-');
    }
    if (it->refcount == 0) {
        if ((it->iflag & ITEM_LINKED) == 0) {
            do_item_free(engine, it);
        }
    }
}

static void do_item_replace(struct demo_engine *engine,
                            hash_item *it, hash_item *new_it)
{
    MEMCACHED_ITEM_REPLACE(dm_item_get_key(it), it->nkey, it->nbytes,
                           dm_item_get_key(new_it), new_it->nkey, new_it->nbytes);
    do_item_unlink(engine, it, ITEM_UNLINK_REPLACE);
    /* Cache item replacement does not drop the prefix item even if it's empty.
     * So, the below do_item_link function always return SUCCESS.
     */
    (void)do_item_link(engine, new_it);
}

static hash_item *do_item_get(struct demo_engine *engine,
                              const char *key, const size_t nkey,
                              bool LRU_reposition)
{
    rel_time_t current_time = engine->server.core->get_current_time();
    hash_item *it = dm_assoc_find(engine, engine->server.core->hash(key, nkey, 0),
                                  key, nkey);

    if (it != NULL) {
        if (do_item_isvalid(engine, it, current_time)==false) {
            do_item_unlink(engine, it, ITEM_UNLINK_INVALID);
            it = NULL;
        }
    }
    if (it != NULL) {
        ITEM_REFCOUNT_INCR(it);
        DEBUG_REFCNT(it, '+');
    }

    if (engine->config.verbose > 2) {
        if (it == NULL) {
            logger->log(EXTENSION_LOG_INFO, NULL, "> NOT FOUND %s\n",
                        key);
        } else {
            logger->log(EXTENSION_LOG_INFO, NULL, "> FOUND KEY %s\n",
                        (const char*)dm_item_get_key(it));
        }
    }
    return it;
}

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns the state of storage.
 */
static ENGINE_ERROR_CODE do_item_store(struct demo_engine *engine,
                                       hash_item *it, uint64_t *cas,
                                       ENGINE_STORE_OPERATION operation,
                                       const void *cookie)
{
    const char *key = dm_item_get_key(it);
    hash_item *old_it;
    hash_item *new_it = NULL;
    ENGINE_ERROR_CODE stored;

    old_it = do_item_get(engine, key, it->nkey, true);

    if (old_it != NULL) {
        if (operation == OPERATION_ADD) {
            do_item_release(engine, old_it);
            return ENGINE_NOT_STORED;
        }
    } else {
        if (operation == OPERATION_REPLACE ||
            operation == OPERATION_APPEND || operation == OPERATION_PREPEND) {
            return ENGINE_NOT_STORED;
        }
        if (operation == OPERATION_CAS) {
            return ENGINE_KEY_ENOENT;
        }
    }

    stored = ENGINE_NOT_STORED;

    if (operation == OPERATION_CAS) {
        assert(old_it != NULL);
        if (dm_item_get_cas(it) == dm_item_get_cas(old_it)) {
            // cas validates
            // it and old_it may belong to different classes.
            // I'm updating the stats for the one that's getting pushed out
            do_item_replace(engine, old_it, it);
            stored = ENGINE_SUCCESS;
        } else {
            if (engine->config.verbose > 1) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                        "CAS:  failure: expected %"PRIu64", got %"PRIu64"\n",
                        dm_item_get_cas(old_it), dm_item_get_cas(it));
            }
            stored = ENGINE_KEY_EEXISTS;
        }
    } else {
        /*
         * Append - combine new and old record into single one. Here it's
         * atomic and thread-safe.
         */
        if (operation == OPERATION_APPEND || operation == OPERATION_PREPEND) {
            assert(old_it != NULL);
            /*
             * Validate CAS
             */
            if (dm_item_get_cas(it) != 0) {
                // CAS much be equal
                if (dm_item_get_cas(it) != dm_item_get_cas(old_it)) {
                    stored = ENGINE_KEY_EEXISTS;
                }
            }
            if (stored == ENGINE_NOT_STORED) {
                /* we have it and old_it here - alloc memory to hold both */
                new_it = do_item_alloc(engine, key, it->nkey,
                                       old_it->flags,
                                       old_it->exptime,
                                       it->nbytes + old_it->nbytes - 2 /* CRLF */,
                                       cookie);
                if (new_it == NULL) {
                    /* SERVER_ERROR out of memory */
                    if (old_it != NULL)
                        do_item_release(engine, old_it);
                    return ENGINE_NOT_STORED;
                }

                /* copy data from it and old_it to new_it */
                if (operation == OPERATION_APPEND) {
                    memcpy(dm_item_get_data(new_it), dm_item_get_data(old_it), old_it->nbytes);
                    memcpy(dm_item_get_data(new_it) + old_it->nbytes - 2 /* CRLF */,
                           dm_item_get_data(it), it->nbytes);
                } else {
                    /* OPERATION_PREPEND */
                    memcpy(dm_item_get_data(new_it), dm_item_get_data(it), it->nbytes);
                    memcpy(dm_item_get_data(new_it) + it->nbytes - 2 /* CRLF */,
                           dm_item_get_data(old_it), old_it->nbytes);
                }

                it = new_it;
            }
        }
        if (stored == ENGINE_NOT_STORED) {
            if (old_it != NULL) {
                do_item_replace(engine, old_it, it);
                stored = ENGINE_SUCCESS;
            } else {
                stored = do_item_link(engine, it);
            }
            if (stored == ENGINE_SUCCESS) {
                *cas = dm_item_get_cas(it);
            }
        }
    }

    if (old_it != NULL) {
        do_item_release(engine, old_it);         /* release our reference */
    }
    if (new_it != NULL) {
        do_item_release(engine, new_it);
    }
    if (stored == ENGINE_SUCCESS) {
        *cas = dm_item_get_cas(it);
    }
    return stored;
}

static ENGINE_ERROR_CODE do_item_delete(struct demo_engine *engine,
                                        const void* key, const size_t nkey,
                                        uint64_t cas)
{
    ENGINE_ERROR_CODE ret;
    hash_item *it = do_item_get(engine, key, nkey, true);
    if (it == NULL) {
        ret = ENGINE_KEY_ENOENT;
    } else {
        if (cas == 0 || cas == dm_item_get_cas(it)) {
            do_item_unlink(engine, it, ITEM_UNLINK_NORMAL);
            ret = ENGINE_SUCCESS;
        } else {
            ret = ENGINE_KEY_EEXISTS;
        }
        do_item_release(engine, it);
    }
    return ret;
}

/********************************* ITEM ACCESS *******************************/

/*
 * Allocates a new item.
 */
hash_item *dm_item_alloc(struct demo_engine *engine,
                      const void *key, size_t nkey, int flags,
                      rel_time_t exptime, int nbytes, const void *cookie)
{
    hash_item *it;
    pthread_mutex_lock(&engine->cache_lock);
    it = do_item_alloc(engine, key, nkey, flags, exptime, nbytes, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return it;
}

/*
 * Returns an item if it hasn't been marked as expired,
 * lazy-expiring as needed.
 */
hash_item *dm_item_get(struct demo_engine *engine, const void *key, const size_t nkey)
{
    hash_item *it;
    pthread_mutex_lock(&engine->cache_lock);
    it = do_item_get(engine, key, nkey, true);
    pthread_mutex_unlock(&engine->cache_lock);
    return it;
}

/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void dm_item_release(struct demo_engine *engine, hash_item *item)
{
    pthread_mutex_lock(&engine->cache_lock);
    do_item_release(engine, item);
    pthread_mutex_unlock(&engine->cache_lock);
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
ENGINE_ERROR_CODE dm_item_store(struct demo_engine *engine,
                             hash_item *item, uint64_t *cas,
                             ENGINE_STORE_OPERATION operation,
                             const void *cookie)
{
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_item_store(engine, item, cas, operation, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE dm_item_arithmetic(struct demo_engine *engine,
                             const void* cookie,
                             const void* key,
                             const int nkey,
                             const bool increment,
                             const bool create,
                             const uint64_t delta,
                             const uint64_t initial,
                             const int flags,
                             const rel_time_t exptime,
                             uint64_t *cas,
                             uint64_t *result)
{
    return ENGINE_ENOTSUP;
}

/*
 * Delete an item.
 */

ENGINE_ERROR_CODE dm_item_delete(struct demo_engine *engine,
                              const void* key, const size_t nkey,
                              uint64_t cas)
{
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_item_delete(engine, key, nkey, cas);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

/*
 * Flushes expired items after a flush_all call
 */

ENGINE_ERROR_CODE dm_item_flush_expired(struct demo_engine *engine,
                                        const char *prefix, const int nprefix,
                                        time_t when, const void* cookie)
{
    return ENGINE_ENOTSUP;

}

void dm_item_stats(struct demo_engine *engine,
                   ADD_STAT add_stat, const void *cookie)
{
    return;
}

void dm_item_stats_sizes(struct demo_engine *engine,
                      ADD_STAT add_stat, const void *cookie)
{
    return;
}

void dm_item_stats_reset(struct demo_engine *engine)
{
    return;
}

ENGINE_ERROR_CODE dm_item_init(struct demo_engine *engine)
{
    logger = engine->server.log->get_logger();
    logger->log(EXTENSION_LOG_INFO, NULL, "DEMO ITEM module initialized.\n");
    return ENGINE_SUCCESS;
}

void dm_item_final(struct demo_engine *engine)
{
    logger->log(EXTENSION_LOG_INFO, NULL, "DEMO ITEM module destroyed.\n");
}

/*
 * Item access functions
 */
uint64_t dm_item_get_cas(const hash_item* item)
{
    if (item->iflag & ITEM_WITH_CAS) {
        return *(uint64_t*)(item + 1);
    }
    return 0;
}

void dm_item_set_cas(const hash_item* item, uint64_t val)
{
    if (item->iflag & ITEM_WITH_CAS) {
        *(uint64_t*)(item + 1) = val;
    }
}

const void* dm_item_get_key(const hash_item* item)
{
    char *ret = (void*)(item + 1);
    if (item->iflag & ITEM_WITH_CAS) {
        ret += sizeof(uint64_t);
    }
    return ret;
}

char* dm_item_get_data(const hash_item* item)
{
    return ((char*)dm_item_get_key(item)) + item->nkey;
}

uint8_t dm_item_get_clsid(const hash_item* item)
{
    return 0;
}
