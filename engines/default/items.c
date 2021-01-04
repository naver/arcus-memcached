/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <sched.h>
#include <inttypes.h>

/* Dummy PERSISTENCE_ACTION Macros */
#define PERSISTENCE_ACTION_BEGIN(a, b)
#define PERSISTENCE_ACTION_END(a)

#include "default_engine.h"
#include "item_clog.h"

static struct default_engine *engine=NULL;
static struct engine_config *config=NULL; // engine config
static struct items         *itemsp=NULL;
static SERVER_CORE_API      *svcore=NULL; // server core api
static EXTENSION_LOGGER_DESCRIPTOR *logger;

/*
 * Static functions
 */
static inline void LOCK_CACHE(void)
{
    pthread_mutex_lock(&engine->cache_lock);
}

static inline void UNLOCK_CACHE(void)
{
    pthread_mutex_unlock(&engine->cache_lock);
}

static inline void TRYLOCK_CACHE(int ntries)
{
    int i;

    for (i = 0; i < ntries; i++) {
        if (pthread_mutex_trylock(&engine->cache_lock) == 0)
            break;
        sched_yield();
    }
    if (i == ntries) {
        pthread_mutex_lock(&engine->cache_lock);
    }
}

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns the state of storage.
 */
static ENGINE_ERROR_CODE do_item_store_set(hash_item *it, uint64_t *cas, const void *cookie)
{
    hash_item *old_it;
    ENGINE_ERROR_CODE stored;

    old_it = do_item_get(item_get_key(it), it->nkey, DONT_UPDATE);
    if (old_it) {
        if (IS_COLL_ITEM(old_it)) {
            stored = ENGINE_EBADTYPE;
        } else {
            do_item_replace(old_it, it);
            stored = ENGINE_SUCCESS;
        }
        do_item_release(old_it);
    } else {
        stored = do_item_link(it);
    }
    if (stored == ENGINE_SUCCESS) {
        *cas = item_get_cas(it);
    }
    return stored;
}

static ENGINE_ERROR_CODE do_item_store_add(hash_item *it, uint64_t *cas, const void *cookie)
{
    hash_item *old_it;
    ENGINE_ERROR_CODE stored;

    old_it = do_item_get(item_get_key(it), it->nkey, DONT_UPDATE);
    if (old_it) {
        if (IS_COLL_ITEM(old_it)) {
            stored = ENGINE_EBADTYPE;
        } else {
            /* add only adds a nonexistent item, but promote to head of LRU */
            do_item_update(old_it, false);
            stored = ENGINE_NOT_STORED;
        }
        do_item_release(old_it);
    } else {
        stored = do_item_link(it);
        if (stored == ENGINE_SUCCESS) {
            *cas = item_get_cas(it);
        }
    }
    return stored;
}

static ENGINE_ERROR_CODE do_item_store_replace(hash_item *it, uint64_t *cas, const void *cookie)
{
    hash_item *old_it;
    ENGINE_ERROR_CODE stored;

    old_it = do_item_get(item_get_key(it), it->nkey, DONT_UPDATE);
    if (old_it == NULL) {
        return ENGINE_NOT_STORED;
    }

    if (IS_COLL_ITEM(old_it)) {
        stored = ENGINE_EBADTYPE;
    } else {
        do_item_replace(old_it, it);
        stored = ENGINE_SUCCESS;
        *cas = item_get_cas(it);
    }
    do_item_release(old_it);
    return stored;
}

static ENGINE_ERROR_CODE do_item_store_cas(hash_item *it, uint64_t *cas, const void *cookie)
{
    hash_item *old_it;
    ENGINE_ERROR_CODE stored;

    old_it = do_item_get(item_get_key(it), it->nkey, DONT_UPDATE);
    if (old_it == NULL) {
        // LRU expired
        return ENGINE_KEY_ENOENT;
    }

    if (IS_COLL_ITEM(old_it)) {
        stored = ENGINE_EBADTYPE;
    } else if (item_get_cas(it) == item_get_cas(old_it)) {
        // cas validates
        // it and old_it may belong to different classes.
        // I'm updating the stats for the one that's getting pushed out
        do_item_replace(old_it, it);
        stored = ENGINE_SUCCESS;
        *cas = item_get_cas(it);
    } else {
        if (config->verbose > 1) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                    "CAS:  failure: expected %"PRIu64", got %"PRIu64"\n",
                    item_get_cas(old_it), item_get_cas(it));
        }
        stored = ENGINE_KEY_EEXISTS;
    }
    do_item_release(old_it);
    return stored;
}

static ENGINE_ERROR_CODE do_item_store_attach(hash_item *it, uint64_t *cas,
                                              ENGINE_STORE_OPERATION operation,
                                              const void *cookie)
{
    hash_item *old_it;
    ENGINE_ERROR_CODE stored;

    old_it = do_item_get(item_get_key(it), it->nkey, DONT_UPDATE);
    if (old_it == NULL) {
        return ENGINE_NOT_STORED;
    }

    if (IS_COLL_ITEM(old_it)) {
        stored = ENGINE_EBADTYPE;
    } else if (item_get_cas(it) != 0 &&
               item_get_cas(it) != item_get_cas(old_it)) {
        // CAS much be equal
        stored = ENGINE_KEY_EEXISTS;
    } else {
        /* we have it and old_it here - alloc memory to hold both */
        hash_item *new_it = do_item_alloc(item_get_key(it), it->nkey,
                                          old_it->flags, old_it->exptime,
                                          it->nbytes + old_it->nbytes - 2 /* CRLF */,
                                          cookie);
        if (new_it) {
            /* copy data from it and old_it to new_it */
            if (operation == OPERATION_APPEND) {
                memcpy(item_get_data(new_it), item_get_data(old_it), old_it->nbytes);
                memcpy(item_get_data(new_it) + old_it->nbytes - 2 /* CRLF */,
                       item_get_data(it), it->nbytes);
            } else {
                /* OPERATION_PREPEND */
                memcpy(item_get_data(new_it), item_get_data(it), it->nbytes);
                memcpy(item_get_data(new_it) + it->nbytes - 2 /* CRLF */,
                       item_get_data(old_it), old_it->nbytes);
            }
            /* replace old item with new item */
            do_item_replace(old_it, new_it);
            stored = ENGINE_SUCCESS;
            *cas = item_get_cas(new_it);
        } else {
            /* SERVER_ERROR out of memory */
            stored = ENGINE_NOT_STORED;
        }
    }
    do_item_release(old_it);
    return stored;
}

/*
 * adds a delta value to a numeric item.
 *
 * c     connection requesting the operation
 * it    item to adjust
 * incr  true to increment value, false to decrement
 * delta amount to adjust value by
 * buf   buffer for response string
 *
 * returns a response string to send back to the client.
 */
static ENGINE_ERROR_CODE do_add_delta(hash_item *it, const bool incr, const int64_t delta,
                                      uint64_t *rcas, uint64_t *result, const void *cookie)
{
    const char *ptr;
    uint64_t value;
    int res;

    ptr = item_get_data(it);
    if (!safe_strtoull(ptr, &value)) {
        return ENGINE_EINVAL;
    }

    if (incr) {
        value += delta;
    } else {
        if (delta > value) {
            value = 0;
        } else {
            value -= delta;
        }
    }

    *result = value;
    char buf[80];
    if ((res = snprintf(buf, sizeof(buf), "%" PRIu64 "\r\n", value)) == -1) {
        return ENGINE_EINVAL;
    }
    hash_item *new_it = do_item_alloc(item_get_key(it), it->nkey,
                                      it->flags, it->exptime, res, cookie);
    if (new_it == NULL) {
        return ENGINE_ENOMEM;
    }
    memcpy(item_get_data(new_it), buf, res);
    do_item_replace(it, new_it);
    *rcas = item_get_cas(new_it);

    return ENGINE_SUCCESS;
}

/********************************* ITEM ACCESS *******************************/

/*
 * Allocates a new item.
 */
hash_item *item_alloc(const void *key, const uint32_t nkey,
                      const uint32_t flags, rel_time_t exptime,
                      const uint32_t nbytes, const void *cookie)
{
    hash_item *it;
    LOCK_CACHE();
    /* key can be NULL */
    it = do_item_alloc(key, nkey, flags, exptime, nbytes, cookie);
    UNLOCK_CACHE();
    return it;
}

/*
 * Frees and adds an item to the freelist.
 */
void item_free(hash_item *item)
{
    LOCK_CACHE();
    do_item_free(item);
    UNLOCK_CACHE();
}

/*
 * Returns an item if it hasn't been marked as expired,
 * lazy-expiring as needed.
 */
hash_item *item_get(const void *key, const uint32_t nkey)
{
    hash_item *it;
    LOCK_CACHE();
    it = do_item_get(key, nkey, DO_UPDATE);
    UNLOCK_CACHE();
    return it;
}

/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void item_release(hash_item *item)
{
    LOCK_CACHE();
    do_item_release(item);
    UNLOCK_CACHE();
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
ENGINE_ERROR_CODE item_store(hash_item *item, uint64_t *cas,
                             ENGINE_STORE_OPERATION operation,
                             const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_STORE);

    LOCK_CACHE();
    switch (operation) {
      case OPERATION_SET:
           ret = do_item_store_set(item, cas, cookie);
           break;
      case OPERATION_ADD:
           ret = do_item_store_add(item, cas, cookie);
           break;
      case OPERATION_REPLACE:
           ret = do_item_store_replace(item, cas, cookie);
           break;
      case OPERATION_CAS:
           ret = do_item_store_cas(item, cas, cookie);
           break;
      case OPERATION_PREPEND:
      case OPERATION_APPEND:
           ret = do_item_store_attach(item, cas, operation, cookie);
           break;
      default:
           ret = ENGINE_NOT_STORED;
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE item_arithmetic(const void *key, const uint32_t nkey,
                                  const bool increment, const bool create,
                                  const uint64_t delta, const uint64_t initial,
                                  const uint32_t flags, const rel_time_t exptime,
                                  uint64_t *cas, uint64_t *result,
                                  const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_STORE);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it) {
        if (IS_COLL_ITEM(it)) {
            ret = ENGINE_EBADTYPE;
        } else {
            ret = do_add_delta(it, increment, delta, cas, result, cookie);
            do_item_release(it);
        }
    } else {
        if (create) {
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "%"PRIu64"\r\n", initial);

            it = do_item_alloc(key, nkey, flags, exptime, len, cookie);
            if (it) {
                memcpy((void*)item_get_data(it), buffer, len);
                ret = do_item_store_add(it, cas, cookie);
                if (ret == ENGINE_SUCCESS) {
                    *result = initial;
                } else {
                    do_item_free(it);
                }
            } else {
                ret = ENGINE_ENOMEM;
            }
        } else {
            ret = ENGINE_KEY_ENOENT;
        }
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

/*
 * Delete an item.
 */
ENGINE_ERROR_CODE item_delete(const void *key, const uint32_t nkey, uint64_t cas,
                              const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_DELETE);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it) {
        if (cas == 0 || cas == item_get_cas(it)) {
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
            ret = ENGINE_SUCCESS;
        } else {
            ret = ENGINE_KEY_EEXISTS;
        }
        do_item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

/*
 * Flushes expired items after a flush_all call
 */

static ENGINE_ERROR_CODE do_item_flush_expired(const char *prefix, const int nprefix,
                                               rel_time_t when, const void *cookie)
{
    hash_item *iter, *next;
    rel_time_t oldest_live;

    if (nprefix >= 0) { /* flush the given prefix */
        prefix_t *pt = prefix_find(prefix, nprefix);
        if (pt == NULL) {
            return ENGINE_PREFIX_ENOENT;
        }

        if (when == 0) {
            pt->oldest_live = svcore->get_current_time() - 1;
        } else {
            pt->oldest_live = when - 1;
        }
        oldest_live = pt->oldest_live;

        if (config->verbose) {
            logger->log(EXTENSION_LOG_INFO, NULL, "flush prefix=%s when=%u client_ip=%s\n",
                        (prefix == NULL ? "<null>" : prefix),
                        when, svcore->get_client_ip(cookie));
        }
    } else { /* flush all */
        if (when == 0) {
            config->oldest_live = svcore->get_current_time() - 1;
        } else {
            config->oldest_live = when - 1;
        }
        oldest_live = config->oldest_live;

        if (config->verbose) {
            logger->log(EXTENSION_LOG_INFO, NULL, "flush all when=%u client_ip=%s\n",
                        when, svcore->get_client_ip(cookie));
        }
    }

    if (oldest_live != 0) {
        for (int i = 0; i <= POWER_LARGEST; i++) {
            /*
             * The LRU is sorted in decreasing time order, and an item's
             * timestamp is never newer than its last access time, so we
             * only need to walk back until we hit an item older than the
             * oldest_live time.
             * The oldest_live checking will auto-expire the remaining items.
             */
            iter = itemsp->heads[i];
            while (iter != NULL) {
                if (iter->time < oldest_live) {
                    /* We've hit the first old item. Continue to the next queue. */
                    /* reset lowMK and curMK to tail pointer */
                    itemsp->lowMK[i] = itemsp->tails[i];
                    itemsp->curMK[i] = itemsp->tails[i];
                    break;
                }
#ifdef NESTED_PREFIX
                if (nprefix < 0 || prefix_isincluded(iter->pfxptr, prefix, nprefix)) {
                    next = iter->next;
                    do_item_unlink(iter, ITEM_UNLINK_INVALID);
                    iter = next;
                } else {
                    iter = iter->next;
                }
#else
                if (nprefix < 0 || prefix_issame(iter->pfxptr, prefix, nprefix)) {
                    next = iter->next;
                    do_item_unlink(iter, ITEM_UNLINK_INVALID);
                    iter = next;
                } else {
                    iter = iter->next;
                }
#endif
            }
#ifdef ENABLE_STICKY_ITEM
            iter = itemsp->sticky_heads[i];
            while (iter != NULL) {
                if (iter->time < oldest_live) {
                    /* We've hit the first old item. Continue to the next queue. */
                    /* reset curMK to tail pointer */
                    itemsp->sticky_curMK[i] = itemsp->sticky_tails[i];
                    break;
                }
#ifdef NESTED_PREFIX
                if (nprefix < 0 || prefix_isincluded(iter->pfxptr, prefix, nprefix)) {
                    next = iter->next;
                    do_item_unlink(iter, ITEM_UNLINK_INVALID);
                    iter = next;
                } else {
                    iter = iter->next;
                }
#else
                if (nprefix < 0 || prefix_issame(iter->pfxptr, prefix, nprefix)) {
                    next = iter->next;
                    do_item_unlink(iter, ITEM_UNLINK_INVALID);
                    iter = next;
                } else {
                    iter = iter->next;
                }
#endif
            }
#endif
        }
        CLOG_ITEM_FLUSH(prefix, nprefix, when);
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE item_flush_expired(const char *prefix, const int nprefix,
                                     rel_time_t when, const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_FLUSH);

    LOCK_CACHE();
    ret = do_item_flush_expired(prefix, nprefix, when, cookie);
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

/*
 * Dumps part of the cache
 */
char *item_cachedump(unsigned int slabs_clsid, unsigned int limit, const bool forward,
                     const bool sticky, unsigned int *bytes)
{
    unsigned int memlimit = 512 * 1024; /* 512KB max response size */
    char *buffer;
    unsigned int bufcurr = 0;
    hash_item *it;
    unsigned int len;
    unsigned int shown = 0;
    char *keybuf;

    buffer = malloc((size_t)memlimit);
    if (buffer == 0) return NULL;

    keybuf = malloc(32*1024); /* must be larger than KEY_MAX_LENGTH */
    if (keybuf == NULL) {
        free(buffer);
        return NULL;
    }

    LOCK_CACHE();
    if (sticky) {
        it = (forward ? itemsp->sticky_heads[slabs_clsid]
                      : itemsp->sticky_tails[slabs_clsid]);
    } else {
        it = (forward ? itemsp->heads[slabs_clsid]
                      : itemsp->tails[slabs_clsid]);
    }
    while (it != NULL && (limit == 0 || shown < limit)) {
        /* Copy the key since it may not be null-terminated in the struct */
        strncpy(keybuf, item_get_key(it), it->nkey);
        keybuf[it->nkey] = 0x00; /* terminate */

        if (bufcurr + it->nkey + 100 > memlimit) break;
        len = sprintf(buffer + bufcurr, "ITEM %s [acctime=%u, exptime=%d]\r\n",
                      keybuf, it->time, (int32_t)it->exptime);
        bufcurr += len;
        shown++;
        it = (forward ? it->next : it->prev);
    }
    UNLOCK_CACHE();

    free(keybuf);

    len = sprintf(buffer + bufcurr, "END [curtime=%u]\r\n",
                  svcore->get_current_time());
    bufcurr += len;

    *bytes = bufcurr;
    return buffer;
}

void item_stats_global(ADD_STAT add_stat, const void *cookie)
{
    char val[128];
    int len;

    LOCK_CACHE();
    len = sprintf(val, "%"PRIu64, (uint64_t)prefix_count());
    add_stat("curr_prefixes", 13, val, len, cookie);

    do_item_stat_get(add_stat, cookie);
    UNLOCK_CACHE();
}

void item_stats(ADD_STAT add_stat, const void *cookie)
{
    const char *prefix = "items";

    LOCK_CACHE();
    for (int i = 0; i <= POWER_LARGEST; i++) {
        if (itemsp->tails[i] == NULL && itemsp->sticky_tails[i] == NULL)
            continue;

        add_statistics(cookie, add_stat, prefix, i, "number", "%u",
                       itemsp->sizes[i]+itemsp->sticky_sizes[i]);
#ifdef ENABLE_STICKY_ITEM
        add_statistics(cookie, add_stat, prefix, i, "sticky", "%u",
                       itemsp->sticky_sizes[i]);
#endif
        add_statistics(cookie, add_stat, prefix, i, "age", "%u",
                       (itemsp->tails[i] != NULL ? itemsp->tails[i]->time : 0));
        add_statistics(cookie, add_stat, prefix, i, "evicted",
                       "%u", itemsp->itemstats[i].evicted);
        add_statistics(cookie, add_stat, prefix, i, "evicted_nonzero",
                       "%u", itemsp->itemstats[i].evicted_nonzero);
        add_statistics(cookie, add_stat, prefix, i, "evicted_time",
                       "%u", itemsp->itemstats[i].evicted_time);
        add_statistics(cookie, add_stat, prefix, i, "outofmemory",
                       "%u", itemsp->itemstats[i].outofmemory);
        add_statistics(cookie, add_stat, prefix, i, "tailrepairs",
                       "%u", itemsp->itemstats[i].tailrepairs);;
        add_statistics(cookie, add_stat, prefix, i, "reclaimed",
                       "%u", itemsp->itemstats[i].reclaimed);;
    }
    UNLOCK_CACHE();
}

/** dumps out a list of objects of each size, with granularity of 32 bytes */
/*@null@*/
void item_stats_sizes(ADD_STAT add_stat, const void *cookie)
{
    /* "stats sizes" has too much overhead to execute,
     * since it traverses all of the items cached in memory.
     * So, we disabled "stats sizes" execution.
     */
    return;

#if 0 // disabled below code.
    /* max 1MB object, divided into 32 bytes size buckets */
    const int num_buckets = 32768;
    unsigned int *histogram = calloc(num_buckets, sizeof(int));

    if (histogram != NULL) {
        int i;

        /* build the histogram */
        LOCK_CACHE();
        for (i = 0; i <= POWER_LARGEST; i++) {
            hash_item *iter = itemsp->heads[i];
            while (iter) {
                int ntotal = ITEM_stotal(iter);
                int bucket = ntotal / 32;
                if ((ntotal % 32) != 0) bucket++;
                if (bucket < num_buckets) histogram[bucket]++;
                iter = iter->next;
            }
#ifdef ENABLE_STICKY_ITEM
            iter = itemsp->sticky_heads[i];
            while (iter) {
                int ntotal = ITEM_stotal(iter);
                int bucket = ntotal / 32;
                if ((ntotal % 32) != 0) bucket++;
                if (bucket < num_buckets) histogram[bucket]++;
                iter = iter->next;
            }
#endif
        }
        UNLOCK_CACHE();

        /* write the buffer */
        for (i = 0; i < num_buckets; i++) {
            if (histogram[i] != 0) {
                char key[8], val[32];
                int klen, vlen;
                klen = snprintf(key, sizeof(key), "%d", i * 32);
                vlen = snprintf(val, sizeof(val), "%u", histogram[i]);
                assert(klen < sizeof(key));
                assert(vlen < sizeof(val));
                add_stat(key, klen, val, vlen, c);
            }
        }
        free(histogram);
    }
#endif
}

void item_stats_reset(void)
{
    LOCK_CACHE();
    do_item_stat_reset();
    UNLOCK_CACHE();
}

/*
 * ITEM ATTRIBUTE Interface Functions
 */
static ENGINE_ERROR_CODE
do_item_getattr(hash_item *it, ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                item_attr *attr_data)
{
    /* item flags */
    attr_data->flags = it->flags;

    /* human readable expiretime */
    if (it->exptime == 0) {
        attr_data->exptime = it->exptime;
#ifdef ENABLE_STICKY_ITEM
    } else if (it->exptime == (rel_time_t)-1) {
        attr_data->exptime = it->exptime;
#endif
    } else {
        rel_time_t current_time = svcore->get_current_time();
        if (it->exptime <= current_time) {
            attr_data->exptime = (rel_time_t)-2;
        } else {
            attr_data->exptime = it->exptime - current_time;
        }
    }

    if (IS_COLL_ITEM(it)) {
        attr_data->type = GET_ITEM_TYPE(it);
        assert(attr_data->type < ITEM_TYPE_MAX);

        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
        if (attr_data->type == ITEM_TYPE_LIST) {
            ret = list_coll_getattr(it, attr_data, attr_ids, attr_count);
        } else if (attr_data->type == ITEM_TYPE_SET) {
            ret = set_coll_getattr(it, attr_data, attr_ids, attr_count);
        } else if (attr_data->type == ITEM_TYPE_MAP) {
            ret = map_coll_getattr(it, attr_data, attr_ids, attr_count);
        } else if (attr_data->type == ITEM_TYPE_BTREE) {
            ret = btree_coll_getattr(it, attr_data, attr_ids, attr_count);
        }
        if (ret != ENGINE_SUCCESS) {
            return ret;
        }
    } else {
        attr_data->type = ITEM_TYPE_KV;
        /* attribute validation check */
        for (int i = 0; i < attr_count; i++) {
            if (attr_ids[i] == ATTR_COUNT      || attr_ids[i] == ATTR_MAXCOUNT ||
                attr_ids[i] == ATTR_OVFLACTION || attr_ids[i] == ATTR_READABLE ||
                attr_ids[i] == ATTR_MAXBKEYRANGE || attr_ids[i] == ATTR_TRIMMED) {
                return ENGINE_EBADATTR;
            }
        }
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE item_getattr(const void *key, const uint32_t nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;

    LOCK_CACHE();
    it = do_item_get(key, nkey, DO_UPDATE);
    if (it == NULL) {
        ret = ENGINE_KEY_ENOENT;
    } else {
        /* get attrributes of the item */
        ret = do_item_getattr(it, attr_ids, attr_count, attr_data);
        if (ret != ENGINE_SUCCESS) {
            /* what should we do ? nothing */
        }
        do_item_release(it);
    }
    UNLOCK_CACHE();

    return ret;
}

static ENGINE_ERROR_CODE
do_item_setattr(hash_item *it, ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                item_attr *attr_data)
{
    bool expiretime_flag = false;

    for (int i = 0; i < attr_count; i++) {
        if (attr_ids[i] == ATTR_EXPIRETIME) {
#ifdef ENABLE_STICKY_ITEM
            /* do not allow sticky toggling */
            if ((it->exptime == (rel_time_t)-1 && attr_data->exptime != (rel_time_t)-1) ||
                (it->exptime != (rel_time_t)-1 && attr_data->exptime == (rel_time_t)-1)) {
                return ENGINE_EBADVALUE;
            }
#endif
            if (it->exptime != attr_data->exptime) {
                expiretime_flag = true;
            }
            break; /* found ATTR_EXPIRETIME */
        }
    }

    /* check and set collection attributes */
    if (IS_COLL_ITEM(it)) {
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
        if (IS_LIST_ITEM(it)) {
            ret = list_coll_setattr(it, attr_data, attr_ids, attr_count);
        } else if (IS_SET_ITEM(it)) {
            ret = set_coll_setattr(it, attr_data, attr_ids, attr_count);
        } else if (IS_MAP_ITEM(it)) {
            ret = map_coll_setattr(it, attr_data, attr_ids, attr_count);
        } else if (IS_BTREE_ITEM(it)) {
            ret = btree_coll_setattr(it, attr_data, attr_ids, attr_count);
        }
        if (ret != ENGINE_SUCCESS) {
            return ret;
        }
    }

    /* set the expiretime */
    if (expiretime_flag) {
        rel_time_t before_exptime = it->exptime;
        it->exptime = attr_data->exptime;
        if (before_exptime == 0 && it->exptime != 0) {
            /* exptime: 0 => positive value */
            /* When change the exptime, we must consider that
             * an item, whose exptime is 0, can be below the lowMK of LRU.
             * If the exptime of the item is changed to a positive value,
             * it might not reclaimed even if it's expired in the future.
             * To resolve this, we move it to the top of LRU list.
             */
            do_item_update(it, true); /* force LRU update */
        }
    }

    CLOG_ITEM_SETATTR(it, attr_ids, attr_count);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE item_setattr(const void *key, const uint32_t nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_SETATTR_EXPTIME);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it == NULL) {
        ret = ENGINE_KEY_ENOENT;
    } else {
        ret = do_item_setattr(it, attr_ids, attr_count, attr_data);
        if (ret != ENGINE_SUCCESS) {
            /* what should we do ? nothing */
        }
        do_item_release(it);
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

/*
 * Get elements from collection hash item
 */
static int do_coll_eresult_realloc(elems_result_t *eresult, uint32_t size)
{
    if (size <= eresult->elem_arrsz) {
        return 0; /* nothing to realloc */
    }

    if (eresult->elem_array != NULL) {
        free(eresult->elem_array);
    }

    eresult->elem_array = malloc(sizeof(void*) * size);
    if (eresult->elem_array != NULL) {
        eresult->elem_arrsz = size;
        eresult->elem_count = 0;
        return 0;
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to reallocate eresult(size=%u).\n", size);
        eresult->elem_arrsz = 0;
        eresult->elem_count = 0;
        return -1;
    }
}

int coll_elem_result_init(elems_result_t *eresult, uint32_t size)
{
    if (size > 0) {
        eresult->elem_array = malloc(sizeof(void*) * size);
        if (eresult->elem_array == NULL) {
            return -1; /* out of memory */
        }
        eresult->elem_arrsz = size;
    } else {
        eresult->elem_array = NULL;
        eresult->elem_arrsz = 0;
    }
    eresult->elem_count = 0;
    return 0;
}

void coll_elem_result_free(elems_result_t *eresult)
{
    if (eresult->elem_array != NULL) {
        free(eresult->elem_array);
        eresult->elem_array = NULL;
        eresult->elem_arrsz = 0;
    }
}

#define GET_ALIGN_SIZE(s,a) \
        (((s)%(a)) == 0 ? (s) : (s)+(a)-((s)%(a)))
ENGINE_ERROR_CODE coll_elem_get_all(hash_item *it, elems_result_t *eresult, bool lock_hold)
{
    coll_meta_info *info;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (lock_hold) LOCK_CACHE();
    do {
        if (!IS_COLL_ITEM(it)) {
            ret = ENGINE_EBADTYPE; break;
        }
        info = (coll_meta_info*)item_get_meta(it);
        if (info->ccnt <= 0) {
            ret = ENGINE_ELEM_ENOENT; break;
        }
        /* init and check result */
        if (eresult->elem_arrsz < info->ccnt) {
            uint32_t new_size = GET_ALIGN_SIZE(info->ccnt, 100);
            if (do_coll_eresult_realloc(eresult, new_size) < 0) {
                ret = ENGINE_ENOMEM; break;
            }
        }
        /* get all elements */
        if (IS_LIST_ITEM(it))       list_elem_get_all((list_meta_info*)info, eresult);
        else if (IS_SET_ITEM(it))   set_elem_get_all((set_meta_info*)info, eresult);
        else if (IS_MAP_ITEM(it))   map_elem_get_all((map_meta_info*)info, eresult);
        else if (IS_BTREE_ITEM(it)) btree_elem_get_all((btree_meta_info*)info, eresult);
    } while(0);
    if (lock_hold) UNLOCK_CACHE();

    return ret;
}

void coll_elem_result_release(elems_result_t *eresult, int type)
{
    switch(type) {
      case ITEM_TYPE_LIST:
           list_elem_release((list_elem_item**)eresult->elem_array, eresult->elem_count);
           break;
      case ITEM_TYPE_SET:
           set_elem_release((set_elem_item**)eresult->elem_array, eresult->elem_count);
           break;
      case ITEM_TYPE_MAP:
           map_elem_release((map_elem_item**)eresult->elem_array, eresult->elem_count);
           break;
      case ITEM_TYPE_BTREE:
           btree_elem_release((btree_elem_item**)eresult->elem_array, eresult->elem_count);
           break;
    }
}

/*
 * Item config functions
 */
bool item_conf_get_evict_to_free(void)
{
    bool value;
    LOCK_CACHE();
    value = config->evict_to_free;
    UNLOCK_CACHE();
    return value;
}

void item_conf_set_evict_to_free(bool value)
{
    LOCK_CACHE();
    config->evict_to_free = value;
    UNLOCK_CACHE();
}

/*
 * Item Scan Facility
 */
/* item scan macros */
#define ITEM_SCAN_MAX_ITEMS 128
#define ITEM_SCAN_MAX_ELEMS 1000

void item_scan_open(item_scan *sp, const char *prefix, const int nprefix, CB_SCAN_OPEN cb_scan_open)
{
    LOCK_CACHE();
    assoc_scan_init(&sp->asscan);
    if (cb_scan_open != NULL) {
        cb_scan_open(&sp->asscan);
    }
    UNLOCK_CACHE();
    sp->prefix = prefix;
    sp->nprefix = nprefix;
    sp->is_used = true;
}

int item_scan_getnext(item_scan *sp, void **item_array, elems_result_t *erst_array, int item_arrsz)
{
    hash_item *it;
    int item_limit = item_arrsz < ITEM_SCAN_MAX_ITEMS
                   ? item_arrsz : ITEM_SCAN_MAX_ITEMS;
    int elem_limit = erst_array ? ITEM_SCAN_MAX_ELEMS : 0;
    int item_count;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    LOCK_CACHE();
    item_count = assoc_scan_next(&sp->asscan, (hash_item**)item_array, item_limit, elem_limit);
    if (item_count > 0) {
        rel_time_t curtime = svcore->get_current_time();
        int i, nfound = 0;
        for (i = 0; i < item_count; i++) {
            it = (hash_item *)item_array[i];
            if ((it->iflag & ITEM_INTERNAL) != 0) { /* internal item */
                item_array[i] = NULL; continue;
            }
            if (do_item_isvalid(it, curtime) != true) { /* invalid item */
                item_array[i] = NULL; continue;
            }
            /* Is it the item of the given prefix ? */
#ifdef NESTED_PREFIX
            if (sp->nprefix >= 0 && !prefix_isincluded(it->pfxptr, sp->prefix, sp->nprefix)) {
                item_array[i] = NULL; continue;
            }
#else
            if (sp->nprefix >= 0 && !prefix_issame(it->pfxptr, sp->prefix, sp->nprefix)) {
                item_array[i] = NULL; continue;
            }
#endif
            /* Found the valid item */
            if (erst_array != NULL && IS_COLL_ITEM(it)) {
                ret = coll_elem_get_all(it, &erst_array[nfound], false);
                if (ret == ENGINE_ENOMEM) break;
            }
            ITEM_REFCOUNT_INCR(it);
            if (nfound < i) {
                item_array[nfound] = it;
            }
            nfound += 1;
        }
        item_count = nfound;
    } else {
        /* item_count == 0: not found item */
        /* item_count <  0: the end of scan */
    }
    UNLOCK_CACHE();

    if (ret == ENGINE_ENOMEM) {
        if (item_count > 0) {
            item_scan_release(sp, item_array, erst_array, item_count);
        }
        item_count = -2; /* OUT OF MEMORY */
    }
    return item_count;
}

void item_scan_release(item_scan *sp, void **item_array, elems_result_t *erst_array, int item_count)
{
    if (erst_array != NULL) {
        for (int i = 0; i < item_count; i++) {
            if (erst_array[i].elem_count > 0) {
                hash_item *it = (hash_item *)item_array[i];
                assert(IS_COLL_ITEM(it));
                coll_elem_result_release(&erst_array[i], GET_ITEM_TYPE(it));
                erst_array[i].elem_count = 0;
            }
        }
    }

    LOCK_CACHE();
    for (int i = 0; i < item_count; i++) {
        do_item_release(item_array[i]);
    }
    UNLOCK_CACHE();
}

void item_scan_close(item_scan *sp, CB_SCAN_CLOSE cb_scan_close, bool success)
{
    sp->prefix = NULL;
    sp->nprefix = 0;
    sp->is_used = false;

    LOCK_CACHE();
    assoc_scan_final(&sp->asscan);
    if (cb_scan_close != NULL) {
        cb_scan_close(success);
    }
    UNLOCK_CACHE();
}

/*
 * ITEM SCRUB functions
 */
static bool do_item_isstale(hash_item *it)
{
    assert(it != NULL);
#ifdef ENABLE_CLUSTER_AWARE
    if ((it->iflag & ITEM_INTERNAL) == 0 &&
        !svcore->is_my_key(item_get_key(it),it->nkey)) {
        return true; /* stale data */
    }
#endif
    return false; /* not-stale data */
}

static void *item_scrubber_main(void *arg)
{
    struct default_engine *engine = arg;
    struct engine_scrubber *scrubber = &engine->scrubber;
    struct assoc_scan scan;
    hash_item *item_array[MAXIMUM_SCRUB_COUNT];
    int        item_count;
    int        scan_execs = 0; /* the number of scan executions */
    int        scan_break = 1; /* break after N scan executions.
                                * N = 1 is the best choice, we think.
                                */
    rel_time_t current_time = svcore->get_current_time();
    assert(scrubber->running == true);

again:
    LOCK_CACHE();
    assoc_scan_init(&scan);
    while (engine->initialized && !scrubber->restart) {
        /* scan and scrub cache items */
        /* NOTE: scrub_count can be changed while scrubbing */
        item_count = assoc_scan_next(&scan, item_array, config->scrub_count, 0);
        if (item_count < 0) { /* reached to the end */
            break;
        }
        /* Currently, item_count > 0.
         * See the internals of assoc_scan_next(). It does not return 0.
         */
        for (int i = 0; i < item_count; i++) {
            scrubber->visited++;
            if (do_item_isvalid(item_array[i], current_time) == false) {
                do_item_unlink(item_array[i], ITEM_UNLINK_INVALID);
                scrubber->cleaned++;
            }
            else if (scrubber->runmode == SCRUB_MODE_STALE &&
                     do_item_isstale(item_array[i]) == true) {
                do_item_unlink(item_array[i], ITEM_UNLINK_STALE);
                scrubber->cleaned++;
            }
        }

        UNLOCK_CACHE();
        if (++scan_execs >= scan_break) {
            struct timespec sleep_time = {0, (64*1000)};
            nanosleep(&sleep_time, NULL); /* 64 usec */
            scan_execs = 0;
        }
        TRYLOCK_CACHE(5);
    }
    assoc_scan_final(&scan);
    UNLOCK_CACHE();

    bool restart = false;
    pthread_mutex_lock(&scrubber->lock);
    if ((restart = scrubber->restart)) {
        scrubber->started = time(NULL);
        scrubber->visited = 0;
        scrubber->cleaned = 0;
        scrubber->restart = false;
    } else {
        scrubber->stopped = time(NULL);
        scrubber->running = false;
    }
    pthread_mutex_unlock(&scrubber->lock);
    if (restart) {
        goto again; /* restart */
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "Scrub is done.\n");
    return NULL;
}

bool item_start_scrub(struct default_engine *engine, int mode, bool autorun)
{
    assert(mode == (int)SCRUB_MODE_NORMAL || mode == (int)SCRUB_MODE_STALE);
    struct engine_scrubber *scrubber = &engine->scrubber;
    pthread_t tid;
    pthread_attr_t attr;
    bool restarted = false;
    bool ok = false;

    pthread_mutex_lock(&scrubber->lock);
    if (scrubber->enabled) {
        if (scrubber->running) {
            if (autorun == true && mode == SCRUB_MODE_STALE) {
                scrubber->restart = true;
                scrubber->runmode = (enum scrub_mode)mode;
                ok = restarted = true;
            }
        } else {
            scrubber->started = time(NULL);
            scrubber->stopped = 0;
            scrubber->visited = 0;
            scrubber->cleaned = 0;
            scrubber->restart = false;
            scrubber->runmode = (enum scrub_mode)mode;
            scrubber->running = true;
            ok = true;

            if (pthread_attr_init(&attr) != 0 ||
                pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
                pthread_create(&tid, &attr, item_scrubber_main, engine) != 0)
            {
                scrubber->running = false;
                ok = false;
            }
        }
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to start scrub. Scrub is disabled.\n");
    }
    pthread_mutex_unlock(&scrubber->lock);

    char *scrub_autostr = (autorun ? "auto" : "manual");
    char *scrub_modestr = (mode == SCRUB_MODE_STALE ? "scrub stale" : "scrub");
    if (ok) {
        if (restarted) {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "The %s %s has restarted.\n", scrub_autostr, scrub_modestr);
        } else {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "The %s %s has newly started.\n", scrub_autostr, scrub_modestr);
        }
    } else {
        if (scrubber->running) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to start %s %s. Scrub is already running.\n",
                        scrub_autostr, scrub_modestr);
        } else {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to start %s %s. Cannot create scrub thread.\n",
                        scrub_autostr, scrub_modestr);
        }
    }
    return ok;
}

bool item_onoff_scrub(struct default_engine *engine, bool val)
{
    struct engine_scrubber *scrubber = &engine->scrubber;
    bool old_val;

    pthread_mutex_lock(&scrubber->lock);
    /* The scrubber can be disabled even if it is running.
     * The on-going scrubbing continues its task. But, the next
     * scrubbing cannot be started until it is enabled again.
     */
    old_val = scrubber->enabled;
    scrubber->enabled = val;
    pthread_mutex_unlock(&scrubber->lock);
    return old_val;
}

void item_stats_scrub(struct default_engine *engine,
                      ADD_STAT add_stat, const void *cookie)
{
    struct engine_scrubber *scrubber = &engine->scrubber;

    pthread_mutex_lock(&scrubber->lock);
    if (scrubber->enabled) {
        if (scrubber->running)
            add_stat("scrubber:status", 15, "running", 7, cookie);
        else
            add_stat("scrubber:status", 15, "stopped", 7, cookie);
    } else {
        add_stat("scrubber:status", 15, "disabled", 8, cookie);
    }
    if (scrubber->started != 0) {
        char val[128];
        int len;
        if (scrubber->runmode == SCRUB_MODE_NORMAL) {
            add_stat("scrubber:run_mode", 17, "scrub", 5, cookie);
        } else {
            add_stat("scrubber:run_mode", 17, "scrub stale", 11, cookie);
        }
        if (scrubber->stopped != 0) {
            time_t diff = scrubber->stopped - scrubber->started;
            len = sprintf(val, "%"PRIu64, (uint64_t)diff);
            add_stat("scrubber:last_run", 17, val, len, cookie);
        }
        len = sprintf(val, "%"PRIu64, scrubber->visited);
        add_stat("scrubber:visited", 16, val, len, cookie);
        len = sprintf(val, "%"PRIu64, scrubber->cleaned);
        add_stat("scrubber:cleaned", 16, val, len, cookie);
    }
    pthread_mutex_unlock(&scrubber->lock);
}

/*
 * Dump all cache items.
 * Currently, only key strings of cache items can be dumped.
 * The values of cache items will be dumped in later version.
 */
static int item_dump_space_key(hash_item *it)
{
    /* dump format : < type, key, exptime > */
    return (int)it->nkey + 15;
}

static int item_dump_write_key(char *buffer, hash_item *it, rel_time_t mc_curtime)
{
    char *bufptr = buffer;
    int   length = 0;

    /* dump format : < type, key, exptime > */
    /* item type: L(list), S(set), M(map), B(b+tree), K(kv) */
    if (IS_LIST_ITEM(it))       memcpy(bufptr, "L ", 2);
    else if (IS_SET_ITEM(it))   memcpy(bufptr, "S ", 2);
    else if (IS_MAP_ITEM(it))   memcpy(bufptr, "M ", 2);
    else if (IS_BTREE_ITEM(it)) memcpy(bufptr, "B ", 2);
    else                        memcpy(bufptr, "K ", 2);
    bufptr += 2;
    length += 2;

    /* key string */
    memcpy(bufptr, item_get_key(it), it->nkey);
    bufptr += it->nkey;
    length += it->nkey;

    /* exptime and new line */
    if (it->exptime == 0) {
        memcpy(bufptr, " 0\n", 3);
        length += 3;
#ifdef ENABLE_STICKY_ITEM
    } else if (it->exptime == (rel_time_t)-1) {
        memcpy(bufptr, " -1\n", 4);
        length += 4;
#endif
    } else {
        if (it->exptime > mc_curtime) {
            snprintf(bufptr, 13, " %u\n", it->exptime - mc_curtime);
            length += strlen(bufptr);
        } else {
            memcpy(bufptr, " -2\n", 4); /* this case may not occur */
            length += 4;
        }
    }

    return length; /* total length */
}

/* dump constants */
#define DUMP_BUFFER_SIZE (64 * 1024)
#define SCAN_ITEM_ARRAY_SIZE 64

/* dump space & write functions */
typedef int (*DUMP_SPACE_FUNC)(hash_item *it);
typedef int (*DUMP_WRITE_FUNC)(char *buffer, hash_item *it, rel_time_t mc_curtime);

static void item_dumper_done(void *engine_ptr)
{
    struct default_engine *engine = engine_ptr;
    struct engine_dumper *dumper = &engine->dumper;

    pthread_mutex_lock(&dumper->lock);
    dumper->running = false;
    pthread_mutex_unlock(&dumper->lock);
}

static void *item_dumper_main(void *arg)
{
    struct default_engine *engine = arg;
    struct engine_dumper *dumper = &engine->dumper;
    int        array_size=SCAN_ITEM_ARRAY_SIZE;
    int        item_count;
    hash_item *item_array[SCAN_ITEM_ARRAY_SIZE];
    hash_item *it;
    item_scan scan;
    int fd, ret = 0;
    int i, nwritten;
    int cur_buflen = 0;
    int max_buflen = DUMP_BUFFER_SIZE;
    static char dump_buffer[DUMP_BUFFER_SIZE];
    char *cur_bufptr = dump_buffer;
    rel_time_t memc_curtime; /* current time of cache server */
    int str_length;
    DUMP_SPACE_FUNC dump_space_func = NULL;
    DUMP_WRITE_FUNC dump_write_func = NULL;

    assert(dumper->running == true);

    /* set dump functions */
    if (dumper->mode == DUMP_MODE_KEY) {
        dump_space_func = item_dump_space_key;
        dump_write_func = item_dump_write_key;
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "Invalid dump mode=%d\n",
                    (int)dumper->mode);
        ret = -1; goto done;
    }

    fd = open(dumper->filepath, O_WRONLY | O_CREAT | O_TRUNC,
                                S_IRUSR | S_IWUSR | S_IRGRP);
    if (fd < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to open the dump file. path=%s err=%s\n",
                    dumper->filepath, strerror(errno));
        ret = -1; goto done;
    }

    item_scan_open(&scan, dumper->prefix, dumper->nprefix, NULL);
    while (true) {
        if (!engine->initialized || dumper->stop) {
            logger->log(EXTENSION_LOG_INFO, NULL, "Stop the current dump.\n");
            ret = -1; break;
        }
        item_count = item_scan_getnext(&scan, (void**)item_array, NULL, array_size);
        if (item_count < 0) { /* reached to the end */
            break;
        }
        if (item_count == 0) { /* No valid item found */
            continue; /* we continue the scan */
        }
        /* write key string to buffer */
        memc_curtime = svcore->get_current_time();
        for (i = 0; i < item_count; i++) {
            it = item_array[i];
            if ((cur_buflen + dump_space_func(it)) > max_buflen) {
                nwritten = write(fd, dump_buffer, cur_buflen);
                if (nwritten != cur_buflen) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "Failed to write the dump: "
                                "nwritten(%d) != writelen(%d)\n", nwritten, cur_buflen);
                    ret = -1; break;
                }
                cur_buflen = 0;
                cur_bufptr = dump_buffer;
            }
            str_length = dump_write_func(cur_bufptr, it, memc_curtime);
            cur_bufptr += str_length;
            cur_buflen += str_length;
            dumper->dumpped++;
        }
        item_scan_release(&scan, (void**)item_array, NULL, item_count);
        if (ret != 0) break;
    }
    item_scan_close(&scan, NULL, (ret==0));

    if (ret == 0) {
        int summary_length = 256; /* just, enough memory space size */
        if ((cur_buflen + summary_length) > max_buflen) {
            nwritten = write(fd, dump_buffer, cur_buflen);
            if (nwritten != cur_buflen) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "Failed to write the dump: "
                            "nwritten(%d) != writelen(%d)\n", nwritten, cur_buflen);
                ret = -1;
            }
            cur_buflen = 0;
            cur_bufptr = dump_buffer;
        }
        if (ret == 0) {
            snprintf(cur_bufptr, summary_length, "DUMP SUMMARY: "
                     "{ prefix=%s, count=%"PRIu64", elapsed=%"PRIu64" }\n",
                     dumper->nprefix > 0 ? dumper->prefix :
                     (dumper->nprefix == 0 ? "<null>" : "<all>"),
                     dumper->dumpped, (uint64_t)(time(NULL)-dumper->started));
            cur_buflen += strlen(cur_bufptr);
            nwritten = write(fd, dump_buffer, cur_buflen);
            if (nwritten != cur_buflen) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "Failed to write the dump: "
                            "nwritten(%d) != writelen(%d)\n", nwritten, cur_buflen);
                ret = -1;
            }
        }
    }
    close(fd);

done:
    dumper->success = (ret == 0 ? true : false);
    dumper->stopped = time(NULL);
    item_dumper_done(engine);
    return NULL;
}

static enum dump_mode do_item_dump_mode_check(const char *modestr)
{
    enum dump_mode mode = DUMP_MODE_MAX;;

    if (memcmp(modestr, "key", 3) == 0) {
        mode = DUMP_MODE_KEY;
    }
    return mode;
}

ENGINE_ERROR_CODE item_dump_start(struct default_engine *engine,
                                  const char *modestr,
                                  const char *prefix, const int nprefix,
                                  const char *filepath)
{
    struct engine_dumper *dumper = &engine->dumper;
    pthread_t tid;
    pthread_attr_t attr;
    enum dump_mode mode;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    mode = do_item_dump_mode_check(modestr);
    if (mode == DUMP_MODE_MAX) {
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "NOT supported dump mode(%s)\n", modestr);
        return ENGINE_ENOTSUP; /* NOT supported */
    }

    pthread_mutex_lock(&dumper->lock);
    do {
        if (dumper->running) {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "Failed to start dumping. Already started.\n");
            ret = ENGINE_FAILED; break;
        }

        snprintf(dumper->filepath, MAX_FILEPATH_LENGTH-1, "%s",
                (filepath != NULL ? filepath : "arcus_dump.txt"));
        dumper->prefix = (char*)prefix;
        dumper->nprefix = nprefix;
        dumper->mode = mode;
        dumper->started = time(NULL);
        dumper->stopped = 0;
        dumper->dumpped = 0;
        dumper->success = false;
        dumper->stop    = false;

#ifdef ENABLE_PERSISTENCE
        if (mode == DUMP_MODE_SNAPSHOT) {
            dumper->running = true;
            ret = chkpt_snapshot_start(CHKPT_SNAPSHOT_MODE_DATA, prefix, nprefix,
                                       filepath, item_dumper_done);
            if (ret != ENGINE_SUCCESS) {
                dumper->running = false;
            }
            break;
        }
#endif

        /* check if filepath is valid ? */
        int fd = open(dumper->filepath, O_WRONLY | O_CREAT | O_TRUNC,
                                         S_IRUSR | S_IWUSR | S_IRGRP);
        if (fd < 0) {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "Failed to open the dump file. path=%s err=%s\n",
                        dumper->filepath, strerror(errno));
            ret = ENGINE_FAILED; break;
        }
        close(fd);

        dumper->running = true;

        if (pthread_attr_init(&attr) != 0 ||
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
            pthread_create(&tid, &attr, item_dumper_main, engine) != 0)
        {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "Failed to create the dump thread. err=%s\n", strerror(errno));
            dumper->running = false;
            ret = ENGINE_FAILED; break;
        }
    } while(0);
    pthread_mutex_unlock(&dumper->lock);

    return ret;
}

void item_dump_stop(struct default_engine *engine)
{
    struct engine_dumper *dumper = &engine->dumper;

    pthread_mutex_lock(&dumper->lock);
    if (dumper->running) {
#ifdef ENABLE_PERSISTENCE
        if (dumper->mode == DUMP_MODE_SNAPSHOT) {
            chkpt_snapshot_stop();
        }
#endif
        /* stop the dumper */
        dumper->stop = true;
    }
    pthread_mutex_unlock(&dumper->lock);
}

static void do_item_dump_stats(struct engine_dumper *dumper,
                               ADD_STAT add_stat, const void *cookie)
{
    char val[MAX_FILEPATH_LENGTH];
    int len;

    if (dumper->running) {
        add_stat("dumper:status", 13, "running", 7, cookie);
    } else {
        add_stat("dumper:status", 13, "stopped", 7, cookie);
        if (dumper->success)
            add_stat("dumper:success", 14, "true", 4, cookie);
        else
            add_stat("dumper:success", 14, "false", 5, cookie);
    }
    if (dumper->started != 0) {
        if (dumper->mode == DUMP_MODE_KEY) {
            add_stat("dumper:mode", 11, "key", 3, cookie);
        } else if (dumper->mode == DUMP_MODE_ITEM) {
            add_stat("dumper:mode", 11, "item", 4, cookie);
        } else if (dumper->mode == DUMP_MODE_RCOUNT) {
            add_stat("dumper:mode", 11, "rcount", 6, cookie);
        } else if (dumper->mode == DUMP_MODE_SNAPSHOT) {
            add_stat("dumper:mode", 11, "snapshot", 8, cookie);
        } else {
            add_stat("dumper:mode", 11, "unknown", 7, cookie);
        }
        if (dumper->stopped != 0) {
            time_t diff = dumper->stopped - dumper->started;
            len = sprintf(val, "%"PRIu64, (uint64_t)diff);
            add_stat("dumper:last_run", 15, val, len, cookie);
        }
        len = sprintf(val, "%"PRIu64, dumper->dumpped);
        add_stat("dumper:dumped", 13, val, len, cookie);
        if (dumper->nprefix > 0) {
            len = sprintf(val, "%s", dumper->prefix);
            add_stat("dumper:prefix", 13, val, len, cookie);
        } else if (dumper->nprefix == 0) {
            add_stat("dumper:prefix", 13, "<null>", 6, cookie);
        } else {
            add_stat("dumper:prefix", 13, "<all>", 5, cookie);
        }
        if (strlen(dumper->filepath) > 0) {
            len = sprintf(val, "%s", dumper->filepath);
            add_stat("dumper:filepath", 15, val, len, cookie);
        }
    }
}

void item_dump_stats(struct default_engine *engine,
                     ADD_STAT add_stat, const void *cookie)
{
    struct engine_dumper *dumper = &engine->dumper;

    pthread_mutex_lock(&dumper->lock);
    do {
#ifdef ENABLE_PERSISTENCE
        if (dumper->mode == DUMP_MODE_SNAPSHOT) {
            chkpt_snapshot_stats(add_stat, cookie);
            break;
        }
#endif
        do_item_dump_stats(dumper, add_stat, cookie);
    } while(0);
    pthread_mutex_unlock(&dumper->lock);
}


/*
 * Item Apply Funtions
 */
ENGINE_ERROR_CODE item_apply_kv_link(void *engine, const char *key, const uint32_t nkey,
                                     const uint32_t flags, const rel_time_t exptime,
                                     const uint32_t nbytes, const char *value,
                                     const uint64_t cas)
{
    hash_item *old_it;
    hash_item *new_it;
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL,
                "item_apply_kv_link. key=%.*s nkey=%u nbytes=%u\n",
                PRINT_NKEY(nkey), key, nkey, nbytes);

    LOCK_CACHE();
    old_it = do_item_get(key, nkey, DONT_UPDATE);
    new_it = do_item_alloc(key, nkey, flags, exptime, nbytes, NULL); /* cookie is NULL */
    if (new_it) {
        /* Assume data is small, and copying with lock held is okay : FIXME */
        memcpy(item_get_data(new_it), value, nbytes);

        /* Now link the new item into the cache hash table */
        if (old_it) {
            do_item_replace(old_it, new_it);
            do_item_release(old_it);
            ret = ENGINE_SUCCESS;
        } else {
            ret = do_item_link(new_it);
        }
        if (ret == ENGINE_SUCCESS) {
            /* Override the cas with the given cas. */
            item_set_cas(new_it, cas);
        } else {
            do_item_free(new_it);
        }
    } else {
        ret = ENGINE_ENOMEM;
        if (old_it) { /* Remove inconsistent hash_item */
            do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
            do_item_release(old_it);
        }
    }
    UNLOCK_CACHE();

    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "item_apply_kv_link failed. key=%.*s nkey=%u, code=%d\n",
                    PRINT_NKEY(nkey), key, nkey, ret);
    }
    return ret;
}

ENGINE_ERROR_CODE item_apply_unlink(void *engine, const char *key, const uint32_t nkey)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "item_apply_unlink. key=%.*s nkey=%u\n",
                PRINT_NKEY(nkey), key, nkey);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it) {
        do_item_unlink(it, ITEM_UNLINK_NORMAL); /* must unlink first. */
        do_item_release(it);
    } else {
        /* The item might have been reclaimed or evicted */
        logger->log(EXTENSION_LOG_DEBUG, NULL,
                    "item_apply_unlink failed. not found key=%.*s nkey=%u\n",
                    PRINT_NKEY(nkey), key, nkey);
        ret = ENGINE_KEY_ENOENT;
    }
    UNLOCK_CACHE();
    /* Does not matter whether the key exists or not.
     * The caller uses ENOENT for statistics purposes.
     */
    return ret;
}

ENGINE_ERROR_CODE item_apply_setattr_exptime(void *engine, const char *key, const uint32_t nkey,
                                             rel_time_t exptime)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "item_apply_setattr_exptime. key=%.*s nkey=%u\n",
                PRINT_NKEY(nkey), key, nkey);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it) {
        it->exptime = exptime;
        do_item_release(it);
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_setattr_exptime failed."
                    " not found key=%.*s nkey=%u\n",
                    PRINT_NKEY(nkey), key, nkey);
        ret = ENGINE_KEY_ENOENT;
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE item_apply_setattr_collinfo(void *engine, hash_item *it,
                                              rel_time_t exptime, const int32_t maxcount,
                                              const uint8_t ovflact, const uint8_t mflags,
                                              bkey_t *maxbkeyrange)
{
    const char *key = item_get_key(it);
    coll_meta_info *info;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "item_apply_setattr_collinfo. key=%.*s nkey=%u\n",
                PRINT_NKEY(it->nkey), key, it->nkey);

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_setattr_collinfo failed."
                        " invalid item. key=%.*s = nkey=%u\n",
                        PRINT_NKEY(it->nkey), key, it->nkey);
            ret = ENGINE_KEY_ENOENT; break;
        }
        if (!IS_COLL_ITEM(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_setattr_collinfo failed."
                        " The item is not a collection. key=%.*s nkey=%u\n",
                        PRINT_NKEY(it->nkey), key, it->nkey);
            ret = ENGINE_EBADTYPE; break;
        }
        if (maxbkeyrange != NULL && !IS_BTREE_ITEM(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_setattr_collinfo failed."
                        " The item is not a btree. key=%.*s nkey=%u\n",
                        PRINT_NKEY(it->nkey), key, it->nkey);
            ret = ENGINE_EBADTYPE; break;
        }
        it->exptime = exptime;
        info = (coll_meta_info*)item_get_meta(it);
        info->mcnt = maxcount;
        info->ovflact = ovflact;
        info->mflags = mflags;
        if (maxbkeyrange) {
            ((btree_meta_info*)info)->maxbkeyrange = *maxbkeyrange;
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent has_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
    }
    UNLOCK_CACHE();

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE item_apply_lru_update(void *engine, const char *key, const uint32_t nkey)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "item_apply_lru_update. key=%.*s nkey=%u\n",
                PRINT_NKEY(nkey), key, nkey);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it) {
        do_item_update(it, true); /* force the LRU update */
        do_item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE item_apply_flush(void *engine, const char *prefix, const int nprefix)
{
    ENGINE_ERROR_CODE ret;

    logger->log(ITEM_APPLY_LOG_LEVEL, NULL, "item_apply_flush. prefix=%s nprefix=%d\n",
                prefix ? prefix : "<null>", nprefix);

    LOCK_CACHE();
    ret = do_item_flush_expired(prefix, nprefix, 0 /* right now */, NULL);
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE item_init(struct default_engine *engine_ptr)
{
    /* initialize global variables */
    engine = engine_ptr;
    config = &engine->config;
    itemsp = &engine->items;
    svcore = engine->server.core;
    logger = engine->server.log->get_logger();

    /* check maximum collection size */
    assert(DEFAULT_LIST_SIZE <= config->max_list_size);
    assert(DEFAULT_SET_SIZE <= config->max_set_size);
    assert(DEFAULT_MAP_SIZE <= config->max_map_size);
    assert(DEFAULT_BTREE_SIZE <= config->max_btree_size);

    logger->log(EXTENSION_LOG_INFO, NULL, "default/maximum list  size = (%u/%u)\n",
                DEFAULT_LIST_SIZE, config->max_list_size);
    logger->log(EXTENSION_LOG_INFO, NULL, "default/maximum set   size = (%u/%u)\n",
                DEFAULT_SET_SIZE, config->max_set_size);
    logger->log(EXTENSION_LOG_INFO, NULL, "default/maximum map   size = (%u/%u)\n",
                DEFAULT_MAP_SIZE, config->max_map_size);
    logger->log(EXTENSION_LOG_INFO, NULL, "default/maximum btree size = (%u/%u)\n",
                DEFAULT_BTREE_SIZE, config->max_btree_size);

    item_clog_init(engine);
    if (item_base_init(engine) < 0) {
        return ENGINE_FAILED;
    }
    item_list_coll_init(engine);
    item_set_coll_init(engine);
    item_map_coll_init(engine);
    item_btree_coll_init(engine);

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM module initialized.\n");
    return ENGINE_SUCCESS;
}

void item_final(struct default_engine *engine_ptr)
{
    if (itemsp == NULL) {
        return; /* nothing to do */
    }

    assert(engine == engine_ptr);
    if (engine->dumper.running) {
        item_dump_stop(engine);
    }

    /* wait until scrubber thread is finished */
    int sleep_count = 0;
    while (engine->scrubber.running) {
        usleep(1000); // 1ms;
        sleep_count++;
    }
    if (sleep_count > 100) { // waited too long
        logger->log(EXTENSION_LOG_INFO, NULL,
                "Waited %d ms for scrubber to be stopped.\n", sleep_count);
    }

    /* wait until dumper thread is finished. */
    sleep_count = 0;
    while (engine->dumper.running) {
        usleep(1000); // 1ms;
        sleep_count++;
    }
    if (sleep_count > 100) { // waited too long
        logger->log(EXTENSION_LOG_INFO, NULL,
                "Waited %d ms for dumper to be stopped.\n", sleep_count);
    }

    item_base_final(engine);
    item_list_coll_final(engine);
    item_set_coll_final(engine);
    item_map_coll_final(engine);
    item_btree_coll_final(engine);
    item_clog_final(engine);
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM module destroyed.\n");
}
