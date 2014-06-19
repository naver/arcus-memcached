/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "default_engine.h"

/* Forward Declarations */
static void item_link_q(struct default_engine *engine, hash_item *it);
static void item_unlink_q(struct default_engine *engine, hash_item *it);
static hash_item *do_item_alloc(struct default_engine *engine,
                                const void *key, const size_t nkey,
                                const int flags, const rel_time_t exptime,
                                const int nbytes, const void *cookie);
static hash_item *do_item_get(struct default_engine *engine,
                              const char *key, const size_t nkey, bool LRU_reposition);
static ENGINE_ERROR_CODE do_item_link(struct default_engine *engine, hash_item *it);
static void do_item_unlink(struct default_engine *engine, hash_item *it);
static void do_item_release(struct default_engine *engine, hash_item *it);
static void do_item_update(struct default_engine *engine, hash_item *it);
static void do_item_lru_reposition(struct default_engine *engine, hash_item *it);
static ENGINE_ERROR_CODE do_item_replace(struct default_engine *engine, hash_item *it, hash_item *new_it);
static void item_free(struct default_engine *engine, hash_item *it);
static void push_coll_del_queue(struct default_engine *engine, hash_item *it);
static void do_coll_all_elem_delete(struct default_engine *engine, hash_item *it);
extern int  genhash_string_hash(const void* p, size_t nkey);

/*
 * We only reposition items in the LRU queue if they haven't been repositioned
 * in this many seconds. That saves us from churning on frequently-accessed
 * items.
 */
#define ITEM_UPDATE_INTERVAL 60

/* LRU id of small memory items */
#define LRU_CLSID_FOR_SMALL 0

/* item type checking */
#define IS_LIST_ITEM(it)  (((it)->iflag & ITEM_IFLAG_LIST) != 0)
#define IS_SET_ITEM(it)   (((it)->iflag & ITEM_IFLAG_SET) != 0)
#define IS_BTREE_ITEM(it) (((it)->iflag & ITEM_IFLAG_BTREE) != 0)
#define IS_COLL_ITEM(it)  (((it)->iflag & ITEM_IFLAG_COLL) != 0)

/* btree item status */
#define BTREE_ITEM_STATUS_USED   2
#define BTREE_ITEM_STATUS_UNLINK 1
#define BTREE_ITEM_STATUS_FREE   0

/* btree scan direction */
#define BTREE_DIRECTION_PREV 2
#define BTREE_DIRECTION_NEXT 1
#define BTREE_DIRECTION_NONE 0

/* bkey type */
#define BKEY_TYPE_UNKNOWN 0
#define BKEY_TYPE_UINT64  1
#define BKEY_TYPE_BINARY  2

/* binary bkey min & max length */
#define BKEY_MIN_BINARY_LENG 1
#define BKEY_MAX_BINARY_LENG MAX_BKEY_LENG

/* uint64 bkey min & max value */
#define BTREE_UINT64_MIN_BKEY 0
#define BTREE_UINT64_MAX_BKEY (uint64_t)((int64_t)-1) /* need check */

/* btree element item or btree node item */
#define BTREE_GET_ELEM_ITEM(node, indx) ((btree_elem_item *)((node)->item[indx]))
#define BTREE_GET_NODE_ITEM(node, indx) ((btree_indx_node *)((node)->item[indx]))

/* get bkey real size */
#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))

/* get btree element size */
#define BTREE_ELEM_SIZE(elem) \
        (sizeof(btree_elem_item_fixed) + BTREE_REAL_NBKEY(elem->nbkey) + elem->neflag + elem->nbytes)

/* overflow type */
#define OVFL_TYPE_NONE  0
#define OVFL_TYPE_COUNT 1
#define OVFL_TYPE_RANGE 2

/* bkey range type */
#define BKEY_RANGE_TYPE_SIN 1 /* single bkey */
#define BKEY_RANGE_TYPE_ASC 2 /* ascending bkey range */
#define BKEY_RANGE_TYPE_DSC 3 /* descending bkey range */

/* special address for representing unlinked status */
#define ADDR_MEANS_UNLINKED  1

/* btree position debugging */
static bool btree_position_debug = false;

/* min & max bkey constants */
static uint64_t      btree_uint64_min_bkey = BTREE_UINT64_MIN_BKEY;
static uint64_t      btree_uint64_max_bkey = BTREE_UINT64_MAX_BKEY;
static unsigned char btree_binary_min_bkey[BKEY_MIN_BINARY_LENG] = { 0x00 };
static unsigned char btree_binary_max_bkey[BKEY_MAX_BINARY_LENG] = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                                                     0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                                                     0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                                                     0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

static EXTENSION_LOGGER_DESCRIPTOR *logger;

void item_stats_reset(struct default_engine *engine)
{
    pthread_mutex_lock(&engine->cache_lock);
    memset(engine->items.itemstats, 0, sizeof(engine->items.itemstats));
    pthread_mutex_unlock(&engine->cache_lock);
}

/* warning: don't use these macros with a function, as it evals its arg twice */
static inline size_t ITEM_ntotal(struct default_engine *engine, const hash_item *item)
{
    size_t ret;
    if (IS_COLL_ITEM(item)) {
        ret = sizeof(*item) + META_OFFSET_IN_ITEM(item->nkey, item->nbytes);
        if (IS_LIST_ITEM(item))     ret += sizeof(list_meta_info);
        else if (IS_SET_ITEM(item)) ret += sizeof(set_meta_info);
        else /* BTREE_ITEM */       ret += sizeof(btree_meta_info);
    } else {
        ret = sizeof(*item) + item->nkey + item->nbytes;
    }
    if (engine->config.use_cas) {
        ret += sizeof(uint64_t);
    }
    return ret;
}

static inline size_t ITEM_stotal(struct default_engine *engine, const hash_item *item)
{
    size_t ntotal = ITEM_ntotal(engine, item);
    size_t stotal = slabs_space_size(engine, ntotal);
    if (IS_COLL_ITEM(item)) {
        coll_meta_info *info = (coll_meta_info *)item_get_meta(item);
        stotal += info->stotal;
    }
    return stotal;
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
         fprintf(stderr, "item %x refcnt(%c) %d %c%c%c\n", \
                        it, op, it->refcount, \
                        (it->it_flags & ITEM_LINKED) ? 'L' : ' ', \
                        (it->it_flags & ITEM_SLABBED) ? 'S' : ' ')
#else
# define DEBUG_REFCNT(it,op) while(0)
#endif

static void increase_collection_space(struct default_engine *engine, ENGINE_ITEM_TYPE item_type,
                                      coll_meta_info *info, const size_t inc_space)
{
    info->stotal += inc_space;
    /* Currently, stats.lock is useless since global cache lock is held. */
    //pthread_mutex_lock(&engine->stats.lock);
#ifdef ENABLE_STICKY_ITEM
    if ((info->mflags & COLL_META_FLAG_STICKY) != 0)
        engine->stats.sticky_bytes += inc_space;
#endif
    assoc_prefix_update_size(info->prefix, item_type, inc_space, true);
    engine->stats.curr_bytes += inc_space;
    //pthread_mutex_unlock(&engine->stats.lock);
}

static void decrease_collection_space(struct default_engine *engine, ENGINE_ITEM_TYPE item_type,
                                      coll_meta_info *info, const size_t dec_space)
{
    assert(info->stotal >= dec_space);
    info->stotal -= dec_space;
    /* Currently, stats.lock is useless since global cache lock is held. */
    //pthread_mutex_lock(&engine->stats.lock);
#ifdef ENABLE_STICKY_ITEM
    if ((info->mflags & COLL_META_FLAG_STICKY) != 0)
        engine->stats.sticky_bytes -= dec_space;
#endif
    assoc_prefix_update_size(info->prefix, item_type, dec_space, false);
    engine->stats.curr_bytes -= dec_space;
    //pthread_mutex_unlock(&engine->stats.lock);
}

static bool do_item_isvalid(struct default_engine *engine, hash_item *it, rel_time_t current_time)
{
    /* check if it's expired */
#ifdef ENABLE_STICKY_ITEM
    /* The sticky item has an exptime((rel_tiem_t)(-1)) larger than current_item.
     * So, it cannot be expired.
     **/
#endif
    if (it->exptime != 0 && it->exptime <= current_time) {
        return false; /* expired */
    }
    /* check junk item time if it was set */
    if (engine->config.junk_item_time != 0) {
        if ((current_time - it->time) > engine->config.junk_item_time)
            return false; /* Not accessed for a very long time => junk item */
    }
    /* check flushed items as well as expired items */
    if (engine->config.oldest_live != 0) {
        if (engine->config.oldest_live <= current_time && it->time <= engine->config.oldest_live)
            return false; /* flushed by flush_all */
    }
    /* check if prefix is valid */
    if (assoc_prefix_isvalid(engine, it) == false) {
        return false;
    }
    return true; /* Yes, it's a valid item */
}

static hash_item *do_item_reclaim(struct default_engine *engine, hash_item *it,
                                  const size_t ntotal, const unsigned int clsid,
                                  const unsigned int lruid)
{
    /* increment # of reclaimed */
    pthread_mutex_lock(&engine->stats.lock);
    engine->stats.reclaimed++;
    pthread_mutex_unlock(&engine->stats.lock);
    engine->items.itemstats[lruid].reclaimed++;

    /* it->refcount == 0 */
#ifdef USE_SINGLE_LRU_LIST
#else
    if (lruid != LRU_CLSID_FOR_SMALL) {
        it->refcount = 1;
        slabs_adjust_mem_requested(engine, it->slabs_clsid, ITEM_ntotal(engine,it), ntotal);
        do_item_unlink(engine, it);
        /* Initialize the item block: */
        it->slabs_clsid = 0;
        it->refcount = 0;
        return it;
    }
    /* collection item or small-sized kv item */
#endif
    if (IS_COLL_ITEM(it))
        do_coll_all_elem_delete(engine, it);
    do_item_unlink(engine, it);

    /* allocate from slab allocator */
    it = slabs_alloc(engine, ntotal, clsid);
    return it;
}

static void do_item_evict(struct default_engine *engine, hash_item *it,
                          const unsigned int lruid,
                          rel_time_t current_time, const void *cookie)
{
    /* increment # of evicted */
    engine->items.itemstats[lruid].evicted++;
    engine->items.itemstats[lruid].evicted_time = current_time - it->time;
    if (it->exptime != 0) {
        engine->items.itemstats[lruid].evicted_nonzero++;
    }
    pthread_mutex_lock(&engine->stats.lock);
    engine->stats.evictions++;
    pthread_mutex_unlock(&engine->stats.lock);
    if (cookie != NULL) {
        engine->server.stat->evicting(cookie, item_get_key(it), it->nkey);
    }

    /* unlink the item */
    if (IS_COLL_ITEM(it))
        do_coll_all_elem_delete(engine, it);
    do_item_unlink(engine, it);
}

static void do_item_repair(struct default_engine *engine, hash_item *it,
                           const unsigned int lruid)
{
    /* increment # of repaired */
    engine->items.itemstats[lruid].tailrepairs++;
    it->refcount = 0;

    /* unlink the item */
    if (IS_COLL_ITEM(it))
        do_coll_all_elem_delete(engine, it);
    do_item_unlink(engine, it);
}

static void do_item_invalidate(struct default_engine *engine, hash_item *it,
                               const unsigned int lruid)
{
    /* increment # of reclaimed */
    pthread_mutex_lock(&engine->stats.lock);
    engine->stats.reclaimed++;
    pthread_mutex_unlock(&engine->stats.lock);
    engine->items.itemstats[lruid].reclaimed++;

    /* it->refcount == 0 */
    do_item_unlink(engine, it);
}

static void *do_item_alloc_internal(struct default_engine *engine,
                                    const size_t ntotal, const unsigned int clsid, const void *cookie)
{
    hash_item *it = NULL;

    /* do a quick check if we have any expired items in the tail.. */
    int tries;
    hash_item *search;
    hash_item *previt = NULL;

    rel_time_t current_time = engine->server.core->get_current_time();

#ifdef USE_SINGLE_LRU_LIST
    unsigned int id = 1;
    unsigned int clsid_based_on_ntotal = 1;

    if ((it = slabs_alloc(engine, ntotal, clsid_based_on_ntotal)) != NULL) {
        it->slabs_clsid = 0;
        return (void*)it;
    }
#else
    unsigned int id;
    unsigned int clsid_based_on_ntotal;

    if (clsid == LRU_CLSID_FOR_SMALL) {
        clsid_based_on_ntotal = slabs_clsid(engine, ntotal);
        id                    = clsid;
    } else {
        clsid_based_on_ntotal = clsid;
        if (ntotal <= MAX_SM_VALUE_SIZE) {
            id = LRU_CLSID_FOR_SMALL;
        } else {
            id = clsid;
        }
    }
#endif

#ifdef ENABLE_STICKY_ITEM
    /* reclaim the flushed sticky items */
    if (engine->config.junk_item_time != 0) {
        while (engine->items.sticky_tails[id] != NULL) {
            search = engine->items.sticky_tails[id];
            if (search->nkey == 0 || search->refcount > 0 ||
                do_item_isvalid(engine, search, current_time)) {
                break; /* No item to reclaim in perspective of junk item time. */
            }
            it = do_item_reclaim(engine, search, ntotal, clsid_based_on_ntotal, id);
            if (it != NULL) break; /* allocated */
        }
        if (it != NULL) {
            /* try one more invalidation */
            search = engine->items.sticky_tails[id];
            if (search != NULL && search->nkey > 0 && search->refcount == 0 &&
                do_item_isvalid(engine, search, current_time) == false) {
                do_item_invalidate(engine, search, id);
            }
            it->slabs_clsid = 0;
            return (void*)it;
        }
    }
    if (engine->items.sticky_curMK[id] != NULL) {
        tries = 20;
        while (engine->items.sticky_curMK[id] != NULL) {
            search = engine->items.sticky_curMK[id];
            engine->items.sticky_curMK[id] = search->prev;
            if (search->nkey > 0 && search->refcount == 0 &&
                do_item_isvalid(engine, search, current_time) == false) {
                it = do_item_reclaim(engine, search, ntotal, clsid_based_on_ntotal, id);
                if (it != NULL) break; /* allocated */
            }
            if ((--tries) == 0) break;
        }
        if (it != NULL) {
            /* try one more invalidation */
            search = engine->items.sticky_curMK[id];
            if (search != NULL && search->nkey > 0 && search->refcount == 0 &&
                do_item_isvalid(engine, search, current_time) == false) {
                do_item_invalidate(engine, search, id);
            }
            it->slabs_clsid = 0;
            return (void*)it;
        }
    }
#endif

    if (engine->config.junk_item_time != 0) {
        while (engine->items.tails[id] != NULL) {
            search = engine->items.tails[id];
            if (search->nkey == 0 || search->refcount > 0 ||
                do_item_isvalid(engine, search, current_time)) {
                break; /* No item to reclaim in perspective of junk item time. */
            }
            it = do_item_reclaim(engine, search, ntotal, clsid_based_on_ntotal, id);
            if (it != NULL) break; /* allocated */
        }
        if (it != NULL) {
            /* try one more invalidation */
            search = engine->items.tails[id];
            if (search != NULL && search->nkey > 0 && search->refcount == 0 &&
                do_item_isvalid(engine, search, current_time) == false) {
                do_item_invalidate(engine, search, id);
            }
            it->slabs_clsid = 0;
            return (void*)it;
        }
    }
    if (engine->items.curMK[id] != NULL) {
        assert(engine->items.lowMK[id] != NULL);
        /* step 1) reclaim items from lowMK position */
        tries = 20;
        search = engine->items.lowMK[id];
        while (search != NULL && search != engine->items.curMK[id]) {
            if (search->nkey > 0 && search->refcount == 0 &&
                do_item_isvalid(engine, search, current_time) == false) {
                previt = search->prev;
                it = do_item_reclaim(engine, search, ntotal, clsid_based_on_ntotal, id);
                if (it != NULL) break; /* allocated */
                search = previt;
            } else {
                if (search->exptime == 0 && search == engine->items.lowMK[id]) {
                    /* The scrub cursor item also corresponds to this case. */
                    engine->items.lowMK[id] = search->prev; /* move lowMK position upward */
                }
                search = search->prev;
            }
            if ((--tries) == 0) break;
        }
        if (it != NULL) {
            /* try one more invalidation */
            if (previt != NULL && previt->nkey > 0 && previt->refcount == 0 &&
                do_item_isvalid(engine, previt, current_time) == false) {
                do_item_invalidate(engine, previt, id);
            }
            it->slabs_clsid = 0;
            return (void *)it;
        }
        /* step 2) reclaim items from curMK position */
        tries += 20;
        while (engine->items.curMK[id] != NULL) {
            search = engine->items.curMK[id];
            engine->items.curMK[id] = search->prev;
            if (search->nkey > 0 && search->refcount == 0 &&
                do_item_isvalid(engine, search, current_time) == false) {
                it = do_item_reclaim(engine, search, ntotal, clsid_based_on_ntotal, id);
                if (it != NULL) break; /* allocated */
            }
            if ((--tries) == 0) break;
        }
        if (engine->items.curMK[id] == NULL) {
            engine->items.curMK[id] = engine->items.lowMK[id];
        }
        if (it != NULL) {
            /* try one more invalidation */
            search = engine->items.curMK[id];
            if (search != NULL && search->nkey > 0 && search->refcount == 0 &&
                do_item_isvalid(engine, search, current_time) == false) {
                do_item_invalidate(engine, search, id);
            }
            it->slabs_clsid = 0;
            return (void *)it;
        }
    }

    it = slabs_alloc(engine, ntotal, clsid_based_on_ntotal);
    if (it == NULL) {
        /*
        ** Could not find an expired item at the tail, and memory allocation
        ** failed. Try to evict some items!
        */

        /* If requested to not push old items out of cache when memory runs out,
         * we're out of luck at this point...
         */
        if (engine->config.evict_to_free == 0) {
            engine->items.itemstats[clsid_based_on_ntotal].outofmemory++;
            return NULL;
        }

        /*
         * try to get one off the right LRU
         * don't necessariuly unlink the tail because it may be locked: refcount>0
         * search up from tail an item with refcount==0 and unlink it; give up after 50
         * tries
         */
        tries  = 200;
        search = engine->items.tails[id];
        while (search != NULL) {
            if (search->refcount == 0 && search->nkey > 0) {
                previt = search->prev;
                if (do_item_isvalid(engine, search, current_time) == false) {
                    it = do_item_reclaim(engine, search, ntotal, clsid_based_on_ntotal, id);
                } else {
                    do_item_evict(engine, search, id, current_time, cookie);
                    it = slabs_alloc(engine, ntotal, clsid_based_on_ntotal);
                }
                if (it != NULL) break; /* allocated */
                search = previt;
            } else { /* search->nkey == 0: scrub cursor item */
                search = search->prev; /* ignore it */
            }
            if ((--tries) == 0) break;
        }
    }

    if (it == NULL) {
        engine->items.itemstats[id].outofmemory++;
        /* Last ditch effort. There is a very rare bug which causes
         * refcount leaks. We've fixed most of them, but it still happens,
         * and it may happen in the future.
         * We can reasonably assume no item can stay locked for more than
         * three hours, so if we find one in the tail which is that old,
         * free it anyway.
         */
        if (id <= POWER_LARGEST) {
            tries  = 50;
            search = engine->items.tails[id];
            while (search != NULL) {
                if (search->nkey > 0 && search->refcount != 0 &&
                    search->time + TAIL_REPAIR_TIME < current_time) {
                    previt = search->prev;
                    do_item_repair(engine, search, id);
                    it = slabs_alloc(engine, ntotal, clsid_based_on_ntotal);
                    if (it != NULL) break; /* allocated */
                    search = previt;
                } else {
                    search = search->prev;
                }
                if ((--tries) == 0) break;
            }
        }
    }

    if (it != NULL) {
        it->slabs_clsid = 0;
    }
    return (void *)it;
}

/*@null@*/
hash_item *do_item_alloc(struct default_engine *engine, const void *key, const size_t nkey,
                         const int flags, const rel_time_t exptime, const int nbytes, const void *cookie)
{
    hash_item *it = NULL;
    size_t ntotal = sizeof(hash_item) + nkey + nbytes;
    if (engine->config.use_cas) {
        ntotal += sizeof(uint64_t);
    }

    unsigned int id = slabs_clsid(engine, ntotal);
    if (id == 0) {
        return NULL;
    }
#ifdef ENABLE_STICKY_ITEM
    /* sticky memory limit check */
    if (exptime == (rel_time_t)(-1)) { /* sticky item */
        if (engine->stats.sticky_bytes >= engine->config.sticky_limit)
            return NULL;
    }
#endif

    it = do_item_alloc_internal(engine, ntotal, id, cookie);
    if (it == NULL)  {
        return NULL;
    }
    assert(it->slabs_clsid == 0);

    it->slabs_clsid = id;
    assert(it != engine->items.heads[it->slabs_clsid]);

    it->next = it->prev = it->h_next = 0;
    it->refcount = 1;     /* the caller will have a reference */
    DEBUG_REFCNT(it, '*');
    it->iflag = engine->config.use_cas ? ITEM_WITH_CAS : 0;
    it->nkey = nkey;
    it->nbytes = nbytes;
    it->flags = flags;
    memcpy((void*)item_get_key(it), key, nkey);
    it->exptime = exptime;
    it->nprefix = 0;
    return it;
}

static void item_free(struct default_engine *engine, hash_item *it)
{
    size_t ntotal = ITEM_ntotal(engine, it);
    unsigned int clsid;
    assert((it->iflag & ITEM_LINKED) == 0);
    assert(it != engine->items.heads[it->slabs_clsid]);
    assert(it != engine->items.tails[it->slabs_clsid]);
    assert(it->refcount == 0);

    if (IS_COLL_ITEM(it)) {
        coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
        if (info->ccnt > 0) { /* NOT empty collection (list or set) */
            push_coll_del_queue(engine, it);
            return;
        }
    }

    /* so slab size changer can tell later if item is already free or not */
    clsid = it->slabs_clsid;
    it->slabs_clsid = 0;
    it->iflag |= ITEM_SLABBED;
    DEBUG_REFCNT(it, 'F');
    slabs_free(engine, it, ntotal, clsid);
}

static void item_link_q(struct default_engine *engine, hash_item *it)
{
    hash_item **head, **tail;
    assert(it->slabs_clsid <= POWER_LARGEST);
    assert((it->iflag & ITEM_SLABBED) == 0);

#ifdef USE_SINGLE_LRU_LIST
    int clsid = 1;
#else
    int clsid = it->slabs_clsid;
    if (IS_COLL_ITEM(it) || ITEM_ntotal(engine, it) <= MAX_SM_VALUE_SIZE) {
        clsid = LRU_CLSID_FOR_SMALL;
    }
#endif

#ifdef ENABLE_STICKY_ITEM
    if (it->exptime == (rel_time_t)(-1)) {
        head = &engine->items.sticky_heads[clsid];
        tail = &engine->items.sticky_tails[clsid];
        engine->items.sticky_sizes[clsid]++;
    } else {
#endif
        head = &engine->items.heads[clsid];
        tail = &engine->items.tails[clsid];
        engine->items.sizes[clsid]++;
        if (it->exptime > 0) { /* expirable item */
            if (engine->items.lowMK[clsid] == NULL) {
                /* set lowMK and curMK pointer in LRU */
                engine->items.lowMK[clsid] = it;
                engine->items.curMK[clsid] = it;
            }
        }
#ifdef ENABLE_STICKY_ITEM
    }
#endif
    assert(it != *head);
    assert((*head && *tail) || (*head == 0 && *tail == 0));
    it->prev = 0;
    it->next = *head;
    if (it->next) it->next->prev = it;
    *head = it;
    if (*tail == 0) *tail = it;
    return;
}

static void item_unlink_q(struct default_engine *engine, hash_item *it)
{
    hash_item **head, **tail;
    assert(it->slabs_clsid <= POWER_LARGEST);
#ifdef USE_SINGLE_LRU_LIST
    int clsid = 1;
#else
    int clsid = it->slabs_clsid;
    if (IS_COLL_ITEM(it) || ITEM_ntotal(engine, it) <= MAX_SM_VALUE_SIZE) {
        clsid = LRU_CLSID_FOR_SMALL;
    }
#endif

#ifdef ENABLE_STICKY_ITEM
    if (it->exptime == (rel_time_t)(-1)) {
        head = &engine->items.sticky_heads[clsid];
        tail = &engine->items.sticky_tails[clsid];
        engine->items.sticky_sizes[clsid]--;
        /* move curMK, srcub pointer in LRU */
        if (engine->items.sticky_curMK[clsid] == it)
            engine->items.sticky_curMK[clsid] = it->prev;
        if (engine->items.sticky_scrub[clsid] == it)
            engine->items.sticky_scrub[clsid] = it->next; /* move forward */
    } else {
#endif
        head = &engine->items.heads[clsid];
        tail = &engine->items.tails[clsid];
        engine->items.sizes[clsid]--;
        /* move lowMK, curMK, srcub pointer in LRU */
        if (engine->items.lowMK[clsid] == it)
            engine->items.lowMK[clsid] = it->prev;
        if (engine->items.curMK[clsid] == it) {
            engine->items.curMK[clsid] = it->prev;
            if (engine->items.curMK[clsid] == NULL)
                engine->items.curMK[clsid] = engine->items.lowMK[clsid];
        }
        if (engine->items.scrub[clsid] == it)
            engine->items.scrub[clsid] = it->next; /* move forward */
#ifdef ENABLE_STICKY_ITEM
    }
#endif
    if (*head == it) {
        assert(it->prev == 0);
        *head = it->next;
    }
    if (*tail == it) {
        assert(it->next == 0);
        *tail = it->prev;
    }
    assert(it->next != it);
    assert(it->prev != it);

    if (it->next) it->next->prev = it->prev;
    if (it->prev) it->prev->next = it->next;
    return;
}

ENGINE_ERROR_CODE do_item_link(struct default_engine *engine, hash_item *it)
{
    MEMCACHED_ITEM_LINK(item_get_key(it), it->nkey, it->nbytes);
    assert((it->iflag & (ITEM_LINKED|ITEM_SLABBED)) == 0);
    assert(it->nbytes < (1024 * 1024));  /* 1MB max size */

    size_t stotal = ITEM_stotal(engine, it);

    /* link given item to prefix */
    prefix_t *pt;
    ENGINE_ERROR_CODE ret = assoc_prefix_link(engine, it, stotal, &pt);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    if (IS_COLL_ITEM(it)) {
        coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
        info->prefix = (void*)pt;
        assert(info->stotal == 0); /* Only empty collection can be linked */
    }

    it->iflag |= ITEM_LINKED;
    it->time = engine->server.core->get_current_time();
    assoc_insert(engine, engine->server.core->hash(item_get_key(it), it->nkey, 0), it);

    pthread_mutex_lock(&engine->stats.lock);
#ifdef ENABLE_STICKY_ITEM
    if (it->exptime == (rel_time_t)(-1)) { /* sticky item */
        engine->stats.sticky_bytes += stotal;
        engine->stats.sticky_items += 1;
    }
#endif
    engine->stats.curr_bytes += stotal;
    engine->stats.curr_items += 1;
    engine->stats.total_items += 1;
    pthread_mutex_unlock(&engine->stats.lock);

    /* Allocate a new CAS ID on link. */
    item_set_cas(NULL, NULL, it, get_cas_id());

    item_link_q(engine, it);

    return ENGINE_SUCCESS;
}

void do_item_unlink(struct default_engine *engine, hash_item *it)
{
    MEMCACHED_ITEM_UNLINK(item_get_key(it), it->nkey, it->nbytes);
    if ((it->iflag & ITEM_LINKED) != 0) {
        it->iflag &= ~ITEM_LINKED;

        size_t stotal = ITEM_stotal(engine, it);

        pthread_mutex_lock(&engine->stats.lock);
#ifdef ENABLE_STICKY_ITEM
        if (it->exptime == (rel_time_t)(-1)) { /* sticky item */
            engine->stats.sticky_bytes -= stotal;
            engine->stats.sticky_items -= 1;
        }
#endif
        engine->stats.curr_bytes -= stotal;
        engine->stats.curr_items -= 1;
        pthread_mutex_unlock(&engine->stats.lock);

        /* unlink given item from prefix */
        assoc_prefix_unlink(engine, it, stotal);
        if (IS_COLL_ITEM(it)) {
            coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
            info->prefix = NULL;
            info->stotal = 0; /* Don't need to decrease space statistics any more */
        }
        assoc_delete(engine, engine->server.core->hash(item_get_key(it), it->nkey, 0),
                     item_get_key(it), it->nkey);
        item_unlink_q(engine, it);
        if (it->refcount == 0) {
            item_free(engine, it);
        }
    }
}

void do_item_release(struct default_engine *engine, hash_item *it)
{
    MEMCACHED_ITEM_REMOVE(item_get_key(it), it->nkey, it->nbytes);
    if (it->refcount != 0) {
        it->refcount--;
        DEBUG_REFCNT(it, '-');
    }
    if (it->refcount == 0 && (it->iflag & ITEM_LINKED) == 0) {
        item_free(engine, it);
    }
}

void do_item_update(struct default_engine *engine, hash_item *it)
{
    rel_time_t current_time = engine->server.core->get_current_time();
    MEMCACHED_ITEM_UPDATE(item_get_key(it), it->nkey, it->nbytes);
    if (it->time < current_time - ITEM_UPDATE_INTERVAL) {
        assert((it->iflag & ITEM_SLABBED) == 0);

        if ((it->iflag & ITEM_LINKED) != 0) {
            item_unlink_q(engine, it);
            it->time = current_time;
            item_link_q(engine, it);
        }
    }
}

void do_item_lru_reposition(struct default_engine *engine, hash_item *it)
{
    assert((it->iflag & ITEM_SLABBED) == 0);
    if ((it->iflag & ITEM_LINKED) != 0) {
        item_unlink_q(engine, it);
        it->time = engine->server.core->get_current_time();
        item_link_q(engine, it);
    }
}

ENGINE_ERROR_CODE do_item_replace(struct default_engine *engine, hash_item *it, hash_item *new_it)
{
    MEMCACHED_ITEM_REPLACE(item_get_key(it), it->nkey, it->nbytes,
                           item_get_key(new_it), new_it->nkey, new_it->nbytes);
    assert((it->iflag & ITEM_SLABBED) == 0);

    do_item_unlink(engine, it);
    ENGINE_ERROR_CODE ret = do_item_link(engine, new_it);
    assert(ret == ENGINE_SUCCESS);
    return ret;
}

/*@null@*/
static char *do_item_cachedump(struct default_engine *engine, const unsigned int slabs_clsid,
                               const unsigned int limit, const bool forward, const bool sticky,
                               unsigned int *bytes)
{
    unsigned int memlimit = 256 * 1024; /* 256KB max response size */
    char *buffer;
    unsigned int bufcurr = 0;
    hash_item *it;
    unsigned int len;
    unsigned int shown = 0;
    char key_temp[256]; /* KEY_MAX_LENGTH + 1 */

    buffer = malloc((size_t)memlimit);
    if (buffer == 0) return NULL;

    if (sticky) {
        it = (forward ? engine->items.sticky_heads[slabs_clsid]
                      : engine->items.sticky_tails[slabs_clsid]);
    } else {
        it = (forward ? engine->items.heads[slabs_clsid]
                      : engine->items.tails[slabs_clsid]);
    }

    while (it != NULL && (limit == 0 || shown < limit)) {
        /* Copy the key since it may not be null-terminated in the struct */
        strncpy(key_temp, item_get_key(it), it->nkey);
        key_temp[it->nkey] = 0x00; /* terminate */

        if (bufcurr + it->nkey + 100 > memlimit) break;
        len = sprintf(buffer + bufcurr, "ITEM %s [acctime=%u, exptime=%d]\r\n",
                      key_temp, it->time, (int32_t)it->exptime);
        bufcurr += len;
        shown++;
        it = (forward ? it->next : it->prev);
    }

    len = sprintf(buffer + bufcurr, "END [curtime=%u]\r\n",
                  engine->server.core->get_current_time());
    bufcurr += len;

    *bytes = bufcurr;
    return buffer;
}

static void do_item_stats(struct default_engine *engine, ADD_STAT add_stats, const void *c)
{
    int i;
    for (i = 0; i <= POWER_LARGEST; i++)
    {
        if (engine->items.tails[i] != NULL || engine->items.sticky_tails[i] != NULL)
        {
            const char *prefix = "items";
            add_statistics(c, add_stats, prefix, i, "number", "%u",
                           engine->items.sizes[i]+engine->items.sticky_sizes[i]);
#ifdef ENABLE_STICKY_ITEM
            add_statistics(c, add_stats, prefix, i, "sticky", "%u",
                           engine->items.sticky_sizes[i]);
#endif
            add_statistics(c, add_stats, prefix, i, "age", "%u",
                           (engine->items.tails[i] != NULL ? engine->items.tails[i]->time : 0));
            add_statistics(c, add_stats, prefix, i, "evicted",
                           "%u", engine->items.itemstats[i].evicted);
            add_statistics(c, add_stats, prefix, i, "evicted_nonzero",
                           "%u", engine->items.itemstats[i].evicted_nonzero);
            add_statistics(c, add_stats, prefix, i, "evicted_time",
                           "%u", engine->items.itemstats[i].evicted_time);
            add_statistics(c, add_stats, prefix, i, "outofmemory",
                           "%u", engine->items.itemstats[i].outofmemory);
            add_statistics(c, add_stats, prefix, i, "tailrepairs",
                           "%u", engine->items.itemstats[i].tailrepairs);;
            add_statistics(c, add_stats, prefix, i, "reclaimed",
                           "%u", engine->items.itemstats[i].reclaimed);;
        }
    }
}

/** dumps out a list of objects of each size, with granularity of 32 bytes */
/*@null@*/
static void do_item_stats_sizes(struct default_engine *engine, ADD_STAT add_stats, const void *c)
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
        for (i = 0; i <= POWER_LARGEST; i++)
        {
            hash_item *iter = engine->items.heads[i];
            while (iter) {
                int ntotal = ITEM_stotal(engine, iter);
                int bucket = ntotal / 32;
                if ((ntotal % 32) != 0) bucket++;
                if (bucket < num_buckets) histogram[bucket]++;
                iter = iter->next;
            }
#ifdef ENABLE_STICKY_ITEM
            iter = engine->items.sticky_heads[i];
            while (iter) {
                int ntotal = ITEM_stotal(engine, iter);
                int bucket = ntotal / 32;
                if ((ntotal % 32) != 0) bucket++;
                if (bucket < num_buckets) histogram[bucket]++;
                iter = iter->next;
            }
#endif
        }

        /* write the buffer */
        for (i = 0; i < num_buckets; i++) {
            if (histogram[i] != 0) {
                char key[8], val[32];
                int klen, vlen;
                klen = snprintf(key, sizeof(key), "%d", i * 32);
                vlen = snprintf(val, sizeof(val), "%u", histogram[i]);
                assert(klen < sizeof(key));
                assert(vlen < sizeof(val));
                add_stats(key, klen, val, vlen, c);
            }
        }
        free(histogram);
    }
#endif
}

/** wrapper around assoc_find which does the lazy expiration logic */
hash_item *do_item_get(struct default_engine *engine, const char *key, const size_t nkey, bool LRU_reposition)
{
    rel_time_t current_time = engine->server.core->get_current_time();
    hash_item *it = assoc_find(engine, engine->server.core->hash(key, nkey, 0), key, nkey);

    if (it != NULL) {
        if (do_item_isvalid(engine, it, current_time)==false) {
            do_item_unlink(engine, it); it = NULL;
        }
    }
    if (it != NULL) {
        it->refcount++;
        DEBUG_REFCNT(it, '+');
        if (LRU_reposition)
            do_item_update(engine, it);
    }

    if (engine->config.verbose > 2) {
        if (it == NULL) {
            fprintf(stderr, "> NOT FOUND %s\n", key);
        } else {
            fprintf(stderr, "> FOUND KEY %s\n", (const char*)item_get_key(it));
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
static ENGINE_ERROR_CODE do_store_item(struct default_engine *engine, hash_item *it, uint64_t *cas,
                                       ENGINE_STORE_OPERATION operation, const void *cookie)
{
    const char *key = item_get_key(it);
    hash_item *old_it = do_item_get(engine, key, it->nkey, true);
    ENGINE_ERROR_CODE stored = ENGINE_NOT_STORED;
    if (old_it != NULL && IS_COLL_ITEM(old_it)) {
        do_item_release(engine, old_it);
        return ENGINE_EBADTYPE;
    }

    hash_item *new_it = NULL;

    if (old_it != NULL && operation == OPERATION_ADD) {
        /* add only adds a nonexistent item, but promote to head of LRU */
        do_item_update(engine, old_it);
    } else if (!old_it && (operation == OPERATION_REPLACE
        || operation == OPERATION_APPEND || operation == OPERATION_PREPEND))
    {
        /* replace only replaces an existing value; don't store */
    } else if (operation == OPERATION_CAS) {
        /* validate cas operation */
        if (old_it == NULL) {
            // LRU expired
            stored = ENGINE_KEY_ENOENT;
#if 0
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.cas_misses++;
            pthread_mutex_unlock(&c->thread->stats.mutex);
#endif
        }
        else if (item_get_cas(it) == item_get_cas(old_it)) {
            // cas validates
            // it and old_it may belong to different classes.
            // I'm updating the stats for the one that's getting pushed out
#if 0
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[item_get_clsid(old_it)].cas_hits++;
            pthread_mutex_unlock(&c->thread->stats.mutex);
#endif

            (void)do_item_replace(engine, old_it, it);
            stored = ENGINE_SUCCESS;
        } else {
#if 0
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[item_get_clsid(old_it)].cas_badval++;
            pthread_mutex_unlock(&c->thread->stats.mutex);
#endif
            if (engine->config.verbose > 1) {
                fprintf(stderr,
                        "CAS:  failure: expected %"PRIu64", got %"PRIu64"\n",
                        item_get_cas(old_it),
                        item_get_cas(it));
            }
            stored = ENGINE_KEY_EEXISTS;
        }
    } else {
        /*
         * Append - combine new and old record into single one. Here it's
         * atomic and thread-safe.
         */
        if (operation == OPERATION_APPEND || operation == OPERATION_PREPEND) {
            /*
             * Validate CAS
             */
            if (item_get_cas(it) != 0) {
                // CAS much be equal
                if (item_get_cas(it) != item_get_cas(old_it)) {
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
                    if (old_it != NULL) {
                        do_item_release(engine, old_it);
                    }

                    return ENGINE_NOT_STORED;
                }

                /* copy data from it and old_it to new_it */

                if (operation == OPERATION_APPEND) {
                    memcpy(item_get_data(new_it), item_get_data(old_it), old_it->nbytes);
                    memcpy(item_get_data(new_it) + old_it->nbytes - 2 /* CRLF */, item_get_data(it), it->nbytes);
                } else {
                    /* OPERATION_PREPEND */
                    memcpy(item_get_data(new_it), item_get_data(it), it->nbytes);
                    memcpy(item_get_data(new_it) + it->nbytes - 2 /* CRLF */, item_get_data(old_it), old_it->nbytes);
                }

                it = new_it;
            }
        }
        if (stored == ENGINE_NOT_STORED) {
            if (old_it != NULL) {
                (void)do_item_replace(engine, old_it, it);
                stored = ENGINE_SUCCESS;
            } else {
                stored = do_item_link(engine, it);
            }
            if (stored == ENGINE_SUCCESS) {
                *cas = item_get_cas(it);
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
        *cas = item_get_cas(it);
    }
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
static ENGINE_ERROR_CODE do_add_delta(struct default_engine *engine, hash_item *it,
                                      const bool incr, const int64_t delta,
                                      uint64_t *rcas, uint64_t *result, const void *cookie)
{
    const char *ptr;
    uint64_t value;
    int res;

    if (IS_COLL_ITEM(it)) {
        return ENGINE_EBADTYPE;
    }

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
    hash_item *new_it = do_item_alloc(engine, item_get_key(it),
                                      it->nkey, it->flags,
                                      it->exptime, res,
                                      cookie );
    if (new_it == NULL) {
        return ENGINE_ENOMEM;
    }
    memcpy(item_get_data(new_it), buf, res);
    (void)do_item_replace(engine, it, new_it);
    *rcas = item_get_cas(new_it);
    do_item_release(engine, new_it);       /* release our reference */

    return ENGINE_SUCCESS;
}

/* common functions for collection memory management */
static void do_mem_slot_free(struct default_engine *engine, void *data, size_t ntotal)
{
    /* so slab size changer can tell later if item is already free or not */
    hash_item *it = (hash_item *)data;
    unsigned int clsid = it->slabs_clsid;;
    it->slabs_clsid = 0;
    slabs_free(engine, it, ntotal, clsid);
}

/*
 * LIST collection management
 */
static ENGINE_ERROR_CODE do_list_item_find(struct default_engine *engine,
                                           const void *key, const size_t nkey,
                                           bool LRU_reposition, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(engine, key, nkey, LRU_reposition);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_LIST_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(engine, it);
        return ENGINE_EBADTYPE;
    }
}

static hash_item *do_list_item_alloc(struct default_engine *engine,
                                     const void *key, const size_t nkey,
                                     item_attr *attrp, const void *cookie)
{
    char *value = "\r\n"; //"LIST ITEM\r\n";
    int nbytes = 2; //11;
    int real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes) + sizeof(list_meta_info) - nkey;

    hash_item *it = do_item_alloc(engine, key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_LIST;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), value, nbytes);

        /* initialize list meta information */
        list_meta_info *info = (list_meta_info *)item_get_meta(it);
        info->mcnt    = attrp->maxcount;
        info->ccnt    = 0;
        info->ovflact = (attrp->ovflaction==0 ? OVFL_TAIL_TRIM : attrp->ovflaction);
#ifdef ENABLE_STICKY_ITEM
        if (attrp->exptime == (rel_time_t)(-1)) info->mflags |= COLL_META_FLAG_STICKY;
        else                                    info->mflags &= ~COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1) info->mflags |= COLL_META_FLAG_READABLE;
        else                      info->mflags &= ~COLL_META_FLAG_READABLE;
        info->stotal  = 0;
        info->prefix  = NULL;
        info->head = info->tail = NULL;
    }
    return it;
}

static list_elem_item *do_list_elem_alloc(struct default_engine *engine,
                                          const int nbytes, const void *cookie)
{
    size_t ntotal = sizeof(list_elem_item) + nbytes;

    list_elem_item *elem = do_item_alloc_internal(engine, ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        assert(elem->slabs_clsid == 0);
        elem->slabs_clsid = slabs_clsid(engine, ntotal);
        elem->refcount    = 1;
        elem->nbytes      = nbytes;
        elem->prev = elem->next = (list_elem_item *)ADDR_MEANS_UNLINKED; /* Unliked state */
    }
    return elem;
}

static void do_list_elem_free(struct default_engine *engine, list_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = sizeof(list_elem_item) + elem->nbytes;
    do_mem_slot_free(engine, elem, ntotal);
}

static void do_list_elem_release(struct default_engine *engine, list_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->next == (list_elem_item *)ADDR_MEANS_UNLINKED) {
        do_list_elem_free(engine, elem);
    }
}

static list_elem_item *do_list_elem_find(list_meta_info *info, int index)
{
    list_elem_item *elem;
    if (index >= 0) {
        elem = info->head;
        for (int i =  0; i < index && elem != NULL; i++) {
            elem = elem->next;
        }
    } else {
        elem = info->tail;
        for (int i = -1; i > index && elem != NULL; i--) {
            elem = elem->prev;
        }
    }
    return elem;
}

static ENGINE_ERROR_CODE do_list_elem_link(struct default_engine *engine,
                                           list_meta_info *info, const int index,
                                           list_elem_item *elem)
{
    list_elem_item *prev, *next;
    if (index >= 0) {
        if (index == 0) {
            prev = NULL;
            next = info->head;
        } else {
            assert (index <= info->ccnt);
            prev = do_list_elem_find(info, (index-1));
            next = prev->next;
        }
    } else { /* index < 0 */
        if (index == -1) {
            next = NULL;
            prev = info->tail;
        } else {
            assert ((-index) <= (info->ccnt+1));
            next = do_list_elem_find(info, (index+1));
            prev = next->prev;
        }
    }
    elem->prev = prev;
    elem->next = next;
    if (prev == NULL) info->head = elem;
    else              prev->next = elem;
    if (next == NULL) info->tail = elem;
    else              next->prev = elem;
    info->ccnt++;

    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(engine, (sizeof(list_elem_item)+elem->nbytes));
        increase_collection_space(engine, ITEM_TYPE_LIST, (coll_meta_info *)info, stotal);
    }
    return ENGINE_SUCCESS;
}

static void do_list_elem_unlink(struct default_engine *engine,
                                list_meta_info *info, list_elem_item *elem)
{
    /* if (elem->next != (list_elem_item *)ADDR_MEANS_UNLINKED) */
    {
        if (elem->prev == NULL) info->head = elem->next;
        else                    elem->prev->next = elem->next;
        if (elem->next == NULL) info->tail = elem->prev;
        else                    elem->next->prev = elem->prev;
        elem->prev = elem->next = (list_elem_item *)ADDR_MEANS_UNLINKED;
        info->ccnt--;

        if (info->stotal > 0) { /* apply memory space */
            size_t stotal = slabs_space_size(engine, (sizeof(list_elem_item)+elem->nbytes));
            decrease_collection_space(engine, ITEM_TYPE_LIST, (coll_meta_info *)info, stotal);
        }

        if (elem->refcount == 0) {
            do_list_elem_free(engine, elem);
        }
    }
}

static uint32_t do_list_elem_delete(struct default_engine *engine,
                                    list_meta_info *info, const int index, const uint32_t count)
{
    uint32_t fcnt = 0;
    list_elem_item *next;
    list_elem_item *elem = do_list_elem_find(info, index);
    while (elem != NULL) {
        next = elem->next;
        fcnt++;
        do_list_elem_unlink(engine, info, elem);
        if (count > 0 && fcnt >= count) break;
        elem = next;
    }
    return fcnt;
}

static uint32_t do_list_elem_get(struct default_engine *engine,
                                 list_meta_info *info,
                                 const int index, const uint32_t count,
                                 const bool forward, const bool delete,
                                 list_elem_item **elem_array)
{
    uint32_t fcnt = 0; /* found count */
    list_elem_item *tobe;
    list_elem_item *elem = do_list_elem_find(info, index);
    while (elem != NULL) {
        tobe = (forward ? elem->next : elem->prev);
        elem->refcount++;
        elem_array[fcnt++] = elem;
        if (delete) do_list_elem_unlink(engine, info, elem);
        if (count > 0 && fcnt >= count) break;
        elem = tobe;
    }
    return fcnt;
}

/*
 * SET collection manangement
 */
static inline int set_hash_eq(const int h1, const void *v1, size_t vlen1,
                              const int h2, const void *v2, size_t vlen2)
{
    return (h1 == h2 && vlen1 == vlen2 && memcmp(v1, v2, vlen1) == 0);
}

#define SET_GET_HASHIDX(hval, hdepth) \
        (((hval) & (SET_HASHIDX_MASK << ((hdepth)*4))) >> ((hdepth)*4))

static ENGINE_ERROR_CODE do_set_item_find(struct default_engine *engine,
                                          const void *key, const size_t nkey,
                                          bool LRU_reposition, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(engine, key, nkey, LRU_reposition);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_SET_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(engine, it);
        return ENGINE_EBADTYPE;
    }
}

static hash_item *do_set_item_alloc(struct default_engine *engine,
                                    const void *key, const size_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    char *value = "\r\n"; //SET ITEM\r\n";
    int nbytes = 2; //10;
    int real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)+sizeof(set_meta_info)-nkey;

    hash_item *it = do_item_alloc(engine, key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_SET;
        it->nbytes = nbytes;
        memcpy(item_get_data(it), value, nbytes);

        /* initialize set meta information */
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        info->mcnt    = attrp->maxcount;
        info->ccnt    = 0;
        info->ovflact = OVFL_ERROR;
#ifdef ENABLE_STICKY_ITEM
        if (attrp->exptime == (rel_time_t)(-1)) info->mflags |= COLL_META_FLAG_STICKY;
        else                                    info->mflags &= ~COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1) info->mflags |= COLL_META_FLAG_READABLE;
        else                      info->mflags &= ~COLL_META_FLAG_READABLE;
        info->stotal  = 0;
        info->prefix  = NULL;
        info->root    = NULL;
    }
    return it;
}

static set_hash_node *do_set_node_alloc(struct default_engine *engine,
                                        uint8_t hash_depth, const void *cookie)
{
    size_t ntotal = sizeof(set_hash_node);

    set_hash_node *node = do_item_alloc_internal(engine, ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (node != NULL) {
        assert(node->slabs_clsid == 0);
        node->slabs_clsid = slabs_clsid(engine, ntotal);
        node->refcount    = 0;
        node->hdepth      = hash_depth;
        node->tot_hash_cnt = 0;
        node->tot_elem_cnt = 0;
        memset(node->hcnt, 0, SET_HASHTAB_SIZE*sizeof(uint16_t));
        memset(node->htab, 0, SET_HASHTAB_SIZE*sizeof(void*));
    }
    return node;
}

static void do_set_node_free(struct default_engine *engine, set_hash_node *node)
{
    do_mem_slot_free(engine, node, sizeof(set_hash_node));
}

static set_elem_item *do_set_elem_alloc(struct default_engine *engine,
                                        const int nbytes, const void *cookie)
{
    size_t ntotal = sizeof(set_elem_item) + nbytes;

    set_elem_item *elem = do_item_alloc_internal(engine, ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        assert(elem->slabs_clsid == 0);
        elem->slabs_clsid = slabs_clsid(engine, ntotal);
        elem->refcount    = 1;
        elem->nbytes      = nbytes;
        elem->next = (set_elem_item *)ADDR_MEANS_UNLINKED; /* Unliked state */
    }
    return elem;
}

static void do_set_elem_free(struct default_engine *engine, set_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = sizeof(set_elem_item) + elem->nbytes;
    do_mem_slot_free(engine, elem, ntotal);
}

static void do_set_elem_release(struct default_engine *engine, set_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->next == (set_elem_item *)ADDR_MEANS_UNLINKED) {
        do_set_elem_free(engine, elem);
    }
}

static void do_set_node_link(struct default_engine *engine,
                             set_meta_info *info,
                             set_hash_node *par_node, const int par_hidx,
                             set_hash_node *node)
{
    if (par_node == NULL) {
        info->root = node;
    } else {
        set_elem_item *elem;
        int num_elems = par_node->hcnt[par_hidx];
        int hidx, fcnt=0;

        while (par_node->htab[par_hidx] != NULL) {
            elem = par_node->htab[par_hidx];
            par_node->htab[par_hidx] = elem->next;

            hidx = SET_GET_HASHIDX(elem->hval, node->hdepth);
            elem->next = node->htab[hidx];
            node->htab[hidx] = elem;
            node->hcnt[hidx] += 1;
            fcnt++;
        }
        assert(fcnt == num_elems);
        node->tot_elem_cnt = fcnt;

        par_node->htab[par_hidx] = node;
        par_node->hcnt[par_hidx] = -1; /* child hash node */
        par_node->tot_elem_cnt -= fcnt;
        par_node->tot_hash_cnt += 1;
    }

    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(engine, sizeof(set_hash_node));
        increase_collection_space(engine, ITEM_TYPE_SET, (coll_meta_info *)info, stotal);
    }
}

static void do_set_node_unlink(struct default_engine *engine,
                               set_meta_info *info,
                               set_hash_node *par_node, const int par_hidx)
{
    set_hash_node *node;

    if (par_node == NULL) {
        node = info->root;
        info->root = NULL;
        assert(node->tot_hash_cnt == 0);
        assert(node->tot_elem_cnt == 0);
    } else {
        assert(par_node->hcnt[par_hidx] == -1); /* child hash node */
        set_elem_item *head = NULL;
        set_elem_item *elem;
        int hidx, fcnt = 0;

        node = (set_hash_node *)par_node->htab[par_hidx];
        assert(node->tot_hash_cnt == 0);

        for (hidx = 0; hidx < SET_HASHTAB_SIZE; hidx++) {
            assert(node->hcnt[hidx] >= 0);
            if (node->hcnt[hidx] > 0) {
                fcnt += node->hcnt[hidx];
                while (node->htab[hidx] != NULL) {
                    elem = node->htab[hidx];
                    node->htab[hidx] = elem->next;
                    node->hcnt[hidx] -= 1;

                    elem->next = head;
                    head = elem;
                }
                assert(node->hcnt[hidx] == 0);
            }
        }
        assert(fcnt == node->tot_elem_cnt);
        node->tot_elem_cnt = 0;

        par_node->htab[par_hidx] = head;
        par_node->hcnt[par_hidx] = fcnt;
        par_node->tot_elem_cnt += fcnt;
        par_node->tot_hash_cnt -= 1;
    }

    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(engine, sizeof(set_hash_node));
        decrease_collection_space(engine, ITEM_TYPE_SET, (coll_meta_info *)info, stotal);
    }

    /* free the node */
    do_set_node_free(engine, node);
}

static ENGINE_ERROR_CODE do_set_elem_link(struct default_engine *engine,
                                          set_meta_info *info, set_elem_item *elem,
                                          const void *cookie)
{
    assert(info->root != NULL);
    set_hash_node *node = info->root;
    set_elem_item *find;
    int hidx;

    /* set hash value */
    elem->hval = genhash_string_hash(elem->value, elem->nbytes);

    while (node != NULL) {
        hidx = SET_GET_HASHIDX(elem->hval, node->hdepth);
        if (node->hcnt[hidx] >= 0) /* set element hash chain */
            break;
        node = node->htab[hidx];
    }
    assert(node != NULL);

    for (find = node->htab[hidx]; find != NULL; find = find->next) {
        if (set_hash_eq(elem->hval, elem->value, elem->nbytes,
                        find->hval, find->value, find->nbytes))
            break;
    }
    if (find != NULL) {
        return ENGINE_ELEM_EEXISTS;
    }

    if (node->hcnt[hidx] >= SET_MAX_HASHCHAIN_SIZE) {
        set_hash_node *n_node = do_set_node_alloc(engine, node->hdepth+1, cookie);
        if (n_node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_set_node_link(engine, info, node, hidx, n_node);

        node = n_node;
        hidx = SET_GET_HASHIDX(elem->hval, node->hdepth);
    }

    elem->next = node->htab[hidx];
    node->htab[hidx] = elem;
    node->hcnt[hidx] += 1;
    node->tot_elem_cnt += 1;

    info->ccnt++;

    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(engine, (sizeof(set_elem_item)+elem->nbytes));
        increase_collection_space(engine, ITEM_TYPE_SET, (coll_meta_info *)info, stotal);
    }

    return ENGINE_SUCCESS;
}

static void do_set_elem_unlink(struct default_engine *engine,
                               set_meta_info *info,
                               set_hash_node *node, const int hidx,
                               set_elem_item *prev, set_elem_item *elem)
{
    if (prev != NULL) prev->next = elem->next;
    else              node->htab[hidx] = elem->next;
    elem->next = (set_elem_item *)ADDR_MEANS_UNLINKED;
    node->hcnt[hidx] -= 1;
    node->tot_elem_cnt -= 1;

    info->ccnt--;

    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(engine, (sizeof(set_elem_item)+elem->nbytes));
        decrease_collection_space(engine, ITEM_TYPE_SET, (coll_meta_info *)info, stotal);
    }

    if (elem->refcount == 0) {
        do_set_elem_free(engine, elem);
    }
}

static set_elem_item *do_set_elem_find(set_meta_info *info, const char *val, const int vlen)
{
    set_elem_item *elem = NULL;

    if (info->root != NULL) {
        set_hash_node *node = info->root;
        int hval = genhash_string_hash(val, vlen);
        int hidx;

        while (node != NULL) {
            hidx = SET_GET_HASHIDX(hval, node->hdepth);
            if (node->hcnt[hidx] >= 0) /* set element hash chain */
                break;
            node = node->htab[hidx];
        }
        assert(node != NULL);

        for (elem = node->htab[hidx]; elem != NULL; elem = elem->next) {
            if (set_hash_eq(hval, val, vlen, elem->hval, elem->value, elem->nbytes))
                break;
        }
    }
    return elem;
}

static ENGINE_ERROR_CODE do_set_elem_traverse_delete(struct default_engine *engine,
                                                     set_meta_info *info, set_hash_node *node,
                                                     const int hval, const char *val, const int vlen)
{
    ENGINE_ERROR_CODE ret;

    int hidx = SET_GET_HASHIDX(hval, node->hdepth);

    if (node->hcnt[hidx] == -1) {
        set_hash_node *child_node = node->htab[hidx];
        ret = do_set_elem_traverse_delete(engine, info, child_node, hval, val, vlen);
        if (ret == ENGINE_SUCCESS) {
            if (child_node->tot_hash_cnt == 0 &&
                child_node->tot_elem_cnt < (SET_MAX_HASHCHAIN_SIZE/2)) {
                do_set_node_unlink(engine, info, node, hidx);
            }
        }
    } else {
        ret = ENGINE_ELEM_ENOENT;
        if (node->hcnt[hidx] > 0) {
            set_elem_item *prev = NULL;
            set_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                if (set_hash_eq(hval, val, vlen, elem->hval, elem->value, elem->nbytes))
                    break;
                prev = elem;
                elem = elem->next;
            }
            if (elem != NULL) {
                do_set_elem_unlink(engine, info, node, hidx, prev, elem);
                ret = ENGINE_SUCCESS;
            }
        }
    }
    return ret;
}

static ENGINE_ERROR_CODE do_set_elem_delete_with_value(struct default_engine *engine,
                                                       set_meta_info *info,
                                                       const char *val, const int vlen)
{
    ENGINE_ERROR_CODE ret;
    if (info->root != NULL) {
        int hval = genhash_string_hash(val, vlen);
        ret = do_set_elem_traverse_delete(engine, info, info->root, hval, val, vlen);
        if (ret == ENGINE_SUCCESS) {
            if (info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
                do_set_node_unlink(engine, info, NULL, 0);
            }
        }
    } else {
        ret = ENGINE_ELEM_ENOENT;
    }
    return ret;
}

static int do_set_elem_traverse_dfs(struct default_engine *engine,
                                    set_meta_info *info, set_hash_node *node,
                                    const uint32_t count, const bool delete,
                                    set_elem_item **elem_array)
{
    int hidx;
    int rcnt = 0; /* request count */
    int fcnt, tot_fcnt = 0; /* found count */

    if (node->tot_hash_cnt > 0) {
        for (hidx = 0; hidx < SET_HASHTAB_SIZE; hidx++) {
            if (node->hcnt[hidx] == -1) {
                set_hash_node *child_node = (set_hash_node *)node->htab[hidx];
                if (count > 0) rcnt = count - tot_fcnt;
                fcnt = do_set_elem_traverse_dfs(engine, info, child_node, rcnt, delete,
                                            (elem_array==NULL ? NULL : &elem_array[tot_fcnt]));
                if (delete) {
                    if  (child_node->tot_hash_cnt == 0 &&
                         child_node->tot_elem_cnt < (SET_MAX_HASHCHAIN_SIZE/2)) {
                         do_set_node_unlink(engine, info, node, hidx);
                     }
                }
                tot_fcnt += fcnt;
                if (count > 0 && tot_fcnt >= count)
                    return tot_fcnt;
            }
        }
    }
    assert(count == 0 || tot_fcnt < count);

    for (hidx = 0; hidx < SET_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] > 0) {
            fcnt = 0;
            set_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                if (elem_array) {
                    elem->refcount++;
                    elem_array[tot_fcnt+fcnt] = elem;
                }
                fcnt++;
                if (delete) do_set_elem_unlink(engine, info, node, hidx, NULL, elem);
                if (count > 0 && (tot_fcnt+fcnt) >= count) break;
                elem = (delete ? node->htab[hidx] : elem->next);
            }
            tot_fcnt += fcnt;
            if (count > 0 && tot_fcnt >= count) break;
        }
    }
    return tot_fcnt;
}

static uint32_t do_set_elem_delete(struct default_engine *engine,
                                   set_meta_info *info, const uint32_t count)
{
    uint32_t fcnt = 0;
    if (info->root != NULL) {
        fcnt = do_set_elem_traverse_dfs(engine, info, info->root, count, true, NULL);
        if (info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
            do_set_node_unlink(engine, info, NULL, 0);
        }
    }
    return fcnt;
}

static uint32_t do_set_elem_get(struct default_engine *engine,
                                set_meta_info *info, const uint32_t count, const bool delete,
                                set_elem_item **elem_array)
{
    uint32_t fcnt = 0;
    if (info->root != NULL) {
        fcnt = do_set_elem_traverse_dfs(engine, info, info->root, count, delete, elem_array);
        if (delete && info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
            do_set_node_unlink(engine, info, NULL, 0);
        }
    }
    return fcnt;
}

/*
 * B+TREE collection management
 */
static ENGINE_ERROR_CODE do_btree_item_find(struct default_engine *engine,
                                            const void *key, const size_t nkey,
                                            bool LRU_reposition, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(engine, key, nkey, LRU_reposition);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_BTREE_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(engine, it);
        return ENGINE_EBADTYPE;
    }
}

static hash_item *do_btree_item_alloc(struct default_engine *engine,
                                      const void *key, const size_t nkey,
                                      item_attr *attrp, const void *cookie)
{
    char *value = "\r\n"; // "BTREE ITEM\r\n";
    int nbytes = 2; // 13;
    int real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes) + sizeof(btree_meta_info) - nkey;

    hash_item *it = do_item_alloc(engine, key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_BTREE;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), value, nbytes);

        /* initialize b+tree meta information */
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        info->mcnt    = attrp->maxcount;
        info->ccnt    = 0;
        info->ovflact = (attrp->ovflaction==0 ? OVFL_SMALLEST_TRIM : attrp->ovflaction);
#ifdef ENABLE_STICKY_ITEM
        if (attrp->exptime == (rel_time_t)(-1)) info->mflags |= COLL_META_FLAG_STICKY;
        else                                    info->mflags &= ~COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1) info->mflags |= COLL_META_FLAG_READABLE;
        else                      info->mflags &= ~COLL_META_FLAG_READABLE;
        info->bktype  = BKEY_TYPE_UNKNOWN;
        info->stotal  = 0;
        info->prefix  = NULL;
        info->has_trimmed = 0;
        info->maxbkeyrange.len = BKEY_NULL;
        info->root    = NULL;
    }
    return it;
}

static btree_indx_node *do_btree_node_alloc(struct default_engine *engine,
                                            const uint8_t node_depth, const void *cookie)
{
    size_t ntotal = (node_depth > 0 ? sizeof(btree_indx_node) : sizeof(btree_leaf_node));

    btree_indx_node *node = do_item_alloc_internal(engine, ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (node != NULL) {
        assert(node->slabs_clsid == 0);
        node->slabs_clsid = slabs_clsid(engine, ntotal);
        node->refcount    = 0;
        node->ndepth      = node_depth;
        node->used_count  = 0;
        node->prev = node->next = NULL;
        memset(node->item, 0, BTREE_ITEM_COUNT*sizeof(void*));
        if (node_depth > 0)
            memset(node->ecnt, 0, BTREE_ITEM_COUNT*sizeof(uint16_t));
    }
    return node;
}

static void do_btree_node_free(struct default_engine *engine, btree_indx_node *node)
{
    size_t ntotal = (node->ndepth > 0 ? sizeof(btree_indx_node) : sizeof(btree_leaf_node));
    do_mem_slot_free(engine, node, ntotal);
}

static btree_elem_item *do_btree_elem_alloc(struct default_engine *engine,
                                            const int nbkey, const int neflag, const int nbytes,
                                            const void *cookie)
{
    size_t ntotal = sizeof(btree_elem_item_fixed) + BTREE_REAL_NBKEY(nbkey) + neflag + nbytes;

    btree_elem_item *elem = do_item_alloc_internal(engine, ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        assert(elem->slabs_clsid == 0);
        elem->slabs_clsid = slabs_clsid(engine, ntotal);
        assert(elem->slabs_clsid > 0);
        elem->refcount    = 1;
        elem->status      = BTREE_ITEM_STATUS_UNLINK; /* unlinked state */
        elem->nbkey       = (uint8_t)nbkey;
        elem->neflag      = (uint8_t)neflag;
        elem->nbytes      = (uint16_t)nbytes;
    }
    return elem;
}

static void do_btree_elem_free(struct default_engine *engine, btree_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = BTREE_ELEM_SIZE(elem);
    do_mem_slot_free(engine, elem, ntotal);
}

static void do_btree_elem_release(struct default_engine *engine, btree_elem_item *elem)
{
    /* assert(elem->status != BTREE_ITEM_STATUS_FREE); */
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->status == BTREE_ITEM_STATUS_UNLINK) {
        elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(engine, elem);
    }
}

static inline btree_elem_item *do_btree_get_first_elem(btree_indx_node *node)
{
    while (node->ndepth > 0) {
        node = (btree_indx_node *)(node->item[0]);
    }
    assert(node->ndepth == 0);
    return (btree_elem_item *)(node->item[0]);
}

static inline btree_elem_item *do_btree_get_last_elem(btree_indx_node *node)
{
    while (node->ndepth > 0) {
        node = (btree_indx_node *)(node->item[node->used_count-1]);
    }
    assert(node->ndepth == 0);
    return (btree_elem_item *)(node->item[node->used_count-1]);
}

static inline btree_indx_node *do_btree_get_first_leaf(btree_indx_node *node,
                                                       btree_elem_posi *path)
{
    while (node->ndepth > 0) {
        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = 0;
        }
        node = (btree_indx_node *)(node->item[0]);
    }
    assert(node->ndepth == 0);
    return node;
}

static inline btree_indx_node *do_btree_get_last_leaf(btree_indx_node *node,
                                                      btree_elem_posi *path)
{
    while (node->ndepth > 0) {
        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = node->used_count-1;
        }
        node = (btree_indx_node *)(node->item[node->used_count-1]);
    }
    assert(node->ndepth == 0);
    return node;
}

static inline void do_btree_get_bkey(btree_elem_item *elem, bkey_t *bkey)
{
    if (elem->nbkey > 0) {
        bkey->len = elem->nbkey;
        memcpy(bkey->val, elem->data, elem->nbkey);
    } else {
        bkey->len = 0;
        memcpy(bkey->val, elem->data, sizeof(uint64_t));
        //*(uint64_t*)bkey->val = *(uint64_t*)elem->data;
        //fprintf(stderr, "ss = %"PRIu64"\r\n", *(uint64_t*)elem->data);
    }
}

/******************* BKEY COMPARISION CODE *************************/
static inline int UINT64_COMP(const uint64_t *v1, const uint64_t *v2)
{
    if (*v1 == *v2) return  0;
    if (*v1 <  *v2) return -1;
    else            return  1;
}

static inline bool UINT64_ISEQ(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 == *v2) ? true : false);
}

static inline bool UINT64_ISNE(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 != *v2) ? true : false);
}

static inline bool UINT64_ISLT(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 <  *v2) ? true : false);
}

static inline bool UINT64_ISLE(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 <= *v2) ? true : false);
}

static inline bool UINT64_ISGT(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 >  *v2) ? true : false);
}

static inline bool UINT64_ISGE(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 >= *v2) ? true : false);
}

static inline int BINARY_COMP(const unsigned char *v1, const int nv1,
                              const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] <  v2[i]) return -1;
        else                return  1;
    }
    if (nv1 == nv2) return  0;
    if (nv1 <  nv2) return -1;
    else            return  1;
}

static inline bool BINARY_ISEQ(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    if (nv1 != nv2) return false;
    for (int i=0; i < nv1; i++) {
        if (v1[i] != v2[i]) return false;
    }
    return true;
}

static inline bool BINARY_ISNE(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    if (nv1 != nv2) return true;
    for (int i=0; i < nv1; i++) {
        if (v1[i] != v2[i]) return true;
    }
    return false;
}

static inline bool BINARY_ISLT(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] <  v2[i]) return true;
        else                return false;
    }
    if (nv1 < nv2) return true;
    else           return false;
}

static inline bool BINARY_ISLE(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] <  v2[i]) return true;
        else                return false;
    }
    if (nv1 <= nv2) return true;
    else            return false;
}

static inline bool BINARY_ISGT(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] >  v2[i]) return true;
        else                return false;
    }
    if (nv1 > nv2) return true;
    else           return false;
}

static inline bool BINARY_ISGE(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] >  v2[i]) return true;
        else                return false;
    }
    if (nv1 >= nv2) return true;
    else            return false;
}

static inline void BINARY_AND(const unsigned char *v1, const unsigned char *v2,
                              const int length, unsigned char *result)
{
    for (int i=0; i < length; i++) {
        result[i] = v1[i] & v2[i];
    }
}

static inline void BINARY_OR(const unsigned char *v1, const unsigned char *v2,
                             const int length, unsigned char *result)
{
    for (int i=0; i < length; i++) {
        result[i] = v1[i] | v2[i];
    }
}

static inline void BINARY_XOR(const unsigned char *v1, const unsigned char *v2,
                              const int length, unsigned char *result)
{
    for (int i=0; i < length; i++) {
        result[i] = v1[i] ^ v2[i];
    }
}

static bool (*BINARY_COMPARE_OP[COMPARE_OP_MAX]) (const unsigned char *v1, const int nv1,
                                                  const unsigned char *v2, const int nv2)
    = { BINARY_ISEQ, BINARY_ISNE, BINARY_ISLT, BINARY_ISLE, BINARY_ISGT, BINARY_ISGE };

static void (*BINARY_BITWISE_OP[BITWISE_OP_MAX]) (const unsigned char *v1, const unsigned char *v2,
                                                  const int length, unsigned char *result)
    = { BINARY_AND, BINARY_OR, BINARY_XOR };

#define BKEY_COMP(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_COMP((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_COMP((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISEQ(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISEQ((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISEQ((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISNE(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISNE((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISNE((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISLT(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISLT((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISLT((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISLE(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISLE((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISLE((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISGT(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISGT((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISGT((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISGE(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISGE((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISGE((bk1),(nbk1),(bk2),(nbk2)))
/******************* BKEY COMPARISION CODE *************************/

/**************** MAX BKEY RANGE MANIPULATION **********************/
static inline void UINT64_COPY(const uint64_t *v, uint64_t *result)
{
    *result = *v;
}

static inline void UINT64_DIFF(const uint64_t *v1, const uint64_t *v2, uint64_t *result)
{
    assert(*v1 >= *v2);
    *result = *v1 - *v2;
}

static inline void UINT64_INCR(uint64_t *v)
{
    assert(*v < UINT64_MAX);
    *v += 1;
}

static inline void UINT64_DECR(uint64_t *v)
{
    assert(*v > 0);
    *v -= 1;
}

static inline void BINARY_COPY(const unsigned char *v, const int length,
                               unsigned char *result)
{
    if (length > 0)
        memcpy(result, v, length);
}

static inline void BINARY_DIFF(unsigned char *v1, const uint8_t nv1,
                               unsigned char *v2, const uint8_t nv2,
                               const int length, unsigned char *result)
{
    assert(length > 0);
    unsigned char bkey1_space[MAX_BKEY_LENG];
    unsigned char bkey2_space[MAX_BKEY_LENG];
    int i, j;

    if (nv1 < length) {
        memcpy(bkey1_space, v1, nv1);
        for (i=nv1; i<length; i++)
            bkey1_space[i] = 0x00;
        v1 = bkey1_space;
    }
    if (nv2 < length) {
        memcpy(bkey2_space, v2, nv2);
        for (i=nv2; i<length; i++)
            bkey2_space[i] = 0x00;
        v2 = bkey2_space;
    }

    for (i = (length-1); i >= 0; ) {
        if (v1[i] >= v2[i]) {
            result[i] = v1[i] - v2[i];
            i -= 1;
        } else {
            result[i] = 0xFF - v2[i] + v1[i] + 1;
            for (j = (i-1); j >= 0; j--) {
               if (v1[j] > v2[j]) {
                   result[j] = v1[j] - 1 - v2[j];
                   break;
               } else {
                   result[j] = 0xFF - v2[j] + v1[j];
               }
            }
            assert(j >= 0);
            i = j-1;
        }
    }
}

static inline void BINARY_INCR(unsigned char *v, const int length)
{
    assert(length > 0);
    int i;
    for (i = (length-1); i >= 0; ) {
        if (v[i] < 0xFF) {
            v[i] += 1;
            break;
        } else {
            v[i] = 0x00;
        }
    }
    assert(i >= 0);
}

static inline void BINARY_DECR(unsigned char *v, const int length)
{
    assert(length > 0);
    int i;
    for (i = (length-1); i >= 0; ) {
        if (v[i] > 0x00) {
            v[i] -= 1;
            break;
        } else {
            v[i] = 0xFF;
        }
    }
    assert(i >= 0);
}

#define BKEY_COPY(bk, nbk, res) \
        ((nbk)==0 ? UINT64_COPY((const uint64_t*)(bk), (uint64_t*)(res)) \
                  : BINARY_COPY((bk), (nbk), (res)))
#define BKEY_DIFF(bk1, nbk1, bk2, nbk2, len, res) \
        ((len)==0 ? UINT64_DIFF((const uint64_t*)(bk1), (const uint64_t*)(bk2), (uint64_t*)(res)) \
                  : BINARY_DIFF((bk1), (nbk1), (bk2), (nbk2), (len), (res)))
#define BKEY_INCR(bk, nbk) \
        ((nbk)==0 ? UINT64_INCR((uint64_t*)(bk)) : BINARY_INCR((bk), (nbk)))
#define BKEY_DECR(bk, nbk) \
        ((nbk)==0 ? UINT64_DECR((uint64_t*)(bk)) : BINARY_DECR((bk), (nbk)))

/**************** MAX BKEY RANGE MANIPULATION **********************/

static int do_btree_bkey_range_type(const bkey_range *bkrange)
{
    if (bkrange->to_nbkey == BKEY_NULL) {
        return BKEY_RANGE_TYPE_SIN;
    } else {
        int comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey,
                             bkrange->to_bkey,   bkrange->to_nbkey);
        if (comp == 0)      return BKEY_RANGE_TYPE_SIN; /* single bkey */
        else if (comp < 0)  return BKEY_RANGE_TYPE_ASC; /* ascending */
        else                return BKEY_RANGE_TYPE_DSC; /* descending */
    }
}

static inline void do_btree_incr_posi(btree_elem_posi *posi)
{
    if (posi->indx < (posi->node->used_count-1)) {
        posi->indx += 1;
    } else {
        posi->node = posi->node->next;
        posi->indx = 0;
    }
}

static inline void do_btree_decr_posi(btree_elem_posi *posi)
{
    if (posi->indx > 0) {
        posi->indx -= 1;
    } else {
        posi->node = posi->node->prev;
        if (posi->node != NULL)
            posi->indx = posi->node->used_count-1;
        else
            posi->indx = BTREE_ITEM_COUNT;
    }
}

static void do_btree_incr_path(btree_elem_posi *path, int depth)
{
    btree_indx_node *saved_node;

    while (depth < BTREE_MAX_DEPTH) {
        saved_node = path[depth].node;
        do_btree_incr_posi(&path[depth]);
        if (path[depth].node == saved_node) break;
        depth += 1;
    }
    assert(depth < BTREE_MAX_DEPTH);
}

static void do_btree_decr_path(btree_elem_posi *path, int depth)
{
    btree_indx_node *saved_node;

    while (depth < BTREE_MAX_DEPTH) {
        saved_node = path[depth].node;
        do_btree_decr_posi(&path[depth]);
        if (path[depth].node == saved_node) break;
        depth += 1;
    }
    assert(depth < BTREE_MAX_DEPTH);
}

static btree_indx_node *do_btree_find_leaf(btree_indx_node *root,
                                           const unsigned char *bkey, const int nbkey,
                                           btree_elem_posi *path,
                                           btree_elem_item **found_elem)
{
    btree_indx_node *node = root;
    btree_elem_item *elem;
    int mid, left, right, comp;

    *found_elem = NULL; /* the same bkey is not found */

    while (node->ndepth > 0) {
        left  = 1;
        right = node->used_count-1;

        while (left <= right) {
            mid  = (left + right) / 2;
            elem = do_btree_get_first_elem((btree_indx_node *)(node->item[mid])); /* separator */
            comp = BKEY_COMP(bkey, nbkey, elem->data, elem->nbkey);
            if (comp == 0) break;
            if (comp <  0) right = mid-1;
            else           left  = mid+1;
        }

        if (left <= right) { /* found the element */
            *found_elem = elem; /* the same bkey is found */
            if (path) {
                path[node->ndepth].node = node;
                path[node->ndepth].indx = mid;
            }
            node = do_btree_get_first_leaf((btree_indx_node *)(node->item[mid]), path);
            assert(node->ndepth == 0);
            break;
        }

        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = right;
        }
        node = (btree_indx_node *)(node->item[right]);
    }
    return node;
}

static ENGINE_ERROR_CODE do_btree_find_insposi(btree_indx_node *root,
                                               const unsigned char *ins_bkey, const int ins_nbkey,
                                               btree_elem_posi *path)
{
    btree_indx_node *node;
    btree_elem_item *elem;
    int mid, left, right, comp;

    /* find leaf node */
    node = do_btree_find_leaf(root, ins_bkey, ins_nbkey, path, &elem);
    if (elem != NULL) { /* the bkey(ins_bkey) is found */
        /* while traversing to leaf node, the bkey can be found.
         * refer to do_btree_find_leaf() function.
         */
        path[0].node = node;
        path[0].indx = 0;
        return ENGINE_ELEM_EEXISTS;
    }

    /* do search the bkey(ins_bkey) in leaf node */
    left  = 0;
    right = node->used_count-1;

    while (left <= right) {
        mid  = (left + right) / 2;
        elem = BTREE_GET_ELEM_ITEM(node, mid);
        comp = BKEY_COMP(ins_bkey, ins_nbkey, elem->data, elem->nbkey);
        if (comp == 0) break;
        if (comp <  0) right = mid-1;
        else           left  = mid+1;
    }

    if (left <= right) { /* the bkey(ins_bkey) is found */
        path[0].node = node;
        path[0].indx = mid;
        return ENGINE_ELEM_EEXISTS;
    } else {             /* the bkey(ins_bkey) is not found */
        path[0].node = node;
        path[0].indx = left;
        return ENGINE_SUCCESS;
    }
}

static btree_elem_item *do_btree_find_first(btree_indx_node *root,
                                            const int bkrtype, const bkey_range *bkrange,
                                            btree_elem_posi *path, const bool path_flag)
{
    btree_indx_node *node;
    btree_elem_item *elem;
    int mid, left, right, comp;

    /* find leaf node */
    node = do_btree_find_leaf(root, bkrange->from_bkey, bkrange->from_nbkey,
                              (path_flag ? path : NULL), &elem);
    if (elem != NULL) { /* the bkey(from_bkey) is found */
        /* while traversing to leaf node, the bkey can be found.
         * refer to do_btree_find_leaf() function.
         */
        path[0].bkeq = true;
        path[0].node = node;
        path[0].indx = 0;
        return elem;
    }

    /* do search the bkey(from_bkey) in leaf node */
    left  = 0;
    right = node->used_count-1;

    while (left <= right) {
        mid  = (left + right) / 2;
        elem = BTREE_GET_ELEM_ITEM(node, mid);
        comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey, elem->data, elem->nbkey);
        if (comp == 0) break;
        if (comp <  0) right = mid-1;
        else           left  = mid+1;
    }

    if (left <= right) { /* the bkey(from_bkey) is found. */
        path[0].bkeq = true;
        path[0].node = node;
        path[0].indx = mid;
        /* elem != NULL */
    } else {             /* the bkey(from_bkey) is not found */
        path[0].bkeq = false;
        switch (bkrtype) {
          case BKEY_RANGE_TYPE_SIN: /* single bkey */
            if (left > 0 && left < node->used_count) {
                /* In order to represent the bkey is NOT outside of btree,
                 * set any existent element position.
                 */
                path[0].node = node;
                path[0].indx = left;
            } else {
                if (left >= node->used_count) {
                    path[0].node = node->next;
                    path[0].indx = 0;
                    if (path[0].node != NULL) {
                        if (path_flag) do_btree_incr_path(path, 1);
                    }
                } else { /* left == 0 && right == -1 */
                    path[0].node = node->prev;
                    if (node->prev != NULL) {
                        path[0].indx = node->prev->used_count-1;
                        if (path_flag) do_btree_decr_path(path, 1);
                    } else {
                        path[0].indx = BTREE_ITEM_COUNT;
                    }
                }
            }
            elem = NULL;
            break;
          case BKEY_RANGE_TYPE_ASC: /* ascending bkey range */
            /* find the next element */
            if (left < node->used_count) {
                path[0].node = node;
                path[0].indx = left;
            } else {
                path[0].node = node->next;
                path[0].indx = 0;
                if (path[0].node != NULL) {
                    if (path_flag) do_btree_incr_path(path, 1);
                }
            }
            if (path[0].node == NULL) {
                elem = NULL;
            } else {
                elem = BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
                if (BKEY_ISGT(elem->data, elem->nbkey, bkrange->to_bkey, bkrange->to_nbkey))
                    elem = NULL;
            }
            break;
          case BKEY_RANGE_TYPE_DSC: /* descending bkey range */
            /* find the prev element */
            if (right >= 0) {
                path[0].node = node;
                path[0].indx = right;
            } else {
                path[0].node = node->prev;
                if (node->prev != NULL) {
                    path[0].indx = node->prev->used_count-1;
                    if (path_flag) do_btree_decr_path(path, 1);
                } else {
                    path[0].indx = BTREE_ITEM_COUNT;
                }
            }
            if (path[0].node == NULL) {
                elem = NULL;
            } else {
                elem = BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
                if (BKEY_ISLT(elem->data, elem->nbkey, bkrange->to_bkey, bkrange->to_nbkey))
                    elem = NULL;
            }
            break;
        }
    }
    return elem;
}

static btree_elem_item *do_btree_find_next(btree_elem_posi *posi, const bkey_range *bkrange)
{
    btree_elem_item *elem;
    int comp;

    do_btree_incr_posi(posi);
    if (posi->node == NULL) {
        posi->bkeq = false;
        elem = NULL;
    } else {
        elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
        comp = BKEY_COMP(elem->data, elem->nbkey, bkrange->to_bkey, bkrange->to_nbkey);
        if (comp == 0) {
            posi->bkeq = true;
        } else {
            posi->bkeq = false;
            if (comp > 0) elem = NULL;
        }
    }
    return elem;
}

static btree_elem_item *do_btree_find_prev(btree_elem_posi *posi, const bkey_range *bkrange)
{
    btree_elem_item *elem;
    int comp;

    do_btree_decr_posi(posi);
    if (posi->node == NULL) {
        posi->bkeq = false;
        elem = NULL;
    } else {
        elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
        comp = BKEY_COMP(elem->data, elem->nbkey, bkrange->to_bkey, bkrange->to_nbkey);
        if (comp == 0) {
            posi->bkeq = true;
        } else {
            posi->bkeq = false;
            if (comp < 0) elem = NULL;
        }
    }
    return elem;
}

static inline bool do_btree_elem_filter(btree_elem_item *elem, const eflag_filter *efilter)
{
    assert(efilter != NULL);
    if (efilter->fwhere >= elem->neflag || efilter->ncompval > (elem->neflag-efilter->fwhere)) {
        return (efilter->compop == COMPARE_OP_NE ? true : false);
    }

    unsigned char result[MAX_EFLAG_LENG];
    unsigned char *operand = elem->data + BTREE_REAL_NBKEY(elem->nbkey) + efilter->fwhere;

    if (efilter->nbitwval > 0) {
        (*BINARY_BITWISE_OP[efilter->bitwop])(operand, efilter->bitwval, efilter->nbitwval, result);
        operand = &result[0];
    }

    if (efilter->compvcnt > 1) {
        assert(efilter->compop == COMPARE_OP_EQ || efilter->compop == COMPARE_OP_NE);
        for (int i = 0; i < efilter->compvcnt; i++) {
            if (BINARY_ISEQ(operand, efilter->ncompval,
                            &efilter->compval[i*efilter->ncompval], efilter->ncompval)) {
                return (efilter->compop == COMPARE_OP_EQ ? true : false);
            }
        }
        return (efilter->compop == COMPARE_OP_EQ ? false : true);
    } else {
        return (*BINARY_COMPARE_OP[efilter->compop])(operand, efilter->ncompval,
                                                     efilter->compval, efilter->ncompval);
    }
}

static void do_btree_consistency_check(btree_indx_node *node, uint32_t ecount, bool detail)
{
    uint32_t i, tot_ecnt;

    if (node == NULL) { /* root node */
        assert(ecount == 0);
        return;
    }

    if (node->prev != NULL) {
        assert(node->prev->next == node);
    }
    if (node->next != NULL) {
        assert(node->next->prev == node);
    }
    if (node->ndepth > 0) { /* nonleaf page check */
        tot_ecnt = 0;
        for (i = 0; i < node->used_count; i++) {
            assert(node->item[i] != NULL);
            assert(node->ecnt[i] > 0);
            do_btree_consistency_check((btree_indx_node*)node->item[i], node->ecnt[i], detail);
            tot_ecnt += node->ecnt[i];
        }
        assert(tot_ecnt == ecount);
    } else { /* node->ndepth == 0: leaf page check */
        for (i = 0; i < node->used_count; i++) {
            assert(node->item[i] != NULL);
        }
        assert(node->used_count == ecount);
        if (detail) {
            btree_elem_item *p_elem;
            btree_elem_item *c_elem;
            int comp;

            if (node->prev == NULL) {
                p_elem = NULL;
            } else {
                p_elem = BTREE_GET_ELEM_ITEM(node->prev, node->prev->used_count-1);
            }
            for (i = 0; i < node->used_count; i++) {
                c_elem = BTREE_GET_ELEM_ITEM(node, i);
                if (p_elem != NULL) {
                    comp = BKEY_COMP(p_elem->data, p_elem->nbkey, c_elem->data, c_elem->nbkey);
                    assert(comp < 0);
                }
                p_elem = c_elem;
            }
            if (node->next == NULL) {
                c_elem = NULL;
            } else {
                c_elem = BTREE_GET_ELEM_ITEM(node->next, 0);
            }
            if (c_elem != NULL) {
                comp = BKEY_COMP(p_elem->data, p_elem->nbkey, c_elem->data, c_elem->nbkey);
                assert(comp < 0);
            }
        }
    }
}

static void do_btree_node_item_move(btree_indx_node *c_node, /* current node */
                                    btree_indx_node *n_node, /* neighbor node */
                                    int direction, int move_count)
{
    assert(move_count > 0);
    int i;

    if (direction == BTREE_DIRECTION_NEXT) {
        if (c_node->ndepth == 0) { /* leaf node */
            for (i = (n_node->used_count-1); i >= 0; i--) {
                n_node->item[move_count+i] = n_node->item[i];
            }
            for (i = 0; i < move_count; i++) {
                n_node->item[i] = c_node->item[c_node->used_count-move_count+i];
                c_node->item[c_node->used_count-move_count+i] = NULL;
            }
        } else { /* c_node->ndepth > 0: nonleaf node */
            for (i = (n_node->used_count-1); i >= 0; i--) {
                n_node->item[move_count+i] = n_node->item[i];
                n_node->ecnt[move_count+i] = n_node->ecnt[i];
            }
            for (i = 0; i < move_count; i++) {
                n_node->item[i] = c_node->item[c_node->used_count-move_count+i];
                c_node->item[c_node->used_count-move_count+i] = NULL;
                n_node->ecnt[i] = c_node->ecnt[c_node->used_count-move_count+i];
                c_node->ecnt[c_node->used_count-move_count+i] = 0;
            }
        }
    } else { /* BTREE_DIRECTION_PREV */
        if (c_node->ndepth == 0) { /* leaf node */
            for (i = 0; i < move_count; i++) {
                n_node->item[n_node->used_count+i] = c_node->item[i];
            }
            for (i = move_count; i < c_node->used_count; i++) {
                c_node->item[i-move_count] = c_node->item[i];
                c_node->item[i] = NULL;
            }
        } else { /* c_node->ndepth > 0: nonleaf node */
            for (i = 0; i < move_count; i++) {
                n_node->item[n_node->used_count+i] = c_node->item[i];
                n_node->ecnt[n_node->used_count+i] = c_node->ecnt[i];
            }
            for (i = move_count; i < c_node->used_count; i++) {
                c_node->item[i-move_count] = c_node->item[i];
                c_node->item[i] = NULL;
                c_node->ecnt[i-move_count] = c_node->ecnt[i];
                c_node->ecnt[i] = 0;
            }
        }
    }
    n_node->used_count += move_count;
    c_node->used_count -= move_count;
}

static void do_btree_ecnt_move_split(btree_elem_posi *path, int depth, int direction, uint32_t elem_count)
{
    btree_elem_posi  posi;
    btree_indx_node *saved_node;

    while (depth < BTREE_MAX_DEPTH) {
        posi = path[depth];
        posi.node->ecnt[posi.indx] -= elem_count;

        saved_node = posi.node;
        if (direction == BTREE_DIRECTION_NEXT) {
            do_btree_incr_posi(&posi);
        } else {
            do_btree_decr_posi(&posi);
        }
        posi.node->ecnt[posi.indx] += elem_count;
        if (saved_node == posi.node) break;
        depth += 1;
    }
    assert(depth < BTREE_MAX_DEPTH);
}

static void do_btree_ecnt_move_merge(btree_elem_posi *path, int depth, int direction, uint32_t elem_count)
{
    btree_elem_posi  posi;
    btree_indx_node *saved_node;

    while (depth < BTREE_MAX_DEPTH) {
        posi = path[depth];
        posi.node->ecnt[posi.indx] -= elem_count;

        saved_node = posi.node;
        if (direction == BTREE_DIRECTION_NEXT) {
            do {
                do_btree_incr_posi(&posi);
            } while (posi.node->used_count == 0 ||
                     posi.node->ecnt[posi.indx] == 0);
        } else {
            do {
                do_btree_decr_posi(&posi);
            } while (posi.node->used_count == 0 ||
                     posi.node->ecnt[posi.indx] == 0);
        }
        posi.node->ecnt[posi.indx] += elem_count;
        if (saved_node == posi.node) break;
        depth += 1;
    }
    assert(depth < BTREE_MAX_DEPTH);
}

static void do_btree_node_sbalance(btree_indx_node *node, btree_elem_posi *path, int depth)
{
    btree_elem_posi *posi;
    int direction;
    int move_count;
    int elem_count; /* total count of elements moved */
    int i;

    /* balance the number of elements with neighber node */
    if (node->next != NULL && node->prev != NULL) {
        direction = (node->next->used_count < node->prev->used_count ?
                     BTREE_DIRECTION_NEXT : BTREE_DIRECTION_PREV);
    } else {
        direction = (node->next != NULL ?
                     BTREE_DIRECTION_NEXT : BTREE_DIRECTION_PREV);
    }
    if (direction == BTREE_DIRECTION_NEXT) {
        if (node->next->used_count > 0) {
            move_count = (node->used_count - node->next->used_count) / 2;
        } else {
            move_count = (node->next->next == NULL ? (node->used_count / 10)
                                                   : (node->used_count / 2));
        }
        if (move_count == 0) move_count = 1;

        if (depth == 0) {
            elem_count = move_count;
        } else {
            elem_count = 0;
            for (i = 0; i < move_count; i++) {
                elem_count += node->ecnt[node->used_count-move_count+i];
            }
        }

        do_btree_node_item_move(node, node->next, direction, move_count);

        /* move element count in upper btree nodes */
        do_btree_ecnt_move_split(path, depth+1, direction, elem_count);

        /* adjust posi information */
        posi = &path[depth];
        if (posi->indx >= node->used_count) {
            posi->node = node->next;
            posi->indx -= node->used_count;
            /* adjust upper path info */
            do_btree_incr_path(path, depth+1);
        }
    } else {
        if (node->prev->used_count > 0) {
            move_count = (node->used_count - node->prev->used_count) / 2;
        } else {
            move_count = (node->prev->prev == NULL ? (node->used_count / 10)
                                                   : (node->used_count / 2));
        }
        if (move_count == 0) move_count = 1;

        if (depth == 0) {
            elem_count = move_count;
        } else {
            elem_count = 0;
            for (i = 0; i < move_count; i++) {
                elem_count += node->ecnt[i];
            }
        }

        do_btree_node_item_move(node, node->prev, direction, move_count);

        /* move element count in upper btree nodes */
        do_btree_ecnt_move_split(path, depth+1, direction, elem_count);

        /* adjust posi information */
        posi = &path[depth];
        if (posi->indx < move_count) {
            posi->node = node->prev;
            posi->indx += (node->prev->used_count-move_count);
            /* adjust upper path info */
            do_btree_decr_path(path, depth+1);
        } else {
            posi->indx -= move_count;
        }
    }
}

static void do_btree_node_link(struct default_engine *engine,
                               btree_meta_info *info, btree_indx_node *node,
                               btree_elem_posi *p_posi)
{
    /*
     * p_posi: the position of to-be-linked node in parent node.
     */
    if (p_posi == NULL) {
        /* No parent node : make a new root node */
        if (info->root == NULL) {
            node->used_count = 0;
        } else {
            node->item[0] = info->root;
            node->ecnt[0] = info->ccnt;
            node->used_count = 1;
        }
        info->root = node;
    } else {
        /* Parent node exists */
        btree_indx_node *p_node = p_posi->node;
        assert(p_node->used_count >= 1);
        assert(p_posi->indx <= p_node->used_count);

        if (p_posi->indx == 0) {
            node->prev = (p_node->prev == NULL ?
                          NULL : p_node->prev->item[p_node->prev->used_count-1]);
            node->next = p_node->item[p_posi->indx];
        } else if (p_posi->indx < p_node->used_count) {
            node->prev = p_node->item[p_posi->indx-1];
            node->next = p_node->item[p_posi->indx];
        } else { /* p_posi->index == p_node->used_count */
            node->prev = p_node->item[p_posi->indx-1];
            node->next = (p_node->next == NULL ?
                          NULL : p_node->next->item[0]);
        }
        if (node->prev != NULL) node->prev->next = node;
        if (node->next != NULL) node->next->prev = node;

        for (int i = (p_node->used_count-1); i >= p_posi->indx; i--) {
            p_node->item[i+1] = p_node->item[i];
            p_node->ecnt[i+1] = p_node->ecnt[i];
        }
        p_node->item[p_posi->indx] = node;
        p_node->ecnt[p_posi->indx] = 0;
        p_node->used_count++;
    }

    if (1) { /* apply memory space */
        size_t stotal;
        if (node->ndepth > 0) stotal = slabs_space_size(engine, sizeof(btree_indx_node));
        else                  stotal = slabs_space_size(engine, sizeof(btree_leaf_node));
        increase_collection_space(engine, ITEM_TYPE_BTREE, (coll_meta_info *)info, stotal);
    }
}

static ENGINE_ERROR_CODE do_btree_node_split(struct default_engine *engine,
                                             btree_meta_info *info, btree_elem_posi *path,
                                             const void *cookie)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    btree_indx_node *s_node;
    btree_indx_node *n_node[BTREE_MAX_DEPTH]; /* neighber nodes */
    btree_elem_posi  p_posi;
    int     i, direction;
    uint8_t btree_depth = 0;

    s_node = path[btree_depth].node;
    do {
        if ((s_node->next != NULL && s_node->next->used_count < (BTREE_ITEM_COUNT/2)) ||
            (s_node->prev != NULL && s_node->prev->used_count < (BTREE_ITEM_COUNT/2))) {
            do_btree_node_sbalance(s_node, path, btree_depth);
            break;
        }

        n_node[btree_depth] = do_btree_node_alloc(engine, btree_depth, cookie);
        if (n_node[btree_depth] == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        btree_depth += 1;
        assert(btree_depth < BTREE_MAX_DEPTH);
        if (btree_depth > info->root->ndepth) {
            btree_indx_node *r_node = do_btree_node_alloc(engine, btree_depth, cookie);
            if (r_node == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            do_btree_node_link(engine, info, r_node, NULL);
            path[btree_depth].node = r_node;
            path[btree_depth].indx = 0;
            break;
        }
        s_node = path[btree_depth].node;
    }
    while (s_node->used_count >= BTREE_ITEM_COUNT);

    if (ret == ENGINE_SUCCESS) {
        for (i = btree_depth-1; i >= 0; i--) {
            s_node = path[i].node;
            if (s_node->prev == NULL && s_node->next == NULL) {
                direction = (path[i].indx < (BTREE_ITEM_COUNT/2) ?
                             BTREE_DIRECTION_PREV : BTREE_DIRECTION_NEXT);
            } else {
                direction = (s_node->prev == NULL ?
                             BTREE_DIRECTION_PREV : BTREE_DIRECTION_NEXT);
            }
            p_posi = path[i+1];
            if (direction == BTREE_DIRECTION_NEXT) p_posi.indx += 1;
            do_btree_node_link(engine, info, n_node[i], &p_posi);

            if (direction == BTREE_DIRECTION_PREV) {
                /* adjust upper path */
                path[i+1].indx += 1;
                //do_btree_incr_path(path, i+1);
            }
            do_btree_node_sbalance(s_node, path, i);
        }
    } else {
        for (i = 0; i < btree_depth; i++) {
            do_btree_node_free(engine, n_node[i]);
        }
    }
    if (btree_position_debug) {
        do_btree_consistency_check(info->root, info->ccnt, true);
    }
    return ret;
}

/* merge check */
static void do_btree_node_mbalance(btree_indx_node *node, btree_elem_posi *path, int depth)
{
    int direction;

    if (node->prev != NULL && node->next != NULL) {
        direction = (node->next->used_count < node->prev->used_count ?
                     BTREE_DIRECTION_NEXT : BTREE_DIRECTION_PREV);
    } else {
        direction = (node->next != NULL ?
                     BTREE_DIRECTION_NEXT : BTREE_DIRECTION_PREV);
    }
    if (direction == BTREE_DIRECTION_NEXT) {
        do_btree_node_item_move(node, node->next, direction, node->used_count);
    } else {
        do_btree_node_item_move(node, node->prev, direction, node->used_count);
    }

    int elem_count = path[depth+1].node->ecnt[path[depth+1].indx];
    do_btree_ecnt_move_merge(path, depth+1, direction, elem_count);
}

static void do_btree_node_unlink(struct default_engine *engine,
                                 btree_meta_info *info, btree_indx_node *node,
                                 btree_elem_posi *p_posi)
{
    if (p_posi == NULL) {
        /* No parent node : remove the root node */
        info->root = NULL;
    } else {
        /* unlink the given node from b+tree */
        if (node->prev != NULL) node->prev->next = node->next;
        if (node->next != NULL) node->next->prev = node->prev;
        node->prev = node->next = NULL;

        /* Parent node exists */
        btree_indx_node *p_node = p_posi->node;
        assert(p_node->ecnt[p_posi->indx] == 0);
        for (int i = p_posi->indx+1; i < p_node->used_count; i++) {
            p_node->item[i-1] = p_node->item[i];
            p_node->ecnt[i-1] = p_node->ecnt[i];
        }
        p_node->item[p_node->used_count-1] = NULL;
        p_node->ecnt[p_node->used_count-1] = 0;
        p_node->used_count--;
    }

    if (info->stotal > 0) { /* apply memory space */
        size_t stotal;
        if (node->ndepth > 0) stotal = slabs_space_size(engine, sizeof(btree_indx_node));
        else                  stotal = slabs_space_size(engine, sizeof(btree_leaf_node));
        decrease_collection_space(engine, ITEM_TYPE_BTREE, (coll_meta_info *)info, stotal);
    }

    /* The amount of space to be decreased become different according to node depth.
     * So, the btree node must be freed after collection space is decreased.
     */
    do_btree_node_free(engine, node);
}

static void do_btree_node_detach(struct default_engine *engine, btree_indx_node *node)
{
    /* unlink the given node from b+tree */
    if (node->prev != NULL) node->prev->next = node->next;
    if (node->next != NULL) node->next->prev = node->prev;
    node->prev = node->next = NULL;

    do_btree_node_free(engine, node);
}

static inline void do_btree_node_remove_null_items(btree_elem_posi *posi, const bool forward, const int null_count)
{
    btree_indx_node *node = posi->node;
    assert(null_count <= node->used_count);

    if (null_count < node->used_count) {
        int f, i;
        int rem_count = 0;
        f = (forward ? posi->indx : 0);
        for ( ; f < node->used_count; f++) {
            if (node->item[f] == NULL) {
                rem_count++;
                break;
            }
        }
        for (i = f+1; i < node->used_count; i++) {
            if (node->item[i] != NULL) {
                node->item[f] = node->item[i];
                node->item[i] = NULL;
                if (node->ndepth > 0) {
                    node->ecnt[f] = node->ecnt[i];
                    node->ecnt[i] = 0;
                }
                f++;
            } else {
                rem_count++;
            }
        }
        assert(rem_count == null_count);
    }
    node->used_count -= null_count;
}

static void do_btree_node_merge(struct default_engine *engine,
                                btree_meta_info *info, btree_elem_posi *path,
                                const bool forward, const int leaf_node_count)
{
    btree_indx_node *node;
    int cur_node_count = leaf_node_count;
    int par_node_count;
    uint8_t btree_depth = 0;

    /*
     * leaf_node_count : # of leaf nodes to be merged.
     * cur_node_count  : # of current nodes to be merged in the current btree depth.
     * par_node_count  : # of parent nodes that might be merged after the current merge.
     */
    while (cur_node_count > 0)
    {
        par_node_count = 0;
        if (cur_node_count == 1) {
            node = path[btree_depth].node;
            if (node == info->root) {
                if (node->used_count == 0) {
                    do_btree_node_unlink(engine, info, node, NULL);
                } else {
                    btree_indx_node *new_root;
                    while (node->used_count == 1 && node->ndepth > 0) {
                        new_root = BTREE_GET_NODE_ITEM(node, 0);
                        do_btree_node_unlink(engine, info, node, NULL);
                        info->root = new_root;
                        node = new_root;
                    }
                }
            } else {
                if (node->used_count == 0) {
                    do_btree_node_unlink(engine, info, node, &path[btree_depth+1]);
                    par_node_count = 1;
                }
                else if (node->used_count < (BTREE_ITEM_COUNT/2)) {
                    if ((node->prev != NULL && node->prev->used_count < (BTREE_ITEM_COUNT/2)) ||
                        (node->next != NULL && node->next->used_count < (BTREE_ITEM_COUNT/2))) {
                        do_btree_node_mbalance(node, path, btree_depth);
                        do_btree_node_unlink(engine, info, node, &path[btree_depth+1]);
                        par_node_count = 1;
                    }
                }
            }
        } else { /* cur_node_count > 1 */
            btree_elem_posi  upth[BTREE_MAX_DEPTH]; /* upper node path */
            btree_elem_posi  s_posi;
            int tot_unlink_cnt = 0;
            int cur_unlink_cnt = 0;
            int i, upp_depth = btree_depth+1;

            /* prepare upper node path */
            for (i = upp_depth; i <= info->root->ndepth; i++) {
                upth[i] = path[i];
            }

            s_posi = upth[upp_depth];
            for (i = 1; i <= cur_node_count; i++) {
                node = BTREE_GET_NODE_ITEM(s_posi.node, s_posi.indx);
                assert(node != NULL);

                if (node->used_count == 0) {
                    do_btree_node_detach(engine, node);
                    s_posi.node->item[s_posi.indx] = NULL;
                    assert(s_posi.node->ecnt[s_posi.indx] == 0);
                }

                if (i == cur_node_count) break;

                if (forward) do_btree_incr_posi(&s_posi);
                else         do_btree_decr_posi(&s_posi);
            }

            s_posi = upth[upp_depth];
            for (i = 1; i <= cur_node_count; i++) {
                node = BTREE_GET_NODE_ITEM(upth[upp_depth].node, upth[upp_depth].indx);
                if (node == NULL) {
                    cur_unlink_cnt++;
                }
                else if (node->used_count < (BTREE_ITEM_COUNT/2)) {
                    if ((node->prev != NULL && node->prev->used_count < (BTREE_ITEM_COUNT/2)) ||
                        (node->next != NULL && node->next->used_count < (BTREE_ITEM_COUNT/2))) {
                        do_btree_node_mbalance(node, upth, btree_depth);
                        do_btree_node_detach(engine, node);
                        upth[upp_depth].node->item[upth[upp_depth].indx] = NULL;
                        assert(upth[upp_depth].node->ecnt[upth[upp_depth].indx] == 0);
                        cur_unlink_cnt++;
                    }
                }

                if (i == cur_node_count) break;

                if (forward) do_btree_incr_path(upth, upp_depth);
                else         do_btree_decr_path(upth, upp_depth);

                if (s_posi.node != upth[upp_depth].node) {
                    if (cur_unlink_cnt > 0) {
                        do_btree_node_remove_null_items(&s_posi, forward, cur_unlink_cnt);
                        tot_unlink_cnt += cur_unlink_cnt; cur_unlink_cnt = 0;
                    }
                    s_posi = upth[upp_depth];
                    par_node_count += 1;
                }
            }
            if (cur_unlink_cnt > 0) {
                do_btree_node_remove_null_items(&s_posi, forward, cur_unlink_cnt);
                tot_unlink_cnt += cur_unlink_cnt;
                par_node_count += 1;
            }
            if (tot_unlink_cnt > 0 && info->stotal > 0) { /* apply memory space */
                size_t stotal;
                if (btree_depth > 0) stotal = tot_unlink_cnt * slabs_space_size(engine, sizeof(btree_indx_node));
                else                 stotal = tot_unlink_cnt * slabs_space_size(engine, sizeof(btree_leaf_node));
                decrease_collection_space(engine, ITEM_TYPE_BTREE, (coll_meta_info *)info, stotal);
            }
        }
        btree_depth += 1;
        cur_node_count = par_node_count;
    }
    if (btree_position_debug) {
        do_btree_consistency_check(info->root, info->ccnt, true);
    }
}

static void do_btree_elem_unlink(struct default_engine *engine,
                                 btree_meta_info *info, btree_elem_posi *path)
{
    btree_elem_posi *posi = &path[0];
    btree_elem_item *elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    int i;

    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(engine, BTREE_ELEM_SIZE(elem));
        decrease_collection_space(engine, ITEM_TYPE_BTREE, (coll_meta_info *)info, stotal);
    }

    if (elem->refcount > 0) {
        elem->status = BTREE_ITEM_STATUS_UNLINK;
    } else  {
        elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(engine, elem);
    }

    /* remove the element from the leaf node */
    btree_indx_node *node = posi->node;
    for (i = posi->indx+1; i < node->used_count; i++) {
        node->item[i-1] = node->item[i];
    }
    node->item[node->used_count-1] = NULL;
    node->used_count--;
    /* decrement element count in upper nodes */
    for (i = 1; i <= info->root->ndepth; i++) {
        path[i].node->ecnt[path[i].indx]--;
    }
    info->ccnt--;

    if (node->used_count < (BTREE_ITEM_COUNT/2)) {
        do_btree_node_merge(engine, info, path, true, 1);
    }
}

static ENGINE_ERROR_CODE do_btree_elem_replace(struct default_engine *engine, btree_meta_info *info,
                                               btree_elem_posi *posi, btree_elem_item *new_elem)
{
    btree_elem_item *old_elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    size_t old_stotal = slabs_space_size(engine, BTREE_ELEM_SIZE(old_elem));
    size_t new_stotal = slabs_space_size(engine, BTREE_ELEM_SIZE(new_elem));

#ifdef ENABLE_STICKY_ITEM
    if (new_stotal > old_stotal) {
        /* sticky memory limit check */
        if ((info->mflags & COLL_META_FLAG_STICKY) != 0) {
            if (engine->stats.sticky_bytes >= engine->config.sticky_limit)
                return ENGINE_ENOMEM;
        }
    }
#endif

    if (old_elem->refcount > 0) {
        old_elem->status = BTREE_ITEM_STATUS_UNLINK;
    } else  {
        old_elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(engine, old_elem);
    }

    new_elem->status = BTREE_ITEM_STATUS_USED;
    posi->node->item[posi->indx] = new_elem;

    if (new_stotal != old_stotal) { /* apply memory space */
        assert(info->stotal > 0);
        if (new_stotal > old_stotal)
            increase_collection_space(engine, ITEM_TYPE_BTREE, (coll_meta_info *)info, (new_stotal-old_stotal));
        else
            decrease_collection_space(engine, ITEM_TYPE_BTREE, (coll_meta_info *)info, (old_stotal-new_stotal));
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_btree_elem_update(struct default_engine *engine, btree_meta_info *info,
                                              const int bkrtype, const bkey_range *bkrange,
                                              const eflag_update *eupdate,
                                              const char *value, const int nbytes, const void *cookie)
{
    ENGINE_ERROR_CODE ret = ENGINE_ELEM_ENOENT;
    btree_elem_posi  posi;
    btree_elem_item *elem;
    unsigned char *ptr;
    int real_nbkey;
    int new_neflag;
    int new_nbytes;

    if (info->root == NULL) return ret;

    elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
    if (elem != NULL) {
        assert(posi.bkeq == true);

        /* check eflag update validation check */
        if (eupdate != NULL && eupdate->neflag > 0 && eupdate->bitwop < BITWISE_OP_MAX) {
            if (eupdate->fwhere >= elem->neflag || eupdate->neflag > (elem->neflag-eupdate->fwhere)) {
                return ENGINE_EBADEFLAG;
            }
        }

        real_nbkey = BTREE_REAL_NBKEY(elem->nbkey);

        new_neflag = (eupdate == NULL || eupdate->bitwop < BITWISE_OP_MAX ? elem->neflag : eupdate->neflag);
        new_nbytes = (value == NULL ? elem->nbytes : nbytes);

        if (elem->refcount == 0 && (elem->neflag+elem->nbytes) == (new_neflag+new_nbytes)) {
            /* old body size == new body size */
            /* do in-place update */
            if (eupdate != NULL) {
                if (eupdate->bitwop < BITWISE_OP_MAX) {
                    ptr = elem->data + real_nbkey + eupdate->fwhere;
                    (*BINARY_BITWISE_OP[eupdate->bitwop])(ptr, eupdate->eflag, eupdate->neflag, ptr);
                } else {
                    if (eupdate->neflag > 0) {
                        memcpy(elem->data + real_nbkey, eupdate->eflag, eupdate->neflag);
                    }
                    elem->neflag = eupdate->neflag;
                }
            }
            if (value != NULL) {
                memcpy(elem->data + real_nbkey + elem->neflag, value, nbytes);
                elem->nbytes = nbytes;
            }
            ret = ENGINE_SUCCESS;
        } else {
            /* old body size != new body size */
            btree_elem_item *new_elem = do_btree_elem_alloc(engine, elem->nbkey, new_neflag, new_nbytes, cookie);
            if (new_elem == NULL) {
                return ENGINE_ENOMEM;
            }

            /* build the new element */
            memcpy(new_elem->data, elem->data, real_nbkey);

            if (eupdate == NULL || eupdate->bitwop < BITWISE_OP_MAX) {
                if (elem->neflag > 0) {
                    memcpy(new_elem->data + real_nbkey, elem->data + real_nbkey, elem->neflag);
                }
                if (eupdate != NULL) {
                    ptr = new_elem->data + real_nbkey + eupdate->fwhere;
                    (*BINARY_BITWISE_OP[eupdate->bitwop])(ptr, eupdate->eflag, eupdate->neflag, ptr);
                }
            } else {
                if (eupdate->neflag > 0) {
                    memcpy(new_elem->data + real_nbkey, eupdate->eflag, eupdate->neflag);
                }
            }

            ptr = new_elem->data + real_nbkey + new_elem->neflag;
            if (value != NULL) {
                memcpy(ptr, value, nbytes);
            } else {
                memcpy(ptr, elem->data + real_nbkey + elem->neflag, elem->nbytes);
            }

            ret = do_btree_elem_replace(engine, info, &posi, new_elem);
            do_btree_elem_release(engine, new_elem);
        }
    }
    return ret;
}

static uint32_t do_btree_elem_delete(struct default_engine *engine, btree_meta_info *info,
                                     const int bkrtype, const bkey_range *bkrange,
                                     const eflag_filter *efilter, const uint32_t count)
{
    btree_elem_posi  path[BTREE_MAX_DEPTH];
    btree_elem_item *elem;
    uint32_t tot_fcnt; /* found count */

    if (info->root == NULL) return 0;

    assert(info->root->ndepth < BTREE_MAX_DEPTH);
    tot_fcnt = 0;
    elem = do_btree_find_first(info->root, bkrtype, bkrange, path, true);
    if (elem != NULL) {
        if (bkrtype == BKEY_RANGE_TYPE_SIN) {
            assert(path[0].bkeq == true);
            if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                do_btree_elem_unlink(engine, info, path);
                tot_fcnt = 1;
            }
        } else {
            btree_elem_posi upth[BTREE_MAX_DEPTH]; /* upper node path */
            btree_elem_posi c_posi = path[0];
            btree_elem_posi s_posi = c_posi;
            size_t stotal = 0;
            int  cur_fcnt = 0;
            int  node_cnt = 1;
            bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);
            int i;

            /* prepare upper node path
             * used to incr/decr element counts  in upper nodes.
             */
            for (i = 1; i <= info->root->ndepth; i++) {
                upth[i] = path[i];
            }

            c_posi.bkeq = false;
            do {
                if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                    stotal += slabs_space_size(engine, BTREE_ELEM_SIZE(elem));

                    if (elem->refcount > 0) {
                        elem->status = BTREE_ITEM_STATUS_UNLINK;
                    } else {
                        elem->status = BTREE_ITEM_STATUS_FREE;
                        do_btree_elem_free(engine, elem);
                    }
                    c_posi.node->item[c_posi.indx] = NULL;

                    cur_fcnt++;
                    if (count > 0 && (tot_fcnt+cur_fcnt) >= count) break;
                }

                if (c_posi.bkeq == true) {
                    elem = NULL; break;
                }
                elem = (forward ? do_btree_find_next(&c_posi, bkrange)
                                : do_btree_find_prev(&c_posi, bkrange));
                if (elem == NULL) break;

                if (s_posi.node != c_posi.node) {
                    if (cur_fcnt > 0) {
                        do_btree_node_remove_null_items(&s_posi, forward, cur_fcnt);
                        /* decrement element count in upper nodes */
                        for (i = 1; i <= info->root->ndepth; i++) {
                            assert(upth[i].node->ecnt[upth[i].indx] >= cur_fcnt);
                            upth[i].node->ecnt[upth[i].indx] -= cur_fcnt;
                        }
                        tot_fcnt += cur_fcnt; cur_fcnt = 0;
                    }
                    if (info->root->ndepth > 0) {
                        /* adjust upper node path */
                        if (forward) do_btree_incr_path(upth, 1);
                        else         do_btree_decr_path(upth, 1);
                    }
                    s_posi = c_posi;
                    node_cnt += 1;
                }
            } while (elem != NULL);

            if (cur_fcnt > 0) {
                do_btree_node_remove_null_items(&s_posi, forward, cur_fcnt);
                /* decrement element count in upper nodes */
                for (i = 1; i <= info->root->ndepth; i++) {
                    assert(upth[i].node->ecnt[upth[i].indx] >= cur_fcnt);
                    upth[i].node->ecnt[upth[i].indx] -= cur_fcnt;
                }
                tot_fcnt += cur_fcnt;
            }
            if (tot_fcnt > 0) {
                info->ccnt -= tot_fcnt;
                if (info->stotal > 0) { /* apply memory space */
                    /* The btree has already been unlinked from hash table.
                     * If then, the btree doesn't have prefix info and has stotal of 0.
                     * So, do not need to descrese space total info.
                     */
                    assert(stotal > 0 && stotal <= info->stotal);
                    decrease_collection_space(engine, ITEM_TYPE_BTREE, (coll_meta_info *)info, stotal);
                }
                do_btree_node_merge(engine, info, path, forward, node_cnt);
            }
        }
    }
    return tot_fcnt;
}

static inline void get_bkey_full_range(const int bktype, const bool ascend, bkey_range *bkrange)
{
    if (bktype == BKEY_TYPE_BINARY) {
        if (ascend) {
            memcpy(bkrange->from_bkey, btree_binary_min_bkey, BKEY_MIN_BINARY_LENG);
            memcpy(bkrange->to_bkey,   btree_binary_max_bkey, BKEY_MAX_BINARY_LENG);
            bkrange->from_nbkey = BKEY_MIN_BINARY_LENG;
            bkrange->to_nbkey   = BKEY_MAX_BINARY_LENG;
        } else {
            memcpy(bkrange->from_bkey, btree_binary_max_bkey, BKEY_MAX_BINARY_LENG);
            memcpy(bkrange->to_bkey,   btree_binary_min_bkey, BKEY_MIN_BINARY_LENG);
            bkrange->from_nbkey = BKEY_MAX_BINARY_LENG;
            bkrange->to_nbkey   = BKEY_MIN_BINARY_LENG;
        }
    } else { /* bktype == BKEY_TYPE_UINT64 or BKEY_TYPE_UNKNOWN */
        if (ascend) {
            memcpy(bkrange->from_bkey, (unsigned char*)&btree_uint64_min_bkey, sizeof(uint64_t));
            memcpy(bkrange->to_bkey,   (unsigned char*)&btree_uint64_max_bkey, sizeof(uint64_t));
            //*(uint64_t*)bkrange->from_bkey = btree_uint64_min_bkey;
            //*(uint64_t*)bkrange->to_bkey   = btree_uint64_max_bkey;
        } else {
            memcpy(bkrange->from_bkey, (unsigned char*)&btree_uint64_max_bkey, sizeof(uint64_t));
            memcpy(bkrange->to_bkey,   (unsigned char*)&btree_uint64_min_bkey, sizeof(uint64_t));
            //*(uint64_t*)bkrange->from_bkey = btree_uint64_max_bkey;
            //*(uint64_t*)bkrange->to_bkey   = btree_uint64_min_bkey;
        }
        bkrange->from_nbkey = bkrange->to_nbkey = 0;
    }
}

static ENGINE_ERROR_CODE do_btree_overflow_check(btree_meta_info *info, btree_elem_item *elem,
                                                 int *overflow_type)
{
    /* info->ccnt >= 1 */
    btree_elem_item *min_bkey_elem = NULL;
    btree_elem_item *max_bkey_elem = NULL;

    /* step 1: overflow check on max bkey range */
    if (info->maxbkeyrange.len != BKEY_NULL) {
        bkey_t newbkeyrange;

        min_bkey_elem = do_btree_get_first_elem(info->root);
        max_bkey_elem = do_btree_get_last_elem(info->root);

        if (BKEY_ISLT(elem->data, elem->nbkey, min_bkey_elem->data, min_bkey_elem->nbkey))
        {
            newbkeyrange.len = info->maxbkeyrange.len;
            BKEY_DIFF(max_bkey_elem->data, max_bkey_elem->nbkey, elem->data, elem->nbkey,
                      newbkeyrange.len, newbkeyrange.val);
            if (BKEY_ISGT(newbkeyrange.val, newbkeyrange.len, info->maxbkeyrange.val, info->maxbkeyrange.len))
            {
                if (info->ovflact == OVFL_LARGEST_TRIM || info->ovflact == OVFL_LARGEST_SILENT_TRIM)
                    *overflow_type = OVFL_TYPE_RANGE;
                else /* OVFL_SMALLEST_TRIM || OVFL_SMALLEST_SILENT_TRIM || OVFL_ERROR */
                    return ENGINE_EBKEYOOR;
            }
        }
        else if (BKEY_ISGT(elem->data, elem->nbkey, max_bkey_elem->data, max_bkey_elem->nbkey))
        {
            newbkeyrange.len = info->maxbkeyrange.len;
            BKEY_DIFF(elem->data, elem->nbkey, min_bkey_elem->data, min_bkey_elem->nbkey,
                      newbkeyrange.len, newbkeyrange.val);
            if (BKEY_ISGT(newbkeyrange.val, newbkeyrange.len, info->maxbkeyrange.val, info->maxbkeyrange.len))
            {
                if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM)
                    *overflow_type = OVFL_TYPE_RANGE;
                else /* OVFL_LARGEST_TRIM || OVFL_LARGEST_SILENT_TRIM || OVFL_ERROR */
                    return ENGINE_EBKEYOOR;
            }
        }
    }

    /* step 2: overflow check on max element count */
    if (info->ccnt >= info->mcnt && *overflow_type == OVFL_TYPE_NONE) {
        if (info->ovflact == OVFL_ERROR) {
            return ENGINE_EOVERFLOW;
        }
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM) {
            if (min_bkey_elem == NULL)
                min_bkey_elem = do_btree_get_first_elem(info->root);
            if (BKEY_ISLT(elem->data, elem->nbkey, min_bkey_elem->data, min_bkey_elem->nbkey))
                return ENGINE_EBKEYOOR;
        } else { /* OVFL_LARGEST_TRIM || OVFL_LARGEST_SILENT_TRIM */
            if (max_bkey_elem == NULL)
                max_bkey_elem = do_btree_get_last_elem(info->root);
            if (BKEY_ISGT(elem->data, elem->nbkey, max_bkey_elem->data, max_bkey_elem->nbkey))
                return ENGINE_EBKEYOOR;
        }
        *overflow_type = OVFL_TYPE_COUNT;
    }

    return ENGINE_SUCCESS;
}

static void do_btree_overflow_trim(struct default_engine *engine, btree_meta_info *info,
                                   btree_elem_item *elem, const int overflow_type,
                                   btree_elem_item **trimmed_elems, uint32_t *trimmed_count)
{
    assert(info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM ||
           info->ovflact == OVFL_LARGEST_TRIM  || info->ovflact == OVFL_LARGEST_SILENT_TRIM);

    if (overflow_type == OVFL_TYPE_RANGE) {
        btree_elem_item *edge_elem;
        uint32_t del_count;
        int      bkrtype;
        bkey_range bkrange_space;
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM) {
            /* the bkey range to be trimmed
               => min bkey ~ (new max bkey - maxbkeyrange - 1)
            */
            edge_elem = do_btree_get_first_elem(info->root); /* min bkey elem */
            BKEY_COPY(edge_elem->data, edge_elem->nbkey, bkrange_space.from_bkey);
            bkrange_space.from_nbkey = edge_elem->nbkey;
            bkrange_space.to_nbkey   = info->maxbkeyrange.len;
            BKEY_DIFF(elem->data, elem->nbkey, info->maxbkeyrange.val, info->maxbkeyrange.len,
                      bkrange_space.to_nbkey, bkrange_space.to_bkey);
            BKEY_DECR(bkrange_space.to_bkey, bkrange_space.to_nbkey);
        } else {
            /* the bkey range to be trimmed
               => (new min bkey + maxbkeyrange + 1) ~ max bkey
               => (max bkey - (max bkey - maxbkeyrange - new min bkey) + 1) ~ max bkey
            */
            edge_elem = do_btree_get_last_elem(info->root);  /* max bkey elem */
            //BKEY_PLUS(elem->data, info->maxbkeyrange.val, info->maxbkeyrange.len, bkrange_space.from_bkey);
            bkrange_space.from_nbkey = info->maxbkeyrange.len;
            BKEY_DIFF(edge_elem->data, edge_elem->nbkey, info->maxbkeyrange.val, info->maxbkeyrange.len,
                      bkrange_space.from_nbkey, bkrange_space.from_bkey);
            BKEY_DIFF(bkrange_space.from_bkey, bkrange_space.from_nbkey, elem->data, elem->nbkey,
                      bkrange_space.from_nbkey, bkrange_space.from_bkey);
            BKEY_DIFF(edge_elem->data, edge_elem->nbkey, bkrange_space.from_bkey, bkrange_space.from_nbkey,
                      bkrange_space.from_nbkey, bkrange_space.from_bkey);
            BKEY_INCR(bkrange_space.from_bkey, bkrange_space.from_nbkey);
            BKEY_COPY(edge_elem->data, edge_elem->nbkey, bkrange_space.to_bkey);
            bkrange_space.to_nbkey   = edge_elem->nbkey;
        }
        bkrtype = do_btree_bkey_range_type(&bkrange_space);
        del_count = do_btree_elem_delete(engine, info, bkrtype, &bkrange_space, NULL, 0);
        assert(del_count > 0);
        assert(info->ccnt > 0);
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_LARGEST_TRIM)
            info->has_trimmed = 0;
    } else { /* overflow_type == OVFL_TYPE_COUNT */
        assert(overflow_type == OVFL_TYPE_COUNT);
        assert((info->ccnt-1) == info->mcnt);

        btree_elem_posi delpath[BTREE_MAX_DEPTH];
        assert(info->root->ndepth < BTREE_MAX_DEPTH);
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM) {
            delpath[0].node = do_btree_get_first_leaf(info->root, delpath);
            delpath[0].indx = 0;
        } else { /* info->ovflact == OVFL_LARGEST_TRIM or OVFL_LARGEST_SILENT_TRIM */
            delpath[0].node = do_btree_get_last_leaf(info->root, delpath);
            delpath[0].indx = delpath[0].node->used_count - 1;
        }
        if (trimmed_elems != NULL) {
            btree_elem_item *edge_elem = BTREE_GET_ELEM_ITEM(delpath[0].node, delpath[0].indx);
            edge_elem->refcount++;
            *trimmed_elems = edge_elem;
            *trimmed_count = 1;
        }
        do_btree_elem_unlink(engine, info, delpath);
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_LARGEST_TRIM)
            info->has_trimmed = 1;
    }
}

static ENGINE_ERROR_CODE do_btree_elem_link(struct default_engine *engine,
                                            btree_meta_info *info, btree_elem_item *elem,
                                            const bool replace_if_exist, bool *replaced,
                                            btree_elem_item **trimmed_elems, uint32_t *trimmed_count,
                                            const void *cookie)
{
    btree_elem_posi path[BTREE_MAX_DEPTH];
    int i, ovfl_type = OVFL_TYPE_NONE;
    ENGINE_ERROR_CODE res;

    *replaced = false;

    assert(info->root->ndepth < BTREE_MAX_DEPTH);
    res = do_btree_find_insposi(info->root, elem->data, elem->nbkey, path);
    if (res == ENGINE_SUCCESS) {
#ifdef ENABLE_STICKY_ITEM
        /* sticky memory limit check */
        if ((info->mflags & COLL_META_FLAG_STICKY) != 0) {
            if (engine->stats.sticky_bytes >= engine->config.sticky_limit)
                return ENGINE_ENOMEM;
        }
#endif

        if (info->ccnt > 0) { /* overflow check */
            res = do_btree_overflow_check(info, elem, &ovfl_type);
            if (res != ENGINE_SUCCESS) {
                return res;
            }
        }

        if (path[0].node->used_count >= BTREE_ITEM_COUNT) {
            res = do_btree_node_split(engine, info, path, cookie);
            if (res != ENGINE_SUCCESS) {
                return res;
            }
        }
        elem->status = BTREE_ITEM_STATUS_USED;
        if (path[0].indx < path[0].node->used_count) {
            for (i = (path[0].node->used_count-1); i >= path[0].indx; i--) {
                path[0].node->item[i+1] = path[0].node->item[i];
            }
        }
        path[0].node->item[path[0].indx] = elem;
        path[0].node->used_count++;
        /* increment element count in upper nodes */
        for (i = 1; i <= info->root->ndepth; i++) {
            path[i].node->ecnt[path[i].indx]++;
        }
        info->ccnt++;

        if (info->ccnt == 1) {
            info->bktype = (elem->nbkey==0 ? BKEY_TYPE_UINT64 : BKEY_TYPE_BINARY);
        }

        if (1) { /* apply memory space */
            size_t stotal = slabs_space_size(engine, BTREE_ELEM_SIZE(elem));
            increase_collection_space(engine, ITEM_TYPE_BTREE, (coll_meta_info *)info, stotal);
        }

        if (ovfl_type != OVFL_TYPE_NONE) {
            do_btree_overflow_trim(engine, info, elem, ovfl_type, trimmed_elems, trimmed_count);
        }
    }
    else if (res == ENGINE_ELEM_EEXISTS) {
        if (replace_if_exist) {
            res = do_btree_elem_replace(engine, info, &path[0], elem);
            if (res == ENGINE_SUCCESS) {
                *replaced = true;
            }
        }
    }
    return res;
}

static bool do_btree_overlapped_with_trimmed_space(btree_meta_info *info,
                                                   btree_elem_posi *posi, const int bkrtype)
{
    assert(info->has_trimmed != 0);
    bool overlapped = false;

    switch (info->ovflact) {
      case OVFL_SMALLEST_TRIM:
           if (posi->node == NULL) {
               if (posi->indx == BTREE_ITEM_COUNT) overlapped = true;
           } else {
               /* the bkey of the found elem isn't same with the from_bkey of bkey range */
               assert(posi->node->ndepth == 0); /* leaf node */
               if (posi->node->prev == NULL && posi->indx == 0 /* the first element */
                   && bkrtype == BKEY_RANGE_TYPE_ASC) overlapped = true;
           }
           break;
      case OVFL_LARGEST_TRIM:
           if (posi->node == NULL) {
               if (posi->indx == 0) overlapped = true;
           } else {
               /* the bkey of the found elem isn't same with the from_bkey of bkey range */
               assert(posi->node->ndepth == 0); /* leaf node */
               if (posi->node->next == NULL && posi->indx == (posi->node->used_count-1) /* the last element */
                   && bkrtype == BKEY_RANGE_TYPE_DSC) overlapped = true;
           }
           break;
    }
    return overlapped;
}

static uint32_t do_btree_elem_get(struct default_engine *engine, btree_meta_info *info,
                                  const int bkrtype, const bkey_range *bkrange, const eflag_filter *efilter,
                                  const uint32_t offset, const uint32_t count, const bool delete,
                                  btree_elem_item **elem_array, bool *potentialbkeytrim)
{
    btree_elem_posi  path[BTREE_MAX_DEPTH];
    btree_elem_item *elem;
    uint32_t tot_fcnt; /* total found count */

    *potentialbkeytrim = false;

    if (info->root == NULL) return 0;

    assert(info->root->ndepth < BTREE_MAX_DEPTH);
    tot_fcnt = 0;
    elem = do_btree_find_first(info->root, bkrtype, bkrange, path, delete);
    if (elem != NULL) {
        if (bkrtype == BKEY_RANGE_TYPE_SIN) { /* single bkey */
            assert(path[0].bkeq == true);
            if (offset == 0) {
                if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                    elem->refcount++;
                    elem_array[tot_fcnt++] = elem;
                    if (delete) {
                        do_btree_elem_unlink(engine, info, path);
                    }
                }
            }
        } else {
            btree_elem_posi upth[BTREE_MAX_DEPTH]; /* upper node path */
            btree_elem_posi c_posi = path[0];
            btree_elem_posi s_posi = c_posi;
            size_t stotal = 0;
            int  cur_fcnt = 0;
            int  skip_cnt = 0;
            int  node_cnt = 1;
            bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);
            int i;

            /* check if start position might be trimmed */
            if (c_posi.bkeq == false && info->has_trimmed != 0) {
                if (do_btree_overlapped_with_trimmed_space(info, &c_posi, bkrtype)) {
                    *potentialbkeytrim = true;
                }
            }

            if (delete) {
                /* prepare upper node path
                 * used to incr/decr element counts  in upper nodes.
                 */
                for (i = 1; i <= info->root->ndepth; i++) {
                    upth[i] = path[i];
                }
            }

            c_posi.bkeq = false;
            do {
                if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                    if (skip_cnt < offset) {
                        skip_cnt++;
                    } else {
                        elem->refcount++;
                        elem_array[tot_fcnt+cur_fcnt] = elem;
                        if (delete) {
                            stotal += slabs_space_size(engine, BTREE_ELEM_SIZE(elem));
                            elem->status = BTREE_ITEM_STATUS_UNLINK;
                            c_posi.node->item[c_posi.indx] = NULL;
                        }
                        cur_fcnt++;
                        if (count > 0 && (tot_fcnt+cur_fcnt) >= count) break;
                    }
                }

                if (c_posi.bkeq == true) {
                    elem = NULL; break;
                }
                elem = (forward ? do_btree_find_next(&c_posi, bkrange)
                                : do_btree_find_prev(&c_posi, bkrange));
                if (elem == NULL) break;

                if (s_posi.node != c_posi.node) {
                    if (cur_fcnt > 0) {
                        if (delete) {
                            do_btree_node_remove_null_items(&s_posi, forward, cur_fcnt);
                            /* decrement element count in upper nodes */
                            for (i = 1; i <= info->root->ndepth; i++) {
                                assert(upth[i].node->ecnt[upth[i].indx] >= cur_fcnt);
                                upth[i].node->ecnt[upth[i].indx] -= cur_fcnt;
                            }
                        }
                        tot_fcnt += cur_fcnt; cur_fcnt = 0;
                    }
                    if (delete) {
                        if (info->root->ndepth > 0) {
                            /* adjust upper node path */
                            if (forward) do_btree_incr_path(upth, 1);
                            else         do_btree_decr_path(upth, 1);
                        }
                    }
                    s_posi = c_posi;
                    node_cnt += 1;
                }
            } while (elem != NULL);

            /* check if end position might be trimmed */
            if (elem == NULL) {
                if (c_posi.node == NULL && info->has_trimmed != 0) {
                    if (do_btree_overlapped_with_trimmed_space(info, &c_posi, bkrtype)) {
                        *potentialbkeytrim = true;
                    }
                }
            }

            if (cur_fcnt > 0) {
                if (delete) {
                    do_btree_node_remove_null_items(&s_posi, forward, cur_fcnt);
                    /* decrement element count in upper nodes */
                    for (i = 1; i <= info->root->ndepth; i++) {
                        assert(upth[i].node->ecnt[upth[i].indx] >= cur_fcnt);
                        upth[i].node->ecnt[upth[i].indx] -= cur_fcnt;
                    }
                }
                tot_fcnt += cur_fcnt;
            }
            if (tot_fcnt > 0 && delete) { /* apply memory space */
                info->ccnt -= tot_fcnt;
                assert(stotal > 0 && stotal <= info->stotal);
                decrease_collection_space(engine, ITEM_TYPE_BTREE, (coll_meta_info *)info, stotal);
                do_btree_node_merge(engine, info, path, forward, node_cnt);
            }
        }
    } else {
        if (info->has_trimmed != 0) {
            if (do_btree_overlapped_with_trimmed_space(info, &path[0], bkrtype)) {
                *potentialbkeytrim = true;
            }
        }
    }
    return tot_fcnt;
}

static uint32_t do_btree_elem_count(struct default_engine *engine, btree_meta_info *info,
                                    const int bkrtype, const bkey_range *bkrange,
                                    const eflag_filter *efilter)
{
    btree_elem_posi  posi;
    btree_elem_item *elem;
    uint32_t tot_fcnt; /* total found count */

    if (info->root == NULL) return 0;

    tot_fcnt = 0;
    elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
    if (elem != NULL) {
        if (bkrtype == BKEY_RANGE_TYPE_SIN) {
            assert(posi.bkeq == true);
            if (efilter == NULL || do_btree_elem_filter(elem, efilter))
                tot_fcnt++;
        } else { /* BKEY_RANGE_TYPE_ASC || BKEY_RANGE_TYPE_DSC */
            bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);
            posi.bkeq = false;
            do {
                if (efilter == NULL || do_btree_elem_filter(elem, efilter))
                    tot_fcnt++;

                if (posi.bkeq == true) {
                    elem = NULL; break;
                }
                elem = (forward ? do_btree_find_next(&posi, bkrange)
                                : do_btree_find_prev(&posi, bkrange));
            } while (elem != NULL);
        }
    }
    return tot_fcnt;
}

static ENGINE_ERROR_CODE do_btree_elem_arithmetic(struct default_engine *engine, btree_meta_info *info,
                                                  const int bkrtype, const bkey_range *bkrange,
                                                  const bool increment, const bool create,
                                                  const uint64_t delta, const uint64_t initial,
                                                  const eflag_t *eflagp,
                                                  uint64_t *result, const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    btree_elem_item *elem;
    btree_elem_posi  posi;
    uint64_t value;
    char     nbuf[128];
    int      nlen;
    int      real_nbkey;

    if (info->root == NULL) {
        assert(create != true);
        return ENGINE_ELEM_ENOENT;
    }

    elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
    if (elem == NULL) {
        if (create != true) return ENGINE_ELEM_ENOENT;

        if ((nlen = snprintf(nbuf, sizeof(nbuf), "%"PRIu64"\r\n", initial)) == -1) {
            return ENGINE_EINVAL;
        }

        elem = do_btree_elem_alloc(engine, bkrange->from_nbkey,
                                   (eflagp == NULL || eflagp->len == EFLAG_NULL ? 0 : eflagp->len),
                                   nlen, cookie);
        if (elem == NULL) {
            return ENGINE_ENOMEM;
        }

        real_nbkey = BTREE_REAL_NBKEY(bkrange->from_nbkey);
        memcpy(elem->data, bkrange->from_bkey, real_nbkey);
        if (eflagp == NULL || eflagp->len == EFLAG_NULL) {
            memcpy(elem->data + real_nbkey, nbuf, nlen);
        } else {
            memcpy(elem->data + real_nbkey, eflagp->val, eflagp->len);
            memcpy(elem->data + real_nbkey + eflagp->len, nbuf, nlen);
        }

        bool dummy_replaced;
        ret = do_btree_elem_link(engine, info, elem, false, &dummy_replaced, NULL, NULL, cookie);
        if (ret != ENGINE_SUCCESS) {
            assert(ret != ENGINE_ELEM_EEXISTS);
            /* ENGINE_ENOMEM || ENGINE_BKEYOOR || ENGINE_OVERFLOW */
        }
        do_btree_elem_release(engine, elem);
        *result = initial;
    } else {
        real_nbkey = BTREE_REAL_NBKEY(elem->nbkey);
        if (! safe_strtoull((const char*)elem->data + real_nbkey + elem->neflag, &value)) {
            return ENGINE_EINVAL;
        }

        if (increment) {
            value += delta;
        } else {
            if (delta >= value) {
                value = 0;
            } else {
                value -= delta;
            }
        }
        if ((nlen = snprintf(nbuf, sizeof(nbuf), "%"PRIu64"\r\n", value)) == -1) {
            return ENGINE_EINVAL;
        }

        if (elem->refcount == 0 && elem->nbytes == nlen) {
            memcpy(elem->data + real_nbkey + elem->neflag, nbuf, elem->nbytes);
            ret = ENGINE_SUCCESS;
        } else {
            btree_elem_item *new_elem = do_btree_elem_alloc(engine, elem->nbkey, elem->neflag, nlen, cookie);
            if (new_elem == NULL) {
                return ENGINE_ENOMEM;
            }
            memcpy(new_elem->data, elem->data, real_nbkey + elem->neflag);
            memcpy(new_elem->data + real_nbkey + new_elem->neflag, nbuf, nlen);
            ret = do_btree_elem_replace(engine, info, &posi, new_elem);
            do_btree_elem_release(engine, new_elem);
        }
        *result = value;
    }
    return ret;
}

static int do_btree_posi_from_path(btree_meta_info *info,
                                   btree_elem_posi *path, ENGINE_BTREE_ORDER order)
{
    int d, i, bpos;

    bpos = path[0].indx;
    for (d = 1; d <= info->root->ndepth; d++) {
        for (i = 0; i < path[d].indx; i++) {
            bpos += path[d].node->ecnt[i];
        }
    }
    if (order == BTREE_ORDER_DESC) {
        bpos = info->ccnt - bpos - 1;
    }
    return bpos; /* btree position */
}

static int do_btree_posi_find(btree_meta_info *info,
                              const int bkrtype, const bkey_range *bkrange,
                              ENGINE_BTREE_ORDER order)
{
    btree_elem_posi  path[BTREE_MAX_DEPTH];
    btree_elem_item *elem;
    int bpos; /* btree position */

    if (info->root == NULL) return -1; /* not found */

    elem = do_btree_find_first(info->root, bkrtype, bkrange, path, true);
    if (elem != NULL) {
        assert(path[0].bkeq == true);
        bpos = do_btree_posi_from_path(info, path, order);
        assert(bpos >= 0);
    } else {
        bpos = -1; /* not found */
    }
    return bpos;
}

static uint32_t do_btree_elem_get_by_posi(struct default_engine *engine, btree_meta_info *info,
                                          const int index, const uint32_t count, const bool forward,
                                          btree_elem_item **elem_array)
{
    btree_elem_posi  posi;
    btree_indx_node *node;
    btree_elem_item *elem;
    int i, tot_ecnt;
    uint32_t nfound; /* found count */

    if (info->root == NULL) return 0;

    node = info->root;
    tot_ecnt = 0;
    while (node->ndepth > 0) {
        for (i = 0; i < node->used_count; i++) {
            assert(node->ecnt[i] > 0);
            if ((tot_ecnt + node->ecnt[i]) > index) break;
            tot_ecnt += node->ecnt[i];
        }
        assert(i < node->used_count);
        node = (btree_indx_node *)node->item[i];
    }
    assert(node->ndepth == 0);
    posi.node = node;
    posi.indx = index-tot_ecnt;

    nfound = 0;
    elem = BTREE_GET_ELEM_ITEM(posi.node, posi.indx);
    while (elem != NULL) {
        elem->refcount++;
        elem_array[nfound++] = elem;
        if (nfound >= count) break;

        if (forward) do_btree_incr_posi(&posi);
        else         do_btree_decr_posi(&posi);
        assert(posi.node != NULL);
        elem = BTREE_GET_ELEM_ITEM(posi.node, posi.indx);
    }
    return nfound;
}

static inline int do_btree_comp_hkey(hash_item *it1, hash_item *it2)
{
    int cmp_res;
    if (it1->nkey == it2->nkey) {
        cmp_res = strncmp(item_get_key(it1), item_get_key(it2), it1->nkey);
    } else {
        if (it1->nkey < it2->nkey) {
            cmp_res = strncmp(item_get_key(it1), item_get_key(it2), it1->nkey);
            if (cmp_res == 0) cmp_res = -1;
        } else {
            cmp_res = strncmp(item_get_key(it1), item_get_key(it2), it2->nkey);
            if (cmp_res == 0) cmp_res =  1;
        }
    }
    return cmp_res;
}

#ifdef SUPPORT_BOP_SMGET
static ENGINE_ERROR_CODE do_btree_smget_scan_sort(struct default_engine *engine,
                                    token_t *key_array, const int key_count,
                                    const int bkrtype, const bkey_range *bkrange,
                                    const eflag_filter *efilter, const uint32_t req_count,
                                    btree_scan_info *btree_scan_buf,
                                    uint16_t *sort_sindx_buf, uint32_t *sort_sindx_cnt,
                                    uint32_t *missed_key_array, uint32_t *missed_key_count,
                                    bool *bkey_duplicated)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    hash_item *it;
    btree_meta_info *info;
    btree_elem_item *elem, *comp;
    btree_elem_posi posi;
    uint16_t comp_idx;
    uint16_t curr_idx = 0;
    uint16_t free_idx = req_count;
    int sort_count = 0; /* sorted scan count */
    int k, i, cmp_res;
    int mid, left, right;
    bool ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);
    bkey_t   maxbkeyrange;
    int32_t  maxelemcount = 0;
    uint8_t  overflowactn = OVFL_SMALLEST_TRIM;

    *missed_key_count = 0;

    maxbkeyrange.len = BKEY_NULL;
    for (k = 0; k < key_count; k++) {
        ret = do_btree_item_find(engine, key_array[k].value, key_array[k].length, true, &it);
        if (ret != ENGINE_SUCCESS) {
            if (ret == ENGINE_KEY_ENOENT) { /* key missed */
                missed_key_array[*missed_key_count] = k;
                *missed_key_count += 1;
                ret = ENGINE_SUCCESS; continue;
            }
            break; /* ret == ENGINE_EBADTYPE */
        }

        info = (btree_meta_info *)item_get_meta(it);
        if ((info->mflags & COLL_META_FLAG_READABLE) == 0) { /* unreadable collection */
            missed_key_array[*missed_key_count] = k;
            *missed_key_count += 1;
            do_item_release(engine, it); continue;
        }
        if (info->ccnt == 0) { /* empty collection */
            do_item_release(engine, it); continue;
        }
        if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
            (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
            do_item_release(engine, it);
            ret = ENGINE_EBADBKEY; break;
        }
        assert(info->root != NULL);

        elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
        if (elem == NULL) { /* No elements within the bkey range */
            if (info->has_trimmed != 0) {
                /* Some elements weren't cached because of overflow trim */
                if (do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                    do_item_release(engine, it);
                    ret = ENGINE_EBKEYOOR; break;
                }
            }
            do_item_release(engine, it);
            continue;
        }

        if (posi.bkeq == false && info->has_trimmed != 0) {
            /* Some elements weren't cached because of overflow trim */
            if (do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                do_item_release(engine, it);
                ret = ENGINE_EBKEYOOR; break;
            }
        }

        posi.bkeq = false;
        do {
           if (efilter == NULL || do_btree_elem_filter(elem, efilter))
               break;

           if (posi.bkeq == true) {
               elem = NULL; break;
           }
           elem = (ascending ? do_btree_find_next(&posi, bkrange)
                             : do_btree_find_prev(&posi, bkrange));
        } while (elem != NULL);

        if (elem == NULL) {
            if (posi.node == NULL && info->has_trimmed != 0) {
                if (do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                    do_item_release(engine, it);
                    ret = ENGINE_EBKEYOOR; break;
                }
            }
            do_item_release(engine, it);
            continue;
        }

        if (sort_count == 0) {
            /* save the b+tree attributes */
            maxbkeyrange = info->maxbkeyrange;
            maxelemcount = info->mcnt;
            overflowactn = info->ovflact;
        } else {
            /* check if the b+trees have same attributes */
            if (maxelemcount != info->mcnt || overflowactn != info->ovflact ||
                maxbkeyrange.len != info->maxbkeyrange.len ||
                (maxbkeyrange.len != BKEY_NULL &&
                 BKEY_ISNE(maxbkeyrange.val, maxbkeyrange.len, info->maxbkeyrange.val, maxbkeyrange.len)))
            {
                do_item_release(engine, it);
                ret = ENGINE_EBADATTR; break;
            }
        }

        /* found the item */
        btree_scan_buf[curr_idx].it   = it;
        btree_scan_buf[curr_idx].posi = posi;
        btree_scan_buf[curr_idx].kidx = k;

        /* add the current scan into the scan sort buffer */

        /* insert the index into the sort_sindx_buf */
        if (sort_count >= req_count) {
            /* compare with the element of the last scan */
            comp_idx = sort_sindx_buf[sort_count-1];
            comp = BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
                                       btree_scan_buf[comp_idx].posi.indx);
            cmp_res = BKEY_COMP(elem->data, elem->nbkey, comp->data, comp->nbkey);
            if (cmp_res == 0) {
                cmp_res = do_btree_comp_hkey(btree_scan_buf[curr_idx].it,
                                             btree_scan_buf[comp_idx].it);
                if (cmp_res == 0) {
                    do_item_release(engine, btree_scan_buf[curr_idx].it);
                    btree_scan_buf[curr_idx].it = NULL;
                    ret = ENGINE_EBADVALUE; break;
                }
                *bkey_duplicated = true;
            }
            if ((ascending ==  true && cmp_res > 0) ||
                (ascending == false && cmp_res < 0)) {
                /* do not need to proceed the current scan */
                do_item_release(engine, btree_scan_buf[curr_idx].it);
                btree_scan_buf[curr_idx].it = NULL;
                continue;
            }
            /* free the last scan */
            do_item_release(engine, btree_scan_buf[comp_idx].it);
            btree_scan_buf[comp_idx].it = NULL;
            free_idx = comp_idx;
            sort_count--;
        }

        /* sort_count < req_count */
        if (sort_count == 0) {
            sort_sindx_buf[sort_count++] = curr_idx;
        } else {
            left = 0;
            right = sort_count-1;
            while (left <= right) {
                mid  = (left + right) / 2;
                comp_idx = sort_sindx_buf[mid];
                comp = BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
                                           btree_scan_buf[comp_idx].posi.indx);
                cmp_res = BKEY_COMP(elem->data, elem->nbkey, comp->data, comp->nbkey);
                if (cmp_res == 0) {
                    cmp_res = do_btree_comp_hkey(btree_scan_buf[curr_idx].it,
                                                 btree_scan_buf[comp_idx].it);
                    if (cmp_res == 0) {
                        do_item_release(engine, btree_scan_buf[curr_idx].it);
                        btree_scan_buf[curr_idx].it = NULL;
                        ret = ENGINE_EBADVALUE; break;
                    }
                    *bkey_duplicated = true;
                }
                if (ascending) {
                    if (cmp_res < 0) right = mid-1;
                    else             left  = mid+1;
                } else {
                    if (cmp_res > 0) right = mid-1;
                    else             left  = mid+1;
                }
            }
            if (ret == ENGINE_EBADVALUE) break;

            assert(left > right);
            /* left : insert position */
            for (i = sort_count-1; i >= left; i--) {
                sort_sindx_buf[i+1] = sort_sindx_buf[i];
            }
            sort_sindx_buf[left] = curr_idx;
            sort_count++;
        }

        if (sort_count < req_count) {
            curr_idx += 1;
        } else {
            curr_idx = free_idx;
        }
    }

    if (ret == ENGINE_SUCCESS) {
        *sort_sindx_cnt = sort_count;
    } else {
        for (i = 0; i < sort_count; i++) {
            curr_idx = sort_sindx_buf[i];
            do_item_release(engine, btree_scan_buf[curr_idx].it);
            btree_scan_buf[curr_idx].it = NULL;
        }
    }
    return ret;
}
#endif

#ifdef SUPPORT_BOP_SMGET
static int do_btree_smget_elem_sort(btree_scan_info *btree_scan_buf,
                                    uint16_t *sort_sindx_buf, const int sort_sindx_cnt,
                                    const int bkrtype, const bkey_range *bkrange, const eflag_filter *efilter,
                                    const uint32_t offset, const uint32_t count,
                                    btree_elem_item **elem_array, uint32_t *kfnd_array, uint32_t *flag_array,
                                    bool *potentialbkeytrim, bool *bkey_duplicated)
{
    btree_meta_info *info;
    btree_elem_item *elem, *comp;
    uint16_t first_idx = 0;
    uint16_t curr_idx;
    uint16_t comp_idx;
    int i, cmp_res;
    int mid, left, right;
    int skip_count = 0;
    int elem_count = 0;
    int sort_count = sort_sindx_cnt;
    bool ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);

    while (sort_count > 0) {
        curr_idx = sort_sindx_buf[first_idx];
        elem = BTREE_GET_ELEM_ITEM(btree_scan_buf[curr_idx].posi.node,
                                   btree_scan_buf[curr_idx].posi.indx);
        if (skip_count < offset) {
            skip_count++;
        } else { /* skip_count == offset */
            elem->refcount++;
            elem_array[elem_count] = elem;
            kfnd_array[elem_count] = btree_scan_buf[curr_idx].kidx;
            flag_array[elem_count] = btree_scan_buf[curr_idx].it->flags;
            elem_count++;
            if (elem_count >= count) break;
        }

        do {
            if (btree_scan_buf[curr_idx].posi.bkeq == true) {
                elem = NULL; break;
            }
            elem = (ascending ? do_btree_find_next(&btree_scan_buf[curr_idx].posi, bkrange)
                              : do_btree_find_prev(&btree_scan_buf[curr_idx].posi, bkrange));
            if (elem != NULL) {
                if (efilter == NULL || do_btree_elem_filter(elem, efilter))
                    break;
            }
        } while (elem != NULL);

        if (elem == NULL) {
            if (btree_scan_buf[curr_idx].posi.node == NULL) {
                /* reached to the end of b+tree scan */
                info = (btree_meta_info *)item_get_meta(btree_scan_buf[curr_idx].it);
                if (info->has_trimmed != 0) {
                    if (do_btree_overlapped_with_trimmed_space(info, &btree_scan_buf[curr_idx].posi, bkrtype)) {
                        *potentialbkeytrim = true;
                        break; /* stop smget */
                    }
                }
            }
            first_idx++; sort_count--;
            continue;
        }

        if (sort_count == 1) {
            continue; /* sorting is not needed */
        }

        /* compare with the last element */
        comp_idx = sort_sindx_buf[first_idx+sort_count-1];
        comp = BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
                                   btree_scan_buf[comp_idx].posi.indx);

        cmp_res = BKEY_COMP(elem->data, elem->nbkey, comp->data, comp->nbkey);
        if (cmp_res == 0) {
            cmp_res = do_btree_comp_hkey(btree_scan_buf[curr_idx].it,
                                         btree_scan_buf[comp_idx].it);
            assert(cmp_res != 0);
            *bkey_duplicated = true;
        }
        if ((ascending ==  true && cmp_res > 0) ||
            (ascending == false && cmp_res < 0)) {
            if ((first_idx + sort_count) >= (offset + count)) {
                first_idx++; sort_count--;
            } else {
                for (i = first_idx+1; i < first_idx+sort_count; i++) {
                    sort_sindx_buf[i-1] = sort_sindx_buf[i];
                }
                sort_sindx_buf[first_idx+sort_count-1] = curr_idx;
            }
            continue;
        }

        if (sort_count == 2) {
            continue; /* sorting has already completed */
        }

        left  = first_idx + 1;
        right = first_idx + sort_count - 2;
        while (left <= right) {
            mid  = (left + right) / 2;
            comp_idx = sort_sindx_buf[mid];
            comp = BTREE_GET_ELEM_ITEM(btree_scan_buf[comp_idx].posi.node,
                                       btree_scan_buf[comp_idx].posi.indx);

            cmp_res = BKEY_COMP(elem->data, elem->nbkey, comp->data, comp->nbkey);
            if (cmp_res == 0) {
                cmp_res = do_btree_comp_hkey(btree_scan_buf[curr_idx].it,
                                             btree_scan_buf[comp_idx].it);
                assert(cmp_res != 0);
                *bkey_duplicated = true;
            }
            if (ascending) {
                if (cmp_res < 0) right = mid-1;
                else             left  = mid+1;
            } else {
                if (cmp_res > 0) right = mid-1;
                else             left  = mid+1;
            }
        }

        assert(left > right);
        /* left : insert position */
        for (i = first_idx+1; i < left; i++) {
            sort_sindx_buf[i-1] = sort_sindx_buf[i];
        }
        sort_sindx_buf[left-1] = curr_idx;
    }
    return elem_count;
}
#endif

/*
 * Collection Delete Queue Management
 */
static void push_coll_del_queue(struct default_engine *engine, hash_item *it)
{
    /* push the item into the tail of delete queue */
    it->next = NULL;
    pthread_mutex_lock(&engine->coll_del_lock);
    if (engine->coll_del_queue.tail == NULL) {
        engine->coll_del_queue.head = it;
    } else {
        engine->coll_del_queue.tail->next = it;
    }
    engine->coll_del_queue.tail = it;
    engine->coll_del_queue.size++;
    if (engine->coll_del_sleep == true) {
        /* wake up collection delete thead */
        pthread_cond_signal(&engine->coll_del_cond);
    }
    pthread_mutex_unlock(&engine->coll_del_lock);
}

static hash_item *pop_coll_del_queue(struct default_engine *engine)
{
    /* pop an item from the head of delete queue */
    hash_item *it = NULL;
    pthread_mutex_lock(&engine->coll_del_lock);
    if (engine->coll_del_queue.head != NULL) {
        it = engine->coll_del_queue.head;
        engine->coll_del_queue.head = it->next;
        if (engine->coll_del_queue.head == NULL) {
            engine->coll_del_queue.tail = NULL;
        }
        engine->coll_del_queue.size--;
    }
    pthread_mutex_unlock(&engine->coll_del_lock);
    return it;
}

/*
 * Item Management Daemon
 */
static int check_expired_collections(struct default_engine *engine, const int clsid, int *space_shortage_level)
{
    rel_time_t current_time = engine->server.core->get_current_time();
    hash_item *search, *it;
    int unlink_count = 0;
    int tries = 10;
    int id = clsid;

    /* Never-expired items are positioned near the head of LRU list.
     * To-be-expired items are ordered with expire time near the tail of LRU list.
     * The smaller expire time leads to near the tail of the LRU list.
     */
    pthread_mutex_lock(&engine->cache_lock);
    *space_shortage_level = slabs_short_of_free_space(engine);
    if (*space_shortage_level > 0)
    {
        search = engine->items.tails[id];
        while (search != NULL && tries > 0) {
            if (search->refcount == 0 && search->nkey > 0) {
                it = search;
                search = search->prev; tries--;

                if (do_item_isvalid(engine, it, current_time) == false) {
                    do_item_invalidate(engine, it, id);
                    if (++unlink_count > 10) break;
                } else {
                    if (engine->config.evict_to_free != 0) {
                        do_item_evict(engine, it, id, current_time, NULL);
                        if (++unlink_count > 10) break;
                    }
                }
            } else {
                search = search->prev; tries--;
            }
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return unlink_count;
}

static void do_coll_all_elem_delete(struct default_engine *engine, hash_item *it)
{
    if (IS_LIST_ITEM(it)) {
        list_meta_info *info = (list_meta_info *)item_get_meta(it);
        (void)do_list_elem_delete(engine, info, 0, info->mcnt);
        assert(info->head == NULL && info->tail == NULL);
    } else if (IS_SET_ITEM(it)) {
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        (void)do_set_elem_delete(engine, info, info->mcnt);
        assert(info->root == NULL);
    } else if (IS_BTREE_ITEM(it)) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        bkey_range bkrange_space;
        get_bkey_full_range(info->bktype, true, &bkrange_space);
        (void)do_btree_elem_delete(engine, info, BKEY_RANGE_TYPE_ASC, &bkrange_space, NULL, info->mcnt);
        assert(info->root == NULL);
    }
}

static void coll_del_thread_sleep(struct default_engine *engine)
{
    struct timeval  tv;
    struct timespec to;
    pthread_mutex_lock(&engine->coll_del_lock);
    if (engine->coll_del_queue.head == NULL) {
        /* 50 mili second sleep */
        gettimeofday(&tv, NULL);
        tv.tv_usec += 50000;
        if (tv.tv_usec >= 1000000) {
            tv.tv_sec += 1;
            tv.tv_usec -= 1000000;
        }
        to.tv_sec = tv.tv_sec;
        to.tv_nsec = tv.tv_usec * 1000;

        engine->coll_del_sleep = true;
        pthread_cond_timedwait(&engine->coll_del_cond,
                               &engine->coll_del_lock, &to);
        engine->coll_del_sleep = false;
    }
    pthread_mutex_unlock(&engine->coll_del_lock);
}

static void *collection_delete_thread(void *arg)
{
    struct default_engine *engine = arg;
    hash_item *it;
    uint32_t expired_cnt;
    //uint32_t deleted_cnt;
    bool     background_evict_flag = false;
    uint32_t background_evict_count = 0;
    int      space_shortage_level;
    int             background_sleep_unit;
    struct timespec background_sleep_time = {0, 10000}; /* 0.001ms */

    while (engine->initialized) {
        it = pop_coll_del_queue(engine);
        if (it == NULL) {
#ifdef USE_SINGLE_LRU_LIST
            expired_cnt = check_expired_collections(engine, 1, &space_shortage_level);
#else
            expired_cnt = check_expired_collections(engine, LRU_CLSID_FOR_SMALL, &space_shortage_level);
#endif
            if (expired_cnt > 0) {
                if (background_evict_flag == false) {
                    /*****
                    if (engine->config.verbose > 1) {
                        logger->log(EXTENSION_LOG_INFO, NULL, "background evict start\n");
                    }
                    *****/
                    background_evict_flag = true;
                }
                assert(space_shortage_level > 0);
                background_sleep_unit = 10 * space_shortage_level;
                background_evict_count += expired_cnt;
                if ((background_evict_count / background_sleep_unit) !=
                    ((background_evict_count-expired_cnt) / background_sleep_unit)) {
                    nanosleep(&background_sleep_time, NULL);
                }
                if (background_evict_count > 10000000) {
                    if (engine->config.verbose > 1) {
                        logger->log(EXTENSION_LOG_INFO, NULL, "background evict(or reclaim): count=%u\n",
                                                               background_evict_count);
                    }
                    background_evict_count = 0;
                }
                continue;
            } else {
                if (background_evict_flag == true) {
                    /*****
                    if (engine->config.verbose > 1) {
                        logger->log(EXTENSION_LOG_INFO, NULL, "background evict(or reclaim) end: count=%u\n",
                                                               background_evict_count);
                    }
                    *****/
                    background_evict_count = 0;
                    background_evict_flag = false;
                }
            }
            coll_del_thread_sleep(engine);
            continue;
        }
        if (IS_LIST_ITEM(it)) {
            bool dropped = false;
            list_meta_info *info;
            while (dropped == false) {
                pthread_mutex_lock(&engine->cache_lock);
                info = (list_meta_info *)item_get_meta(it);
                //deleted_cnt = do_list_elem_delete(engine, info, 0, 30);
                (void)do_list_elem_delete(engine, info, 0, 30);
                if (info->ccnt == 0) {
                    assert(info->head == NULL && info->tail == NULL);
                    item_free(engine, it);
                    dropped = true;
                }
                pthread_mutex_unlock(&engine->cache_lock);
            }
        } else if (IS_SET_ITEM(it)) {
            bool dropped = false;
            set_meta_info *info;
            while (dropped == false) {
                pthread_mutex_lock(&engine->cache_lock);
                info = (set_meta_info *)item_get_meta(it);
                //deleted_cnt = do_set_elem_delete(engine, info, 30);
                (void)do_set_elem_delete(engine, info, 30);
                if (info->ccnt == 0) {
                    assert(info->root == NULL);
                    item_free(engine, it);
                    dropped = true;
                }
                pthread_mutex_unlock(&engine->cache_lock);
            }
        }
        else if (IS_BTREE_ITEM(it)) {
            bkey_range bkrange_space;
            bool dropped = false;
            btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
            get_bkey_full_range(info->bktype, true, &bkrange_space);
            while (dropped == false) {
                pthread_mutex_lock(&engine->cache_lock);
                info = (btree_meta_info *)item_get_meta(it);
                //deleted_cnt = do_btree_elem_delete(engine, info, BKEY_RANGE_TYPE_ASC, &bkrange_space, NULL, 100);
                (void)do_btree_elem_delete(engine, info, BKEY_RANGE_TYPE_ASC, &bkrange_space, NULL, 100);
                if (info->ccnt == 0) {
                    assert(info->root == NULL);
                    item_free(engine, it);
                    dropped = true;
                }
                pthread_mutex_unlock(&engine->cache_lock);
            }
        }
    }
    return NULL;
}

void coll_del_thread_wakeup(struct default_engine *engine)
{
    pthread_mutex_lock(&engine->coll_del_lock);
    if (engine->coll_del_sleep == true) {
        /* wake up collection delete thead */
        pthread_cond_signal(&engine->coll_del_cond);
    }
    pthread_mutex_unlock(&engine->coll_del_lock);
}

/********************************* ITEM ACCESS *******************************/

/*
 * Allocates a new item.
 */
hash_item *item_alloc(struct default_engine *engine,
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
hash_item *item_get(struct default_engine *engine, const void *key, const size_t nkey)
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
void item_release(struct default_engine *engine, hash_item *item)
{
    pthread_mutex_lock(&engine->cache_lock);
    do_item_release(engine, item);
    pthread_mutex_unlock(&engine->cache_lock);
}

/*
 * Unlinks an item from the LRU and hashtable.
 */
void item_unlink(struct default_engine *engine, hash_item *item)
{
    pthread_mutex_lock(&engine->cache_lock);
    do_item_unlink(engine, item);
    pthread_mutex_unlock(&engine->cache_lock);
}

/*
 * Does arithmetic on a numeric item value.
 */
ENGINE_ERROR_CODE add_delta(struct default_engine *engine,
                            hash_item *item, const bool incr,
                            const int64_t delta, uint64_t *rcas,
                            uint64_t *result, const void *cookie)
{
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_add_delta(engine, item, incr, delta, rcas, result, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
ENGINE_ERROR_CODE store_item(struct default_engine *engine,
                             hash_item *item, uint64_t *cas,
                             ENGINE_STORE_OPERATION operation,
                             const void *cookie)
{
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_store_item(engine, item, cas, operation, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

static ENGINE_ERROR_CODE do_arithmetic(struct default_engine *engine,
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
    hash_item *item = do_item_get(engine, key, nkey, true);
    ENGINE_ERROR_CODE ret;

    if (item == NULL) {
        if (!create) {
            return ENGINE_KEY_ENOENT;
        } else {
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "%"PRIu64"\r\n",
                    (uint64_t)initial);

            item = do_item_alloc(engine, key, nkey, flags, exptime, len, cookie);
            if (item == NULL) {
                return ENGINE_ENOMEM;
            }
            memcpy((void*)item_get_data(item), buffer, len);

            ret = do_store_item(engine, item, cas, OPERATION_ADD, cookie);
            if (ret == ENGINE_SUCCESS) {
                *result = initial;
            }
            do_item_release(engine, item);
        }
    } else {
        ret = do_add_delta(engine, item, increment, delta, cas, result, cookie);
        do_item_release(engine, item);
    }

    return ret;
}

ENGINE_ERROR_CODE arithmetic(struct default_engine *engine,
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
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_arithmetic(engine, cookie, key, nkey, increment,
                        create, delta, initial, flags, exptime, cas, result);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

/*
 * Flushes expired items after a flush_all call
 */

static ENGINE_ERROR_CODE do_item_flush_expired(struct default_engine *engine,
                                               const char *prefix, const int nprefix,
                                               time_t when, const void* cookie)
{
    hash_item *iter, *next;
    rel_time_t oldest_live;

    if (nprefix >= 0) { /* flush the given prefix */
        prefix_t *pt;
        if (nprefix == 0) { /* null prefix */
            assert(prefix == NULL);
            pt = &engine->assoc.noprefix_stats;
        } else {
            assert(prefix != NULL);
            pt = assoc_prefix_find(engine, engine->server.core->hash(prefix, nprefix, 0), prefix, nprefix);
        }
        if (pt == NULL) {
            return ENGINE_PREFIX_ENOENT;
        }

        if (when <= 0) {
            pt->oldest_live = engine->server.core->get_current_time() - 1;
        } else {
            pt->oldest_live = engine->server.core->realtime(when) - 1;
        }
        oldest_live = pt->oldest_live;

        if (engine->config.verbose) {
            logger->log(EXTENSION_LOG_INFO, NULL, "flush prefix=%s when=%u client_ip=%s",
                        ((prefix==NULL) ? "null" : prefix), (unsigned)when, engine->server.core->get_client_ip(cookie));
        }
    } else { /* flush all */
        if (when <= 0) {
            engine->config.oldest_live = engine->server.core->get_current_time() - 1;
        } else {
            engine->config.oldest_live = engine->server.core->realtime(when) - 1;
        }
        oldest_live = engine->config.oldest_live;

        if (engine->config.verbose) {
            logger->log(EXTENSION_LOG_INFO, NULL, "flush all when=%u client_ip=%s",
                                                  (unsigned)when, engine->server.core->get_client_ip(cookie));
        }
    }

    if (oldest_live != 0) {
        for (int i = 0; i <= POWER_LARGEST; i++)
        {
            /*
             * The LRU is sorted in decreasing time order, and an item's
             * timestamp is never newer than its last access time, so we
             * only need to walk back until we hit an item older than the
             * oldest_live time.
             * The oldest_live checking will auto-expire the remaining items.
             */
            for (iter = engine->items.heads[i]; iter != NULL; iter = next) {
                if (iter->time >= oldest_live) {
                    next = iter->next;
                    if (nprefix >= 0) {
                        bool found = false;
                        if (nprefix == 0) {
                            if (iter->nprefix == iter->nkey)
                                found = true;
                        } else { /* nprefix > 0 */
                            char *iter_key = (char*)item_get_key(iter);
                            if (iter->nkey > nprefix && memcmp(prefix,iter_key,nprefix) == 0 &&
                                *(iter_key + nprefix) == engine->config.prefix_delimiter)
                                found = true;
                        }
                        if (found == true && (iter->iflag & ITEM_SLABBED) == 0) {
                            do_item_unlink(engine, iter);
                        }
                    } else { /* flush all */
                        if ((iter->iflag & ITEM_SLABBED) == 0) {
                            do_item_unlink(engine, iter);
                        }
                    }
                } else {
                    /* We've hit the first old item. Continue to the next queue. */
                    /* reset lowMK and curMK to tail pointer */
                    engine->items.lowMK[i] = engine->items.tails[i];
                    engine->items.curMK[i] = engine->items.tails[i];
                    break;
                }
            }
#ifdef ENABLE_STICKY_ITEM
            for (iter = engine->items.sticky_heads[i]; iter != NULL; iter = next) {
                if (iter->time >= oldest_live) {
                    next = iter->next;

                    if (nprefix >= 0) {
                        bool found = false;
                        if (nprefix == 0) {
                            if (iter->nprefix == iter->nkey)
                                found = true;
                        } else { /* nprefix > 0 */
                            char *iter_key = (char*)item_get_key(iter);
                            if (iter->nkey > nprefix && memcmp(prefix,iter_key,nprefix) == 0 &&
                                *(iter_key + nprefix) == engine->config.prefix_delimiter)
                                found = true;
                        }
                        if (found == true && (iter->iflag & ITEM_SLABBED) == 0) {
                            do_item_unlink(engine, iter);
                        }
                    } else { /* flush all */
                        if ((iter->iflag & ITEM_SLABBED) == 0) {
                            do_item_unlink(engine, iter);
                        }
                    }
                } else {
                    /* We've hit the first old item. Continue to the next queue. */
                    /* reset curMK to tail pointer */
                    engine->items.sticky_curMK[i] = engine->items.sticky_tails[i];
                    break;
                }
            }
#endif
        }
    }
    return ENGINE_SUCCESS;
}

void item_flush_expired(struct default_engine *engine, time_t when, const void* cookie)
{
    pthread_mutex_lock(&engine->cache_lock);
    /* flush all items */
    do_item_flush_expired(engine, NULL, -1, when, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
}

ENGINE_ERROR_CODE item_flush_prefix_expired(struct default_engine *engine,
                                            const char *prefix, const int nprefix,
                                            time_t when, const void* cookie)
{
    ENGINE_ERROR_CODE ret;
    pthread_mutex_lock(&engine->cache_lock);
    ret = do_item_flush_expired(engine, prefix, nprefix, when, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

/*
 * Dumps part of the cache
 */
char *item_cachedump(struct default_engine *engine,
                     unsigned int slabs_clsid, unsigned int limit, const bool forward,
                     const bool sticky, unsigned int *bytes)
{
    char *ret;
    pthread_mutex_lock(&engine->cache_lock);
    ret = do_item_cachedump(engine, slabs_clsid, limit, forward, sticky, bytes);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

void item_stats(struct default_engine *engine,
                   ADD_STAT add_stat, const void *cookie)
{
    pthread_mutex_lock(&engine->cache_lock);
    do_item_stats(engine, add_stat, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
}


void item_stats_sizes(struct default_engine *engine,
                      ADD_STAT add_stat, const void *cookie)
{
    pthread_mutex_lock(&engine->cache_lock);
    do_item_stats_sizes(engine, add_stat, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
}

ENGINE_ERROR_CODE item_init(struct default_engine *engine)
{
    logger = engine->server.log->get_logger();

    pthread_mutex_init(&engine->coll_del_lock, NULL);
    pthread_cond_init(&engine->coll_del_cond, NULL);
    engine->coll_del_queue.head = engine->coll_del_queue.tail = NULL;
    engine->coll_del_queue.size = 0;
    engine->coll_del_sleep = false;

    pthread_t tid;
    int ret = pthread_create(&tid, NULL, collection_delete_thread, engine);
    if (ret != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return ENGINE_FAILED;
    }
    return ENGINE_SUCCESS;
}

/*
 * LIST Interface Functions
 */
ENGINE_ERROR_CODE list_struct_create(struct default_engine *engine,
                                     const char *key, const size_t nkey,
                                     item_attr *attrp, const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    it = do_item_get(engine, key, nkey, false);
    if (it != NULL) {
        do_item_release(engine, it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_list_item_alloc(engine, key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(engine, it);
            do_item_release(engine, it);
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

list_elem_item *list_elem_alloc(struct default_engine *engine,
                                const int nbytes, const void *cookie)
{
    list_elem_item *elem;
    pthread_mutex_lock(&engine->cache_lock);
    elem = do_list_elem_alloc(engine, nbytes, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return elem;
}

void list_elem_release(struct default_engine *engine,
                       list_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    pthread_mutex_lock(&engine->cache_lock);
    while (cnt < elem_count) {
        do_list_elem_release(engine, elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            pthread_mutex_unlock(&engine->cache_lock);
            pthread_mutex_lock(&engine->cache_lock);
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
}

ENGINE_ERROR_CODE list_elem_insert(struct default_engine *engine,
                                   const char *key, const size_t nkey,
                                   int index, list_elem_item *elem,
                                   item_attr *attrp,
                                   bool *created, const void *cookie)
{
    hash_item      *it;
    list_meta_info *info=NULL;
    ENGINE_ERROR_CODE ret;

    *created = false;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_list_item_find(engine, key, nkey, false, &it);
    do {
        if (ret == ENGINE_SUCCESS) { /* it != NULL */
            info = (list_meta_info *)item_get_meta(it);
            /* validation check: index value */
            if (index >= 0) {
                if (index > info->ccnt || index > (info->mcnt-1)) {
                    ret = ENGINE_EINDEXOOR; break;
                }
            } else {
                if ((-index) > (info->ccnt+1) || (-index) > info->mcnt) {
                    ret = ENGINE_EINDEXOOR; break;
                }
            }
#ifdef ENABLE_STICKY_ITEM
            /* sticky memory limit check */
            if ((info->mflags & COLL_META_FLAG_STICKY) != 0) {
                if (engine->stats.sticky_bytes >= engine->config.sticky_limit) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
#endif
            /* overflow check */
            if (info->ccnt >= info->mcnt) {
                if (info->ovflact == OVFL_ERROR) {
                    ret = ENGINE_EOVERFLOW; break;
                }
                if (index >= 0) {
                    if (index == (info->mcnt-1))
                        index = -1;
                } else {
                    if ((-index) == info->mcnt)
                        index = 0;
                }
                if (index == 0 || index == -1) {
                    /* delete an element item of opposite side to make room */
                    uint32_t deleted = do_list_elem_delete(engine, info,
                                                           (index==-1 ? 0 : -1), 1);
                    assert(deleted == 1);
                } else {
                    /* delete an element item that ovflow action indicates */
                    uint32_t deleted = do_list_elem_delete(engine, info,
                                                           (info->ovflact==OVFL_HEAD_TRIM ? 0 : -1), 1);
                    assert(deleted == 1);
                }
            }
        } else if (ret == ENGINE_KEY_ENOENT) {
            if (attrp != NULL) {
                /* validation check: index value */
                if (index != 0 && index != -1) {
                    ret = ENGINE_EINDEXOOR; break;
                }
                /* allocate list item */
                it = do_list_item_alloc(engine, key, nkey, attrp, cookie);
                if (it == NULL) {
                    ret = ENGINE_ENOMEM; break;
                }
                ret = do_item_link(engine, it);
                if (ret != ENGINE_SUCCESS) {
                    /* The list item is to be released in the outside of do-while loop */
                    break;
                }
                info = (list_meta_info *)item_get_meta(it);
                *created = true;
                ret = ENGINE_SUCCESS;
            }
        }

        if (ret == ENGINE_SUCCESS) {
            ret = do_list_elem_link(engine, info, index, elem);
            if (ret != ENGINE_SUCCESS) {
                if (*created) {
                    /* unlink the created list item and free it*/
                    do_item_release(engine, it);
                    do_item_unlink(engine, it);
                    it = NULL;
                }
                break;
            }
        } else {
            /* ENGINE_KEY_ENOENT | ENGINE_EBADTYPE */
        }
    } while(0);

    if (it != NULL) do_item_release(engine, it);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

static int adjust_list_range(int num_elems, int *from_index, int *to_index)
{
    if (num_elems <= 0) return -1; /* out of range */

    if (*from_index >= 0) {
        if (*from_index >= num_elems) {
            if (*to_index >= num_elems) return -1; /* out of range */
            *from_index = num_elems - 1;
        }
        if (*to_index < 0) {
            *to_index += num_elems;
            if (*to_index < 0) *to_index = 0;
        }
    } else { /* *from_index < 0 */
        if (*from_index < -num_elems) {
            if (*to_index < -num_elems) return -1; /* out of range */
            *from_index = -num_elems;
        }
        if (*to_index >= 0) {
            *to_index -= num_elems;
            if (*to_index >= 0) *to_index = -1;
        }
    }
    return 0;
}

ENGINE_ERROR_CODE list_elem_delete(struct default_engine *engine,
                                   const char *key, const size_t nkey,
                                   int from_index, int to_index,
                                   const bool drop_if_empty,
                                   uint32_t *del_count, bool *dropped)
{
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_list_item_find(engine, key, nkey, false, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        list_meta_info *info = (list_meta_info *)item_get_meta(it);
        if (adjust_list_range(info->ccnt, &from_index, &to_index) != 0) {
            ret = ENGINE_ELEM_ENOENT;
        } else {
            int      index;
            uint32_t count;
            if (from_index <= to_index) {
                index = from_index;
                count = to_index - from_index + 1;
            } else {
                index = to_index;
                count = from_index - to_index + 1;
            }
            *del_count = do_list_elem_delete(engine, info, index, count);
            if (*del_count > 0) {
                if (info->ccnt == 0 && drop_if_empty) {
                    do_item_unlink(engine, it);
                    *dropped = true;
                } else {
                    *dropped = false;
                }
            } else {
                ret = ENGINE_ELEM_ENOENT;
            }
        }
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE list_elem_get(struct default_engine *engine,
                                const char *key, const size_t nkey,
                                int from_index, int to_index,
                                const bool delete, const bool drop_if_empty,
                                list_elem_item **elem_array, uint32_t *elem_count,
                                uint32_t *flags, bool *dropped)
{
    hash_item      *it;
    list_meta_info *info;
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_list_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        info = (list_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (adjust_list_range(info->ccnt, &from_index, &to_index) != 0) {
                ret = ENGINE_ELEM_ENOENT;
            } else {
                bool forward = (from_index <= to_index ? true : false);
                int  index = from_index;
                uint32_t count = (forward ? (to_index - from_index + 1)
                                          : (from_index - to_index + 1));
                *elem_count = do_list_elem_get(engine, info, index, count, forward, delete, elem_array);
                if (*elem_count > 0) {
                    if (info->ccnt == 0 && drop_if_empty) {
                        assert(delete == true);
                        do_item_unlink(engine, it);
                        *dropped = true;
                    } else {
                        *dropped = false;
                    }
                    *flags = it->flags;
                } else {
                    ret = ENGINE_ELEM_ENOENT; /* SERVER_ERROR internal */
                }
            }
        } while (0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

/*
 * SET Interface Functions
 */
ENGINE_ERROR_CODE set_struct_create(struct default_engine *engine,
                                    const char *key, const size_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    it = do_item_get(engine, key, nkey, false);
    if (it != NULL) {
        do_item_release(engine, it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_set_item_alloc(engine, key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(engine, it);
            do_item_release(engine, it);
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

set_elem_item *set_elem_alloc(struct default_engine *engine, const int nbytes, const void *cookie)
{
    set_elem_item *elem;
    pthread_mutex_lock(&engine->cache_lock);
    elem = do_set_elem_alloc(engine, nbytes, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return elem;
}

void set_elem_release(struct default_engine *engine, set_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    pthread_mutex_lock(&engine->cache_lock);
    while (cnt < elem_count) {
        do_set_elem_release(engine, elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            pthread_mutex_unlock(&engine->cache_lock);
            pthread_mutex_lock(&engine->cache_lock);
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
}

ENGINE_ERROR_CODE set_elem_insert(struct default_engine *engine, const char *key, const size_t nkey,
                                  set_elem_item *elem, item_attr *attrp, bool *created, const void *cookie)
{
    hash_item     *it;
    set_meta_info *info=NULL;
    ENGINE_ERROR_CODE ret;

    *created = false;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_set_item_find(engine, key, nkey, false, &it);
    do {
        if (ret == ENGINE_SUCCESS) { /* it != NULL */
            info = (set_meta_info *)item_get_meta(it);
#ifdef ENABLE_STICKY_ITEM
            /* sticky memory limit check */
            if ((info->mflags & COLL_META_FLAG_STICKY) != 0) {
                if (engine->stats.sticky_bytes >= engine->config.sticky_limit) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
#endif
            /* overflow check */
            if (info->ccnt >= info->mcnt) {
                ret = ENGINE_EOVERFLOW; break;
            }
        } else if (ret == ENGINE_KEY_ENOENT) {
            if (attrp != NULL) {
                it = do_set_item_alloc(engine, key, nkey, attrp, cookie);
                if (it == NULL) {
                    ret = ENGINE_ENOMEM; break;
                }
                ret = do_item_link(engine, it);
                if (ret != ENGINE_SUCCESS) {
                    /* The set item is to be released in the outside of do-while loop */
                    break;
                }
                info = (set_meta_info *)item_get_meta(it);
                *created = true;
                ret = ENGINE_SUCCESS;
            }
        }

        if (ret == ENGINE_SUCCESS) {
            bool new_root_flag = false;
            if (info->root == NULL) { /* empty set */
                set_hash_node *r_node = do_set_node_alloc(engine, 0, cookie);
                if (r_node == NULL) {
                    if (*created) {
                        /* unlink the created set item and free it */
                        do_item_release(engine, it);
                        do_item_unlink(engine, it);
                        it = NULL;
                    }
                    ret = ENGINE_ENOMEM; break;
                }
                do_set_node_link(engine, info, NULL, 0, r_node);
                new_root_flag = true;
            }

            ret = do_set_elem_link(engine, info, elem, cookie);
            if (ret != ENGINE_SUCCESS) {
                if (new_root_flag) {
                    /* unlink the root node and free it */
                    do_set_node_unlink(engine, info, NULL, 0);
                }
                if (*created) {
                    /* unlink the created set item and free it*/
                    do_item_release(engine, it);
                    do_item_unlink(engine, it);
                    it = NULL;
                }
                break;
            }
        } else {
            /* ENGINE_KEY_ENOENT | ENGINE_EBADTYPE */
        }
    } while(0);

    if (it != NULL) do_item_release(engine, it);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE set_elem_delete(struct default_engine *engine,
                                  const char *key, const size_t nkey,
                                  const char *value, const size_t nbytes,
                                  const bool drop_if_empty, bool *dropped)
{
    hash_item     *it;
    set_meta_info *info;
    ENGINE_ERROR_CODE ret;

    *dropped = false;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_set_item_find(engine, key, nkey, false, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        info = (set_meta_info *)item_get_meta(it);
        ret = do_set_elem_delete_with_value(engine, info, value, nbytes);
        if (ret == ENGINE_SUCCESS) {
            if (info->ccnt == 0 && drop_if_empty) {
                do_item_unlink(engine, it);
                *dropped = true;
            }
        }
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE set_elem_exist(struct default_engine *engine,
                                 const char *key, const size_t nkey,
                                 const char *value, const size_t nbytes,
                                 bool *exist)
{
    hash_item     *it;
    set_meta_info *info;
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_set_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) {
        info = (set_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (do_set_elem_find(info, value, nbytes) != NULL)
                *exist = true;
            else
                *exist = false;
        } while (0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE set_elem_get(struct default_engine *engine,
                               const char *key, const size_t nkey, const uint32_t count,
                               const bool delete, const bool drop_if_empty,
                               set_elem_item **elem_array, uint32_t *elem_count,
                               uint32_t *flags, bool *dropped)
{
    hash_item     *it;
    set_meta_info *info;
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_set_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) {
        info = (set_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            *elem_count = do_set_elem_get(engine, info, count, delete, elem_array);
            if (*elem_count > 0) {
                if (info->ccnt == 0 && drop_if_empty) {
                    assert(delete == true);
                    do_item_unlink(engine, it);
                    *dropped = true;
                } else {
                    *dropped = false;
                }
                *flags = it->flags;
            } else {
                ret = ENGINE_ELEM_ENOENT; break;
            }
        } while (0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

/*
 * B+TREE Interface Functions
 */
ENGINE_ERROR_CODE btree_struct_create(struct default_engine *engine,
                                      const char *key, const size_t nkey,
                                      item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    it = do_item_get(engine, key, nkey, false);
    if (it != NULL) {
        do_item_release(engine, it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_btree_item_alloc(engine, key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(engine, it);
            do_item_release(engine, it);
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

btree_elem_item *btree_elem_alloc(struct default_engine *engine,
                                  const int nbkey, const int neflag, const int nbytes,
                                  const void *cookie)
{
    btree_elem_item *elem;
    pthread_mutex_lock(&engine->cache_lock);
    elem = do_btree_elem_alloc(engine, nbkey, neflag, nbytes, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return elem;
}

void btree_elem_release(struct default_engine *engine,
                        btree_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    pthread_mutex_lock(&engine->cache_lock);
    while (cnt < elem_count) {
        do_btree_elem_release(engine, elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            pthread_mutex_unlock(&engine->cache_lock);
            pthread_mutex_lock(&engine->cache_lock);
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
}

ENGINE_ERROR_CODE btree_elem_insert(struct default_engine *engine,
                                    const char *key, const size_t nkey,
                                    btree_elem_item *elem, const bool replace_if_exist, item_attr *attrp,
                                    bool *replaced, bool *created, btree_elem_item **trimmed_elems,
                                    uint32_t *trimmed_count, uint32_t *trimmed_flags, const void *cookie)
{
    hash_item       *it;
    btree_meta_info *info=NULL;
    ENGINE_ERROR_CODE ret;

    *created = false;
    if (trimmed_elems != NULL) {
        /* initialize as no trimmed element */
        *trimmed_elems = NULL;
        *trimmed_count = 0;
    }

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, false, &it);
    do {
        if (ret == ENGINE_SUCCESS) {
            info = (btree_meta_info *)item_get_meta(it);
            if (info->ccnt > 0 || info->maxbkeyrange.len != BKEY_NULL) {
                if ((info->bktype == BKEY_TYPE_UINT64 && elem->nbkey >  0) ||
                    (info->bktype == BKEY_TYPE_BINARY && elem->nbkey == 0)) {
                    ret = ENGINE_EBADBKEY; break;
                }
            }
        } else if (ret == ENGINE_KEY_ENOENT) {
            if (attrp != NULL) {
                it = do_btree_item_alloc(engine, key, nkey, attrp, cookie);
                if (it == NULL) {
                    ret = ENGINE_ENOMEM; break;
                }
                ret = do_item_link(engine, it);
                if (ret != ENGINE_SUCCESS) {
                    break; /* The btree item will be released in the outside of do-while loop */
                }
                info = (btree_meta_info *)item_get_meta(it);
                *created = true;
                ret = ENGINE_SUCCESS;
            }
        }

        if (ret == ENGINE_SUCCESS) {
            bool new_root_flag = false;
            if (info->root == NULL) {
                btree_indx_node *r_node = do_btree_node_alloc(engine, 0, cookie);
                if (r_node == NULL) {
                    if (*created) {
                        /* unlink the created btree item and free it */
                        do_item_release(engine, it);
                        do_item_unlink(engine, it);
                        it = NULL;
                    }
                    ret = ENGINE_ENOMEM; break;
                }
                do_btree_node_link(engine, info, r_node, NULL);
                new_root_flag = true;
            }
            ret = do_btree_elem_link(engine, info, elem, replace_if_exist, replaced,
                                     trimmed_elems, trimmed_count, cookie);
            if (ret != ENGINE_SUCCESS) {
                if (new_root_flag) {
                    /* unlink the root node and free it */
                    do_btree_node_unlink(engine, info, info->root, NULL);
                }
                if (*created) {
                    /* unlink the created btree item and free it */
                    do_item_release(engine, it);
                    do_item_unlink(engine, it);
                    it = NULL;
                }
                break;
            }
            if (trimmed_elems != NULL && *trimmed_elems != NULL) {
                *trimmed_flags = it->flags;
            }
        } else {
            /* ENGINE_KEY_ENOENT | ENGINE_EBADTYPE */
        }
    } while(0);

    if (it != NULL) do_item_release(engine, it);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_update(struct default_engine *engine,
                                    const char *key, const size_t nkey, const bkey_range *bkrange,
                                    const eflag_update *eupdate, const char *value, const int nbytes,
                                    const void *cookie)
{
    hash_item       *it;
    btree_meta_info *info;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    ENGINE_ERROR_CODE ret;

    assert(bkrtype == BKEY_RANGE_TYPE_SIN); /* single bkey */

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, false, &it);
    if (ret == ENGINE_SUCCESS) {
        info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            ret = do_btree_elem_update(engine, info, bkrtype, bkrange, eupdate, value, nbytes, cookie);
        } while(0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_delete(struct default_engine *engine,
                                    const char *key, const size_t nkey,
                                    const bkey_range *bkrange, const eflag_filter *efilter,
                                    const uint32_t req_count, const bool drop_if_empty,
                                    uint32_t *del_count, bool *dropped)
{
    hash_item       *it;
    btree_meta_info *info;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, false, &it);
    if (ret == ENGINE_SUCCESS) {
        info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *del_count = do_btree_elem_delete(engine, info, bkrtype, bkrange, efilter, req_count);
            if (*del_count > 0) {
                if (info->ccnt == 0 && drop_if_empty) {
                    assert(info->root == NULL);
                    do_item_unlink(engine, it);
                    *dropped = true;
                } else {
                    *dropped = false;
                }
            } else {
                ret = ENGINE_ELEM_ENOENT;
            }
        } while(0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_arithmetic(struct default_engine *engine,
                                        const char* key, const size_t nkey,
                                        const bkey_range *bkrange,
                                        const bool increment, const bool create,
                                        const uint64_t delta, const uint64_t initial,
                                        const eflag_t *eflagp,
                                        uint64_t *result, const void* cookie)
{
    hash_item       *it;
    btree_meta_info *info;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    ENGINE_ERROR_CODE ret;

    assert(bkrtype == BKEY_RANGE_TYPE_SIN); /* single bkey */

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, false, &it);
    if (ret == ENGINE_SUCCESS) {
        bool new_root_flag = false;
        info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt > 0 || info->maxbkeyrange.len != BKEY_NULL) {
                if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                    (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                    ret = ENGINE_EBADBKEY; break;
                }
            }
            if (info->root == NULL && create == true) {
                /* create new root node */
                btree_indx_node *r_node = do_btree_node_alloc(engine, 0, cookie);
                if (r_node == NULL) {
                    ret = ENGINE_ENOMEM; break;
                }
                do_btree_node_link(engine, info, r_node, NULL);
                new_root_flag = true;
            }
            ret = do_btree_elem_arithmetic(engine, info, bkrtype, bkrange, increment, create,
                                           delta, initial, eflagp, result, cookie);
            if (ret != ENGINE_SUCCESS) {
                if (new_root_flag) {
                    /* unlink the root node and free it */
                    do_btree_node_unlink(engine, info, info->root, NULL);
                }
            }
        } while(0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_get(struct default_engine *engine,
                                 const char *key, const size_t nkey,
                                 const bkey_range *bkrange, const eflag_filter *efilter,
                                 const uint32_t offset, const uint32_t req_count,
                                 const bool delete, const bool drop_if_empty,
                                 btree_elem_item **elem_array, uint32_t *elem_count,
                                 uint32_t *flags, bool *dropped_trimmed)
{
    hash_item       *it;
    btree_meta_info *info;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    bool potentialbkeytrim;
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) {
        info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *elem_count = do_btree_elem_get(engine, info, bkrtype, bkrange, efilter, offset, req_count,
                                            delete, elem_array, &potentialbkeytrim);
            if (*elem_count > 0) {
                if (delete) {
                    if (info->ccnt == 0 && drop_if_empty) {
                        assert(info->root == NULL);
                        do_item_unlink(engine, it);
                        *dropped_trimmed = true;
                    } else {
                        *dropped_trimmed = false;
                    }
                } else {
                    *dropped_trimmed = potentialbkeytrim;
                }
                *flags = it->flags;
            } else {
                if (potentialbkeytrim == true)
                    ret = ENGINE_EBKEYOOR;
                else
                    ret = ENGINE_ELEM_ENOENT;
            }
        } while (0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_count(struct default_engine *engine,
                                   const char *key, const size_t nkey,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   uint32_t *elem_count, uint32_t *flags)
{
    hash_item       *it;
    btree_meta_info *info;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    ENGINE_ERROR_CODE ret;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) {
        info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *elem_count = do_btree_elem_count(engine, info, bkrtype, bkrange, efilter);
            *flags = it->flags;
        } while (0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE btree_posi_find(struct default_engine *engine,
                                  const char *key, const size_t nkey, const bkey_range *bkrange,
                                  ENGINE_BTREE_ORDER order, int *position)
{
    hash_item       *it;
    btree_meta_info *info;
    ENGINE_ERROR_CODE ret;

    int bkrtype = do_btree_bkey_range_type(bkrange);
    assert(bkrtype == BKEY_RANGE_TYPE_SIN);

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) {
        info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *position = do_btree_posi_find(info, bkrtype, bkrange, order);
            if (*position < 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
        } while (0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_get_by_posi(struct default_engine *engine,
                                         const char *key, const size_t nkey,
                                         ENGINE_BTREE_ORDER order, int from_posi, int to_posi,
                                         btree_elem_item **elem_array, uint32_t *elem_count, uint32_t *flags)
{
    hash_item       *it;
    btree_meta_info *info;
    ENGINE_ERROR_CODE ret;
    uint32_t rqcount;
    bool     forward;

    assert(from_posi >= 0 && to_posi >= 0);

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) {
        info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            /* adjust from_posi and to_posi considering given order */
            if (from_posi >= info->ccnt && to_posi >= info->ccnt) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if (from_posi >= info->ccnt) from_posi = info->ccnt-1;
            if (to_posi   >= info->ccnt) to_posi   = info->ccnt-1;
            if (order == BTREE_ORDER_DESC) {
                from_posi = info->ccnt - from_posi - 1;
                to_posi   = info->ccnt - to_posi   - 1;
            }
            if (from_posi <= to_posi) {
                forward = true;
                rqcount = to_posi - from_posi + 1;
            } else {
                forward = false;
                rqcount = from_posi - to_posi + 1;
            }
            *elem_count = do_btree_elem_get_by_posi(engine, info, from_posi, rqcount, forward, elem_array);
            if (*elem_count == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            *flags = it->flags;
        } while (0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

#ifdef SUPPORT_BOP_SMGET
ENGINE_ERROR_CODE btree_elem_smget(struct default_engine *engine,
                                   token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   btree_elem_item **elem_array, uint32_t *kfnd_array,
                                   uint32_t *flag_array, uint32_t *elem_count,
                                   uint32_t *missed_key_array, uint32_t *missed_key_count,
                                   bool *trimmed, bool *duplicated)
{
    assert(key_count > 0);
    assert(count > 0 && (offset+count) <= MAX_SMGET_REQ_COUNT);
    btree_scan_info btree_scan_buf[offset+count+1];
    uint16_t        sort_sindx_buf[offset+count]; /* sorted scan index buffer */
    uint32_t        sort_sindx_cnt, i;
    int             bkrtype = do_btree_bkey_range_type(bkrange);
    ENGINE_ERROR_CODE ret;

    /* prepare */
    for (i = 0; i <= (offset+count); i++) {
        btree_scan_buf[i].it = NULL;
    }

    *trimmed = false;
    *duplicated = false;

    pthread_mutex_lock(&engine->cache_lock);

    /* the 1st phase: get the sorted scans */
    ret = do_btree_smget_scan_sort(engine, key_array, key_count,
                                   bkrtype, bkrange, efilter, (offset+count),
                                   btree_scan_buf, sort_sindx_buf, &sort_sindx_cnt,
                                   missed_key_array, missed_key_count, duplicated);
    if (ret == ENGINE_SUCCESS) {
        /* the 2nd phase: get the sorted elems */
        *elem_count = do_btree_smget_elem_sort(btree_scan_buf, sort_sindx_buf, sort_sindx_cnt,
                                               bkrtype, bkrange, efilter, offset, count,
                                               elem_array, kfnd_array, flag_array,
                                               trimmed, duplicated);
        for (i = 0; i <= (offset+count); i++) {
            if (btree_scan_buf[i].it != NULL)
                do_item_release(engine, btree_scan_buf[i].it);
        }
    }

    pthread_mutex_unlock(&engine->cache_lock);

    return ret;
}
#endif

/*
 * ITEM ATTRIBUTE Interface Functions
 */
ENGINE_ERROR_CODE item_getattr(struct default_engine *engine,
                               const void* key, const int nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    it = do_item_get(engine, key, nkey, true);
    if (it == NULL) {
        ret = ENGINE_KEY_ENOENT;
    } else {
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
            rel_time_t current_time = engine->server.core->get_current_time();
            if (it->exptime <= current_time) {
                attr_data->exptime = (rel_time_t)-2;
            } else {
                attr_data->exptime = it->exptime - current_time;
            }
        }

        if (IS_COLL_ITEM(it)) {
            if (IS_LIST_ITEM(it))       attr_data->type = ITEM_TYPE_LIST;
            else if (IS_SET_ITEM(it))   attr_data->type = ITEM_TYPE_SET;
            else if (IS_BTREE_ITEM(it)) attr_data->type = ITEM_TYPE_BTREE;
            else                        attr_data->type = ITEM_TYPE_UNKNOWN;
            /* attribute validation check */
            if (attr_data->type != ITEM_TYPE_BTREE) {
                for (int i = 0; i < attr_count; i++) {
                    if (attr_ids[i] == ATTR_MAXBKEYRANGE || attr_ids[i] == ATTR_TRIMMED) {
                        ret = ENGINE_EBADATTR; break;
                    }
                }
            }
            /* get collection attributes */
            if (ret == ENGINE_SUCCESS) {
                coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
                attr_data->count      = info->ccnt;
                attr_data->maxcount   = info->mcnt;
                attr_data->ovflaction = info->ovflact;
                attr_data->readable   = (((info->mflags & COLL_META_FLAG_READABLE) != 0) ? 1 : 0);
                if (attr_data->type == ITEM_TYPE_BTREE) {
                    btree_meta_info *binfo = (btree_meta_info *)info;
                    attr_data->maxbkeyrange = binfo->maxbkeyrange;
                    attr_data->trimmed = binfo->has_trimmed;
                    if (info->ccnt > 0) {
                        btree_elem_item *min_bkey_elem = do_btree_get_first_elem(binfo->root);
                        do_btree_get_bkey(min_bkey_elem, &attr_data->minbkey);
                        btree_elem_item *max_bkey_elem = do_btree_get_last_elem(binfo->root);
                        do_btree_get_bkey(max_bkey_elem, &attr_data->maxbkey);
                    }
                }
            }
        } else {
            attr_data->type = ITEM_TYPE_KV;
            /* attribute validation check */
            for (int i = 0; i < attr_count; i++) {
                if (attr_ids[i] == ATTR_COUNT      || attr_ids[i] == ATTR_MAXCOUNT ||
                    attr_ids[i] == ATTR_OVFLACTION || attr_ids[i] == ATTR_READABLE ||
                    attr_ids[i] == ATTR_MAXBKEYRANGE || attr_ids[i] == ATTR_TRIMMED) {
                    ret = ENGINE_EBADATTR; break;
                }
            }
        }
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE item_setattr(struct default_engine *engine,
                               const void* key, const int nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    it = do_item_get(engine, key, nkey, true);
    if (it == NULL) {
        ret = ENGINE_KEY_ENOENT;
    } else {
        int i;
        coll_meta_info *info = NULL;
        if (IS_COLL_ITEM(it)) {
            info = (coll_meta_info *)item_get_meta(it);
        }
        for (i = 0; i < attr_count; i++) {
#ifdef ENABLE_STICKY_ITEM
            if (attr_ids[i] == ATTR_EXPIRETIME) {
                /* do not allow sticky toggling */
                if ((it->exptime == (rel_time_t)-1 && attr_data->exptime != (rel_time_t)-1) ||
                    (it->exptime != (rel_time_t)-1 && attr_data->exptime == (rel_time_t)-1)) {
                    ret = ENGINE_EBADVALUE; break;
                }
                continue;
            }
#endif
            /* other attributes : collection attributes */
            if (info == NULL) { /* k/v item */
                ret = ENGINE_EBADATTR; break;
            }
            if (attr_ids[i] == ATTR_MAXCOUNT) {
                if (IS_LIST_ITEM(it)) {
                    if (attr_data->maxcount > MAX_LIST_SIZE)
                        attr_data->maxcount = MAX_LIST_SIZE;
                } else if (IS_SET_ITEM(it)) {
                    if (attr_data->maxcount > MAX_SET_SIZE)
                        attr_data->maxcount = MAX_SET_SIZE;
                } else { /* IS_BTREE_ITEM(it) */
                    if (attr_data->maxcount > MAX_BTREE_SIZE)
                        attr_data->maxcount = MAX_BTREE_SIZE;
                }
                if (info->ccnt > attr_data->maxcount) {
                    ret = ENGINE_EBADVALUE; break;
                }
            } else if (attr_ids[i] == ATTR_OVFLACTION) {
                if (attr_data->ovflaction == OVFL_ERROR) {
                    /* nothing to check */
                } else if (attr_data->ovflaction == OVFL_HEAD_TRIM ||
                           attr_data->ovflaction == OVFL_TAIL_TRIM) {
                    if (! IS_LIST_ITEM(it)) {
                        ret = ENGINE_EBADVALUE; break;
                    }
                } else if (attr_data->ovflaction == OVFL_SMALLEST_TRIM ||
                           attr_data->ovflaction == OVFL_LARGEST_TRIM ||
                           attr_data->ovflaction == OVFL_SMALLEST_SILENT_TRIM ||
                           attr_data->ovflaction == OVFL_LARGEST_SILENT_TRIM) {
                    if (! IS_BTREE_ITEM(it)) {
                        ret = ENGINE_EBADVALUE; break;
                    }
                } else {
                    ret = ENGINE_EBADVALUE; break;
                }
            } else if (attr_ids[i] == ATTR_READABLE) {
                if (attr_data->readable != 1) {
                    ret = ENGINE_EBADVALUE; break;
                }
            } else if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
                if (! IS_BTREE_ITEM(it)) {
                    ret = ENGINE_EBADATTR; break;
                }
                btree_meta_info *binfo = (btree_meta_info *)info;
                if (attr_data->maxbkeyrange.len == BKEY_NULL) {
                    /* nothing to check */
                } else { /* attr_data->maxbkeyrange.len != BKEY_NULL */
                    if (binfo->ccnt == 0) {
                        /* nothing to check */
                    } else { /* binfo->ccnt > 0 */
                        /* check bkey type of maxbkeyrange */
                        if ((binfo->bktype == BKEY_TYPE_UINT64 && attr_data->maxbkeyrange.len >  0) ||
                            (binfo->bktype == BKEY_TYPE_BINARY && attr_data->maxbkeyrange.len == 0)) {
                            ret = ENGINE_EBADVALUE; break;
                        }
                        if (binfo->ccnt >= 2) { /* two or more elements where key range exists. */
                            /* check if current bkey range can be contained within new maxbkeyrange */
                            if (attr_data->maxbkeyrange.len != binfo->maxbkeyrange.len ||
                                BKEY_ISNE(attr_data->maxbkeyrange.val, attr_data->maxbkeyrange.len,
                                          binfo->maxbkeyrange.val, binfo->maxbkeyrange.len)) {
                                bkey_t curbkeyrange;
                                btree_elem_item *min_bkey_elem = do_btree_get_first_elem(binfo->root);
                                btree_elem_item *max_bkey_elem = do_btree_get_last_elem(binfo->root);
                                curbkeyrange.len = attr_data->maxbkeyrange.len;
                                BKEY_DIFF(max_bkey_elem->data, max_bkey_elem->nbkey, min_bkey_elem->data, min_bkey_elem->nbkey,
                                          curbkeyrange.len, curbkeyrange.val);
                                if (BKEY_ISGT(curbkeyrange.val, curbkeyrange.len,
                                              attr_data->maxbkeyrange.val, attr_data->maxbkeyrange.len)) {
                                    ret = ENGINE_EBADVALUE; break;
                                }
                            }
                        } else { /* binfo->ccnt == 1 */
                            /* nothing to check bkey range */
                        }
                    }
                }
            }
        }
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < attr_count; i++) {
                if (attr_ids[i] == ATTR_EXPIRETIME) {
                    if (it->exptime != attr_data->exptime) {
                        if (attr_data->exptime == 0 || it->exptime == 0) {
                            it->exptime = attr_data->exptime;
                            /* reposition it in LRU in order to keep curMK/lowMK concept. */
                            do_item_lru_reposition(engine, it);
                        } else {
                            it->exptime = attr_data->exptime;
                        }
                    }
                }
                else if (attr_ids[i] == ATTR_MAXCOUNT) {
                    info->mcnt = attr_data->maxcount;
                }
                else if (attr_ids[i] == ATTR_OVFLACTION) {
                    if (IS_BTREE_ITEM(it)) {
                        btree_meta_info *binfo = (btree_meta_info *)info;
                        if (binfo->ovflact != attr_data->ovflaction) {
                            binfo->has_trimmed = 0;
                        }
                    }
                    info->ovflact = attr_data->ovflaction;
                }
                else if (attr_ids[i] == ATTR_READABLE) {
                    info->mflags |= COLL_META_FLAG_READABLE;
                }
                else if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
                    btree_meta_info *binfo = (btree_meta_info *)info;
                    if (attr_data->maxbkeyrange.len == BKEY_NULL) {
                        if (binfo->maxbkeyrange.len != BKEY_NULL) {
                            binfo->maxbkeyrange = attr_data->maxbkeyrange;
                            if (binfo->ccnt == 0) {
                                if (binfo->bktype != BKEY_TYPE_UNKNOWN)
                                    binfo->bktype = BKEY_TYPE_UNKNOWN;
                            }
                        }
                    } else { /* attr_data->maxbkeyrange.len != BKEY_NULL */
                        if (binfo->ccnt == 0) {
                            /* just reset maxbkeyrange with new value */
                            binfo->maxbkeyrange = attr_data->maxbkeyrange;
                            if (attr_data->maxbkeyrange.len == 0) {
                                binfo->bktype = BKEY_TYPE_UINT64;
                            } else {
                                binfo->bktype = BKEY_TYPE_BINARY;
                            }
                        } else { /* binfo->ccnt > 0 */
                            binfo->maxbkeyrange = attr_data->maxbkeyrange;
                        }
                    }
                }
            }
        }
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

/*
 * ITEM SCRUB functions
 */
static bool item_scrub(struct default_engine *engine, hash_item *item)
{
    engine->scrubber.visited++;
    rel_time_t current_time = engine->server.core->get_current_time();
    if (item->refcount == 0 && do_item_isvalid(engine, item, current_time) == false) {
        do_item_unlink(engine, item);
        engine->scrubber.cleaned++;
        return true;
    } else {
        return false;
    }
}

static bool do_item_isstale(struct default_engine *engine, hash_item *it)
{
    assert(it != NULL);
#ifdef ENABLE_CLUSTER_AWARE
    if (! engine->server.core->is_my_key(item_get_key(it),it->nkey)) {
        return true; /* stale data */
    }
#endif
    return false; /* not-stale data */
}

static bool item_scrub_stale(struct default_engine *engine, hash_item *item)
{
    engine->scrubber.visited++;
    if (do_item_isstale(engine, item) == true) {
        /* item->refcount might be 0 */
        do_item_unlink(engine, item);
        engine->scrubber.cleaned++;
        return true;
    } else {
        return false;
    }
}

static bool do_item_walk_cursor(struct default_engine *engine, int lruid, bool sticky, int steplength)
{
    hash_item *check;
    int        ii = 0;

    while (ii++ < steplength) {
        if (sticky) {
            if (engine->items.sticky_scrub[lruid] == NULL) return false;
            check = engine->items.sticky_scrub[lruid];
            engine->items.sticky_scrub[lruid] = check->next;
        } else {
            if (engine->items.scrub[lruid] == NULL) return false;
            check = engine->items.scrub[lruid];
            engine->items.scrub[lruid] = check->next;
        }

        if (item_scrub(engine, check) == false) {
            if (engine->scrubber.runmode == SCRUB_MODE_STALE)
                (void)item_scrub_stale(engine, check);
        }
    }
    return true;
}

static void item_scrub_class(struct default_engine *engine, int lruid, bool sticky)
{
    struct timespec sleep_time = {0, 1000};
    long    tot_execs = 0;
    int     i,try_cnt = 9;
    bool    more;
#if 0 // stats
    long    c[10] = {0,0,0,0,0,0,0,0,0,0};
    long    difftime_us;
    struct timeval start_time;
    struct timeval end_time;
#endif

#if 0 // stats
    gettimeofday(&start_time, 0);
#endif
    do {
        /* long-running background task.
         * hold the cache lock lazily in order to give priority to normal workers.
         */
        for (i = 0; i < try_cnt; i++) {
            if (pthread_mutex_trylock(&engine->cache_lock) == 0) break;
            nanosleep(&sleep_time, NULL);
        }
        if (i == try_cnt) pthread_mutex_lock(&engine->cache_lock);
        more = do_item_walk_cursor(engine, lruid, sticky, 10);
        pthread_mutex_unlock(&engine->cache_lock);
        if ((++tot_execs % 50) == 0) {
            nanosleep(&sleep_time, NULL);
        }
#if 0 // stats
        if (engine->config.verbose) {
            c[i]++;
            if ((tot_execs % 100000) == 0) {
                fprintf(stderr, "SCRUB: tot=%lu, c[0]=%lu, c[1]=%lu, c[2]=%lu, c[3]=%lu, c[4]=%lu,\
                                                 c[5]=%lu, c[6]=%lu, c[7]=%lu, c[8]=%lu, c[9]=%lu\n",
                        tot_execs, c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8], c[9]);
            }
        }
#endif
#if 0 // stats
        gettimeofday(&end_time, 0);
        difftime_us = (end_time.tv_sec*1000000+end_time.tv_usec) -
                      (start_time.tv_sec*1000000 + start_time.tv_usec);
        if (difftime_us >= 100) {
            nanosleep(&sleep_time, NULL);
            gettimeofday(&start_time, 0);
        }
#endif
    } while (more);
}

static void *item_scubber_main(void *arg)
{
    struct default_engine *engine = arg;
    assert(engine->scrubber.running == true);

    for (int ii = 0; ii < MAX_NUMBER_OF_SLAB_CLASSES; ++ii)
    {
        pthread_mutex_lock(&engine->cache_lock);
        if (engine->items.heads[ii] != NULL) {
            engine->items.scrub[ii] = engine->items.heads[ii];
            pthread_mutex_unlock(&engine->cache_lock);
            item_scrub_class(engine, ii, false);
            pthread_mutex_lock(&engine->cache_lock);
        }
        pthread_mutex_unlock(&engine->cache_lock);

#ifdef ENABLE_STICKY_ITEM
        pthread_mutex_lock(&engine->cache_lock);
        if (engine->items.sticky_heads[ii] != NULL) {
            engine->items.sticky_scrub[ii] = engine->items.sticky_heads[ii];
            pthread_mutex_unlock(&engine->cache_lock);
            item_scrub_class(engine, ii, true);
            pthread_mutex_lock(&engine->cache_lock);
        }
        pthread_mutex_unlock(&engine->cache_lock);
#endif
    }

    pthread_mutex_lock(&engine->scrubber.lock);
    engine->scrubber.stopped = time(NULL);
    engine->scrubber.running = false;
    pthread_mutex_unlock(&engine->scrubber.lock);
    return NULL;
}

bool item_start_scrub(struct default_engine *engine, int mode)
{
    assert(mode == (int)SCRUB_MODE_NORMAL || mode == (int)SCRUB_MODE_STALE);
    bool ret = false;
    pthread_mutex_lock(&engine->scrubber.lock);
    if (!engine->scrubber.running) {
        engine->scrubber.started = time(NULL);
        engine->scrubber.stopped = 0;
        engine->scrubber.visited = 0;
        engine->scrubber.cleaned = 0;
        engine->scrubber.runmode = (enum scrub_mode)mode;
        engine->scrubber.running = true;

        pthread_t t;
        pthread_attr_t attr;

        if (pthread_attr_init(&attr) != 0 ||
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
            pthread_create(&t, &attr, item_scubber_main, engine) != 0)
        {
            engine->scrubber.running = false;
        } else {
            ret = true;
        }
    }
    pthread_mutex_unlock(&engine->scrubber.lock);

    return ret;
}
