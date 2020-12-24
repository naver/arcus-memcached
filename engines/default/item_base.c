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
#if 0 // check it later
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sched.h>
#include <inttypes.h>
#endif
#include <assert.h>
#include <sys/time.h> /* gettimeofday() */
#include <string.h>
#include "default_engine.h"
#include "item_base.h"
#include "item_clog.h"

static struct default_engine *engine=NULL;
static struct engine_config *config=NULL; // engine config
static struct items         *itemsp=NULL;
static struct engine_stats  *statsp=NULL;
static SERVER_STAT_API      *svstat=NULL; // server stat api
static SERVER_CORE_API      *svcore=NULL; // server core api
static EXTENSION_LOGGER_DESCRIPTOR *logger;

/* We only reposition items in the LRU queue if they haven't been repositioned
 * in this many seconds. That saves us from churning on frequently-accessed
 * items.
 */
#define ITEM_UPDATE_INTERVAL 60

/* How long an object can reasonably be assumed to be locked before
 * harvesting it on a low memory condition.
 */
#define TAIL_REPAIR_TIME (3 * 3600)

/* item queue */
typedef struct {
   hash_item   *head;
   hash_item   *tail;
   unsigned int size;
} item_queue;

/* collection delete queue */
static item_queue      coll_del_queue;
static pthread_mutex_t coll_del_lock;
static pthread_cond_t  coll_del_cond;
static pthread_t       coll_del_tid; /* thread id */
static bool            coll_del_sleep = false;
static volatile bool   coll_del_thread_running = false;

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

#define ITEM_REFCOUNT_FULL 65535
#define ITEM_REFCOUNT_MOVE 32768

//static inline void ITEM_REFCOUNT_INCR(hash_item *it)
void ITEM_REFCOUNT_INCR(hash_item *it)
{
    it->refcount++;
    if (it->refcount == ITEM_REFCOUNT_FULL) {
        it->refchunk += 1;
        it->refcount -= ITEM_REFCOUNT_MOVE;
        assert(it->refchunk != 0); /* overflow */
    }
}

//static inline void ITEM_REFCOUNT_DECR(hash_item *it)
void ITEM_REFCOUNT_DECR(hash_item *it)
{
    it->refcount--;
    if (it->refcount == 0 && it->refchunk > 0) {
        it->refchunk -= 1;
        it->refcount = ITEM_REFCOUNT_MOVE;
    }
}

static inline uint32_t _hash_item_size(const hash_item *item)
{
    uint32_t ntotal = sizeof(hash_item);
    if (item) {
        if (item->iflag & ITEM_WITH_CAS) {
            ntotal += sizeof(uint64_t);
        }
    } else {
        if (config->use_cas) {
            ntotal += sizeof(uint64_t);
        }
    }
    return ntotal;
}

/* warning: don't use these macros with a function, as it evals its arg twice */
static inline size_t ITEM_ntotal(const hash_item *item)
{
    size_t ntotal = _hash_item_size(item);
    if (IS_COLL_ITEM(item)) {
        ntotal += META_OFFSET_IN_ITEM(item->nkey, item->nbytes);
        if (IS_LIST_ITEM(item))     ntotal += sizeof(list_meta_info);
        else if (IS_SET_ITEM(item)) ntotal += sizeof(set_meta_info);
        else if (IS_MAP_ITEM(item)) ntotal += sizeof(map_meta_info);
        else /* BTREE_ITEM */       ntotal += sizeof(btree_meta_info);
    } else {
        ntotal += (item->nkey + item->nbytes);
    }
    return ntotal;
}

static inline size_t ITEM_stotal(const hash_item *item)
{
    size_t ntotal = ITEM_ntotal(item);
    size_t stotal = slabs_space_size(ntotal);
    if (IS_COLL_ITEM(item)) {
        coll_meta_info *info = (coll_meta_info *)item_get_meta(item);
        stotal += info->stotal;
    }
    return stotal;
}

static inline void LOCK_STATS(void)
{
    //pthread_mutex_lock(&statsp->lock);
}

static inline void UNLOCK_STATS(void)
{
    //pthread_mutex_unlock(&statsp->lock);
}

static inline void do_item_stat_reclaim(const unsigned int lruid)
{
    itemsp->itemstats[lruid].reclaimed++;
    LOCK_STATS();
    statsp->reclaimed++;
    UNLOCK_STATS();
}

static inline void do_item_stat_evict(const unsigned int lruid,
                                      rel_time_t current_time,
                                      hash_item *it, const void *cookie)
{
    itemsp->itemstats[lruid].evicted++;
    itemsp->itemstats[lruid].evicted_time = current_time - it->time;
    if (it->exptime > 0) {
        itemsp->itemstats[lruid].evicted_nonzero++;
    }
    LOCK_STATS();
    statsp->evictions++;
    UNLOCK_STATS();
    if (cookie != NULL) {
        svstat->evicting(cookie, item_get_key(it), it->nkey);
    }
}

static inline void do_item_stat_outofmemory(const unsigned int lruid)
{
    itemsp->itemstats[lruid].outofmemory++;
    LOCK_STATS();
    statsp->outofmemorys++;
    UNLOCK_STATS();
}

static inline void do_item_stat_link(hash_item *it, size_t stotal)
{
    LOCK_STATS();
#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_EXPTIME(it->exptime)) {
        statsp->sticky_bytes += stotal;
        statsp->sticky_items += 1;
    }
#endif
    statsp->curr_bytes += stotal;
    statsp->curr_items += 1;
    statsp->total_items += 1;
    UNLOCK_STATS();
}

static inline void do_item_stat_unlink(hash_item *it, size_t stotal)
{
    LOCK_STATS();
#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_EXPTIME(it->exptime)) {
        statsp->sticky_bytes -= stotal;
        statsp->sticky_items -= 1;
    }
#endif
    statsp->curr_bytes -= stotal;
    statsp->curr_items -= 1;
    UNLOCK_STATS();
}

static inline void do_item_stat_replace(hash_item *old_it, hash_item *new_it)
{
    prefix_t *pt = new_it->pfxptr;
    size_t old_stotal = ITEM_stotal(old_it);
    size_t new_stotal = ITEM_stotal(new_it);

    int item_type = GET_ITEM_TYPE(old_it);

    LOCK_STATS();
    if (new_stotal != old_stotal) {
        if (new_stotal > old_stotal) {
            prefix_bytes_incr(pt, item_type, new_stotal - old_stotal);
        } else {
            prefix_bytes_decr(pt, item_type, old_stotal - new_stotal);
        }
    }
    /* update item stats imformation */
#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_EXPTIME(new_it->exptime)) {
        statsp->sticky_bytes += new_stotal - old_stotal;
    }
#endif
    statsp->curr_bytes += new_stotal - old_stotal;
    statsp->total_items += 1;
    UNLOCK_STATS();
}

static inline void do_item_stat_bytes_incr(hash_item *it, size_t stotal)
{
    LOCK_STATS();
#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_EXPTIME(it->exptime)) {
        statsp->sticky_bytes += stotal;
    }
#endif
    statsp->curr_bytes += stotal;
    UNLOCK_STATS();
}

static inline void do_item_stat_bytes_decr(hash_item *it, size_t stotal)
{
    LOCK_STATS();
#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_EXPTIME(it->exptime)) {
        statsp->sticky_bytes -= stotal;
    }
#endif
    statsp->curr_bytes -= stotal;
    UNLOCK_STATS();
}

//static inline void do_item_stat_get(ADD_STAT add_stat, const void *cookie)
void do_item_stat_get(ADD_STAT add_stat, const void *cookie)
{
    char val[128];
    int len;

    LOCK_STATS();
    len = sprintf(val, "%"PRIu64, statsp->reclaimed);
    add_stat("reclaimed", 9, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)statsp->evictions);
    add_stat("evictions", 9, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)statsp->outofmemorys);
    add_stat("outofmemorys", 12, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)statsp->sticky_items);
    add_stat("sticky_items", 12, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)statsp->curr_items);
    add_stat("curr_items", 10, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)statsp->total_items);
    add_stat("total_items", 11, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)statsp->sticky_bytes);
    add_stat("sticky_bytes", 12, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)statsp->curr_bytes);
    add_stat("bytes", 5, val, len, cookie);
    UNLOCK_STATS();

    len = sprintf(val, "%"PRIu64, (uint64_t)config->sticky_limit);
    add_stat("sticky_limit", 12, val, len, cookie);
    len = sprintf(val, "%"PRIu64, (uint64_t)config->maxbytes);
    add_stat("engine_maxbytes", 15, val, len, cookie);
}

//static inline void do_item_stat_reset(void)
void do_item_stat_reset(void)
{
    /* reset engine->items.itemstats */
    memset(itemsp->itemstats, 0, sizeof(itemsp->itemstats));

    /* reset engine->stats */
    LOCK_STATS();
    statsp->evictions = 0;
    statsp->reclaimed = 0;
    statsp->outofmemorys = 0;
    statsp->total_items = 0;
    UNLOCK_STATS();
}

#ifdef ENABLE_STICKY_ITEM
//static inline bool do_item_sticky_overflowed(void)
bool do_item_sticky_overflowed(void)
{
    if (statsp->sticky_bytes < config->sticky_limit) {
        return false;
    }
    return true;
}
#endif

/*
static void do_coll_space_incr(coll_meta_info *info, ENGINE_ITEM_TYPE item_type,
                               const size_t nspace)
*/
void do_coll_space_incr(coll_meta_info *info, ENGINE_ITEM_TYPE item_type,
                   const size_t nspace)
{
    info->stotal += nspace;

    hash_item *it = (hash_item*)COLL_GET_HASH_ITEM(info);
    do_item_stat_bytes_incr(it, nspace);
    prefix_bytes_incr(it->pfxptr, item_type, nspace);
}

/*
static void do_coll_space_decr(coll_meta_info *info, ENGINE_ITEM_TYPE item_type,
                               const size_t nspace)
*/
void do_coll_space_decr(coll_meta_info *info, ENGINE_ITEM_TYPE item_type,
                   const size_t nspace)
{
    assert(info->stotal >= nspace);
    info->stotal -= nspace;

    hash_item *it = (hash_item*)COLL_GET_HASH_ITEM(info);
    do_item_stat_bytes_decr(it, nspace);
    prefix_bytes_decr(it->pfxptr, item_type, nspace);
}

/* Max hash key length for calculating hash value */
#define MAX_HKEY_LEN 250

static inline uint32_t GEN_ITEM_KEY_HASH(const char *key, const uint32_t nkey)
{
    if (nkey > MAX_HKEY_LEN) {
        /* The last MAX_HKEY_LEN bytes of the key is used */
        const char *hkey = key + (nkey-MAX_HKEY_LEN);
        return svcore->hash(hkey, MAX_HKEY_LEN, 0);
    } else {
        return svcore->hash(key, nkey, 0);
    }
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

/*
 * Collection Delete Queue Management
 */
static void push_coll_del_queue(hash_item *it)
{
    /* push the item into the tail of delete queue */
    it->next = NULL;
    pthread_mutex_lock(&coll_del_lock);
    if (coll_del_queue.tail == NULL) {
        coll_del_queue.head = it;
    } else {
        coll_del_queue.tail->next = it;
    }
    coll_del_queue.tail = it;
    coll_del_queue.size++;
    if (coll_del_sleep == true) {
        /* wake up collection delete thead */
        pthread_cond_signal(&coll_del_cond);
    }
    pthread_mutex_unlock(&coll_del_lock);
}

static hash_item *pop_coll_del_queue(void)
{
    /* pop an item from the head of delete queue */
    hash_item *it = NULL;
    pthread_mutex_lock(&coll_del_lock);
    if (coll_del_queue.head != NULL) {
        it = coll_del_queue.head;
        coll_del_queue.head = it->next;
        if (coll_del_queue.head == NULL) {
            coll_del_queue.tail = NULL;
        }
        coll_del_queue.size--;
    }
    pthread_mutex_unlock(&coll_del_lock);
    return it;
}

static uint32_t do_coll_elem_delete_with_count(hash_item *it, uint32_t count)
{
    coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
    uint32_t ndeleted = 0;

    if (info->ccnt > 0) {
        if (IS_BTREE_ITEM(it)) {
            ndeleted = btree_elem_delete_with_count((void *)info, count);
        } else if (IS_SET_ITEM(it)) {
            ndeleted = set_elem_delete_with_count((void *)info, count);
        } else if (IS_MAP_ITEM(it)) {
            ndeleted = map_elem_delete_with_count((void *)info, count);
        } else if (IS_LIST_ITEM(it)) {
            ndeleted = list_elem_delete_with_count((void *)info, count);
        }
    }
    return ndeleted;
}

static void item_link_q(hash_item *it)
{
    hash_item **head, **tail;
    assert(it->slabs_clsid <= POWER_LARGEST);

#ifdef USE_SINGLE_LRU_LIST
    int clsid = 1;
#else
    int clsid = it->slabs_clsid;
    if (IS_COLL_ITEM(it) || ITEM_ntotal(it) <= MAX_SM_VALUE_LEN) {
        clsid = LRU_CLSID_FOR_SMALL;
    }
#endif

#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_EXPTIME(it->exptime)) {
        head = &itemsp->sticky_heads[clsid];
        tail = &itemsp->sticky_tails[clsid];
        itemsp->sticky_sizes[clsid]++;
    } else {
#endif
        head = &itemsp->heads[clsid];
        tail = &itemsp->tails[clsid];
        itemsp->sizes[clsid]++;
        if (it->exptime > 0) { /* expirable item */
            if (itemsp->lowMK[clsid] == NULL) {
                /* set lowMK and curMK pointer in LRU */
                itemsp->lowMK[clsid] = it;
                itemsp->curMK[clsid] = it;
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

static void item_unlink_q(hash_item *it)
{
    hash_item **head, **tail;
    assert(it->slabs_clsid <= POWER_LARGEST);
#ifdef USE_SINGLE_LRU_LIST
    int clsid = 1;
#else
    int clsid = it->slabs_clsid;
    if (IS_COLL_ITEM(it) || ITEM_ntotal(it) <= MAX_SM_VALUE_LEN) {
        clsid = LRU_CLSID_FOR_SMALL;
    }
#endif

    if (it->prev == it && it->next == it) { /* special meaning: unlinked from LRU */
        return; /* Already unlinked from LRU list */
    }

#ifdef ENABLE_STICKY_ITEM
    if (IS_STICKY_EXPTIME(it->exptime)) {
        head = &itemsp->sticky_heads[clsid];
        tail = &itemsp->sticky_tails[clsid];
        itemsp->sticky_sizes[clsid]--;
        /* move curMK pointer in LRU */
        if (itemsp->sticky_curMK[clsid] == it)
            itemsp->sticky_curMK[clsid] = it->prev;
    } else {
#endif
        head = &itemsp->heads[clsid];
        tail = &itemsp->tails[clsid];
        itemsp->sizes[clsid]--;
        /* move lowMK, curMK pointer in LRU */
        if (itemsp->lowMK[clsid] == it)
            itemsp->lowMK[clsid] = it->prev;
        if (itemsp->curMK[clsid] == it) {
            itemsp->curMK[clsid] = it->prev;
            if (itemsp->curMK[clsid] == NULL)
                itemsp->curMK[clsid] = itemsp->lowMK[clsid];
        }
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
    it->prev = it->next = it; /* special meaning: unlinked from LRU */
    return;
}

//static bool do_item_isvalid(hash_item *it, rel_time_t current_time)
bool do_item_isvalid(hash_item *it, rel_time_t current_time)
{
    /* check if it's expired */
#ifdef ENABLE_STICKY_ITEM
    /* The sticky item has an exptime((rel_tiem_t)(-1)) that is
     * larger than any current_time.  So, it never be expired.
     **/
#endif
    if (it->exptime != 0 && it->exptime <= current_time) {
        return false; /* expired */
    }
    /* check if it's flushed */
    if (config->oldest_live != 0 &&
        config->oldest_live <= current_time && it->time <= config->oldest_live) {
        return false; /* flushed by flush_all */
    }
    /* check if its prefix is invalid */
    if (prefix_isvalid(it, current_time) == false) {
        return false;
    }
    return true; /* Yes, it's a valid item */
}

static hash_item *do_item_reclaim(hash_item *it,
                                  const size_t ntotal, const unsigned int clsid,
                                  const unsigned int lruid)
{
    /* increment # of reclaimed */
    do_item_stat_reclaim(lruid);

    /* it->refcount == 0 */
#ifdef USE_SINGLE_LRU_LIST
#else
    if (lruid != LRU_CLSID_FOR_SMALL) {
        it->refcount = 1;
        slabs_adjust_mem_requested(it->slabs_clsid, ITEM_ntotal(it), ntotal);
        do_item_unlink(it, ITEM_UNLINK_INVALID);
        /* Initialize the item block: */
        it->slabs_clsid = 0;
        it->refcount = 0;
        return it;
    }
    /* collection item or small-sized kv item */
#endif

    if (IS_COLL_ITEM(it)) {
        (void)do_coll_elem_delete_with_count(it, 500);
    }
    do_item_unlink(it, ITEM_UNLINK_INVALID);

    /* allocate from slab allocator */
    it = slabs_alloc(ntotal, clsid);
    return it;
}

static void do_item_invalidate(hash_item *it, const unsigned int lruid, bool immediate)
{
    /* increment # of reclaimed */
    do_item_stat_reclaim(lruid);

    /* it->refcount == 0 */
    if (immediate && IS_COLL_ITEM(it)) {
        (void)do_coll_elem_delete_with_count(it, 500);
    }
    do_item_unlink(it, ITEM_UNLINK_INVALID);
}

static void do_item_evict(hash_item *it, const unsigned int lruid,
                          rel_time_t current_time, const void *cookie)
{
    /* increment # of evicted */
    do_item_stat_evict(lruid, current_time, it, cookie);

    /* unlink the item */
    if (IS_COLL_ITEM(it)) {
        (void)do_coll_elem_delete_with_count(it, 500);
    }
    do_item_unlink(it, ITEM_UNLINK_EVICT);
}

static void do_item_repair(hash_item *it, const unsigned int lruid)
{
    /* increment # of repaired */
    itemsp->itemstats[lruid].tailrepairs++;
    it->refcount = 0;
    it->refchunk = 0;

    /* unlink the item */
    if (IS_COLL_ITEM(it)) {
        (void)do_coll_elem_delete_with_count(it, 500);
    }
    do_item_unlink(it, ITEM_UNLINK_EVICT);
}

static uint32_t do_item_regain(const uint32_t count, rel_time_t current_time,
                               const void *cookie)
{
    hash_item *previt;
    hash_item *search;
    uint32_t tries = count;
    uint32_t nregains = 0;

#ifdef USE_SINGLE_LRU_LIST
    unsigned int clsid = 1;
#else
    unsigned int clsid = LRU_CLSID_FOR_SMALL;
#endif

    search = itemsp->tails[clsid];
    while (search != NULL) {
        assert(search->nkey > 0);
        previt = search->prev;
        if (search->refcount == 0) {
            if (do_item_isvalid(search, current_time)) {
                do_item_evict(search, clsid, current_time, cookie);
            } else {
                do_item_invalidate(search, clsid, true);
            }
            nregains += 1;
        } else { /* search->refcount > 0 */
            /* We just unlink the item from LRU list.
             * It will be linked to LRU list when the refcount become 0.
             * See do_item_release().
             */
            item_unlink_q(search);
        }
        search = previt;
        if ((--tries) == 0) break;
    }
    return nregains;
}

//static void *do_item_mem_alloc(const size_t ntotal, const unsigned int clsid,
//                               const void *cookie)
void *do_item_mem_alloc(const size_t ntotal, const unsigned int clsid,
                        const void *cookie)
{
    hash_item *it = NULL;

    /* do a quick check if we have any expired items in the tail.. */
    int tries;
    hash_item *search;
    hash_item *previt = NULL;

    rel_time_t current_time = svcore->get_current_time();

#ifdef USE_SINGLE_LRU_LIST
    unsigned int lruid = 1;
    unsigned int clsid_based_on_ntotal = 1;

    if ((it = slabs_alloc(ntotal, clsid_based_on_ntotal)) != NULL) {
        it->slabs_clsid = 0;
        return (void*)it;
    }
#else
    unsigned int lruid;
    unsigned int clsid_based_on_ntotal;

    if (clsid == LRU_CLSID_FOR_SMALL) {
        clsid_based_on_ntotal = slabs_clsid(ntotal);
        lruid                 = clsid;
    } else {
        clsid_based_on_ntotal = clsid;
        if (ntotal <= MAX_SM_VALUE_LEN) {
            lruid = LRU_CLSID_FOR_SMALL;
        } else {
            lruid = clsid;
        }
    }
#endif

    /* Let's regain item space when space shortage level > 0. */
    if (config->evict_to_free && lruid == LRU_CLSID_FOR_SMALL) {
        int current_ssl = slabs_space_shortage_level();
        if (current_ssl > 0) {
            (void)do_item_regain(current_ssl, current_time, cookie);
        }
    }

#ifdef ENABLE_STICKY_ITEM
    /* reclaim the flushed sticky items */
    if (itemsp->sticky_curMK[lruid] != NULL) {
        tries = 10;
        while (itemsp->sticky_curMK[lruid] != NULL) {
            search = itemsp->sticky_curMK[lruid];
            itemsp->sticky_curMK[lruid] = search->prev;
            if (search->refcount == 0 && !do_item_isvalid(search, current_time)) {
                it = do_item_reclaim(search, ntotal, clsid_based_on_ntotal, lruid);
                if (it != NULL) break; /* allocated */
            }
            if ((--tries) == 0) break;
        }
        if (it != NULL) {
            /* try one more invalidation */
            search = itemsp->sticky_curMK[lruid];
            if (search != NULL &&
                search->refcount == 0 && !do_item_isvalid(search, current_time)) {
                do_item_invalidate(search, lruid, false);
            }
            it->slabs_clsid = 0;
            return (void*)it;
        }
    }
#endif

    if (itemsp->curMK[lruid] != NULL) {
        assert(itemsp->lowMK[lruid] != NULL);
        /* step 1) reclaim items from lowMK position */
        tries = 10;
        search = itemsp->lowMK[lruid];
        while (search != NULL && search != itemsp->curMK[lruid]) {
            if (search->refcount == 0 && !do_item_isvalid(search, current_time)) {
                previt = search->prev;
                it = do_item_reclaim(search, ntotal, clsid_based_on_ntotal, lruid);
                if (it != NULL) break; /* allocated */
                search = previt;
            } else {
                if (search->exptime == 0 && search == itemsp->lowMK[lruid]) {
                    itemsp->lowMK[lruid] = search->prev; /* move lowMK position upward */
                }
                search = search->prev;
            }
            if ((--tries) == 0) break;
        }
        if (it != NULL) {
            /* try one more invalidation */
            if (previt != NULL &&
                previt->refcount == 0 && !do_item_isvalid(previt, current_time)) {
                do_item_invalidate(previt, lruid, false);
            }
            it->slabs_clsid = 0;
            return (void *)it;
        }
        /* step 2) reclaim items from curMK position */
        tries += 20;
        while (itemsp->curMK[lruid] != NULL) {
            search = itemsp->curMK[lruid];
            itemsp->curMK[lruid] = search->prev;
            if (search->refcount == 0 && !do_item_isvalid(search, current_time)) {
                it = do_item_reclaim(search, ntotal, clsid_based_on_ntotal, lruid);
                if (it != NULL) break; /* allocated */
            }
            if ((--tries) == 0) break;
        }
        if (itemsp->curMK[lruid] == NULL) {
            itemsp->curMK[lruid] = itemsp->lowMK[lruid];
        }
        if (it != NULL) {
            /* try one more invalidation */
            search = itemsp->curMK[lruid];
            if (search != NULL &&
                search->refcount == 0 && !do_item_isvalid(search, current_time)) {
                do_item_invalidate(search, lruid, false);
            }
            it->slabs_clsid = 0;
            return (void *)it;
        }
    }

    it = slabs_alloc(ntotal, clsid_based_on_ntotal);
    if (it == NULL) {
        /*
        ** Could not find an expired item at the tail, and memory allocation
        ** failed. Try to evict some items!
        */

        /* If requested to not push old items out of cache when memory runs out,
         * we're out of luck at this point...
         */
        if (!config->evict_to_free) {
            do_item_stat_outofmemory(lruid);
            return NULL;
        }

        /* try to get one off the right LRU
         * don't necessariuly unlink the tail because it may be locked: refcount>0
         * search up from tail an item with refcount==0 and unlink it; give up after 50
         * tries
         */
        tries  = 200;
        search = itemsp->tails[lruid];
        while (search != NULL) {
            assert(search->nkey > 0);
            previt = search->prev;
            if (search->refcount == 0) {
                if (do_item_isvalid(search, current_time)) {
                    do_item_evict(search, lruid, current_time, cookie);
                    it = slabs_alloc(ntotal, clsid_based_on_ntotal);
                } else {
                    it = do_item_reclaim(search, ntotal, clsid_based_on_ntotal, lruid);
                }
                if (it != NULL) break; /* allocated */
            } else { /* search->refcount > 0 */
                /* just unlink the item from LRU list. */
                item_unlink_q(search);
            }
            search = previt;
            if ((--tries) == 0) break;
        }
        if (it != NULL) {
            if (config->verbose > 1) {
                logger->log(EXTENSION_LOG_INFO, NULL,
                        "Succeed to allocate an item in %d retries of eviction.\n",
                        (201-tries));
            }
        } else {
            /* Failed to allocate an item even if we evict itmes */
            do_item_stat_outofmemory(lruid);

            if (lruid == LRU_CLSID_FOR_SMALL) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                        "No more small memory. space_shortage_level=%d, size=%lu\n",
                        slabs_space_shortage_level(), ntotal);
                slabs_dump_SM_info();
            } else {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                        "No more memory. lruid=%d, size=%lu\n", lruid, ntotal);
            }
        }
    }

    if (it == NULL && lruid != LRU_CLSID_FOR_SMALL) {
        /* Last ditch effort. There is a very rare bug which causes
         * refcount leaks. We've fixed most of them, but it still happens,
         * and it may happen in the future.
         * We can reasonably assume no item can stay locked for more than
         * three hours, so if we find one in the tail which is that old,
         * free it anyway.
         */
        if (lruid <= POWER_LARGEST) {
            tries  = 50;
            search = itemsp->tails[lruid];
            while (search != NULL) {
                assert(search->nkey > 0);
                if (search->refcount != 0 &&
                    search->time + TAIL_REPAIR_TIME < current_time) {
                    previt = search->prev;
                    do_item_repair(search, lruid);
                    it = slabs_alloc(ntotal, clsid_based_on_ntotal);
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

//static void do_item_mem_free(void *item, size_t ntotal)
void do_item_mem_free(void *item, size_t ntotal)
{
    hash_item *it = (hash_item *)item;
    unsigned int clsid = it->slabs_clsid;
    it->slabs_clsid = 0; /* to notify the item memory is freed */
    slabs_free(it, ntotal, clsid);
}

/*@null@*/
/*
static hash_item *do_item_alloc(const void *key, const uint32_t nkey,
                                const uint32_t flags, const rel_time_t exptime,
                                const uint32_t nbytes, const void *cookie)
*/
hash_item *do_item_alloc(const void *key, const uint32_t nkey,
                         const uint32_t flags, const rel_time_t exptime,
                         const uint32_t nbytes, const void *cookie)
{
    assert(nkey > 0);
    hash_item *it = NULL;
    size_t ntotal = item_kv_size(nkey, nbytes);
    unsigned int id = slabs_clsid(ntotal);
    if (id == 0) {
        return NULL;
    }

#ifdef ENABLE_STICKY_ITEM
    /* sticky memory limit check */
    if (IS_STICKY_EXPTIME(exptime)) {
        if (do_item_sticky_overflowed())
            return NULL;
    }
#endif

    it = do_item_mem_alloc(ntotal, id, cookie);
    if (it == NULL)  {
        return NULL;
    }
    it->slabs_clsid = id;
    assert(it != itemsp->heads[it->slabs_clsid]);

    it->next = it->prev = it; /* special meaning: unlinked from LRU */
    it->h_next = 0;
    it->refcount = 0;
    it->refchunk = 0;
    DEBUG_REFCNT(it, '*');
    it->iflag = config->use_cas ? ITEM_WITH_CAS : 0;
    it->nkey = nkey;
    it->nbytes = nbytes;
    it->flags = flags;
    if (key != NULL) {
        memcpy((void*)item_get_key(it), key, nkey);
    }
    it->exptime = exptime;
    it->pfxptr = NULL;
    return it;
}

//static void do_item_free(hash_item *it)
void do_item_free(hash_item *it)
{
    assert((it->iflag & ITEM_LINKED) == 0);
    assert(it != itemsp->heads[it->slabs_clsid]);
    assert(it != itemsp->tails[it->slabs_clsid]);
    assert(it->refcount == 0);

    if (IS_COLL_ITEM(it)) {
        coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
        if (info->ccnt > 0) { /* still NOT empty */
            push_coll_del_queue(it);
            return;
        }
    }

    /* so slab size changer can tell later if item is already free or not */
    DEBUG_REFCNT(it, 'F');
    do_item_mem_free(it, ITEM_ntotal(it));
}

//static ENGINE_ERROR_CODE do_item_link(hash_item *it)
ENGINE_ERROR_CODE do_item_link(hash_item *it)
{
    const char *key = item_get_key(it);
    assert((it->iflag & ITEM_LINKED) == 0);

    MEMCACHED_ITEM_LINK(key, it->nkey, it->nbytes);

    /* Allocate a new CAS ID on link. */
    item_set_cas(it, get_cas_id());

    /* link the item to prefix info */
    size_t stotal = ITEM_stotal(it);
    bool internal_prefix = false;
    ENGINE_ERROR_CODE ret = prefix_link(it, stotal, &internal_prefix);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    if (internal_prefix) {
        /* It's an internal item whose prefix name is "arcus". */
        it->iflag |= ITEM_INTERNAL;
    }

    /* link the item to the hash table */
    it->iflag |= ITEM_LINKED;
    it->time = svcore->get_current_time();
    it->khash = GEN_ITEM_KEY_HASH(key, it->nkey);
    assoc_insert(it, it->khash);

    /* link the item to LRU list */
    item_link_q(it);
    CLOG_ITEM_LINK(it);

    /* update item statistics */
    do_item_stat_link(it, stotal);

    return ENGINE_SUCCESS;
}

//static void do_item_unlink(hash_item *it, enum item_unlink_cause cause)
void do_item_unlink(hash_item *it, enum item_unlink_cause cause)
{
    /* cause: item unlink cause will be used, later
    */
    const char *key = item_get_key(it);
    MEMCACHED_ITEM_UNLINK(key, it->nkey, it->nbytes);

    if ((it->iflag & ITEM_LINKED) != 0) {
        CLOG_ITEM_UNLINK(it, cause);

        /* unlink the item from LRU list */
        item_unlink_q(it);

        /* unlink the item from hash table */
        assoc_delete(key, it->nkey, it->khash);
        it->iflag &= ~ITEM_LINKED;

        /* unlink the item from prefix info */
        size_t stotal = ITEM_stotal(it);
        prefix_unlink(it, stotal, (cause != ITEM_UNLINK_REPLACE ? true : false));

        /* update item statistics */
        do_item_stat_unlink(it, stotal);

        if (IS_COLL_ITEM(it)) {
            /* IMPORTANT NOTE)
             * The element space statistics has already been decreased.
             * So, we must not decrease the space statistics any more
             * even if the elements are freed later.
             * For that purpose, we set info->stotal to 0 like below.
             */
            coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
            info->stotal = 0;
        }

        /* free the item if no one reference it */
        if (it->refcount == 0) {
            do_item_free(it);
        }
    }
}

//static void do_item_replace(hash_item *old_it, hash_item *new_it)
void do_item_replace(hash_item *old_it, hash_item *new_it)
{
    MEMCACHED_ITEM_REPLACE(item_get_key(old_it), old_it->nkey, old_it->nbytes,
                           item_get_key(new_it), new_it->nkey, new_it->nbytes);

    assert((old_it->iflag & ITEM_LINKED) != 0);

    CLOG_ITEM_UNLINK(old_it, ITEM_UNLINK_REPLACE);

    /* unlink the item from LRU list */
    item_unlink_q(old_it);

    /* Allocate a new CAS ID on link. */
    item_set_cas(new_it, get_cas_id());

    /* link the item to the hash table */
    new_it->iflag |= ITEM_LINKED;
    new_it->time = svcore->get_current_time();
    new_it->khash = old_it->khash;

    /* replace the item from hash table */
    assoc_replace(old_it, new_it);
    old_it->iflag &= ~ITEM_LINKED;

    /* update prefix information */
    prefix_t *pt = old_it->pfxptr;
    new_it->pfxptr = old_it->pfxptr;
    old_it->pfxptr = NULL;
    assert(pt != NULL);

    /* if old_it->iflag has ITEM_INTERNAL */
    if (old_it->iflag & ITEM_INTERNAL) {
        new_it->iflag |= ITEM_INTERNAL;
    }

    /* update prefix info and stats */
    do_item_stat_replace(old_it, new_it);

    if (IS_COLL_ITEM(old_it)) {
        /* IMPORTANT NOTE)
         * The element space statistics has already been decreased.
         * So, we must not decrease the space statistics any more
         * even if the elements are freed later.
         * For that purpose, we set info->stotal to 0 like below.
         */
        coll_meta_info *info = (coll_meta_info *)item_get_meta(old_it);
        info->stotal = 0;
    }

    /* free the item if no one reference it */
    if (old_it->refcount == 0) {
        do_item_free(old_it);
    }

    /* link the item to LRU list */
    item_link_q(new_it);
    CLOG_ITEM_LINK(new_it);
}

//static void do_item_update(hash_item *it, bool force)
void do_item_update(hash_item *it, bool force)
{
    MEMCACHED_ITEM_UPDATE(item_get_key(it), it->nkey, it->nbytes);

    if ((it->iflag & ITEM_LINKED) != 0) {
        rel_time_t current_time = svcore->get_current_time();
        if (force) {
            /* The exceptional case when exptime is changed.
             * See do_item_setattr() for specific explanation.
             */
            item_unlink_q(it);
            it->time = current_time;
            item_link_q(it);
        } else if (it->time < (current_time - ITEM_UPDATE_INTERVAL)) {
            /* The normal case when the given item is read.
             */
            item_unlink_q(it);
            it->time = current_time;
            item_link_q(it);
            CLOG_ITEM_UPDATE(it);
        }
    }
}

/** wrapper around assoc_find which does the lazy expiration logic */
//static hash_item *do_item_get(const char *key, const uint32_t nkey, bool do_update)
hash_item *do_item_get(const char *key, const uint32_t nkey, bool do_update)
{
    hash_item *it = assoc_find(key, nkey, GEN_ITEM_KEY_HASH(key, nkey));
    if (it) {
        rel_time_t current_time = svcore->get_current_time();
        if (do_item_isvalid(it, current_time)) {
            ITEM_REFCOUNT_INCR(it);
            DEBUG_REFCNT(it, '+');
            if (do_update) {
                do_item_update(it, false);
            }
        } else {
            do_item_unlink(it, ITEM_UNLINK_INVALID);
            it = NULL;
        }
    }
    if (config->verbose > 2) {
        char keybuf[MAX_HKEY_LEN];
        if (nkey < MAX_HKEY_LEN) {
            memcpy(keybuf, key, nkey);
            keybuf[nkey] = '\0';
        } else {
            memcpy(keybuf, key, MAX_HKEY_LEN-4);
            keybuf[MAX_HKEY_LEN-4] = '#';
            keybuf[MAX_HKEY_LEN-3] = '#';
            keybuf[MAX_HKEY_LEN-2] = '#';
            keybuf[MAX_HKEY_LEN-1] = '\0';
        }

        logger->log(EXTENSION_LOG_INFO, NULL, "> %s %s\n",
                    it ? "FOUND KEY" : "NOT FOUND",
                    keybuf);
    }
    return it;
}

//static void do_item_release(hash_item *it)
void do_item_release(hash_item *it)
{
    MEMCACHED_ITEM_REMOVE(item_get_key(it), it->nkey, it->nbytes);

    if (it->refcount != 0) {
        ITEM_REFCOUNT_DECR(it);
        DEBUG_REFCNT(it, '-');
    }
    if (it->refcount == 0) {
        if ((it->iflag & ITEM_LINKED) == 0) {
            do_item_free(it);
        }
        else if (it->prev == it && it->next == it) {
            /* re-link the item into the LRU list */
            rel_time_t current_time = svcore->get_current_time();
            if (do_item_isvalid(it, current_time)) {
                it->time = current_time;
                item_link_q(it);
            } else {
                do_item_unlink(it, ITEM_UNLINK_INVALID);
            }
        }
    }
}

/*
 * Item Management Daemon
 */
static void coll_del_thread_sleep(void)
{
    struct timeval  tv;
    struct timespec to;
    pthread_mutex_lock(&coll_del_lock);
    if (coll_del_queue.head == NULL) {
        /* 50 mili second sleep */
        gettimeofday(&tv, NULL);
        tv.tv_usec += 50000;
        if (tv.tv_usec >= 1000000) {
            tv.tv_sec += 1;
            tv.tv_usec -= 1000000;
        }
        to.tv_sec = tv.tv_sec;
        to.tv_nsec = tv.tv_usec * 1000;

        coll_del_sleep = true;
        pthread_cond_timedwait(&coll_del_cond,
                               &coll_del_lock, &to);
        coll_del_sleep = false;
    }
    pthread_mutex_unlock(&coll_del_lock);
}

static void *collection_delete_thread(void *arg)
{
    struct default_engine *engine = arg;
    hash_item      *it;
    struct timespec sleep_time = {0, 0};
    uint32_t        delete_count;
    int             current_ssl;
    uint32_t        evict_count;
    uint32_t        bg_evict_count = 0;
    bool            bg_evict_start = false;

    coll_del_thread_running = true;

    while (engine->initialized) {
        it = pop_coll_del_queue();
        if (it != NULL) {
            LOCK_CACHE();
            delete_count = do_coll_elem_delete_with_count(it, 100);
            while (delete_count >= 100) {
                UNLOCK_CACHE();
                if (slabs_space_shortage_level() <= 2) {
                    sleep_time.tv_nsec = 10000; /* 10 us */
                    nanosleep(&sleep_time, NULL);
                }
                LOCK_CACHE();
                delete_count = do_coll_elem_delete_with_count(it, 100);
            }
            /* it has become an empty collection. */
            do_item_free(it);
            UNLOCK_CACHE();
            continue;
        }

        evict_count = 0;
        if (config->evict_to_free) {
            current_ssl = slabs_space_shortage_level();
            if (current_ssl >= 10) {
                LOCK_CACHE();
                if (config->evict_to_free) {
                    rel_time_t current_time = svcore->get_current_time();
                    evict_count = do_item_regain(current_ssl, current_time, NULL);
                }
                UNLOCK_CACHE();
            }
        }
        if (evict_count > 0) {
            if (bg_evict_start == false) {
                /*****
                if (config->verbose > 1) {
                    logger->log(EXTENSION_LOG_INFO, NULL, "background evict: start\n");
                }
                *****/
                bg_evict_start = true;
                bg_evict_count = 0;
            }
            bg_evict_count += evict_count;
            if (bg_evict_count >= 10000000) {
                if (config->verbose > 1) {
                    logger->log(EXTENSION_LOG_INFO, NULL, "background evict: count=%u\n",
                                                           bg_evict_count);
                }
                bg_evict_count = 0;
            }
            sleep_time.tv_nsec = 10000000 / current_ssl; /* 10000us / ssl */
            nanosleep(&sleep_time, NULL);
        } else {
            if (bg_evict_start == true) {
                /*****
                if (config->verbose > 1) {
                    logger->log(EXTENSION_LOG_INFO, NULL, "background evict: stop count=%u\n",
                                                           bg_evict_count);
                }
                *****/
                bg_evict_start = false;
            }
            coll_del_thread_sleep();
        }
    }

    coll_del_thread_running = false;
    return NULL;
}

void coll_del_thread_wakeup(void)
{
    pthread_mutex_lock(&coll_del_lock);
    if (coll_del_sleep == true) {
        /* wake up collection delete thead */
        pthread_cond_signal(&coll_del_cond);
    }
    pthread_mutex_unlock(&coll_del_lock);
}

/*
 * Item access functions
 */
uint64_t item_get_cas(const hash_item* item)
{
    if (item->iflag & ITEM_WITH_CAS) {
        return *(uint64_t*)(item + 1);
    }
    return 0;
}

void item_set_cas(const hash_item* item, uint64_t val)
{
    if (item->iflag & ITEM_WITH_CAS) {
        *(uint64_t*)(item + 1) = val;
    }
}

const void* item_get_key(const hash_item* item)
{
    return ((char*)item + _hash_item_size(item));
}

char* item_get_data(const hash_item* item)
{
    return ((char*)item_get_key(item)) + item->nkey;
}

const void* item_get_meta(const hash_item* item)
{
    if (IS_COLL_ITEM(item))
        return (void*)((char*)item_get_key(item) +
                       META_OFFSET_IN_ITEM(item->nkey, item->nbytes));
    else
        return NULL;
}

/*
 * Item size functions
 */
uint32_t item_kv_size(const uint32_t nkey, const uint32_t nbytes)
{
    return (_hash_item_size(NULL) + nkey + nbytes);
}

uint32_t item_ntotal(hash_item *item)
{
    return (uint32_t)ITEM_ntotal(item);
}

/*
 * Check item validity
 */
bool item_is_valid(hash_item* item)
{
    if (item->iflag & ITEM_LINKED) {
        rel_time_t current_time = svcore->get_current_time();
        return do_item_isvalid(item, current_time);
    } else {
        return false;
    }
}

/*
 * Initialize change log module
 */
int item_base_init(void *engine_ptr)
{
    engine = (struct default_engine *)engine_ptr;
    config = &engine->config;
    itemsp = &engine->items;
    statsp = &engine->stats;
    svstat = engine->server.stat;
    svcore = engine->server.core;
    logger = engine->server.log->get_logger();

    /* collection delete queue */
    pthread_mutex_init(&coll_del_lock, NULL);
    pthread_cond_init(&coll_del_cond, NULL);
    coll_del_queue.head = coll_del_queue.tail = NULL;
    coll_del_queue.size = 0;

    int ret = pthread_create(&coll_del_tid, NULL, collection_delete_thread, engine);
    if (ret != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Can't create thread: %s\n", strerror(ret));
        return -1;
    }

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM base module initialized.\n");
    return 0;
}

void item_base_final(void *engine_ptr)
{
    if (coll_del_thread_running) {
        coll_del_thread_wakeup();
        pthread_join(coll_del_tid, NULL);
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM base module destroyed.\n");
}
