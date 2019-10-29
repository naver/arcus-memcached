/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2016 JaM2in Co., Ltd.
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
#include <sys/time.h> /* gettimeofday() */

#include "default_engine.h"
#include "item_clog.h"

#ifdef ENABLE_PERSISTENCE
#include "cmdlogmgr.h"
#include "cmdlogbuf.h"
#endif

//#define SET_DELETE_NO_MERGE
//#define BTREE_DELETE_NO_MERGE

/* Forward Declarations */
static void item_link_q(hash_item *it);
static void item_unlink_q(hash_item *it);
static ENGINE_ERROR_CODE do_item_link(hash_item *it);
static void do_item_unlink(hash_item *it, enum item_unlink_cause cause);
static inline void do_coll_elem_delete(coll_meta_info *info, int type, uint32_t count);
static uint32_t do_map_elem_delete(map_meta_info *info,
                                   const uint32_t count, enum elem_delete_cause cause);

extern int genhash_string_hash(const void* p, size_t nkey);

/*
 * We only reposition items in the LRU queue if they haven't been repositioned
 * in this many seconds. That saves us from churning on frequently-accessed
 * items.
 */
#define ITEM_UPDATE_INTERVAL 60

/* A do_update argument value representing that
 * we should check and reposition items in the LRU list.
 */
#define DO_UPDATE true
#define DONT_UPDATE false

/* LRU id of small memory items */
#define LRU_CLSID_FOR_SMALL 0

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

/* btree element item or btree node item */
#define BTREE_GET_ELEM_ITEM(node, indx) ((btree_elem_item *)((node)->item[indx]))
#define BTREE_GET_NODE_ITEM(node, indx) ((btree_indx_node *)((node)->item[indx]))

/* get bkey real size */
#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))

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

/** How long an object can reasonably be assumed to be locked before
 *     harvesting it on a low memory condition. */
#define TAIL_REPAIR_TIME (3 * 3600)

/* collection meta info offset */
#define META_OFFSET_IN_ITEM(nkey,nbytes) ((((nkey)+(nbytes)-1)/8+1)*8)

/* max hash key length for calculation hash value */
#define MAX_HKEY_LEN 250

#ifdef ENABLE_STICKY_ITEM
/* macros for identifying sticky items */
#define IS_STICKY_EXPTIME(e) ((e) == (rel_time_t)(-1))
#define IS_STICKY_COLLFLG(i) (((i)->mflags & COLL_META_FLAG_STICKY) != 0)
#endif

/* scan count definitions */
#define SCRUB_MIN_COUNT 32
#define SCRUB_MAX_COUNT 320

/* btree position debugging */
static bool btree_position_debug = false;

/* bkey min & max value */
static uint64_t      bkey_uint64_min;
static uint64_t      bkey_uint64_max;
static unsigned char bkey_binary_min[MIN_BKEY_LENG];
static unsigned char bkey_binary_max[MAX_BKEY_LENG];

/* maximum collection size  */
static int32_t coll_size_limit = 1000000;
static int32_t max_list_size   = 50000;
static int32_t max_set_size    = 50000;
static int32_t max_map_size    = 50000;
static int32_t max_btree_size  = 50000;

/* default collection size */
static int32_t default_list_size  = 4000;
static int32_t default_set_size   = 4000;
static int32_t default_map_size  = 4000;
static int32_t default_btree_size = 4000;

/* Temporary Facility */
/* forced btree overflow action */
static char    forced_action_prefix[256];
static int32_t forced_action_pfxlen = 0;
static uint8_t forced_btree_ovflact = 0;

/* collection delete queue */
static item_queue      coll_del_queue;
static pthread_mutex_t coll_del_lock;
static pthread_cond_t  coll_del_cond;
static pthread_t       coll_del_tid; /* thread id */
static bool            coll_del_sleep = false;
static volatile bool   coll_del_thread_running = false;

static struct default_engine *ngnptr=NULL;
static struct engine_config *config=NULL; // engine config
static struct items         *itemsp=NULL;
static struct engine_stats  *statsp=NULL;
static SERVER_CORE_API      *svcore=NULL; // server core api
static SERVER_STAT_API      *svstat=NULL; // server stat api
static EXTENSION_LOGGER_DESCRIPTOR *logger;

/* map element previous info internally used */
typedef struct _map_prev_info {
    map_hash_node *node;
    map_elem_item *prev;
    uint16_t       hidx;
} map_prev_info;

/*
 * Static functions
 */
static inline void LOCK_CACHE(void)
{
    pthread_mutex_lock(&ngnptr->cache_lock);
}

static inline void UNLOCK_CACHE(void)
{
    pthread_mutex_unlock(&ngnptr->cache_lock);
}

static inline void TRYLOCK_CACHE(int ntries)
{
    int i;

    for (i = 0; i < ntries; i++) {
        if (pthread_mutex_trylock(&ngnptr->cache_lock) == 0)
            break;
        sched_yield();
    }
    if (i == ntries) {
        pthread_mutex_lock(&ngnptr->cache_lock);
    }
}

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

#ifdef ENABLE_STICKY_ITEM
static inline bool do_item_sticky_overflowed(void)
{
    if (statsp->sticky_bytes < config->sticky_limit) {
        return false;
    }
    return true;
}
#endif

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

static inline void do_item_stat_get(ADD_STAT add_stat, const void *cookie)
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

static inline void do_item_stat_reset(void)
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

/* warning: don't use these macros with a function, as it evals its arg twice */
static inline size_t ITEM_ntotal(const hash_item *item)
{
    size_t ntotal;
    if (IS_COLL_ITEM(item)) {
        ntotal = sizeof(*item) + META_OFFSET_IN_ITEM(item->nkey, item->nbytes);
        if (IS_LIST_ITEM(item))     ntotal += sizeof(list_meta_info);
        else if (IS_SET_ITEM(item)) ntotal += sizeof(set_meta_info);
        else if (IS_MAP_ITEM(item)) ntotal += sizeof(map_meta_info);
        else /* BTREE_ITEM */       ntotal += sizeof(btree_meta_info);
    } else {
        ntotal = sizeof(*item) + item->nkey + item->nbytes;
    }
    if (config->use_cas) {
        ntotal += sizeof(uint64_t);
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

static bool do_item_isvalid(hash_item *it, rel_time_t current_time)
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
    /* check flushed items as well as expired items */
    if (config->oldest_live != 0) {
        if (config->oldest_live <= current_time && it->time <= config->oldest_live)
            return false; /* flushed by flush_all */
    }
    /* check if prefix is valid */
    if (assoc_prefix_isvalid(it, current_time) == false) {
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
        coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
        if (info->ccnt > 0) {
            do_coll_elem_delete(info, GET_ITEM_TYPE(it), 500);
        }
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
        coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
        if (info->ccnt > 0) {
            do_coll_elem_delete(info, GET_ITEM_TYPE(it), 500);
        }
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
        coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
        if (info->ccnt > 0) {
            do_coll_elem_delete(info, GET_ITEM_TYPE(it), 500);
        }
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
        coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
        if (info->ccnt > 0) {
            do_coll_elem_delete(info, GET_ITEM_TYPE(it), 500);
        }
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
            if (do_item_isvalid(search, current_time) == false) {
                do_item_invalidate(search, clsid, true);
            } else {
                do_item_evict(search, clsid, current_time, cookie);
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

static void *do_item_mem_alloc(const size_t ntotal, const unsigned int clsid,
                               const void *cookie)
{
    hash_item *it = NULL;

    /* do a quick check if we have any expired items in the tail.. */
    int tries;
    hash_item *search;
    hash_item *previt = NULL;

    rel_time_t current_time = svcore->get_current_time();

#ifdef USE_SINGLE_LRU_LIST
    unsigned int id = 1;
    unsigned int clsid_based_on_ntotal = 1;

    if ((it = slabs_alloc(ntotal, clsid_based_on_ntotal)) != NULL) {
        it->slabs_clsid = 0;
        return (void*)it;
    }
#else
    unsigned int id;
    unsigned int clsid_based_on_ntotal;

    if (clsid == LRU_CLSID_FOR_SMALL) {
        clsid_based_on_ntotal = slabs_clsid(ntotal);
        id                    = clsid;
    } else {
        clsid_based_on_ntotal = clsid;
        if (ntotal <= MAX_SM_VALUE_LEN) {
            id = LRU_CLSID_FOR_SMALL;
        } else {
            id = clsid;
        }
    }
#endif

    /* Let's regain item space when space shortage level > 0. */
    if (config->evict_to_free && id == LRU_CLSID_FOR_SMALL) {
        int current_ssl = slabs_space_shortage_level();
        if (current_ssl > 0) {
            (void)do_item_regain(current_ssl, current_time, cookie);
        }
    }

#ifdef ENABLE_STICKY_ITEM
    /* reclaim the flushed sticky items */
    if (itemsp->sticky_curMK[id] != NULL) {
        tries = 20;
        while (itemsp->sticky_curMK[id] != NULL) {
            search = itemsp->sticky_curMK[id];
            itemsp->sticky_curMK[id] = search->prev;
            if (search->refcount == 0 &&
                do_item_isvalid(search, current_time) == false) {
                it = do_item_reclaim(search, ntotal, clsid_based_on_ntotal, id);
                if (it != NULL) break; /* allocated */
            }
            if ((--tries) == 0) break;
        }
        if (it != NULL) {
            /* try one more invalidation */
            search = itemsp->sticky_curMK[id];
            if (search != NULL && search->refcount == 0 &&
                do_item_isvalid(search, current_time) == false) {
                do_item_invalidate(search, id, false);
            }
            it->slabs_clsid = 0;
            return (void*)it;
        }
    }
#endif

    if (itemsp->curMK[id] != NULL) {
        assert(itemsp->lowMK[id] != NULL);
        /* step 1) reclaim items from lowMK position */
        tries = 20;
        search = itemsp->lowMK[id];
        while (search != NULL && search != itemsp->curMK[id]) {
            if (search->refcount == 0 &&
                do_item_isvalid(search, current_time) == false) {
                previt = search->prev;
                it = do_item_reclaim(search, ntotal, clsid_based_on_ntotal, id);
                if (it != NULL) break; /* allocated */
                search = previt;
            } else {
                if (search->exptime == 0 && search == itemsp->lowMK[id]) {
                    itemsp->lowMK[id] = search->prev; /* move lowMK position upward */
                }
                search = search->prev;
            }
            if ((--tries) == 0) break;
        }
        if (it != NULL) {
            /* try one more invalidation */
            if (previt != NULL && previt->refcount == 0 &&
                do_item_isvalid(previt, current_time) == false) {
                do_item_invalidate(previt, id, false);
            }
            it->slabs_clsid = 0;
            return (void *)it;
        }
        /* step 2) reclaim items from curMK position */
        tries += 40;
        while (itemsp->curMK[id] != NULL) {
            search = itemsp->curMK[id];
            itemsp->curMK[id] = search->prev;
            if (search->refcount == 0 &&
                do_item_isvalid(search, current_time) == false) {
                it = do_item_reclaim(search, ntotal, clsid_based_on_ntotal, id);
                if (it != NULL) break; /* allocated */
            }
            if ((--tries) == 0) break;
        }
        if (itemsp->curMK[id] == NULL) {
            itemsp->curMK[id] = itemsp->lowMK[id];
        }
        if (it != NULL) {
            /* try one more invalidation */
            search = itemsp->curMK[id];
            if (search != NULL && search->refcount == 0 &&
                do_item_isvalid(search, current_time) == false) {
                do_item_invalidate(search, id, false);
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
            do_item_stat_outofmemory(clsid_based_on_ntotal);
            return NULL;
        }

        /*
         * try to get one off the right LRU
         * don't necessariuly unlink the tail because it may be locked: refcount>0
         * search up from tail an item with refcount==0 and unlink it; give up after 50
         * tries
         */
        tries  = 200;
        search = itemsp->tails[id];
        while (search != NULL) {
            assert(search->nkey > 0);
            previt = search->prev;
            if (search->refcount == 0) {
                if (do_item_isvalid(search, current_time) == false) {
                    it = do_item_reclaim(search, ntotal, clsid_based_on_ntotal, id);
                } else {
                    do_item_evict(search, id, current_time, cookie);
                    it = slabs_alloc(ntotal, clsid_based_on_ntotal);
                }
                if (it != NULL) break; /* allocated */
            } else { /* search->refcount > 0 */
                /* just unlink the item from LRU list. */
                item_unlink_q(search);
            }
            search = previt;
            if ((--tries) == 0) break;
        }
        if (config->verbose > 1) {
            logger->log(EXTENSION_LOG_INFO, NULL,
                    "Allocation retries with evict. count=%d\n", (tries == 0 ? 200 : (200-tries+1)));
        }
    }

    if (it == NULL) {
        do_item_stat_outofmemory(id);
        if (id == LRU_CLSID_FOR_SMALL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "No more small memory. space_shortage_level=%d, size=%lu\n",
                        slabs_space_shortage_level(), ntotal);
        } else {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "No more memory. clsid=%d, size=%lu\n", id, ntotal);
        }

        /* Last ditch effort. There is a very rare bug which causes
         * refcount leaks. We've fixed most of them, but it still happens,
         * and it may happen in the future.
         * We can reasonably assume no item can stay locked for more than
         * three hours, so if we find one in the tail which is that old,
         * free it anyway.
         */
        if (id <= POWER_LARGEST) {
            tries  = 50;
            search = itemsp->tails[id];
            while (search != NULL) {
                assert(search->nkey > 0);
                if (search->refcount != 0 &&
                    search->time + TAIL_REPAIR_TIME < current_time) {
                    previt = search->prev;
                    do_item_repair(search, id);
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

/*@null@*/
static hash_item *do_item_alloc(const void *key, const uint32_t nkey,
                                const uint32_t flags, const rel_time_t exptime,
                                const uint32_t nbytes, const void *cookie)
{
    assert(nkey > 0);
    hash_item *it = NULL;
    size_t ntotal = sizeof(hash_item) + nkey + nbytes;
    if (config->use_cas) {
        ntotal += sizeof(uint64_t);
    }

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
    assert(it->slabs_clsid == 0);

    it->slabs_clsid = id;
    assert(it->slabs_clsid > 0);
    assert(it != itemsp->heads[it->slabs_clsid]);

    it->next = it->prev = it; /* special meaning: unlinked from LRU */
    it->h_next = 0;
    it->refcount = 1;     /* the caller will have a reference */
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

static void do_item_mem_free(void *item, size_t ntotal)
{
    hash_item *it = (hash_item *)item;
    unsigned int clsid = it->slabs_clsid;
    it->slabs_clsid = 0; /* to notify the item memory is freed */
    slabs_free(it, ntotal, clsid);
}

static void do_item_free(hash_item *it)
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

static ENGINE_ERROR_CODE do_item_link(hash_item *it)
{
    const char *key = item_get_key(it);
    const char *hkey = (it->nkey > MAX_HKEY_LEN) ? key+(it->nkey-MAX_HKEY_LEN) : key;
    const uint32_t hnkey = (it->nkey > MAX_HKEY_LEN) ? MAX_HKEY_LEN : it->nkey;
    assert((it->iflag & ITEM_LINKED) == 0);
    assert(it->nbytes < (1024 * 1024));  /* 1MB max size */

    MEMCACHED_ITEM_LINK(key, it->nkey, it->nbytes);

    /* Allocate a new CAS ID on link. */
    item_set_cas(it, get_cas_id());

    /* link the item to prefix info */
    size_t stotal = ITEM_stotal(it);
    bool internal_prefix = false;
    ENGINE_ERROR_CODE ret = assoc_prefix_link(it, stotal, &internal_prefix);
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
    it->khash = svcore->hash(hkey, hnkey, 0);
    assoc_insert(it, it->khash);

    /* link the item to LRU list */
    item_link_q(it);
    CLOG_ITEM_LINK(it);

    /* update item statistics */
    do_item_stat_link(it, stotal);

    return ENGINE_SUCCESS;
}

static void do_item_unlink(hash_item *it, enum item_unlink_cause cause)
{
    /* cause: item unlink cause will be used, later
    */
    const char *key = item_get_key(it);
    MEMCACHED_ITEM_UNLINK(key, it->nkey, it->nbytes);

    if ((it->iflag & ITEM_LINKED) != 0) {
        CLOG_ITEM_UNLINK(it, cause);

        /* unlink the item from LUR list */
        item_unlink_q(it);

        /* unlink the item from hash table */
        assoc_delete(key, it->nkey, it->khash);
        it->iflag &= ~ITEM_LINKED;

        /* unlink the item from prefix info */
        size_t stotal = ITEM_stotal(it);
        assoc_prefix_unlink(it, stotal, (cause != ITEM_UNLINK_REPLACE ? true : false));

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

static void do_item_release(hash_item *it)
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

static void do_item_update(hash_item *it, bool force)
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
static hash_item *do_item_get(const char *key, const uint32_t nkey, bool do_update)
{
    hash_item *it;
    const char *hkey = (nkey > MAX_HKEY_LEN) ? key+(nkey-MAX_HKEY_LEN) : key;
    const uint32_t hnkey = (nkey > MAX_HKEY_LEN) ? MAX_HKEY_LEN : nkey;

    it = assoc_find(key, nkey, svcore->hash(hkey, hnkey, 0));
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
        if (it) {
            logger->log(EXTENSION_LOG_INFO, NULL, "> FOUND KEY %s\n",
                        (const char*)item_get_key(it));
        } else {
            logger->log(EXTENSION_LOG_INFO, NULL, "> NOT FOUND %s\n",
                        key);
        }
    }
    return it;
}

static void do_item_replace(hash_item *old_it, hash_item *new_it)
{
    MEMCACHED_ITEM_REPLACE(item_get_key(old_it), old_it->nkey, old_it->nbytes,
                           item_get_key(new_it), new_it->nkey, new_it->nbytes);
    do_item_unlink(old_it, ITEM_UNLINK_REPLACE);
    /* Cache item replacement does not drop the prefix item even if it's empty.
     * So, the below do_item_link function always return SUCCESS.
     */
    (void)do_item_link(new_it);
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
            do_item_release(new_it);
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
    do_item_release(new_it);       /* release our reference */

    return ENGINE_SUCCESS;
}

static void do_coll_space_incr(coll_meta_info *info, ENGINE_ITEM_TYPE item_type,
                               const size_t nspace)
{
    info->stotal += nspace;

    hash_item *it = (hash_item*)COLL_GET_HASH_ITEM(info);
    do_item_stat_bytes_incr(it, nspace);
    assoc_prefix_bytes_incr(it->pfxptr, item_type, nspace);
}

static void do_coll_space_decr(coll_meta_info *info, ENGINE_ITEM_TYPE item_type,
                               const size_t nspace)
{
    assert(info->stotal >= nspace);
    info->stotal -= nspace;

    hash_item *it = (hash_item*)COLL_GET_HASH_ITEM(info);
    do_item_stat_bytes_decr(it, nspace);
    assoc_prefix_bytes_decr(it->pfxptr, item_type, nspace);
}

/* get real maxcount for each collection type */
static int32_t do_coll_real_maxcount(hash_item *it, int32_t maxcount)
{
    int32_t real_maxcount = maxcount;

    if (IS_LIST_ITEM(it)) {
        if (maxcount < 0 || maxcount > max_list_size)
            real_maxcount = -1;
        else if (maxcount == 0)
            real_maxcount = default_list_size;
    } else if (IS_SET_ITEM(it)) {
        if (maxcount < 0 || maxcount > max_set_size)
            real_maxcount = -1;
        else if (maxcount == 0)
            real_maxcount = default_set_size;
    } else if (IS_MAP_ITEM(it)) {
        if (maxcount < 0 || maxcount > max_map_size)
            real_maxcount = -1;
        else if (maxcount == 0)
            real_maxcount = default_map_size;
    } else if (IS_BTREE_ITEM(it)) {
        if (maxcount < 0 || maxcount > max_btree_size)
            real_maxcount = -1;
        else if (maxcount == 0)
            real_maxcount = default_btree_size;
    }
    return real_maxcount;
}

static inline uint32_t do_list_elem_ntotal(list_elem_item *elem)
{
    return sizeof(list_elem_item) + elem->nbytes;
}

static inline uint32_t do_set_elem_ntotal(set_elem_item *elem)
{
    return sizeof(set_elem_item) + elem->nbytes;
}

static inline uint32_t do_map_elem_ntotal(map_elem_item *elem)
{
    return sizeof(map_elem_item) + elem->nfield + elem->nbytes;
}

static inline uint32_t do_btree_elem_ntotal(btree_elem_item *elem)
{
    return sizeof(btree_elem_item_fixed) + BTREE_REAL_NBKEY(elem->nbkey)
           + elem->neflag + elem->nbytes;
}

/*
 * LIST collection management
 */
static ENGINE_ERROR_CODE do_list_item_find(const void *key, const uint32_t nkey,
                                           bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_LIST_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static hash_item *do_list_item_alloc(const void *key, const uint32_t nkey,
                                     item_attr *attrp, const void *cookie)
{
    uint32_t nbytes = 2; /* "\r\n" */
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(list_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_LIST;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), "\r\n", nbytes);

        /* initialize list meta information */
        list_meta_info *info = (list_meta_info *)item_get_meta(it);
        info->mcnt = do_coll_real_maxcount(it, attrp->maxcount);
        info->ccnt = 0;
        info->ovflact = (attrp->ovflaction==0 ? OVFL_TAIL_TRIM : attrp->ovflaction);
        info->mflags  = 0;
#ifdef ENABLE_STICKY_ITEM
        if (IS_STICKY_EXPTIME(attrp->exptime)) info->mflags |= COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1)              info->mflags |= COLL_META_FLAG_READABLE;
        info->itdist  = (uint16_t)((size_t*)info-(size_t*)it);
        info->stotal  = 0;
        info->head = info->tail = NULL;
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);
    }
    return it;
}

static list_elem_item *do_list_elem_alloc(const uint32_t nbytes, const void *cookie)
{
    size_t ntotal = sizeof(list_elem_item) + nbytes;

    list_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        assert(elem->slabs_clsid == 0);
        elem->slabs_clsid = slabs_clsid(ntotal);
        assert(elem->slabs_clsid > 0);
        elem->refcount    = 0;
        elem->nbytes      = nbytes;
        elem->prev = elem->next = (list_elem_item *)ADDR_MEANS_UNLINKED; /* Unliked state */
    }
    return elem;
}

static void do_list_elem_free(list_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = do_list_elem_ntotal(elem);
    do_item_mem_free(elem, ntotal);
}

static void do_list_elem_release(list_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->next == (list_elem_item *)ADDR_MEANS_UNLINKED) {
        do_list_elem_free(elem);
    }
}

static list_elem_item *do_list_elem_find(list_meta_info *info, int index)
{
    list_elem_item *elem;

    if (index <= (info->ccnt/2)) {
        assert(index >= 0);
        elem = info->head;
        for (int i = 0; i < index && elem != NULL; i++) {
            elem = elem->next;
        }
    } else {
        assert(index < info->ccnt);
        elem = info->tail;
        for (int i = info->ccnt-1; i > index && elem != NULL; i--) {
            elem = elem->prev;
        }
    }
    return elem;
}

static ENGINE_ERROR_CODE do_list_elem_link(list_meta_info *info, const int index,
                                           list_elem_item *elem)
{
    list_elem_item *prev, *next;

    assert(index >= 0);
    if (index == 0) {
        prev = NULL;
        next = info->head;
    } else if (index >= info->ccnt) {
        prev = info->tail;
        next = NULL;
    } else {
        prev = do_list_elem_find(info, (index-1));
        next = prev->next;
    }

    elem->prev = prev;
    elem->next = next;
    if (prev == NULL) info->head = elem;
    else              prev->next = elem;
    if (next == NULL) info->tail = elem;
    else              next->prev = elem;
    info->ccnt++;

    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(do_list_elem_ntotal(elem));
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_LIST, stotal);
    }
    return ENGINE_SUCCESS;
}

static void do_list_elem_unlink(list_meta_info *info, list_elem_item *elem,
                                enum elem_delete_cause cause)
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
            size_t stotal = slabs_space_size(do_list_elem_ntotal(elem));
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_LIST, stotal);
        }

        if (elem->refcount == 0) {
            do_list_elem_free(elem);
        }
    }
}

static uint32_t do_list_elem_delete(list_meta_info *info,
                                    const int index, const uint32_t count,
                                    enum elem_delete_cause cause)
{
    list_elem_item *elem;
    list_elem_item *next;
    uint32_t fcnt = 0;

    CLOG_LIST_ELEM_DELETE(info, index, count, true, cause);

    elem = do_list_elem_find(info, index);
    while (elem != NULL) {
        next = elem->next;
        fcnt++;
        do_list_elem_unlink(info, elem, cause);
        if (count > 0 && fcnt >= count) break;
        elem = next;
    }
    return fcnt;
}

static ENGINE_ERROR_CODE do_list_elem_get(list_meta_info *info,
                                          const int index, const uint32_t count,
                                          const bool forward, const bool delete,
                                          list_elem_item **elem_array, uint32_t *elem_count)
{
    list_elem_item *elem;
    list_elem_item *tobe;
    uint32_t fcnt = 0; /* found count */
    enum elem_delete_cause cause = ELEM_DELETE_NORMAL;

    if (delete) {
        CLOG_LIST_ELEM_DELETE(info, index, count, forward, ELEM_DELETE_NORMAL);
    }

    elem = do_list_elem_find(info, index);
    while (elem != NULL) {
        tobe = (forward ? elem->next : elem->prev);
        elem->refcount++;
        elem_array[fcnt++] = elem;
        if (delete) do_list_elem_unlink(info, elem, cause);
        if (count > 0 && fcnt >= count) break;
        elem = tobe;
    }

    *elem_count = fcnt;
    if (fcnt > 0) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ELEM_ENOENT;
    }
}

static ENGINE_ERROR_CODE do_list_elem_insert(hash_item *it,
                                             int index, list_elem_item *elem,
                                             const void *cookie)
{
    list_meta_info *info = (list_meta_info *)item_get_meta(it);
    int32_t real_mcnt = (info->mcnt == -1 ? max_list_size : info->mcnt);
    ENGINE_ERROR_CODE ret;

    /* validation check: index value */
    if (index >= 0) {
        if (index > info->ccnt || index > (real_mcnt-1))
            return ENGINE_EINDEXOOR;
    } else {
        if ((-index) > (info->ccnt+1) || (-index) > real_mcnt)
            return ENGINE_EINDEXOOR;
    }

#ifdef ENABLE_STICKY_ITEM
    /* sticky memory limit check */
    if (IS_STICKY_EXPTIME(it->exptime)) {
        if (do_item_sticky_overflowed())
            return ENGINE_ENOMEM;
    }
#endif

    /* overflow check */
    if (info->ovflact == OVFL_ERROR && info->ccnt >= real_mcnt) {
        return ENGINE_EOVERFLOW;
    }

    CLOG_LIST_ELEM_INSERT(info, index, elem);

    if (info->ccnt >= real_mcnt) {
        /* info->ovflact: OVFL_HEAD_TRIM or OVFL_TAIL_TRIM */
        int      delidx;
        uint32_t delcnt;
        if (index == 0 || index == -1) {
            /* delete an element item of opposite side to make room */
            delidx = (index == -1 ? 0 : info->ccnt-1);
            delcnt = do_list_elem_delete(info, delidx, 1, ELEM_DELETE_TRIM);
            assert(delcnt == 1);
        } else {
            /* delete an element item that overflow action indicates */
            delidx = (info->ovflact == OVFL_HEAD_TRIM ? 0 : info->ccnt-1);
            delcnt = do_list_elem_delete(info, delidx, 1, ELEM_DELETE_TRIM);
            assert(delcnt == 1);
            /* adjust list index value */
            if (info->ovflact == OVFL_HEAD_TRIM) {
              if (index > 0) index -= 1;
            } else { /* ovflact == OVFL_TAIL_TRIM */
              if (index < 0) index += 1;
            }
        }
    }

    if (index < 0) {
        /* Change the negative index to a positive index.
         * by adding (current element count + 1).  One more addition
         * is needed since the direction of insertion is reversed.
         */
        index += (info->ccnt+1);
        if (index < 0)
            index = 0;
    }

    ret = do_list_elem_link(info, index, elem);
    assert(ret == ENGINE_SUCCESS);
    return ret;
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

static ENGINE_ERROR_CODE do_set_item_find(const void *key, const uint32_t nkey,
                                          bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_SET_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static hash_item *do_set_item_alloc(const void *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    uint32_t nbytes = 2; /* "\r\n" */
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(set_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_SET;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), "\r\n", nbytes);

        /* initialize set meta information */
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        info->mcnt = do_coll_real_maxcount(it, attrp->maxcount);
        info->ccnt = 0;
        info->ovflact = OVFL_ERROR;
        info->mflags  = 0;
#ifdef ENABLE_STICKY_ITEM
        if (IS_STICKY_EXPTIME(attrp->exptime)) info->mflags |= COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1)              info->mflags |= COLL_META_FLAG_READABLE;
        info->itdist  = (uint16_t)((size_t*)info-(size_t*)it);
        info->stotal  = 0;
        info->root    = NULL;
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);
    }
    return it;
}

static set_hash_node *do_set_node_alloc(uint8_t hash_depth, const void *cookie)
{
    size_t ntotal = sizeof(set_hash_node);

    set_hash_node *node = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (node != NULL) {
        assert(node->slabs_clsid == 0);
        node->slabs_clsid = slabs_clsid(ntotal);
        assert(node->slabs_clsid > 0);
        node->refcount    = 0;
        node->hdepth      = hash_depth;
        node->tot_hash_cnt = 0;
        node->tot_elem_cnt = 0;
        memset(node->hcnt, 0, SET_HASHTAB_SIZE*sizeof(uint16_t));
        memset(node->htab, 0, SET_HASHTAB_SIZE*sizeof(void*));
    }
    return node;
}

static void do_set_node_free(set_hash_node *node)
{
    do_item_mem_free(node, sizeof(set_hash_node));
}

static set_elem_item *do_set_elem_alloc(const uint32_t nbytes, const void *cookie)
{
    size_t ntotal = sizeof(set_elem_item) + nbytes;

    set_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        assert(elem->slabs_clsid == 0);
        elem->slabs_clsid = slabs_clsid(ntotal);
        assert(elem->slabs_clsid > 0);
        elem->refcount    = 0;
        elem->nbytes      = nbytes;
        elem->next = (set_elem_item *)ADDR_MEANS_UNLINKED; /* Unliked state */
    }
    return elem;
}

static void do_set_elem_free(set_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = do_set_elem_ntotal(elem);
    do_item_mem_free(elem, ntotal);
}

static void do_set_elem_release(set_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->next == (set_elem_item *)ADDR_MEANS_UNLINKED) {
        do_set_elem_free(elem);
    }
}

static void do_set_node_link(set_meta_info *info,
                             set_hash_node *par_node, const int par_hidx,
                             set_hash_node *node)
{
    if (par_node == NULL) {
        info->root = node;
    } else {
        set_elem_item *elem;
        int num_elems = par_node->hcnt[par_hidx];
        int num_found =0;

        while (par_node->htab[par_hidx] != NULL) {
            elem = par_node->htab[par_hidx];
            par_node->htab[par_hidx] = elem->next;

            int hidx = SET_GET_HASHIDX(elem->hval, node->hdepth);
            elem->next = node->htab[hidx];
            node->htab[hidx] = elem;
            node->hcnt[hidx] += 1;
            num_found++;
        }
        assert(num_found == num_elems);
        node->tot_elem_cnt = num_found;

        par_node->htab[par_hidx] = node;
        par_node->hcnt[par_hidx] = -1; /* child hash node */
        par_node->tot_elem_cnt -= num_found;
        par_node->tot_hash_cnt += 1;
    }

    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(sizeof(set_hash_node));
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_SET, stotal);
    }
}

static void do_set_node_unlink(set_meta_info *info,
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
        size_t stotal = slabs_space_size(sizeof(set_hash_node));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_SET, stotal);
    }

    /* free the node */
    do_set_node_free(node);
}

static ENGINE_ERROR_CODE do_set_elem_link(set_meta_info *info, set_elem_item *elem,
                                          const void *cookie)
{
    assert(info->root != NULL);
    set_hash_node *node = info->root;
    set_elem_item *find;
    int hidx = -1;

    /* set hash value */
    elem->hval = genhash_string_hash(elem->value, elem->nbytes);

    while (node != NULL) {
        hidx = SET_GET_HASHIDX(elem->hval, node->hdepth);
        if (node->hcnt[hidx] >= 0) /* set element hash chain */
            break;
        node = node->htab[hidx];
    }
    assert(node != NULL);
    assert(hidx != -1);

    for (find = node->htab[hidx]; find != NULL; find = find->next) {
        if (set_hash_eq(elem->hval, elem->value, elem->nbytes,
                        find->hval, find->value, find->nbytes))
            break;
    }
    if (find != NULL) {
        return ENGINE_ELEM_EEXISTS;
    }

    if (node->hcnt[hidx] >= SET_MAX_HASHCHAIN_SIZE) {
        set_hash_node *n_node = do_set_node_alloc(node->hdepth+1, cookie);
        if (n_node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_set_node_link(info, node, hidx, n_node);

        node = n_node;
        hidx = SET_GET_HASHIDX(elem->hval, node->hdepth);
    }

    elem->next = node->htab[hidx];
    node->htab[hidx] = elem;
    node->hcnt[hidx] += 1;
    node->tot_elem_cnt += 1;

    info->ccnt++;

    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(do_set_elem_ntotal(elem));
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_SET, stotal);
    }

    return ENGINE_SUCCESS;
}

static void do_set_elem_unlink(set_meta_info *info,
                               set_hash_node *node, const int hidx,
                               set_elem_item *prev, set_elem_item *elem,
                               enum elem_delete_cause cause)
{
    if (prev != NULL) prev->next = elem->next;
    else              node->htab[hidx] = elem->next;
    elem->next = (set_elem_item *)ADDR_MEANS_UNLINKED;
    node->hcnt[hidx] -= 1;
    node->tot_elem_cnt -= 1;
    info->ccnt--;

    CLOG_SET_ELEM_DELETE(info, elem, cause);

    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(do_set_elem_ntotal(elem));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_SET, stotal);
    }

    if (elem->refcount == 0) {
        do_set_elem_free(elem);
    }
}

static set_elem_item *do_set_elem_find(set_meta_info *info, const char *val, const int vlen)
{
    set_elem_item *elem = NULL;

    if (info->root != NULL) {
        set_hash_node *node = info->root;
        int hval = genhash_string_hash(val, vlen);
        int hidx = 0;

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

static ENGINE_ERROR_CODE do_set_elem_traverse_delete(set_meta_info *info, set_hash_node *node,
                                                     const int hval, const char *val, const int vlen)
{
    ENGINE_ERROR_CODE ret;

    int hidx = SET_GET_HASHIDX(hval, node->hdepth);

    if (node->hcnt[hidx] == -1) {
        set_hash_node *child_node = node->htab[hidx];
        ret = do_set_elem_traverse_delete(info, child_node, hval, val, vlen);
        if (ret == ENGINE_SUCCESS) {
            if (child_node->tot_hash_cnt == 0 &&
                child_node->tot_elem_cnt < (SET_MAX_HASHCHAIN_SIZE/2)) {
                do_set_node_unlink(info, node, hidx);
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
                do_set_elem_unlink(info, node, hidx, prev, elem, ELEM_DELETE_NORMAL);
                ret = ENGINE_SUCCESS;
            }
        }
    }
    return ret;
}

static ENGINE_ERROR_CODE do_set_elem_delete_with_value(set_meta_info *info,
                                                       const char *val, const int vlen,
                                                       enum elem_delete_cause cause)
{
    assert(cause == ELEM_DELETE_NORMAL);
    ENGINE_ERROR_CODE ret;
    if (info->root != NULL) {
        int hval = genhash_string_hash(val, vlen);
        ret = do_set_elem_traverse_delete(info, info->root, hval, val, vlen);
        if (ret == ENGINE_SUCCESS) {
            if (info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
                do_set_node_unlink(info, NULL, 0);
            }
        }
    } else {
        ret = ENGINE_ELEM_ENOENT;
    }
    return ret;
}

#ifdef SET_DELETE_NO_MERGE
static uint32_t do_set_elem_traverse_fast(set_meta_info *info,
                                          set_hash_node *node, const uint32_t count)
{
    int hidx;
    int fcnt = 0;

    /* node has child node */
    if (node->tot_hash_cnt > 0) {
        for (hidx = 0; hidx < SET_HASHTAB_SIZE; hidx++) {
            if (node->hcnt[hidx] == -1) {
                set_hash_node *childnode = (set_hash_node *)node->htab[hidx];
                fcnt += do_set_elem_traverse_fast(info, childnode,
                                                  (count == 0 ? 0 : (count - fcnt)));

                if (childnode->tot_hash_cnt == 0 && childnode->tot_elem_cnt == 0) {
                    node->htab[hidx] = NULL;
                    node->hcnt[hidx] = 0;
                    do_set_node_free(childnode);
                    node->tot_hash_cnt -= 1;
                }
                if (count > 0 && fcnt >= count) {
                    return fcnt;
                }
            }
        }
    }
    for (hidx = 0; hidx < SET_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] > 0) {
            set_elem_item *elem;
            while ((elem = node->htab[hidx]) != NULL) {
                node->htab[hidx] = elem->next;
                elem->next = (set_elem_item *)ADDR_MEANS_UNLINKED;
                if (elem->refcount == 0)
                    do_set_elem_free(elem);
            }
            fcnt += node->hcnt[hidx];
            node->tot_elem_cnt -= node->hcnt[hidx];
            node->hcnt[hidx] = 0;
        }
    }
    return fcnt;
}
#endif

static int do_set_elem_traverse_dfs(set_meta_info *info, set_hash_node *node,
                                    const uint32_t count, const bool delete,
                                    set_elem_item **elem_array)
{
    int hidx;
    int fcnt = 0; /* found count */

    if (node->tot_hash_cnt > 0) {
        set_hash_node *child_node;
        int rcnt; /* request count */
        for (hidx = 0; hidx < SET_HASHTAB_SIZE; hidx++) {
            if (node->hcnt[hidx] == -1) {
                child_node = (set_hash_node *)node->htab[hidx];
                rcnt = (count > 0 ? (count - fcnt) : 0);
                fcnt += do_set_elem_traverse_dfs(info, child_node, rcnt, delete,
                                            (elem_array==NULL ? NULL : &elem_array[fcnt]));
                if (delete) {
                    if  (child_node->tot_hash_cnt == 0 &&
                         child_node->tot_elem_cnt < (SET_MAX_HASHCHAIN_SIZE/2)) {
                         do_set_node_unlink(info, node, hidx);
                     }
                }
                if (count > 0 && fcnt >= count)
                    return fcnt;
            }
        }
    }
    assert(count == 0 || fcnt < count);

    for (hidx = 0; hidx < SET_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] > 0) {
            set_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                if (elem_array) {
                    elem->refcount++;
                    elem_array[fcnt] = elem;
                }
                fcnt++;
                if (delete) do_set_elem_unlink(info, node, hidx, NULL, elem,
                                               (elem_array==NULL ? ELEM_DELETE_COLL
                                                                 : ELEM_DELETE_NORMAL));
                if (count > 0 && fcnt >= count) break;
                elem = (delete ? node->htab[hidx] : elem->next);
            }
            if (count > 0 && fcnt >= count) break;
        }
    }
    return fcnt;
}

#ifdef SET_DELETE_NO_MERGE
static uint32_t do_set_elem_delete_fast(set_meta_info *info, const uint32_t count)
{
    uint32_t fcnt = 0;
    if (info->root != NULL) {
        fcnt = do_set_elem_traverse_fast(info, info->root, count);
        if (info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
            do_set_node_free(info->root);
            info->root = NULL;
            info->ccnt = 0;
            if (info->stotal > 0) { /* apply memory space */
                do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_SET, info->stotal);
            }
        }
    }
    return fcnt;
}
#endif

static uint32_t do_set_elem_delete(set_meta_info *info, const uint32_t count,
                                   enum elem_delete_cause cause)
{
    assert(cause == ELEM_DELETE_COLL);
    uint32_t fcnt = 0;
    if (info->root != NULL) {
        fcnt = do_set_elem_traverse_dfs(info, info->root, count, true, NULL);
        if (info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
            do_set_node_unlink(info, NULL, 0);
        }
    }
    return fcnt;
}

static ENGINE_ERROR_CODE do_set_elem_get(set_meta_info *info, const uint32_t count, const bool delete,
                                         set_elem_item **elem_array, uint32_t *elem_count)
{
    uint32_t fcnt = 0;
    if (info->root != NULL) {
        fcnt = do_set_elem_traverse_dfs(info, info->root, count, delete, elem_array);
        if (delete && info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
            do_set_node_unlink(info, NULL, 0);
        }
    }

    *elem_count = fcnt;
    if (fcnt > 0) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ELEM_ENOENT;
    }
}

static ENGINE_ERROR_CODE do_set_elem_insert(hash_item *it, set_elem_item *elem,
                                            const void *cookie)
{
    set_meta_info *info = (set_meta_info *)item_get_meta(it);
    int32_t real_mcnt = (info->mcnt == -1 ? max_set_size : info->mcnt);
    ENGINE_ERROR_CODE ret;

#ifdef ENABLE_STICKY_ITEM
    /* sticky memory limit check */
    if (IS_STICKY_EXPTIME(it->exptime)) {
        if (do_item_sticky_overflowed())
            return ENGINE_ENOMEM;
    }
#endif

    /* overflow check */
    assert(info->ovflact == OVFL_ERROR);
    if (info->ccnt >= real_mcnt) {
        return ENGINE_EOVERFLOW;
    }

    /* create the root hash node if it does not exist */
    bool new_root_flag = false;
    if (info->root == NULL) { /* empty set */
        set_hash_node *r_node = do_set_node_alloc(0, cookie);
        if (r_node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_set_node_link(info, NULL, 0, r_node);
        new_root_flag = true;
    }

    /* insert the element */
    ret = do_set_elem_link(info, elem, cookie);
    if (ret != ENGINE_SUCCESS) {
        if (new_root_flag) {
            do_set_node_unlink(info, NULL, 0);
        }
        return ret;
    }

    CLOG_SET_ELEM_INSERT(info, elem);
    return ENGINE_SUCCESS;
}

/*
 * B+TREE collection management
 */
static ENGINE_ERROR_CODE do_btree_item_find(const void *key, const uint32_t nkey,
                                            bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_BTREE_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static hash_item *do_btree_item_alloc(const void *key, const uint32_t nkey,
                                      item_attr *attrp, const void *cookie)
{
    uint32_t nbytes = 2; /* "\r\n" */
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(btree_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_BTREE;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), "\r\n", nbytes);

        /* initialize b+tree meta information */
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        info->mcnt = do_coll_real_maxcount(it, attrp->maxcount);
        info->ccnt = 0;
        info->ovflact = (attrp->ovflaction==0 ? OVFL_SMALLEST_TRIM : attrp->ovflaction);
        if (forced_btree_ovflact != 0 && forced_action_pfxlen < nkey &&
            memcmp(forced_action_prefix, key, forced_action_pfxlen) == 0) {
            info->ovflact = forced_btree_ovflact;
        }
        info->mflags  = 0;
#ifdef ENABLE_STICKY_ITEM
        if (IS_STICKY_EXPTIME(attrp->exptime)) info->mflags |= COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1)              info->mflags |= COLL_META_FLAG_READABLE;
        info->itdist  = (uint16_t)((size_t*)info-(size_t*)it);
        info->stotal  = 0;
        info->bktype  = BKEY_TYPE_UNKNOWN;
        info->maxbkeyrange.len = BKEY_NULL;
        info->root    = NULL;
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);
    }
    return it;
}

static btree_indx_node *do_btree_node_alloc(const uint8_t node_depth, const void *cookie)
{
    size_t ntotal = (node_depth > 0 ? sizeof(btree_indx_node) : sizeof(btree_leaf_node));

    btree_indx_node *node = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (node != NULL) {
        assert(node->slabs_clsid == 0);
        node->slabs_clsid = slabs_clsid(ntotal);
        assert(node->slabs_clsid > 0);
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

static void do_btree_node_free(btree_indx_node *node)
{
    size_t ntotal = (node->ndepth > 0 ? sizeof(btree_indx_node) : sizeof(btree_leaf_node));
    do_item_mem_free(node, ntotal);
}

static btree_elem_item *do_btree_elem_alloc(const uint32_t nbkey, const uint32_t neflag,
                                            const uint32_t nbytes, const void *cookie)
{
    size_t ntotal = sizeof(btree_elem_item_fixed) + BTREE_REAL_NBKEY(nbkey) + neflag + nbytes;

    btree_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        assert(elem->slabs_clsid == 0);
        elem->slabs_clsid = slabs_clsid(ntotal);
        assert(elem->slabs_clsid > 0);
        elem->refcount    = 0;
        elem->status      = BTREE_ITEM_STATUS_UNLINK; /* unlinked state */
        elem->nbkey       = (uint8_t)nbkey;
        elem->neflag      = (uint8_t)neflag;
        elem->nbytes      = (uint16_t)nbytes;
    }
    return elem;
}

static void do_btree_elem_free(btree_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = do_btree_elem_ntotal(elem);
    do_item_mem_free(elem, ntotal);
}

static void do_btree_elem_release(btree_elem_item *elem)
{
    /* assert(elem->status != BTREE_ITEM_STATUS_FREE); */
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->status == BTREE_ITEM_STATUS_UNLINK) {
        elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(elem);
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

static bool (*UINT64_COMPARE_OP[COMPARE_OP_MAX]) (const uint64_t *v1, const uint64_t *v2)
    = { UINT64_ISEQ, UINT64_ISNE, UINT64_ISLT, UINT64_ISLE, UINT64_ISGT, UINT64_ISGE };

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

#if 0 // OLD_CODE
static inline void UINT64_INCR(uint64_t *v)
{
    assert(*v < UINT64_MAX);
    *v += 1;
}
#endif

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
    int i, subtraction;

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

    /* assume that the value of v1 >= the value of v2 */
    subtraction = 0;
    for (i = (length-1); i >= 0; i--) {
        if (subtraction == 0) {
            if (v1[i] >= v2[i]) {
                result[i] = v1[i] - v2[i];
            } else {
                result[i] = 0xFF - v2[i] + v1[i] + 1;
                subtraction = 1;
            }
        } else {
            if (v1[i] > v2[i]) {
                result[i] = v1[i] - v2[i] - 1;
                subtraction = 0;
            } else {
                result[i] = 0xFF - v2[i] + v1[i];
            }
        }
    }
}

#if 0 // OLD_CODE
static inline void BINARY_INCR(unsigned char *v, const int length)
{
    assert(length > 0);
    int i;
    for (i = (length-1); i >= 0; i--) {
        if (v[i] < 0xFF) {
            v[i] += 1;
            break;
        }
        v[i] = 0x00;
    }
    assert(i >= 0);
}
#endif

static inline void BINARY_DECR(unsigned char *v, const int length)
{
    assert(length > 0);
    int i;
    for (i = (length-1); i >= 0; i--) {
        if (v[i] > 0x00) {
            v[i] -= 1;
            break;
        }
        v[i] = 0xFF;
    }
    assert(i >= 0);
}

#define BKEY_COPY(bk, nbk, res) \
        ((nbk)==0 ? UINT64_COPY((const uint64_t*)(bk), (uint64_t*)(res)) \
                  : BINARY_COPY((bk), (nbk), (res)))
#define BKEY_DIFF(bk1, nbk1, bk2, nbk2, len, res) \
        ((len)==0 ? UINT64_DIFF((const uint64_t*)(bk1), (const uint64_t*)(bk2), (uint64_t*)(res)) \
                  : BINARY_DIFF((bk1), (nbk1), (bk2), (nbk2), (len), (res)))
#if 0 // OLD_CODE
#define BKEY_INCR(bk, nbk) \
        ((nbk)==0 ? UINT64_INCR((uint64_t*)(bk)) : BINARY_INCR((bk), (nbk)))
#endif
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
                                           const unsigned char *bkey, const uint32_t nbkey,
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
            elem = do_btree_get_first_elem(node->item[mid]); /* separator */
            comp = BKEY_COMP(bkey, nbkey, elem->data, elem->nbkey);
            if (comp == 0) { /* the same bkey is found */
                *found_elem = elem;
                if (path) {
                    path[node->ndepth].node = node;
                    path[node->ndepth].indx = mid;
                }
                node = do_btree_get_first_leaf(node->item[mid], path);
                assert(node->ndepth == 0);
                break;
            }
            if (comp <  0) right = mid-1;
            else           left  = mid+1;
        }
        if (left <= right) { /* found the element */
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
                                               const unsigned char *ins_bkey, const uint32_t ins_nbkey,
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

    if (bkrange == NULL) {
        assert(bkrtype != BKEY_RANGE_TYPE_SIN);
        if (bkrtype == BKEY_RANGE_TYPE_ASC) {
            path[0].node = do_btree_get_first_leaf(root, (path_flag ? path : NULL));
            path[0].indx = 0;
        } else {
            path[0].node = do_btree_get_last_leaf(root, (path_flag ? path : NULL));
            path[0].indx = path[0].node->used_count - 1;
        }
        path[0].bkeq = false;

        elem = BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
        assert(elem != NULL);
        return elem;
    }

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

static btree_elem_item *do_btree_find_next(btree_elem_posi *posi,
                                           const bkey_range *bkrange)
{
    btree_elem_item *elem;

    do_btree_incr_posi(posi);
    if (posi->node == NULL) {
        posi->bkeq = false;
        return NULL;
    }

    elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    if (bkrange != NULL) {
        int comp = BKEY_COMP(elem->data, elem->nbkey,
                             bkrange->to_bkey, bkrange->to_nbkey);
        if (comp == 0) {
            posi->bkeq = true;
        } else {
            posi->bkeq = false;
            if (comp > 0) elem = NULL;
        }
    } else {
        posi->bkeq = false;
    }
    return elem;
}

static btree_elem_item *do_btree_find_prev(btree_elem_posi *posi,
                                           const bkey_range *bkrange)
{
    btree_elem_item *elem;

    do_btree_decr_posi(posi);
    if (posi->node == NULL) {
        posi->bkeq = false;
        return NULL;
    }

    elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    if (bkrange != NULL) {
        int comp = BKEY_COMP(elem->data, elem->nbkey,
                             bkrange->to_bkey, bkrange->to_nbkey);
        if (comp == 0) {
            posi->bkeq = true;
        } else {
            posi->bkeq = false;
            if (comp < 0) elem = NULL;
        }
    } else {
        posi->bkeq = false;
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

static void do_btree_node_link(btree_meta_info *info, btree_indx_node *node,
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
        if (node->ndepth > 0) stotal = slabs_space_size(sizeof(btree_indx_node));
        else                  stotal = slabs_space_size(sizeof(btree_leaf_node));
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
    }
}

static ENGINE_ERROR_CODE do_btree_node_split(btree_meta_info *info, btree_elem_posi *path,
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

        n_node[btree_depth] = do_btree_node_alloc(btree_depth, cookie);
        if (n_node[btree_depth] == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        btree_depth += 1;
        assert(btree_depth < BTREE_MAX_DEPTH);
        if (btree_depth > info->root->ndepth) {
            btree_indx_node *r_node = do_btree_node_alloc(btree_depth, cookie);
            if (r_node == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            do_btree_node_link(info, r_node, NULL);
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
            do_btree_node_link(info, n_node[i], &p_posi);

            if (direction == BTREE_DIRECTION_PREV) {
                /* adjust upper path */
                path[i+1].indx += 1;
                //do_btree_incr_path(path, i+1);
            }
            do_btree_node_sbalance(s_node, path, i);
        }
    } else {
        for (i = 0; i < btree_depth; i++) {
            do_btree_node_free(n_node[i]);
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

static void do_btree_node_unlink(btree_meta_info *info, btree_indx_node *node,
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
        if (node->ndepth > 0) stotal = slabs_space_size(sizeof(btree_indx_node));
        else                  stotal = slabs_space_size(sizeof(btree_leaf_node));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
    }

    /* The amount of space to be decreased become different according to node depth.
     * So, the btree node must be freed after collection space is decreased.
     */
    do_btree_node_free(node);
}

static void do_btree_node_detach(btree_indx_node *node)
{
    /* unlink the given node from b+tree */
    if (node->prev != NULL) node->prev->next = node->next;
    if (node->next != NULL) node->next->prev = node->prev;
    node->prev = node->next = NULL;

    do_btree_node_free(node);
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

static void do_btree_node_merge(btree_meta_info *info, btree_elem_posi *path,
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
                    do_btree_node_unlink(info, node, NULL);
                } else {
                    btree_indx_node *new_root;
                    while (node->used_count == 1 && node->ndepth > 0) {
                        new_root = BTREE_GET_NODE_ITEM(node, 0);
                        do_btree_node_unlink(info, node, NULL);
                        info->root = new_root;
                        node = new_root;
                    }
                }
            } else {
                if (node->used_count == 0) {
                    do_btree_node_unlink(info, node, &path[btree_depth+1]);
                    par_node_count = 1;
                }
                else if (node->used_count < (BTREE_ITEM_COUNT/2)) {
                    if ((node->prev != NULL && node->prev->used_count < (BTREE_ITEM_COUNT/2)) ||
                        (node->next != NULL && node->next->used_count < (BTREE_ITEM_COUNT/2))) {
                        do_btree_node_mbalance(node, path, btree_depth);
                        do_btree_node_unlink(info, node, &path[btree_depth+1]);
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
                    do_btree_node_detach(node);
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
                        do_btree_node_detach(node);
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
                if (btree_depth > 0) stotal = tot_unlink_cnt * slabs_space_size(sizeof(btree_indx_node));
                else                 stotal = tot_unlink_cnt * slabs_space_size(sizeof(btree_leaf_node));
                do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
            }
        }
        btree_depth += 1;
        cur_node_count = par_node_count;
    }
    if (btree_position_debug) {
        do_btree_consistency_check(info->root, info->ccnt, true);
    }
}

static void do_btree_elem_unlink(btree_meta_info *info, btree_elem_posi *path,
                                 enum elem_delete_cause cause)
{
    btree_elem_posi *posi = &path[0];
    btree_elem_item *elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    int i;

    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(do_btree_elem_ntotal(elem));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
    }

    CLOG_BTREE_ELEM_DELETE(info, elem, cause);

    if (elem->refcount > 0) {
        elem->status = BTREE_ITEM_STATUS_UNLINK;
    } else  {
        elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(elem);
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
        do_btree_node_merge(info, path, true, 1);
    }
}

static void do_btree_elem_replace(btree_meta_info *info,
                                  btree_elem_posi *posi, btree_elem_item *new_elem)
{
    btree_elem_item *old_elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    size_t old_stotal;
    size_t new_stotal;

    old_stotal = slabs_space_size(do_btree_elem_ntotal(old_elem));
    new_stotal = slabs_space_size(do_btree_elem_ntotal(new_elem));

    CLOG_BTREE_ELEM_INSERT(info, old_elem, new_elem);

    if (old_elem->refcount > 0) {
        old_elem->status = BTREE_ITEM_STATUS_UNLINK;
    } else  {
        old_elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(old_elem);
    }

    new_elem->status = BTREE_ITEM_STATUS_USED;
    posi->node->item[posi->indx] = new_elem;

    if (new_stotal != old_stotal) { /* apply memory space */
        assert(info->stotal > 0);
        if (new_stotal > old_stotal)
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_BTREE, (new_stotal-old_stotal));
        else
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, (old_stotal-new_stotal));
    }
}

static ENGINE_ERROR_CODE do_btree_elem_update(btree_meta_info *info,
                                              const int bkrtype, const bkey_range *bkrange,
                                              const eflag_update *eupdate,
                                              const char *value, const uint32_t nbytes,
                                              const void *cookie)
{
    btree_elem_posi  posi;
    btree_elem_item *elem;
    unsigned char *ptr;
    uint32_t real_nbkey;
    uint32_t new_neflag;
    uint32_t new_nbytes;

    if (info->root == NULL) {
        return ENGINE_ELEM_ENOENT;
    }

    elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
    if (elem == NULL) {
        return ENGINE_ELEM_ENOENT;
    }

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
        CLOG_BTREE_ELEM_INSERT(info, elem, elem);
    } else {
        /* old body size != new body size */
#ifdef ENABLE_STICKY_ITEM
         /* sticky memory limit check */
         if (IS_STICKY_COLLFLG(info)) {
             if ((elem->neflag + elem->nbytes) < (new_neflag + new_nbytes)) {
                 if (do_item_sticky_overflowed())
                     return ENGINE_ENOMEM;
             }
         }
#endif

        btree_elem_item *new_elem = do_btree_elem_alloc(elem->nbkey, new_neflag, new_nbytes, cookie);
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

        do_btree_elem_replace(info, &posi, new_elem);
    }

    return ENGINE_SUCCESS;
}

#ifdef BTREE_DELETE_NO_MERGE
static int do_btree_elem_delete_fast(btree_meta_info *info,
                                     btree_elem_posi *path, const uint32_t count)
{
    btree_indx_node *node;
    btree_elem_item *elem;
    int i, delcnt=0;
    int cur_depth;

    if (info->root == NULL) {
        return 0;
    }
    assert(info->root->ndepth < BTREE_MAX_DEPTH);

    if (path[0].node == NULL) {
        path[0].node = do_btree_get_first_leaf(info->root, path);
        cur_depth = 0;
    } else {
        cur_depth = path[0].indx; /* it's used to keep btree depth on delete */
    }

    node = path[cur_depth].node;
    while (node != NULL) {
        /* delete element items or lower nodes */
        if (node->ndepth == 0) { /* leaf node */
            for (i = 0; i < node->used_count; i++) {
                elem = (btree_elem_item *)node->item[i];
                if (elem->refcount > 0) {
                    elem->status = BTREE_ITEM_STATUS_UNLINK;
                } else {
                    elem->status = BTREE_ITEM_STATUS_FREE;
                    do_btree_elem_free(elem);
                }
            }
        } else {
            for (i = 0; i < node->used_count; i++) {
                do_btree_node_free(node->item[i]);
            }
        }
        delcnt += node->used_count;

        /* get the next node */
        node = node->next;
        if (node == NULL && cur_depth < info->root->ndepth) {
            cur_depth += 1;
            node = path[cur_depth].node;
        }

        /* check if current deletion should be stopped */
        if (count > 0 && delcnt >= count) {
            path[cur_depth].node = node;
            path[0].indx = cur_depth;
            break;
        }
    }
    if (node == NULL) {
        info->root = NULL;
        info->ccnt = 0;
        if (info->stotal > 0) {
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, info->stotal);
        }
    }
    return delcnt;
}
#endif

static uint32_t do_btree_elem_delete(btree_meta_info *info,
                                     const int bkrtype, const bkey_range *bkrange,
                                     const eflag_filter *efilter, const uint32_t count,
                                     uint32_t *access_count, enum elem_delete_cause cause)
{
    btree_elem_posi  path[BTREE_MAX_DEPTH];
    btree_elem_item *elem;
    uint32_t tot_found = 0; /* found count */
    uint32_t tot_access = 0; /* access count */

    if (info->root == NULL) {
        if (access_count)
            *access_count = 0;
        return 0;
    }

    assert(info->root->ndepth < BTREE_MAX_DEPTH);
    elem = do_btree_find_first(info->root, bkrtype, bkrange, path, true);
    if (elem != NULL) {
        if (bkrtype == BKEY_RANGE_TYPE_SIN) {
            assert(path[0].bkeq == true);
            tot_access++;
            if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                /* cause == ELEM_DELETE_NORMAL */
                do_btree_elem_unlink(info, path, cause);
                tot_found = 1;
            }
        } else {
            btree_elem_posi upth[BTREE_MAX_DEPTH]; /* upper node path */
            btree_elem_posi c_posi = path[0];
            btree_elem_posi s_posi = c_posi;
            size_t stotal = 0;
            int cur_found = 0;
            int node_cnt = 1;
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
                tot_access++;
                if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                    stotal += slabs_space_size(do_btree_elem_ntotal(elem));

                    CLOG_BTREE_ELEM_DELETE(info, elem, cause);
                    if (elem->refcount > 0) {
                        elem->status = BTREE_ITEM_STATUS_UNLINK;
                    } else {
                        elem->status = BTREE_ITEM_STATUS_FREE;
                        do_btree_elem_free(elem);
                    }
                    c_posi.node->item[c_posi.indx] = NULL;

                    cur_found++;
                    if (count > 0 && (tot_found+cur_found) >= count) break;
                }

                if (c_posi.bkeq == true) {
                    elem = NULL; break;
                }
                elem = (forward ? do_btree_find_next(&c_posi, bkrange)
                                : do_btree_find_prev(&c_posi, bkrange));
                if (elem == NULL) break;

                if (s_posi.node != c_posi.node) {
                    if (cur_found > 0) {
                        do_btree_node_remove_null_items(&s_posi, forward, cur_found);
                        /* decrement element count in upper nodes */
                        for (i = 1; i <= info->root->ndepth; i++) {
                            assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                            upth[i].node->ecnt[upth[i].indx] -= cur_found;
                        }
                        tot_found += cur_found; cur_found = 0;
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

            if (cur_found > 0) {
                do_btree_node_remove_null_items(&s_posi, forward, cur_found);
                /* decrement element count in upper nodes */
                for (i = 1; i <= info->root->ndepth; i++) {
                    assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                    upth[i].node->ecnt[upth[i].indx] -= cur_found;
                }
                tot_found += cur_found;
            }
            if (tot_found > 0) {
                info->ccnt -= tot_found;
                if (info->stotal > 0) { /* apply memory space */
                    /* The btree has already been unlinked from hash table.
                     * If then, the btree doesn't have prefix info and has stotal of 0.
                     * So, do not need to descrese space total info.
                     */
                    assert(stotal > 0 && stotal <= info->stotal);
                    do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
                }
                do_btree_node_merge(info, path, forward, node_cnt);
            }
        }
    }
    if (access_count)
        *access_count = tot_access;
    return tot_found;
}

#if 0 /* Might be used later */
static inline void get_bkey_full_range(const int bktype, const bool ascend, bkey_range *bkrange)
{
    if (bktype == BKEY_TYPE_BINARY) {
        if (ascend) {
            memcpy(bkrange->from_bkey, bkey_binary_min, MIN_BKEY_LENG);
            memcpy(bkrange->to_bkey,   bkey_binary_max, MAX_BKEY_LENG);
            bkrange->from_nbkey = MIN_BKEY_LENG;
            bkrange->to_nbkey   = MAX_BKEY_LENG;
        } else {
            memcpy(bkrange->from_bkey, bkey_binary_max, MAX_BKEY_LENG);
            memcpy(bkrange->to_bkey,   bkey_binary_min, MIN_BKEY_LENG);
            bkrange->from_nbkey = MAX_BKEY_LENG;
            bkrange->to_nbkey   = MIN_BKEY_LENG;
        }
    } else { /* bktype == BKEY_TYPE_UINT64 or BKEY_TYPE_UNKNOWN */
        if (ascend) {
            memcpy(bkrange->from_bkey, (void*)&bkey_uint64_min, sizeof(uint64_t));
            memcpy(bkrange->to_bkey,   (void*)&bkey_uint64_max, sizeof(uint64_t));
        } else {
            memcpy(bkrange->from_bkey, (void*)&bkey_uint64_max, sizeof(uint64_t));
            memcpy(bkrange->to_bkey,   (void*)&bkey_uint64_min, sizeof(uint64_t));
        }
        bkrange->from_nbkey = bkrange->to_nbkey = 0;
    }
}
#endif

static ENGINE_ERROR_CODE do_btree_overflow_check(btree_meta_info *info, btree_elem_item *elem,
                                                 int *overflow_type)
{
    /* info->ccnt >= 1 */
    btree_elem_item *min_bkey_elem = NULL;
    btree_elem_item *max_bkey_elem = NULL;
    int32_t real_mcnt = (info->mcnt == -1 ? max_btree_size : info->mcnt);

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
    if (info->ccnt >= real_mcnt && *overflow_type == OVFL_TYPE_NONE) {
        if (info->ovflact == OVFL_ERROR) {
            return ENGINE_EOVERFLOW;
        }
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_SMALLEST_SILENT_TRIM) {
            if (min_bkey_elem == NULL)
                min_bkey_elem = do_btree_get_first_elem(info->root);
            if (BKEY_ISLT(elem->data, elem->nbkey, min_bkey_elem->data, min_bkey_elem->nbkey)) {
                if (info->ovflact == OVFL_SMALLEST_TRIM) {
                    /* It means the implicit trim. */
                    info->mflags |= COLL_META_FLAG_TRIMMED; // set trimmed
                }
                return ENGINE_EBKEYOOR;
            }
        } else { /* OVFL_LARGEST_TRIM || OVFL_LARGEST_SILENT_TRIM */
            if (max_bkey_elem == NULL)
                max_bkey_elem = do_btree_get_last_elem(info->root);
            if (BKEY_ISGT(elem->data, elem->nbkey, max_bkey_elem->data, max_bkey_elem->nbkey)) {
                if (info->ovflact == OVFL_LARGEST_TRIM) {
                    /* It means the implicit trim. */
                    info->mflags |= COLL_META_FLAG_TRIMMED; // set trimmed
                }
                return ENGINE_EBKEYOOR;
            }
        }
        *overflow_type = OVFL_TYPE_COUNT;
    }

    return ENGINE_SUCCESS;
}

static void do_btree_overflow_trim(btree_meta_info *info,
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
            /* bkey range that must be trimmed.
             * => min bkey ~ (new max bkey - maxbkeyrange - 1)
             */
            /* from bkey */
            edge_elem = do_btree_get_first_elem(info->root); /* min bkey elem */
            bkrange_space.from_nbkey = edge_elem->nbkey;
            BKEY_COPY(edge_elem->data, edge_elem->nbkey, bkrange_space.from_bkey);
            /* to bkey */
            bkrange_space.to_nbkey = info->maxbkeyrange.len;
            BKEY_DIFF(elem->data, elem->nbkey,
                      info->maxbkeyrange.val, info->maxbkeyrange.len,
                      bkrange_space.to_nbkey, bkrange_space.to_bkey);
            BKEY_DECR(bkrange_space.to_bkey, bkrange_space.to_nbkey);
        } else {
            /* bkey range that must be trimmed.
             * => (new min bkey + maxbkeyrange + 1) ~ max bkey
             * => max bkey - (max bkey - new min bkey - maxbkeyrange - 1) ~ max bkey
             */
            /* from bkey */
            edge_elem = do_btree_get_last_elem(info->root);  /* max bkey elem */
            bkrange_space.from_nbkey = info->maxbkeyrange.len;
            BKEY_DIFF(edge_elem->data, edge_elem->nbkey,
                      elem->data, elem->nbkey,
                      bkrange_space.from_nbkey, bkrange_space.from_bkey);
            BKEY_DIFF(bkrange_space.from_bkey, bkrange_space.from_nbkey,
                      info->maxbkeyrange.val, info->maxbkeyrange.len,
                      bkrange_space.from_nbkey, bkrange_space.from_bkey);
            BKEY_DECR(bkrange_space.from_bkey, bkrange_space.from_nbkey);
            BKEY_DIFF(edge_elem->data, edge_elem->nbkey,
                      bkrange_space.from_bkey, bkrange_space.from_nbkey,
                      bkrange_space.from_nbkey, bkrange_space.from_bkey);
            /* to bkey */
            bkrange_space.to_nbkey = edge_elem->nbkey;
            BKEY_COPY(edge_elem->data, edge_elem->nbkey, bkrange_space.to_bkey);
        }
        bkrtype = do_btree_bkey_range_type(&bkrange_space);
        del_count = do_btree_elem_delete(info, bkrtype, &bkrange_space, NULL, 0,
                                         NULL, ELEM_DELETE_TRIM);
        assert(del_count > 0);
        assert(info->ccnt > 0);
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_LARGEST_TRIM)
            info->mflags &= ~COLL_META_FLAG_TRIMMED; // clear trimmed
    } else { /* overflow_type == OVFL_TYPE_COUNT */
        assert(overflow_type == OVFL_TYPE_COUNT);

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
        do_btree_elem_unlink(info, delpath, ELEM_DELETE_TRIM);
        if (info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_LARGEST_TRIM)
            info->mflags |= COLL_META_FLAG_TRIMMED; // set trimmed
    }
}

static ENGINE_ERROR_CODE do_btree_elem_link(btree_meta_info *info, btree_elem_item *elem,
                                            const bool replace_if_exist, bool *replaced,
                                            btree_elem_item **trimmed_elems, uint32_t *trimmed_count,
                                            const void *cookie)
{
    btree_elem_posi path[BTREE_MAX_DEPTH];
    int ovfl_type = OVFL_TYPE_NONE;
    ENGINE_ERROR_CODE res;

    if (replaced) *replaced = false;

    assert(info->root->ndepth < BTREE_MAX_DEPTH);
    res = do_btree_find_insposi(info->root, elem->data, elem->nbkey, path);
    if (res == ENGINE_SUCCESS) {
#ifdef ENABLE_STICKY_ITEM
        /* sticky memory limit check */
        if (IS_STICKY_COLLFLG(info)) {
            if (do_item_sticky_overflowed())
                return ENGINE_ENOMEM;
        }
#endif

        /* If the leaf node is full of elements, split it ahead. */
        if (path[0].node->used_count >= BTREE_ITEM_COUNT) {
            res = do_btree_node_split(info, path, cookie);
            if (res != ENGINE_SUCCESS) {
                return res;
            }
        }

        if (info->ccnt > 0) {
            /* overflow check */
            res = do_btree_overflow_check(info, elem, &ovfl_type);
            if (res != ENGINE_SUCCESS) {
                return res;
            }
        } else { /* info->ccnt == 0 */
            /* set bkey type */
            if (elem->nbkey == 0)
                info->bktype = BKEY_TYPE_UINT64;
            else
                info->bktype = BKEY_TYPE_BINARY;
        }

        CLOG_BTREE_ELEM_INSERT(info, NULL, elem);

        /* insert the element into the leaf page */
        elem->status = BTREE_ITEM_STATUS_USED;
        if (path[0].indx < path[0].node->used_count) {
            for (int i = (path[0].node->used_count-1); i >= path[0].indx; i--) {
                path[0].node->item[i+1] = path[0].node->item[i];
            }
        }
        path[0].node->item[path[0].indx] = elem;
        path[0].node->used_count++;
        /* increment element count in upper nodes */
        for (int i = 1; i <= info->root->ndepth; i++) {
            path[i].node->ecnt[path[i].indx]++;
        }
        info->ccnt++;

        if (1) { /* apply memory space */
            size_t stotal = slabs_space_size(do_btree_elem_ntotal(elem));
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
        }

        if (ovfl_type != OVFL_TYPE_NONE) {
            do_btree_overflow_trim(info, elem, ovfl_type, trimmed_elems, trimmed_count);
        }
    }
    else if (res == ENGINE_ELEM_EEXISTS) {
        if (replace_if_exist) {
#ifdef ENABLE_STICKY_ITEM
            /* sticky memory limit check */
            if (IS_STICKY_COLLFLG(info)) {
                btree_elem_item *find = BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
                if ((find->neflag + find->nbytes) < (elem->neflag + elem->nbytes)) {
                    if (do_item_sticky_overflowed())
                        return ENGINE_ENOMEM;
                }
            }
#endif

            do_btree_elem_replace(info, &path[0], elem);
            if (replaced) *replaced = true;
            res = ENGINE_SUCCESS;
        }
    }
    return res;
}

static bool do_btree_overlapped_with_trimmed_space(btree_meta_info *info,
                                                   btree_elem_posi *posi, const int bkrtype)
{
    assert((info->mflags & COLL_META_FLAG_TRIMMED) != 0);
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

static ENGINE_ERROR_CODE do_btree_elem_get(btree_meta_info *info,
                                           const int bkrtype, const bkey_range *bkrange, const eflag_filter *efilter,
                                           const uint32_t offset, const uint32_t count, const bool delete,
                                           btree_elem_item **elem_array, uint32_t *elem_count,
                                           uint32_t *access_count, bool *potentialbkeytrim)
{
    btree_elem_posi  path[BTREE_MAX_DEPTH];
    btree_elem_item *elem;
    uint32_t tot_found = 0; /* total found count */
    uint32_t tot_access = 0; /* total access count */

    *potentialbkeytrim = false;

    if (info->root == NULL) {
        if (access_count)
            *access_count = 0;
        return ENGINE_ELEM_ENOENT;
    }

    assert(info->root->ndepth < BTREE_MAX_DEPTH);
    elem = do_btree_find_first(info->root, bkrtype, bkrange, path, delete);
    if (elem != NULL) {
        if (bkrtype == BKEY_RANGE_TYPE_SIN) { /* single bkey */
            assert(path[0].bkeq == true);
            tot_access++;
            if (offset == 0) {
                if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                    elem->refcount++;
                    elem_array[tot_found++] = elem;
                    if (delete) {
                        do_btree_elem_unlink(info, path, ELEM_DELETE_NORMAL);
                    }
                }
            }
        } else {
            btree_elem_posi upth[BTREE_MAX_DEPTH]; /* upper node path */
            btree_elem_posi c_posi = path[0];
            btree_elem_posi s_posi = c_posi;
            size_t stotal = 0;
            int cur_found = 0;
            int skip_cnt = 0;
            int node_cnt = 1;
            bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);
            int i;

            /* check if start position might be trimmed */
            if (c_posi.bkeq == false && (info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
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
                tot_access++;
                if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                    if (skip_cnt < offset) {
                        skip_cnt++;
                    } else {
                        elem->refcount++;
                        elem_array[tot_found+cur_found] = elem;
                        if (delete) {
                            stotal += slabs_space_size(do_btree_elem_ntotal(elem));
                            elem->status = BTREE_ITEM_STATUS_UNLINK;
                            c_posi.node->item[c_posi.indx] = NULL;
                            CLOG_BTREE_ELEM_DELETE(info, elem, ELEM_DELETE_NORMAL);
                        }
                        cur_found++;
                        if (count > 0 && (tot_found+cur_found) >= count) break;
                    }
                }

                if (c_posi.bkeq == true) {
                    elem = NULL; break;
                }
                elem = (forward ? do_btree_find_next(&c_posi, bkrange)
                                : do_btree_find_prev(&c_posi, bkrange));
                if (elem == NULL) break;

                if (s_posi.node != c_posi.node) {
                    if (cur_found > 0) {
                        if (delete) {
                            do_btree_node_remove_null_items(&s_posi, forward, cur_found);
                            /* decrement element count in upper nodes */
                            for (i = 1; i <= info->root->ndepth; i++) {
                                assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                                upth[i].node->ecnt[upth[i].indx] -= cur_found;
                            }
                        }
                        tot_found += cur_found; cur_found = 0;
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
                if (c_posi.node == NULL && (info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
                    if (do_btree_overlapped_with_trimmed_space(info, &c_posi, bkrtype)) {
                        *potentialbkeytrim = true;
                    }
                }
            }

            if (cur_found > 0) {
                if (delete) {
                    do_btree_node_remove_null_items(&s_posi, forward, cur_found);
                    /* decrement element count in upper nodes */
                    for (i = 1; i <= info->root->ndepth; i++) {
                        assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                        upth[i].node->ecnt[upth[i].indx] -= cur_found;
                    }
                }
                tot_found += cur_found;
            }
            if (tot_found > 0 && delete) { /* apply memory space */
                info->ccnt -= tot_found;
                assert(stotal > 0 && stotal <= info->stotal);
                do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_BTREE, stotal);
                do_btree_node_merge(info, path, forward, node_cnt);
            }
        }
    } else {
        if ((info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
            if (do_btree_overlapped_with_trimmed_space(info, &path[0], bkrtype)) {
                *potentialbkeytrim = true;
            }
        }
    }
    if (access_count)
        *access_count = tot_access;

    *elem_count = tot_found;
    if (tot_found > 0) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ELEM_ENOENT;
    }
}

static uint32_t do_btree_elem_count(btree_meta_info *info,
                                    const int bkrtype, const bkey_range *bkrange,
                                    const eflag_filter *efilter, uint32_t *access_count)
{
    btree_elem_posi  posi;
    btree_elem_item *elem;
    uint32_t tot_found = 0; /* total found count */
    uint32_t tot_access = 0; /* total access count */

    if (info->root == NULL) {
        if (access_count)
            *access_count = 0;
        return 0;
    }

#ifdef BOP_COUNT_OPTIMIZE
    if (bkrtype != BKEY_RANGE_TYPE_SIN && efilter == NULL) {
        btree_elem_item *min_bkey_elem = do_btree_get_first_elem(info->root);
        btree_elem_item *max_bkey_elem = do_btree_get_last_elem(info->root);
        int min_comp, max_comp;
        if (bkrtype == BKEY_RANGE_TYPE_ASC) {
            min_comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey,
                                 min_bkey_elem->data, min_bkey_elem->nbkey);
            max_comp = BKEY_COMP(bkrange->to_bkey, bkrange->to_nbkey,
                                 max_bkey_elem->data, max_bkey_elem->nbkey);
        } else { /* BKEY_RANGE_TYPE_DSC */
            min_comp = BKEY_COMP(bkrange->to_bkey, bkrange->to_nbkey,
                                 min_bkey_elem->data, min_bkey_elem->nbkey);
            max_comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey,
                                 max_bkey_elem->data, max_bkey_elem->nbkey);
        }
        if (min_comp <= 0 && max_comp >= 0) {
            return info->ccnt;
        }
    }
#endif

    elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
    if (elem != NULL) {
        if (bkrtype == BKEY_RANGE_TYPE_SIN) {
            assert(posi.bkeq == true);
            tot_access++;
            if (efilter == NULL || do_btree_elem_filter(elem, efilter))
                tot_found++;
        } else { /* BKEY_RANGE_TYPE_ASC || BKEY_RANGE_TYPE_DSC */
            bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);
            posi.bkeq = false;
            do {
                tot_access++;
                if (efilter == NULL || do_btree_elem_filter(elem, efilter))
                    tot_found++;

                if (posi.bkeq == true) {
                    elem = NULL; break;
                }
                elem = (forward ? do_btree_find_next(&posi, bkrange)
                                : do_btree_find_prev(&posi, bkrange));
            } while (elem != NULL);
        }
    }
    if (access_count)
        *access_count = tot_access;
    return tot_found;
}

static ENGINE_ERROR_CODE do_btree_elem_insert(hash_item *it, btree_elem_item *elem,
                                              const bool replace_if_exist, bool *replaced,
                                              btree_elem_item **trimmed_elems,
                                              uint32_t *trimmed_count, const void *cookie)
{
    btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
    ENGINE_ERROR_CODE ret;

    /* validation check: bkey type */
    if (info->ccnt > 0 || info->maxbkeyrange.len != BKEY_NULL) {
        if ((info->bktype == BKEY_TYPE_UINT64 && elem->nbkey >  0) ||
            (info->bktype == BKEY_TYPE_BINARY && elem->nbkey == 0)) {
            return ENGINE_EBADBKEY;
        }
    }

    /* Both sticky memory limit check and overflow check
     * are to be performed in the below do_btree_elem_link().
     */

    /* create the root node if it does not exist */
    bool new_root_flag = false;
    if (info->root == NULL) {
        btree_indx_node *r_node = do_btree_node_alloc(0, cookie);
        if (r_node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_btree_node_link(info, r_node, NULL);
        new_root_flag = true;
    }

    /* insert the element */
    ret = do_btree_elem_link(info, elem, replace_if_exist, replaced,
                             trimmed_elems, trimmed_count, cookie);
    if (ret != ENGINE_SUCCESS) {
        if (new_root_flag) {
            do_btree_node_unlink(info, info->root, NULL);
        }
        return ret;
    }

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_btree_elem_arithmetic(btree_meta_info *info,
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
    uint32_t real_nbkey;

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

        elem = do_btree_elem_alloc(bkrange->from_nbkey,
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

        ret = do_btree_elem_link(info, elem, false, NULL, NULL, NULL, cookie);
        if (ret != ENGINE_SUCCESS) {
            assert(ret != ENGINE_ELEM_EEXISTS);
            /* ENGINE_ENOMEM || ENGINE_BKEYOOR || ENGINE_OVERFLOW */
            do_btree_elem_free(elem);
        }
        *result = initial;
    } else {
        real_nbkey = BTREE_REAL_NBKEY(elem->nbkey);
        if (! safe_strtoull((const char*)elem->data + real_nbkey + elem->neflag, &value) || elem->nbytes == 2) {
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
            CLOG_BTREE_ELEM_INSERT(info, elem, elem);
        } else {
#ifdef ENABLE_STICKY_ITEM
            /* Do not check sticky memory limit.
             * Because, the space difference is negligible.
             */
#endif
            btree_elem_item *new_elem = do_btree_elem_alloc(elem->nbkey, elem->neflag, nlen, cookie);
            if (new_elem == NULL) {
                return ENGINE_ENOMEM;
            }
            memcpy(new_elem->data, elem->data, real_nbkey + elem->neflag);
            memcpy(new_elem->data + real_nbkey + new_elem->neflag, nbuf, nlen);

            do_btree_elem_replace(info, &posi, new_elem);
        }
        ret = ENGINE_SUCCESS;
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

static int do_btree_elem_batch_get(btree_elem_posi posi, const int count,
                                   const bool forward, const bool reverse,
                                   btree_elem_item **elem_array)
{
    btree_elem_item *elem;
    int nfound = 0;
    while (nfound < count) {
        if (forward) do_btree_incr_posi(&posi);
        else         do_btree_decr_posi(&posi);
        if (posi.node == NULL) break;

        elem = BTREE_GET_ELEM_ITEM(posi.node, posi.indx);
        elem->refcount++;
        if (reverse) elem_array[count-nfound-1] = elem;
        else         elem_array[nfound] = elem;
        nfound += 1;
    }
    return nfound;
}

static int do_btree_posi_find_with_get(btree_meta_info *info,
                                       const int bkrtype, const bkey_range *bkrange,
                                       ENGINE_BTREE_ORDER order, const int count,
                                       btree_elem_item **elem_array,
                                       uint32_t *elem_count, uint32_t *elem_index)
{
    btree_elem_posi  path[BTREE_MAX_DEPTH];
    btree_elem_item *elem;
    int bpos = -1; /* NOT found */

    if (info->root == NULL) return -1; /* not found */

    elem = do_btree_find_first(info->root, bkrtype, bkrange, path, true);
    if (elem != NULL) {
        int ecnt, eidx;
        assert(path[0].bkeq == true);
        bpos = do_btree_posi_from_path(info, path, order);
        assert(bpos >= 0);

        ecnt = 1;                             /* elem count */
        eidx = (bpos < count) ? bpos : count; /* elem index in elem array */
        elem->refcount++;
        elem_array[eidx] = elem;

        if (order == BTREE_ORDER_ASC) {
            ecnt += do_btree_elem_batch_get(path[0], eidx,  false, true,  &elem_array[0]);
            assert((ecnt-1) == eidx);
            ecnt += do_btree_elem_batch_get(path[0], count, true,  false, &elem_array[eidx+1]);
        } else {
            ecnt += do_btree_elem_batch_get(path[0], eidx,  true,  true,  &elem_array[0]);
            assert((ecnt-1) == eidx);
            ecnt += do_btree_elem_batch_get(path[0], count, false, false, &elem_array[eidx+1]);
        }
        *elem_count = (uint32_t)ecnt;
        *elem_index = (uint32_t)eidx;
    }
    return bpos; /* btree_position */
}

static ENGINE_ERROR_CODE do_btree_elem_get_by_posi(btree_meta_info *info,
                                                   const int index, const uint32_t count, const bool forward,
                                                   btree_elem_item **elem_array, uint32_t *elem_count)
{
    btree_elem_posi  posi;
    btree_indx_node *node;
    btree_elem_item *elem;
    int i, tot_ecnt;
    uint32_t nfound; /* found count */

    if (info->root == NULL) return ENGINE_ELEM_ENOENT;

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
    posi.bkeq = false;

    elem = BTREE_GET_ELEM_ITEM(posi.node, posi.indx);
    elem->refcount++;
    elem_array[0] = elem;
    nfound = 1;
    nfound += do_btree_elem_batch_get(posi, count-1, forward, false, &elem_array[nfound]);

    *elem_count = nfound;
    if (*elem_count > 0) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ELEM_ENOENT;
    }
}

#ifdef SUPPORT_BOP_SMGET
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

/*** NOT USED CODE ***
static inline int do_comp_key_string(const char *key1, const int len1,
                                     const char *key2, const int len2)
{
    int res;
    if (len1 == len2) {
        retrun strncmp(key1, key2, len1);
    }
    if (len1 < len2) {
        res = strncmp(key1, key2, len1);
        if (res == 0) res = -1;
    } else {
        res = strncmp(key1, key2, len2);
        if (res == 0) res =  1;
    }
    return res;
}
**********************/

static btree_elem_item *do_btree_scan_next(btree_elem_posi *posi,
                             const int bkrtype, const bkey_range *bkrange)
{
    if (posi->bkeq == true)
        return NULL;

    if (bkrtype != BKEY_RANGE_TYPE_DSC) // ascending
        return do_btree_find_next(posi, bkrange);
    else // descending
        return do_btree_find_prev(posi, bkrange);
}

static void do_btree_smget_add_miss(smget_result_t *smres,
                                    uint16_t kidx, uint16_t erid)
{
    /* miss_kinfo: forward array */
    smres->miss_kinfo[smres->miss_count].kidx = kidx;
    smres->miss_kinfo[smres->miss_count].code = erid; /* error id */
    smres->miss_count++;
}

static void do_btree_smget_add_trim(smget_result_t *smres,
                                    uint16_t kidx, btree_elem_item *elem)
{
    /* trim_elems & trim_kinfo: backward array */
    assert(smres->trim_count < smres->keys_arrsz);
    smres->trim_elems[smres->keys_arrsz-1-smres->trim_count] = elem;
    smres->trim_kinfo[smres->keys_arrsz-1-smres->trim_count].kidx = kidx;
    //smres->trim_kinfo[smres->keys_arrsz-1-smres->trim_count].code = 0;
    smres->trim_count++;
}

#if 0 // JHPARK_SMGET_OFFSET_HANDLING
static bool do_btree_smget_check_trim(smget_result_t *smres)
{
    btree_elem_item *head_elem = smres->elem_array[0];
    btree_elem_item *trim_elem;
    bool valid = true;

    /* Check if all the trimmed elements(actually the last element before trim)
     * are behind the first found element of smget.
     */
    if (smres->ascending) {
        for (int i = 0; i < smres->trim_count; i++) {
            trim_elem = smres->trim_elems[smres->keys_arrsz-1-i];
            if (BKEY_COMP(trim_elem->data, trim_elem->nbkey,
                          head_elem->data, head_elem->nbkey) < 0) {
                valid = false; break;
            }
        }
    } else {
        for (int i = 0; i < smres->trim_count; i++) {
            trim_elem = smres->trim_elems[smres->keys_arrsz-1-i];
            if (BKEY_COMP(trim_elem->data, trim_elem->nbkey,
                          head_elem->data, head_elem->nbkey) > 0) {
                valid = false; break;
            }
        }
    }
    return valid;
}
#endif

static void do_btree_smget_adjust_trim(smget_result_t *smres)
{
    eitem       **new_trim_elems = &smres->elem_array[smres->elem_count];
    smget_emis_t *new_trim_kinfo = &smres->miss_kinfo[smres->miss_count];
    uint32_t      new_trim_count = 0;
    btree_elem_item *tail_elem = NULL;
    btree_elem_item *comp_elem;
    btree_elem_item *trim_elem;
    uint16_t         trim_kidx;
    int idx, res, pos, i;
    int left, right, mid;

    if (smres->elem_count == smres->elem_arrsz) {
        /* We found the elements as many as the requested count. In this case,
         * we might trim the trimmed keys if the bkey-before-trim is behind
         * the bkey of the last found element.
         */
        tail_elem = smres->elem_array[smres->elem_count-1];
    }

    for (idx = smres->trim_count-1; idx >= 0; idx--)
    {
        trim_elem = smres->trim_elems[smres->keys_arrsz-1-idx];
        trim_kidx = smres->trim_kinfo[smres->keys_arrsz-1-idx].kidx;
        /* check if the trim elem is valid */
        if (tail_elem != NULL) {
            res = BKEY_COMP(trim_elem->data, trim_elem->nbkey,
                            tail_elem->data, tail_elem->nbkey);
            if ((smres->ascending == true && res >= 0) ||
                (smres->ascending != true && res <= 0)) {
                continue; /* invalid trim */
            }
        }
        /* add the valid trim info in sorted arry */
        if (new_trim_count == 0) {
            pos = 0;
        } else {
            left  = 0;
            right = new_trim_count-1;
            while (left <= right) {
                mid = (left + right) / 2;
                comp_elem = new_trim_elems[mid];
                res = BKEY_COMP(trim_elem->data, trim_elem->nbkey,
                                comp_elem->data, comp_elem->nbkey);
                if (res == 0) {
                    right = mid; left = mid+1;
                    break;
                }
                if (smres->ascending) {
                    if (res < 0) right = mid-1;
                    else         left  = mid+1;
                } else {
                    if (res > 0) right = mid-1;
                    else         left  = mid+1;
                }
            }
            /* left: insertion position */
            for (i = new_trim_count-1; i >= left; i--) {
                new_trim_elems[i+1] = new_trim_elems[i];
                new_trim_kinfo[i+1] = new_trim_kinfo[i];
            }
            pos = left;
        }
        trim_elem->refcount++;
        new_trim_elems[pos] = trim_elem;
        new_trim_kinfo[pos].kidx = trim_kidx;
        new_trim_count++;
    }
    smres->trim_elems = new_trim_elems;
    smres->trim_kinfo = new_trim_kinfo;
    smres->trim_count = new_trim_count;
}

#ifdef JHPARK_OLD_SMGET_INTERFACE
static ENGINE_ERROR_CODE
do_btree_smget_scan_sort_old(token_t *key_array, const int key_count,
                             const int bkrtype, const bkey_range *bkrange,
                             const eflag_filter *efilter, const uint32_t req_count,
                             btree_scan_info *btree_scan_buf,
                             uint16_t *sort_sindx_buf, uint32_t *sort_sindx_cnt,
                             uint32_t *missed_key_array, uint32_t *missed_key_count)
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
    bool is_first;

    *missed_key_count = 0;

    for (k = 0; k < key_count; k++) {
        ret = do_btree_item_find(key_array[k].value, key_array[k].length, DO_UPDATE, &it);
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
            do_item_release(it); continue;
        }
        if (info->ccnt == 0) { /* empty collection */
            do_item_release(it); continue;
        }
        if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
            (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
            do_item_release(it);
            ret = ENGINE_EBADBKEY; break;
        }
        assert(info->root != NULL);

        elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
        if (elem == NULL) { /* No elements within the bkey range */
            if ((info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
                /* Some elements weren't cached because of overflow trim */
                if (do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                    do_item_release(it);
                    ret = ENGINE_EBKEYOOR; break;
                }
            }
            do_item_release(it);
            continue;
        }

        if (posi.bkeq == false && (info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
            /* Some elements weren't cached because of overflow trim */
            if (do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                do_item_release(it);
                ret = ENGINE_EBKEYOOR; break;
            }
        }

        /* initialize for the next scan */
        is_first = true;
        posi.bkeq = false;

scan_next:
        if (is_first != true) {
            assert(elem != NULL);
            elem = do_btree_scan_next(&posi, bkrtype, bkrange);
            if (elem == NULL) {
                if (posi.node == NULL && (info->mflags & COLL_META_FLAG_TRIMMED) != 0) {
                    if (do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                        do_item_release(it);
                        ret = ENGINE_EBKEYOOR; break;
                    }
                }
                do_item_release(it);
                continue;
            }
        }
        is_first = false;

        if (efilter != NULL && !do_btree_elem_filter(elem, efilter)) {
            goto scan_next;
        }

        /* found the item */
        btree_scan_buf[curr_idx].it   = it;
        btree_scan_buf[curr_idx].posi = posi;
        btree_scan_buf[curr_idx].kidx = k;

        /* add the current scan into the scan sort buffer */
        if (sort_count == 0) {
            sort_sindx_buf[sort_count++] = curr_idx;
            curr_idx += 1;
            continue;
        }

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
                    do_item_release(btree_scan_buf[curr_idx].it);
                    btree_scan_buf[curr_idx].it = NULL;
                    ret = ENGINE_EBADVALUE; break;
                }
            }
            if ((ascending ==  true && cmp_res > 0) ||
                (ascending == false && cmp_res < 0)) {
                /* do not need to proceed the current scan */
                do_item_release(btree_scan_buf[curr_idx].it);
                btree_scan_buf[curr_idx].it = NULL;
                continue;
            }
        }

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
                    ret = ENGINE_EBADVALUE; break;
                }
            }
            if (ascending) {
                if (cmp_res < 0) right = mid-1;
                else             left  = mid+1;
            } else {
                if (cmp_res > 0) right = mid-1;
                else             left  = mid+1;
            }
        }
        if (ret == ENGINE_EBADVALUE) {
            do_item_release(btree_scan_buf[curr_idx].it);
            btree_scan_buf[curr_idx].it = NULL;
            break;
        }

        if (sort_count >= req_count) {
            /* free the last scan */
            comp_idx = sort_sindx_buf[sort_count-1];
            do_item_release(btree_scan_buf[comp_idx].it);
            btree_scan_buf[comp_idx].it = NULL;
            free_idx = comp_idx;
            sort_count--;
        }
        for (i = sort_count-1; i >= left; i--) {
            sort_sindx_buf[i+1] = sort_sindx_buf[i];
        }
        sort_sindx_buf[left] = curr_idx;
        sort_count++;

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
            do_item_release(btree_scan_buf[curr_idx].it);
            btree_scan_buf[curr_idx].it = NULL;
        }
    }
    return ret;
}
#endif

static ENGINE_ERROR_CODE
do_btree_smget_scan_sort(token_t *key_array, const int key_count,
                         const int bkrtype, const bkey_range *bkrange,
                         const eflag_filter *efilter,
                         const uint32_t req_count, const bool unique,
                         btree_scan_info *btree_scan_buf,
                         uint16_t *sort_sindx_buf, uint32_t *sort_sindx_cnt,
                         smget_result_t *smres)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    hash_item *it;
    btree_meta_info *info;
    btree_elem_item *elem, *comp;
    btree_elem_posi posi;
    int comp_idx;
    int curr_idx = -1; /* curr scan index */
    int free_idx = 0;  /* free scan list */
    int sort_count = 0; /* sorted scan count */
    int k, i, kidx, cmp_res;
    int mid, left, right;
    bool ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);
    bool is_first;

    for (k = 0; k < key_count; k++) {
        kidx = k;
        ret = do_btree_item_find(key_array[k].value, key_array[k].length, DO_UPDATE, &it);
        if (ret != ENGINE_SUCCESS) {
            if (ret == ENGINE_KEY_ENOENT) { /* key missed */
                do_btree_smget_add_miss(smres, kidx, ENGINE_KEY_ENOENT);
                ret = ENGINE_SUCCESS; continue;
            }
            break; /* ret == ENGINE_EBADTYPE */
        }

        info = (btree_meta_info *)item_get_meta(it);
        if ((info->mflags & COLL_META_FLAG_READABLE) == 0) { /* unreadable collection */
            do_btree_smget_add_miss(smres, kidx, ENGINE_UNREADABLE);
            do_item_release(it); continue;
        }
        if (info->ccnt == 0) { /* empty collection */
            do_item_release(it); continue;
        }
        if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
            (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
            do_item_release(it);
            ret = ENGINE_EBADBKEY; break;
        }
        assert(info->root != NULL);

        elem = do_btree_find_first(info->root, bkrtype, bkrange, &posi, false);
        if (elem == NULL) { /* No elements within the bkey range */
            if ((info->mflags & COLL_META_FLAG_TRIMMED) != 0 &&
                do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                /* Some elements weren't cached because of overflow trim */
                do_btree_smget_add_miss(smres, kidx, ENGINE_EBKEYOOR);
            }
            do_item_release(it); continue;
        }

        if (posi.bkeq == false && (info->mflags & COLL_META_FLAG_TRIMMED) != 0 &&
            do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
            /* Some elements weren't cached because of overflow trim */
            do_btree_smget_add_miss(smres, kidx, ENGINE_EBKEYOOR);
            do_item_release(it); continue;
        }

        /* initialize for the next scan */
        is_first = true;
        posi.bkeq = false;

scan_next:
        if (is_first != true) {
            assert(elem != NULL);
            btree_elem_item *prev = elem;
            elem = do_btree_scan_next(&posi, bkrtype, bkrange);
            if (elem == NULL) {
                if (posi.node == NULL && (info->mflags & COLL_META_FLAG_TRIMMED) != 0 &&
                    do_btree_overlapped_with_trimmed_space(info, &posi, bkrtype)) {
                    /* Some elements weren't cached because of overflow trim */
                    do_btree_smget_add_trim(smres, kidx, prev);
                }
                do_item_release(it); continue;
            }
        }
        is_first = false;

        if (efilter != NULL && !do_btree_elem_filter(elem, efilter)) {
            goto scan_next;
        }

        /* found the item */
        if (curr_idx == -1) {
            /* allocate free scan */
            assert(free_idx != -1);
            curr_idx = free_idx;
            free_idx = btree_scan_buf[free_idx].next;
        }
        btree_scan_buf[curr_idx].it   = it;
        btree_scan_buf[curr_idx].posi = posi;
        btree_scan_buf[curr_idx].kidx = kidx;

        /* add the current scan into the scan sort buffer */
        if (sort_count == 0) {
            sort_sindx_buf[sort_count++] = curr_idx;
            curr_idx = -1;
            continue;
        }

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
                    do_item_release(btree_scan_buf[curr_idx].it);
                    btree_scan_buf[curr_idx].it = NULL;
                    ret = ENGINE_EBADVALUE; break;
                }
            }
            if ((ascending ==  true && cmp_res > 0) ||
                (ascending == false && cmp_res < 0)) {
                /* do not need to proceed the current scan */
                do_item_release(btree_scan_buf[curr_idx].it);
                btree_scan_buf[curr_idx].it = NULL;
                continue;
            }
        }

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
                    ret = ENGINE_EBADVALUE; break;
                }
                if (unique) break;
            }
            if (ascending) {
                if (cmp_res < 0) right = mid-1;
                else             left  = mid+1;
            } else {
                if (cmp_res > 0) right = mid-1;
                else             left  = mid+1;
            }
        }
        if (ret == ENGINE_EBADVALUE) {
            do_item_release(btree_scan_buf[curr_idx].it);
            btree_scan_buf[curr_idx].it = NULL;
            break;
        }
        if (left <= right) {
            assert(unique == true);
            if ((ascending ==  true && cmp_res < 0) ||
                (ascending == false && cmp_res > 0)) {
                assert(sort_sindx_buf[mid] == comp_idx);
                sort_sindx_buf[mid] = curr_idx;
                it = btree_scan_buf[comp_idx].it;
                posi = btree_scan_buf[comp_idx].posi;
                kidx = btree_scan_buf[comp_idx].kidx;
                info = (btree_meta_info *)item_get_meta(it);
                btree_scan_buf[comp_idx].it = NULL;
                curr_idx = comp_idx;
            }
            goto scan_next;
        }

        if (sort_count >= req_count) {
            /* free the last scan */
            comp_idx = sort_sindx_buf[sort_count-1];
            do_item_release(btree_scan_buf[comp_idx].it);
            btree_scan_buf[comp_idx].it = NULL;
            sort_count--;
            btree_scan_buf[comp_idx].next = free_idx;
            free_idx = comp_idx;
        }
        for (i = sort_count-1; i >= left; i--) {
            sort_sindx_buf[i+1] = sort_sindx_buf[i];
        }
        sort_sindx_buf[left] = curr_idx;
        sort_count++;
        curr_idx = -1;
    }

    if (ret == ENGINE_SUCCESS) {
        *sort_sindx_cnt = sort_count;
    } else {
        for (i = 0; i < sort_count; i++) {
            curr_idx = sort_sindx_buf[i];
            do_item_release(btree_scan_buf[curr_idx].it);
            btree_scan_buf[curr_idx].it = NULL;
        }
    }
    return ret;
}
#endif

#ifdef SUPPORT_BOP_SMGET
#ifdef JHPARK_OLD_SMGET_INTERFACE
static ENGINE_ERROR_CODE do_btree_smget_elem_sort_old(btree_scan_info *btree_scan_buf,
                                   uint16_t *sort_sindx_buf, const int sort_sindx_cnt,
                                   const int bkrtype, const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count, btree_elem_item **elem_array,
                                   uint32_t *kfnd_array, uint32_t *flag_array, uint32_t *elem_count,
                                   bool *potentialbkeytrim, bool *bkey_duplicated)
{
    btree_meta_info *info;
    btree_elem_item *elem, *comp;
    btree_elem_item *prev = NULL;
    uint16_t first_idx = 0;
    uint16_t curr_idx;
    uint16_t comp_idx;
    int i, cmp_res;
    int mid, left, right;
    int skip_count = 0;
    int sort_count = sort_sindx_cnt;
    bool ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);
    bool key_trim_found = false;
    bool dup_bkey_found;
    *elem_count = 0;

    while (sort_count > 0) {
        curr_idx = sort_sindx_buf[first_idx];
        elem = BTREE_GET_ELEM_ITEM(btree_scan_buf[curr_idx].posi.node,
                                   btree_scan_buf[curr_idx].posi.indx);
        dup_bkey_found = false;
        if (prev != NULL) { /* check duplicate bkeys */
            if (BKEY_COMP(prev->data, prev->nbkey, elem->data, elem->nbkey) == 0) {
                dup_bkey_found = true;
            }
        }
        prev = elem;

        if (key_trim_found && !dup_bkey_found) {
            break; /* stop smget */
        }

        if (skip_count < offset) {
            skip_count++;
        } else { /* skip_count == offset */
            if (*elem_count > 0 && dup_bkey_found) {
                *bkey_duplicated = true;
            }
            elem->refcount++;
            elem_array[*elem_count] = elem;
            kfnd_array[*elem_count] = btree_scan_buf[curr_idx].kidx;
            flag_array[*elem_count] = btree_scan_buf[curr_idx].it->flags;
            *elem_count += 1;
            if (*elem_count >= count) break;
        }

scan_next:
        elem = do_btree_scan_next(&btree_scan_buf[curr_idx].posi, bkrtype, bkrange);
        if (elem == NULL) {
            if (btree_scan_buf[curr_idx].posi.node == NULL) {
                /* reached to the end of b+tree scan */
                info = (btree_meta_info *)item_get_meta(btree_scan_buf[curr_idx].it);
                if ((info->mflags & COLL_META_FLAG_TRIMMED) != 0 &&
                    do_btree_overlapped_with_trimmed_space(info, &btree_scan_buf[curr_idx].posi, bkrtype)) {
                    key_trim_found = true;
                }
            }
            first_idx++; sort_count--;
            continue;
        }

        if (efilter != NULL && !do_btree_elem_filter(elem, efilter)) {
            goto scan_next;
        }

        if (sort_count == 1) {
            continue; /* sorting is not needed */
        }

        left  = first_idx + 1;
        right = first_idx + sort_count - 1;
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
        /* right : insertion position */
        for (i = first_idx+1; i <= right; i++) {
            sort_sindx_buf[i-1] = sort_sindx_buf[i];
        }
        sort_sindx_buf[right] = curr_idx;
    }

    if (key_trim_found && *elem_count < count) {
        *potentialbkeytrim = true;
    }
    return ENGINE_SUCCESS;
}
#endif

static ENGINE_ERROR_CODE
do_btree_smget_elem_sort(btree_scan_info *btree_scan_buf,
                         uint16_t *sort_sindx_buf, const int sort_sindx_cnt,
                         const int bkrtype, const bkey_range *bkrange,
                         const eflag_filter *efilter,
                         const uint32_t offset, const uint32_t count,
                         const bool unique,
                         smget_result_t *smres)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    btree_meta_info *info;
    btree_elem_item *elem, *comp;
    btree_elem_item *last;
    btree_elem_item *prev = NULL;
    uint16_t first_idx = 0;
    uint16_t curr_idx;
    uint16_t comp_idx;
    int i, cmp_res;
    int mid, left, right;
    int skip_count = 0;
    int sort_count = sort_sindx_cnt;
    bool ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);
    bool dup_bkey_found;

    while (sort_count > 0) {
        curr_idx = sort_sindx_buf[first_idx];
        elem = BTREE_GET_ELEM_ITEM(btree_scan_buf[curr_idx].posi.node,
                                   btree_scan_buf[curr_idx].posi.indx);
        dup_bkey_found = false;
        if (prev != NULL) { /* check duplicate bkeys */
            if (BKEY_COMP(prev->data, prev->nbkey, elem->data, elem->nbkey) == 0) {
                dup_bkey_found = true;
            }
        }
        prev = elem;

        if (unique && dup_bkey_found) {
            /* give up current duplicate bkey */
            goto scan_next;
        }

        if (skip_count < offset) {
            skip_count++;
        } else { /* skip_count == offset */
            if (smres->elem_count > 0 && dup_bkey_found) {
                smres->duplicated = true;
            }
            smres->elem_array[smres->elem_count] = elem;
            smres->elem_kinfo[smres->elem_count].kidx = btree_scan_buf[curr_idx].kidx;
            smres->elem_kinfo[smres->elem_count].flag = btree_scan_buf[curr_idx].it->flags;
            smres->elem_count += 1;
#if 0 // JHPARK_SMGET_OFFSET_HANDLING
            if (smres->elem_count == 1) { /* the first element is found */
                if (offset > 0 && smres->trim_count > 0 &&
                    do_btree_smget_check_trim(smres) != true) {
                    /* Some elements are trimmed in 0 ~ offset range.
                     * So, we cannot make the correct smget result.
                     */
                    ret = ENGINE_EBKEYOOR; break;
                }
            }
#endif
            elem->refcount++;
            if (smres->elem_count >= count) break;
        }

scan_next:
        last = elem;
        elem = do_btree_scan_next(&btree_scan_buf[curr_idx].posi, bkrtype, bkrange);
        if (elem == NULL) {
            if (btree_scan_buf[curr_idx].posi.node == NULL) {
                /* reached to the end of b+tree scan */
                info = (btree_meta_info *)item_get_meta(btree_scan_buf[curr_idx].it);
                if ((info->mflags & COLL_META_FLAG_TRIMMED) != 0 &&
                    do_btree_overlapped_with_trimmed_space(info, &btree_scan_buf[curr_idx].posi, bkrtype)) {
#if 0 // JHPARK_SMGET_OFFSET_HANDLING
                    if (skip_count < offset) {
                        /* Some elements are trimmed in 0 ~ offset range.
                         * So, we cannot make correct smget result.
                         */
                        assert(smres->elem_count == 0);
                        ret = ENGINE_EBKEYOOR; break;
                    }
#endif
                    do_btree_smget_add_trim(smres, btree_scan_buf[curr_idx].kidx, last);
                }
            }
            first_idx++; sort_count--;
            continue;
        }

        if (efilter != NULL && !do_btree_elem_filter(elem, efilter)) {
            goto scan_next;
        }

        if (sort_count == 1) {
            continue; /* sorting is not needed */
        }

        left  = first_idx + 1;
        right = first_idx + sort_count - 1;
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
            }
            if (ascending) {
                if (cmp_res < 0) right = mid-1;
                else             left  = mid+1;
            } else {
                if (cmp_res > 0) right = mid-1;
                else             left  = mid+1;
            }
        }
        if (left <= right) { /* Duplicate bkey is found */
            goto scan_next;
        }

        /* right : insertion position */
        for (i = first_idx+1; i <= right; i++) {
            sort_sindx_buf[i-1] = sort_sindx_buf[i];
        }
        sort_sindx_buf[right] = curr_idx;
    }
    if (ret == ENGINE_SUCCESS) {
        if (smres->trim_count > 0) {
            do_btree_smget_adjust_trim(smres);
        }
    }
    return ret;
}
#endif

/*
 * Item Management Daemon
 */
static inline void do_coll_elem_delete(coll_meta_info *info, int type, uint32_t count)
{
    switch (type) {
      case ITEM_TYPE_BTREE:
           (void)do_btree_elem_delete((void *)info, BKEY_RANGE_TYPE_ASC, NULL,
                                      NULL, count, NULL, ELEM_DELETE_COLL);
           break;
      case ITEM_TYPE_SET:
           (void)do_set_elem_delete((void *)info, count, ELEM_DELETE_COLL);
           break;
      case ITEM_TYPE_MAP:
           (void)do_map_elem_delete((void *)info, count, ELEM_DELETE_COLL);
           break;
      case ITEM_TYPE_LIST:
           (void)do_list_elem_delete((void *)info, 0, count, ELEM_DELETE_COLL);
           break;
      default: break;
    }
}

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
    coll_meta_info *info;
    struct timespec sleep_time = {0, 0};
    ENGINE_ITEM_TYPE item_type;
    int             current_ssl;
    uint32_t        evict_count;
    uint32_t        bg_evict_count = 0;
    bool            bg_evict_start = false;

    coll_del_thread_running = true;

    while (engine->initialized) {
        it = pop_coll_del_queue();
        if (it != NULL) {
            item_type = GET_ITEM_TYPE(it);

            LOCK_CACHE();
            info = (coll_meta_info *)item_get_meta(it);
            while (info->ccnt > 0) {
                do_coll_elem_delete(info, item_type, 100);
                if (info->ccnt > 0) {
                    UNLOCK_CACHE();
                    if (slabs_space_shortage_level() <= 2) {
                        sleep_time.tv_nsec = 10000; /* 10 us */
                        nanosleep(&sleep_time, NULL);
                    }
                    LOCK_CACHE();
                }
            }
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
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "item store - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
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
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
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
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "item arithmetic - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
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
                }
                do_item_release(it);
            } else {
                ret = ENGINE_ENOMEM;
            }
        } else {
            ret = ENGINE_KEY_ENOENT;
        }
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
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
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "item delete - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
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
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
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
        prefix_t *pt = assoc_prefix_find(prefix, nprefix);
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
                if (nprefix < 0 || assoc_prefix_issame(iter->pfxptr, prefix, nprefix)) {
                    next = iter->next;
                    do_item_unlink(iter, ITEM_UNLINK_INVALID);
                    iter = next;
                } else {
                    iter = iter->next;
                }
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
                if (nprefix < 0 || assoc_prefix_issame(iter->pfxptr, prefix, nprefix)) {
                    next = iter->next;
                    do_item_unlink(iter, ITEM_UNLINK_INVALID);
                    iter = next;
                } else {
                    iter = iter->next;
                }
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
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "item flush - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_item_flush_expired(prefix, nprefix, when, cookie);
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
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
    len = sprintf(val, "%"PRIu64, (uint64_t)assoc_prefix_count());
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

static void _check_forced_btree_overflow_action(void)
{
    char *envstr;
    char *envval;

    envstr = getenv("ARCUS_FORCED_BTREE_OVERFLOW_ACTION");
    if (envstr != NULL) {
        char *delimiter = memchr(envstr, ':', strlen(envstr));
        if (delimiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ARCUS_FORCED_BTREE_OVERFLOW_ACTION: NO prefix delimiter\n");
            return;
        }
        envval = delimiter + 1;

        forced_action_pfxlen = envval - envstr;
        if (forced_action_pfxlen >= 256) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ARCUS_FORCED_BTREE_OVERFLOW_ACTION: Too long prefix name\n");
            return;
        }
        memcpy(forced_action_prefix, envstr, forced_action_pfxlen);
        forced_action_prefix[forced_action_pfxlen] = '\0';

        if (strcmp(envval, "smallest_trim") == 0)
            forced_btree_ovflact = OVFL_SMALLEST_TRIM;
        else if (strcmp(envval, "smallest_silent_trim") == 0)
            forced_btree_ovflact = OVFL_SMALLEST_SILENT_TRIM;
        else if (strcmp(envval, "largest_trim") == 0)
            forced_btree_ovflact = OVFL_LARGEST_TRIM;
        else if (strcmp(envval, "largest_silent_trim") == 0)
            forced_btree_ovflact = OVFL_LARGEST_SILENT_TRIM;
        else if (strcmp(envval, "error") == 0)
            forced_btree_ovflact = OVFL_ERROR;
        else {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ARCUS_FORCED_BTREE_OVERFLOW_ACTION: Invalid overflow action\n");
            forced_action_prefix[0] = '\0';
            return;
        }

        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "ARCUS_FORCED_BTREE_OVERFLOW_ACTION: prefix=%s action=%s\n",
                    forced_action_prefix, envval);
    }
}

ENGINE_ERROR_CODE item_init(struct default_engine *engine)
{
    /* initialize global variables */
    ngnptr = engine;
    config = &engine->config;
    itemsp = &engine->items;
    statsp = &engine->stats;
    svcore = engine->server.core;
    svstat = engine->server.stat;
    logger = engine->server.log->get_logger();

    pthread_mutex_init(&coll_del_lock, NULL);
    pthread_cond_init(&coll_del_cond, NULL);
    coll_del_queue.head = coll_del_queue.tail = NULL;
    coll_del_queue.size = 0;

    /* adjust maximum collection size */
    if (config->max_list_size > max_list_size) {
        max_list_size = config->max_list_size < coll_size_limit
                      ? (int32_t)config->max_list_size : coll_size_limit;
    }
    if (config->max_set_size > max_set_size) {
        max_set_size = config->max_set_size < coll_size_limit
                     ? (int32_t)config->max_set_size : coll_size_limit;
    }
    if (config->max_map_size > max_map_size) {
        max_map_size = config->max_map_size < coll_size_limit
                     ? (int32_t)config->max_map_size : coll_size_limit;
    }
    if (config->max_btree_size > max_btree_size) {
        max_btree_size = config->max_btree_size < coll_size_limit
                       ? (int32_t)config->max_btree_size : coll_size_limit;
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "maximum list  size = %d\n", max_list_size);
    logger->log(EXTENSION_LOG_INFO, NULL, "maximum set   size = %d\n", max_set_size);
    logger->log(EXTENSION_LOG_INFO, NULL, "maximum map   size = %d\n", max_map_size);
    logger->log(EXTENSION_LOG_INFO, NULL, "maximum btree size = %d\n", max_btree_size);

    item_clog_init(engine);

    /* check forced btree overflow action */
    _check_forced_btree_overflow_action();

    int ret = pthread_create(&coll_del_tid, NULL, collection_delete_thread, engine);
    if (ret != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Can't create thread: %s\n", strerror(ret));
        return ENGINE_FAILED;
    }

    /* prepare bkey min & max value */
    bkey_uint64_min = 0;
    bkey_uint64_max = (uint64_t)((int64_t)-1); /* need check */
    bkey_binary_min[0] = 0x00;
    for (int i=0; i < MAX_BKEY_LENG; i++) {
        bkey_binary_max[i] = 0xFF;
    }

    /* remove unused function warnings */
    if (1) {
        uint64_t val1 = 10;
        uint64_t val2 = 20;
        assert(UINT64_COMPARE_OP[COMPARE_OP_LT](&val1, &val2) == true);
    }

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM module initialized.\n");
    return ENGINE_SUCCESS;
}

void item_final(struct default_engine *engine)
{
    if (itemsp == NULL) {
        return; /* nothing to do */
    }

    if (engine->dumper.running) {
        item_stop_dump(engine);
    }
    if (coll_del_thread_running) {
        coll_del_thread_wakeup();
        pthread_join(coll_del_tid, NULL);
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

    item_clog_final(engine);
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM module destroyed.\n");
}

/*
 * LIST Interface Functions
 */
ENGINE_ERROR_CODE list_struct_create(const char *key, const uint32_t nkey,
                                     item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "list struct create - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_list_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            do_item_release(it);
        }
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

list_elem_item *list_elem_alloc(const uint32_t nbytes, const void *cookie)
{
    list_elem_item *elem;
    LOCK_CACHE();
    elem = do_list_elem_alloc(nbytes, cookie);
    UNLOCK_CACHE();
    return elem;
}

void list_elem_free(list_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->next == (list_elem_item *)ADDR_MEANS_UNLINKED);
    do_list_elem_free(elem);
    UNLOCK_CACHE();
}

void list_elem_release(list_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    LOCK_CACHE();
    while (cnt < elem_count) {
        do_list_elem_release(elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            UNLOCK_CACHE();
            LOCK_CACHE();
        }
    }
    UNLOCK_CACHE();
}

ENGINE_ERROR_CODE list_elem_insert(const char *key, const uint32_t nkey,
                                   int index, list_elem_item *elem,
                                   item_attr *attrp,
                                   bool *created, const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    *created = false;

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "list elem insert - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_list_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_list_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            if (ret == ENGINE_SUCCESS) {
                *created = true;
            } else {
                /* The item is to be released, below */
            }
        }
    }
    if (ret == ENGINE_SUCCESS) {
        ret = do_list_elem_insert(it, index, elem, cookie);
        if (ret != ENGINE_SUCCESS && *created) {
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
        }
    }
    if (it != NULL) do_item_release(it);
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

static int adjust_list_range(list_meta_info *info, int *from_index, int *to_index)
{
    if (info->ccnt <= 0) return -1; /* out of range */

    if (*from_index < 0) {
        *from_index += info->ccnt;
    }
    if (*to_index < 0) {
        *to_index += info->ccnt;
    }

    if (*from_index < 0) {
        if (*to_index < 0) return -1; /* out of range */
        *from_index = 0;
    } else if (*from_index >= info->ccnt) {
        if (*to_index >= info->ccnt) return -1; /* out of range */
        *from_index = info->ccnt-1;
    }
    if (*to_index < 0) {
        *to_index = 0;
    } else if (*to_index >= info->ccnt) {
        *to_index = info->ccnt-1;
    }
    return 0;
}

ENGINE_ERROR_CODE list_elem_delete(const char *key, const uint32_t nkey,
                                   int from_index, int to_index,
                                   const bool drop_if_empty,
                                   uint32_t *del_count, bool *dropped,
                                   const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "list elem delete - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_list_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        int      index;
        uint32_t count;
        list_meta_info *info = (list_meta_info *)item_get_meta(it);
        do {
            if (adjust_list_range(info, &from_index, &to_index) != 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if (from_index <= to_index) {
                index = from_index;
                count = to_index - from_index + 1;
            } else {
                index = to_index;
                count = from_index - to_index + 1;
            }

            *del_count = do_list_elem_delete(info, index, count, ELEM_DELETE_NORMAL);
            if (*del_count > 0) {
                if (info->ccnt == 0 && drop_if_empty) {
                    do_item_unlink(it, ITEM_UNLINK_NORMAL);
                    *dropped = true;
                } else {
                    *dropped = false;
                }
            } else {
                ret = ENGINE_ELEM_ENOENT;
            }
        } while(0);
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE list_elem_get(const char *key, const uint32_t nkey,
                                int from_index, int to_index,
                                const bool delete, const bool drop_if_empty,
                                struct elems_result *eresult,
                                const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    eresult->elem_array = NULL;
    eresult->elem_count = 0;

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence && delete) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "list elem get - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_list_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        int      index;
        uint32_t count;
        bool forward;
        list_meta_info *info = (list_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (adjust_list_range(info, &from_index, &to_index) != 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            index = from_index;
            if (from_index <= to_index) {
                count = to_index - from_index + 1;
                forward = true;
            } else {
                count = from_index - to_index + 1;
                forward = false;
            }
            if ((eresult->elem_array = (eitem **)malloc(count * sizeof(eitem*))) == NULL) {
                ret = ENGINE_ENOMEM; break;
            }

            ret = do_list_elem_get(info, index, count, forward, delete,
                                  (list_elem_item**)(eresult->elem_array), &(eresult->elem_count));
            if (ret == ENGINE_SUCCESS) {
                if (info->ccnt == 0 && drop_if_empty) {
                    assert(delete == true);
                    do_item_unlink(it, ITEM_UNLINK_NORMAL);
                    eresult->dropped = true;
                } else {
                    eresult->dropped = false;
                }
                eresult->flags = it->flags;
            } else {
                /* ret = ENGINE_ELEM_ENOENT */
                free(eresult->elem_array);
                eresult->elem_array = NULL;
            }
        } while(0);
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL && delete) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

/*
 * SET Interface Functions
 */
ENGINE_ERROR_CODE set_struct_create(const char *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "set struct create - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_set_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            do_item_release(it);
        }
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

set_elem_item *set_elem_alloc(const uint32_t nbytes, const void *cookie)
{
    set_elem_item *elem;
    LOCK_CACHE();
    elem = do_set_elem_alloc(nbytes, cookie);
    UNLOCK_CACHE();
    return elem;
}

void set_elem_free(set_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->next == (set_elem_item *)ADDR_MEANS_UNLINKED);
    do_set_elem_free(elem);
    UNLOCK_CACHE();
}

void set_elem_release(set_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    LOCK_CACHE();
    while (cnt < elem_count) {
        do_set_elem_release(elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            UNLOCK_CACHE();
            LOCK_CACHE();
        }
    }
    UNLOCK_CACHE();
}

ENGINE_ERROR_CODE set_elem_insert(const char *key, const uint32_t nkey,
                                  set_elem_item *elem, item_attr *attrp,
                                  bool *created, const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    *created = false;

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "set elem insert - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_set_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_set_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            if (ret == ENGINE_SUCCESS) {
                *created = true;
            } else {
                /* The item is to be released, below */
            }
        }
    }
    if (ret == ENGINE_SUCCESS) {
        ret = do_set_elem_insert(it, elem, cookie);
        if (ret != ENGINE_SUCCESS && *created) {
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
        }
    }
    if (it != NULL) do_item_release(it);
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE set_elem_delete(const char *key, const uint32_t nkey,
                                  const char *value, const uint32_t nbytes,
                                  const bool drop_if_empty, bool *dropped,
                                  const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    *dropped = false;

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "set elem delete - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_set_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        ret = do_set_elem_delete_with_value(info, value, nbytes, ELEM_DELETE_NORMAL);
        if (ret == ENGINE_SUCCESS) {
            if (info->ccnt == 0 && drop_if_empty) {
                do_item_unlink(it, ITEM_UNLINK_NORMAL);
                *dropped = true;
            }
        }
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE set_elem_exist(const char *key, const uint32_t nkey,
                                 const char *value, const uint32_t nbytes,
                                 bool *exist)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;

    LOCK_CACHE();
    ret = do_set_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (do_set_elem_find(info, value, nbytes) != NULL)
                *exist = true;
            else
                *exist = false;
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE set_elem_get(const char *key, const uint32_t nkey,
                               const uint32_t count,
                               const bool delete, const bool drop_if_empty,
                               struct elems_result *eresult,
                               const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    eresult->elem_array = NULL;
    eresult->elem_count = 0;

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence && delete) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "set elem get - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_set_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (count == 0 || info->ccnt < count) {
                eresult->elem_array = (eitem **)malloc(info->ccnt * sizeof(eitem*));
            } else {
                eresult->elem_array = (eitem **)malloc(count * sizeof(eitem*));
            }
            if (eresult->elem_array == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            ret = do_set_elem_get(info, count, delete,
                                  (set_elem_item**)(eresult->elem_array), &(eresult->elem_count));
            if (ret == ENGINE_SUCCESS) {
                if (info->ccnt == 0 && drop_if_empty) {
                    assert(delete == true);
                    do_item_unlink(it, ITEM_UNLINK_NORMAL);
                    eresult->dropped = true;
                } else {
                    eresult->dropped = false;
                }
                eresult->flags = it->flags;
            } else {
                /* ret = ENGINE_ELEM_ENOENT */
                free(eresult->elem_array);
                eresult->elem_array = NULL;
            }
        } while (0);
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL && delete) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

/*
 * B+TREE Interface Functions
 */
ENGINE_ERROR_CODE btree_struct_create(const char *key, const uint32_t nkey,
                                      item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "btree struct create - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_btree_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            do_item_release(it);
        }
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

btree_elem_item *btree_elem_alloc(const uint32_t nbkey, const uint32_t neflag, const uint32_t nbytes,
                                  const void *cookie)
{
    btree_elem_item *elem;
    LOCK_CACHE();
    elem = do_btree_elem_alloc(nbkey, neflag, nbytes, cookie);
    UNLOCK_CACHE();
    return elem;
}

void btree_elem_free(btree_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->status == BTREE_ITEM_STATUS_UNLINK);
    /* for cases with trimmed element, refcount can be over 0 */
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0) {
        elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(elem);
    }
    UNLOCK_CACHE();
}

void btree_elem_release(btree_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    LOCK_CACHE();
    while (cnt < elem_count) {
        do_btree_elem_release(elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            UNLOCK_CACHE();
            LOCK_CACHE();
        }
    }
    UNLOCK_CACHE();
}

ENGINE_ERROR_CODE btree_elem_insert(const char *key, const uint32_t nkey,
                                    btree_elem_item *elem, const bool replace_if_exist, item_attr *attrp,
                                    bool *replaced, bool *created, btree_elem_item **trimmed_elems,
                                    uint32_t *trimmed_count, uint32_t *trimmed_flags, const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    *created = false;
    if (trimmed_elems != NULL) {
        /* initialize as no trimmed element */
        *trimmed_elems = NULL;
        *trimmed_count = 0;
    }

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "btree elem insert - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_btree_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_btree_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            if (ret == ENGINE_SUCCESS) {
                *created = true;
            } else {
                /* The item is to be released, below */
            }
        }
    }
    if (ret == ENGINE_SUCCESS) {
        ret = do_btree_elem_insert(it, elem, replace_if_exist, replaced,
                                   trimmed_elems, trimmed_count, cookie);
        if (ret != ENGINE_SUCCESS && *created) {
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
        }
        if (trimmed_elems != NULL && *trimmed_elems != NULL) {
            *trimmed_flags = it->flags;
        }
    }
    if (it != NULL) do_item_release(it);
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE btree_elem_update(const char *key, const uint32_t nkey, const bkey_range *bkrange,
                                    const eflag_update *eupdate, const char *value, const uint32_t nbytes,
                                    const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    assert(bkrtype == BKEY_RANGE_TYPE_SIN); /* single bkey */
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "btree elem update - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_btree_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            ret = do_btree_elem_update(info, bkrtype, bkrange, eupdate, value, nbytes, cookie);
        } while(0);
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE btree_elem_delete(const char *key, const uint32_t nkey,
                                    const bkey_range *bkrange, const eflag_filter *efilter,
                                    const uint32_t req_count, const bool drop_if_empty,
                                    uint32_t *del_count, uint32_t *access_count, bool *dropped,
                                    const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "btree elem delete - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_btree_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *del_count = do_btree_elem_delete(info, bkrtype, bkrange, efilter, req_count,
                                              access_count, ELEM_DELETE_NORMAL);
            if (*del_count > 0) {
                if (info->ccnt == 0 && drop_if_empty) {
                    assert(info->root == NULL);
                    do_item_unlink(it, ITEM_UNLINK_NORMAL);
                    *dropped = true;
                } else {
                    *dropped = false;
                }
            } else {
                ret = ENGINE_ELEM_ENOENT;
            }
        } while(0);
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE btree_elem_arithmetic(const char *key, const uint32_t nkey,
                                        const bkey_range *bkrange,
                                        const bool increment, const bool create,
                                        const uint64_t delta, const uint64_t initial,
                                        const eflag_t *eflagp,
                                        uint64_t *result, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    assert(bkrtype == BKEY_RANGE_TYPE_SIN); /* single bkey */
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "btree elem arithmetic - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_btree_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        bool new_root_flag = false;
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt > 0 || info->maxbkeyrange.len != BKEY_NULL) {
                if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                    (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                    ret = ENGINE_EBADBKEY; break;
                }
            }
            if (info->root == NULL && create == true) {
                /* create new root node */
                btree_indx_node *r_node = do_btree_node_alloc(0, cookie);
                if (r_node == NULL) {
                    ret = ENGINE_ENOMEM; break;
                }
                do_btree_node_link(info, r_node, NULL);
                new_root_flag = true;
            }
            ret = do_btree_elem_arithmetic(info, bkrtype, bkrange, increment, create,
                                           delta, initial, eflagp, result, cookie);
            if (ret != ENGINE_SUCCESS) {
                if (new_root_flag) {
                    /* unlink the root node and free it */
                    do_btree_node_unlink(info, info->root, NULL);
                }
            }
        } while(0);
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE btree_elem_get(const char *key, const uint32_t nkey,
                                 const bkey_range *bkrange, const eflag_filter *efilter,
                                 const uint32_t offset, const uint32_t req_count,
                                 const bool delete, const bool drop_if_empty,
                                 struct elems_result *eresult,
                                 const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    bool potentialbkeytrim;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    eresult->elem_array = NULL;
    eresult->elem_count = 0;

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence && delete) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "btree elem get - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_btree_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt == 0 || offset >= info->ccnt) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            if (req_count == 0 || info->ccnt < req_count) {
                eresult->elem_array = (eitem **)malloc(info->ccnt * sizeof(eitem*));
            } else {
                eresult->elem_array = (eitem **)malloc(req_count * sizeof(eitem*));
            }
            if (eresult->elem_array == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            ret = do_btree_elem_get(info, bkrtype, bkrange, efilter,
                                    offset, req_count, delete, (btree_elem_item **)(eresult->elem_array),
                                    &(eresult->elem_count), &(eresult->access_count), &potentialbkeytrim);
            if (ret == ENGINE_SUCCESS) {
                if (delete) {
                    if (info->ccnt == 0 && drop_if_empty) {
                        assert(info->root == NULL);
                        do_item_unlink(it, ITEM_UNLINK_NORMAL);
                        eresult->dropped = true;
                    } else {
                        eresult->dropped = false;
                    }
                } else {
                    eresult->trimmed = potentialbkeytrim;
                }
                eresult->flags = it->flags;
            } else {
                if (potentialbkeytrim == true)
                    ret = ENGINE_EBKEYOOR;
                /* ret = ENGINE_ELEM_ENOENT; */
                free(eresult->elem_array);
                eresult->elem_array = NULL;
            }
        } while (0);
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL && delete) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE btree_elem_count(const char *key, const uint32_t nkey,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   uint32_t *elem_count, uint32_t *access_count)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *elem_count = do_btree_elem_count(info, bkrtype, bkrange, efilter, access_count);
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE btree_posi_find(const char *key, const uint32_t nkey, const bkey_range *bkrange,
                                  ENGINE_BTREE_ORDER order, int *position)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    assert(bkrtype == BKEY_RANGE_TYPE_SIN);

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
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
        do_item_release(it);
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE btree_posi_find_with_get(const char *key, const uint32_t nkey,
                                           const bkey_range *bkrange, ENGINE_BTREE_ORDER order,
                                           const int count, int *position,
                                           btree_elem_item **elem_array, uint32_t *elem_count,
                                           uint32_t *elem_index, uint32_t *flags)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
    int bkrtype = do_btree_bkey_range_type(bkrange);
    assert(bkrtype == BKEY_RANGE_TYPE_SIN);

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
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
            *position = do_btree_posi_find_with_get(info, bkrtype, bkrange, order, count,
                                                    elem_array, elem_count, elem_index);
            if (*position < 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            *flags = it->flags;
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE btree_elem_get_by_posi(const char *key, const uint32_t nkey,
                                         ENGINE_BTREE_ORDER order, int from_posi, int to_posi,
                                         btree_elem_item **elem_array, uint32_t *elem_count, uint32_t *flags)
{
    assert(from_posi >= 0 && to_posi >= 0);
    hash_item *it;
    ENGINE_ERROR_CODE ret;

    LOCK_CACHE();
    ret = do_btree_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        uint32_t rqcount;
        bool     forward;
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
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
            ret = do_btree_elem_get_by_posi(info, from_posi, rqcount, forward, elem_array, elem_count);
            if (ret != ENGINE_SUCCESS) /* ret == ENGINE_ELEM_ENOENT */
                break;
            *flags = it->flags;
        } while (0);
        do_item_release(it);
    }
    UNLOCK_CACHE();
    return ret;
}

#ifdef SUPPORT_BOP_SMGET
#ifdef JHPARK_OLD_SMGET_INTERFACE
ENGINE_ERROR_CODE btree_elem_smget_old(token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   btree_elem_item **elem_array, uint32_t *kfnd_array,
                                   uint32_t *flag_array, uint32_t *elem_count,
                                   uint32_t *missed_key_array, uint32_t *missed_key_count,
                                   bool *trimmed, bool *duplicated)
{
    btree_scan_info btree_scan_buf[offset+count+1]; /* one more scan needed */
    uint16_t        sort_sindx_buf[offset+count];   /* sorted scan index buffer */
    uint32_t        sort_sindx_cnt, i;
    int             bkrtype = do_btree_bkey_range_type(bkrange);
    ENGINE_ERROR_CODE ret;

    /* prepare */
    for (i = 0; i <= (offset+count); i++) {
        btree_scan_buf[i].it = NULL;
    }

    *trimmed = false;
    *duplicated = false;

    LOCK_CACHE();
    /* the 1st phase: get the sorted scans */
    ret = do_btree_smget_scan_sort_old(key_array, key_count,
                                   bkrtype, bkrange, efilter, (offset+count),
                                   btree_scan_buf, sort_sindx_buf, &sort_sindx_cnt,
                                   missed_key_array, missed_key_count);
    if (ret == ENGINE_SUCCESS) {
        /* the 2nd phase: get the sorted elems */
        ret = do_btree_smget_elem_sort_old(btree_scan_buf, sort_sindx_buf, sort_sindx_cnt,
                                           bkrtype, bkrange, efilter, offset, count,
                                           elem_array, kfnd_array, flag_array, elem_count,
                                           trimmed, duplicated);
        for (i = 0; i <= (offset+count); i++) {
            if (btree_scan_buf[i].it != NULL)
                do_item_release(btree_scan_buf[i].it);
        }
    }
    UNLOCK_CACHE();

    return ret;
}
#endif

ENGINE_ERROR_CODE btree_elem_smget(token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   const bool unique,
                                   smget_result_t *result)
{
    btree_scan_info btree_scan_buf[offset+count+1]; /* one more scan needed */
    uint16_t        sort_sindx_buf[offset+count];   /* sorted scan index buffer */
    uint32_t        sort_sindx_cnt, i;
    int             bkrtype = do_btree_bkey_range_type(bkrange);
    ENGINE_ERROR_CODE ret;

    /* prepare */
    for (i = 0; i <= (offset+count); i++) {
        btree_scan_buf[i].it = NULL;
        btree_scan_buf[i].next = (i < (offset+count)) ? (i+1) : -1;
    }

    /* initialize smget result structure */
    assert(result->elem_array != NULL);
    result->trim_elems = (eitem *)&result->elem_array[count];
    result->elem_kinfo = (smget_ehit_t *)&result->elem_array[count + key_count];
    result->miss_kinfo = (smget_emis_t *)&result->elem_kinfo[count];
    result->trim_kinfo = result->miss_kinfo;
    result->elem_count = 0;
    result->miss_count = 0;
    result->trim_count = 0;
    result->elem_arrsz = count;
    result->keys_arrsz = key_count;
    result->duplicated = false;
    result->ascending = (bkrtype != BKEY_RANGE_TYPE_DSC ? true : false);

    LOCK_CACHE();
    do {
        /* the 1st phase: get the sorted scans */
        ret = do_btree_smget_scan_sort(key_array, key_count,
                                       bkrtype, bkrange, efilter, (offset+count), unique,
                                       btree_scan_buf, sort_sindx_buf, &sort_sindx_cnt,
                                       result);
        if (ret != ENGINE_SUCCESS) {
            break;
        }

        /* the 2nd phase: get the sorted elems */
        ret = do_btree_smget_elem_sort(btree_scan_buf, sort_sindx_buf, sort_sindx_cnt,
                                       bkrtype, bkrange, efilter, offset, count, unique,
                                       result);
        if (ret != ENGINE_SUCCESS) {
            break;
        }

        for (i = 0; i <= (offset+count); i++) {
            if (btree_scan_buf[i].it != NULL)
                do_item_release(btree_scan_buf[i].it);
        }
    } while(0);
    UNLOCK_CACHE();

    return ret;
}
#endif

/*
 * ITEM ATTRIBUTE Interface Functions
 */
static ENGINE_ERROR_CODE
do_item_getattr(hash_item *it,
                ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
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
        coll_meta_info *info = (coll_meta_info *)item_get_meta(it);
        attr_data->type = GET_ITEM_TYPE(it);
        assert(attr_data->type < ITEM_TYPE_MAX);
        /* attribute validation check */
        if (attr_data->type != ITEM_TYPE_BTREE) {
            for (int i = 0; i < attr_count; i++) {
                if (attr_ids[i] == ATTR_MAXBKEYRANGE || attr_ids[i] == ATTR_TRIMMED) {
                    return ENGINE_EBADATTR;
                }
            }
        }
        /* get collection attributes */
        attr_data->count = info->ccnt;
        if (info->mcnt > 0) {
            attr_data->maxcount = info->mcnt;
        } else {
            switch (attr_data->type) {
              case ITEM_TYPE_LIST:
                   attr_data->maxcount = max_list_size;
                   break;
              case ITEM_TYPE_SET:
                   attr_data->maxcount = max_set_size;
                   break;
              case ITEM_TYPE_MAP:
                   attr_data->maxcount = max_map_size;
                   break;
              case ITEM_TYPE_BTREE:
                   attr_data->maxcount = max_btree_size;
                   break;
              default:
                   attr_data->maxcount = 0;
            }
        }
        attr_data->ovflaction = info->ovflact;
        attr_data->readable = (((info->mflags & COLL_META_FLAG_READABLE) != 0) ? 1 : 0);

        if (attr_data->type == ITEM_TYPE_BTREE) {
            btree_meta_info *binfo = (btree_meta_info *)info;
            attr_data->maxbkeyrange = binfo->maxbkeyrange;
            attr_data->trimmed = (((binfo->mflags & COLL_META_FLAG_TRIMMED) != 0) ? 1 : 0);
            if (info->ccnt > 0) {
                btree_elem_item *min_bkey_elem = do_btree_get_first_elem(binfo->root);
                do_btree_get_bkey(min_bkey_elem, &attr_data->minbkey);
                btree_elem_item *max_bkey_elem = do_btree_get_last_elem(binfo->root);
                do_btree_get_bkey(max_bkey_elem, &attr_data->maxbkey);
            }
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
do_item_setattr_check(hash_item *it,
                      ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                      item_attr *attr_data)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    coll_meta_info *info = NULL;
    if (IS_COLL_ITEM(it)) {
        info = (coll_meta_info *)item_get_meta(it);
    }

    for (int i = 0; i < attr_count; i++) {
        /* Attributes for all items */
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
        /* Attributes for collection items */
        if (info == NULL) continue;
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            attr_data->maxcount = do_coll_real_maxcount(it, attr_data->maxcount);
            if (attr_data->maxcount > 0 && attr_data->maxcount < info->ccnt) {
                ret = ENGINE_EBADVALUE; break;
            }
            continue;
        }
        if (attr_ids[i] == ATTR_OVFLACTION) {
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
            continue;
        }
        if (attr_ids[i] == ATTR_READABLE) {
            if (attr_data->readable != 1) {
                ret = ENGINE_EBADVALUE; break;
            }
            continue;
        }
        /* Attributes for only b+tree items */
        if (!IS_BTREE_ITEM(it)) continue;
        if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
            if (attr_data->maxbkeyrange.len != BKEY_NULL && info->ccnt > 0) {
                btree_meta_info *binfo = (btree_meta_info *)info;
                /* check bkey type of maxbkeyrange */
                if ((binfo->bktype == BKEY_TYPE_UINT64 && attr_data->maxbkeyrange.len >  0) ||
                    (binfo->bktype == BKEY_TYPE_BINARY && attr_data->maxbkeyrange.len == 0)) {
                    ret = ENGINE_EBADVALUE; break;
                }
                /* New maxbkeyrange must contain the current bkey range */
                if ((binfo->ccnt >= 2) && /* current key range exists */
                    (attr_data->maxbkeyrange.len != binfo->maxbkeyrange.len ||
                     BKEY_ISNE(attr_data->maxbkeyrange.val, attr_data->maxbkeyrange.len,
                               binfo->maxbkeyrange.val, binfo->maxbkeyrange.len))) {
                    bkey_t curbkeyrange;
                    btree_elem_item *min_bkey_elem = do_btree_get_first_elem(binfo->root);
                    btree_elem_item *max_bkey_elem = do_btree_get_last_elem(binfo->root);
                    curbkeyrange.len = attr_data->maxbkeyrange.len;
                    BKEY_DIFF(max_bkey_elem->data, max_bkey_elem->nbkey,
                              min_bkey_elem->data, min_bkey_elem->nbkey,
                              curbkeyrange.len, curbkeyrange.val);
                    if (BKEY_ISGT(curbkeyrange.val, curbkeyrange.len,
                                  attr_data->maxbkeyrange.val, attr_data->maxbkeyrange.len)) {
                        ret = ENGINE_EBADVALUE; break;
                    }
                }
            }
            continue;
        }
    }
    return ret;
}

static void
do_item_setattr_exec(hash_item *it,
                     ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                     item_attr *attr_data)
{
    coll_meta_info *info = NULL;
    if (IS_COLL_ITEM(it)) {
        info = (coll_meta_info *)item_get_meta(it);
    }

    for (int i = 0; i < attr_count; i++) {
        /* Attributes for all items */
        if (attr_ids[i] == ATTR_EXPIRETIME) {
            if (it->exptime != attr_data->exptime) {
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
            continue;
        }
        /* Attributes for collection items */
        if (info == NULL) continue;
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            info->mcnt = attr_data->maxcount;
            continue;
        }
        if (attr_ids[i] == ATTR_OVFLACTION) {
            if (IS_BTREE_ITEM(it)) {
                btree_meta_info *binfo = (btree_meta_info *)info;
                if (binfo->ovflact != attr_data->ovflaction) {
                    binfo->mflags &= ~COLL_META_FLAG_TRIMMED; // clear trimmed
                }
            }
            info->ovflact = attr_data->ovflaction;
            if (IS_BTREE_ITEM(it)) {
                if (forced_btree_ovflact != 0 && forced_action_pfxlen < it->nkey &&
                    memcmp(forced_action_prefix, item_get_key(it), forced_action_pfxlen) == 0) {
                    info->ovflact = forced_btree_ovflact;
                }
            }
            continue;
        }
        if (attr_ids[i] == ATTR_READABLE) {
            info->mflags |= COLL_META_FLAG_READABLE;
            continue;
        }
        /* Attributes for only b+tree items */
        if (!IS_BTREE_ITEM(it)) continue;
        if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
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
            continue;
        }
    }

    CLOG_ITEM_SETATTR(it, attr_ids, attr_count);
}

ENGINE_ERROR_CODE item_setattr(const void *key, const uint32_t nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "item setattr - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it == NULL) {
        ret = ENGINE_KEY_ENOENT;
    } else {
        ret = do_item_setattr_check(it, attr_ids, attr_count, attr_data);
        if (ret == ENGINE_SUCCESS) {
            /* do setattr operation */
            do_item_setattr_exec(it, attr_ids, attr_count, attr_data);
        }
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();

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

/* See do_list_elem_delete. */
static void do_list_elem_get_all(list_meta_info *info, elems_result_t *eresult)
{
    assert(eresult->elem_arrsz >= info->ccnt && eresult->elem_count == 0);
    list_elem_item *elem;

    elem = do_list_elem_find(info, 0);
    while (elem != NULL) {
        elem->refcount++;
        eresult->elem_array[eresult->elem_count++] = elem;
        elem = elem->next;
    }
    assert(eresult->elem_count == info->ccnt);
}

/* See do_set_elem_traverse_dfs and do_set_elem_link. do_set_elem_traverse_dfs
 * can visit all elements, but only supports get and delete operations.
 * Do something similar and visit all elements.
 */
static void do_set_elem_get_all(set_meta_info *info, elems_result_t *eresult)
{
    assert(eresult->elem_arrsz >= info->ccnt && eresult->elem_count == 0);
    set_hash_node *node;
    set_elem_item *elem;
    int cur_depth, i;
    bool push;

    /* Temporay stack we use to do dfs. Static is ugly but is okay...
     * This function runs with the cache lock acquired.
     */
    static int stack_max = 0;
    static struct {
        set_hash_node *node;
        int idx;
    } *stack = NULL;

    node = info->root;
    cur_depth = 0;
    push = true;
    while (node != NULL) {
        if (push) {
            push = false;
            if (stack_max <= cur_depth) {
                stack_max += 16;
                stack = realloc(stack, sizeof(*stack) * stack_max);
            }
            stack[cur_depth].node = node;
            stack[cur_depth].idx = 0;
        }

        /* Scan the current node */
        for (i = stack[cur_depth].idx; i < SET_HASHTAB_SIZE; i++) {
            if (node->hcnt[i] >= 0) {
                /* Hash chain.  Insert all elements on the chain into the
                 * to-be-copied list.
                 */
                for (elem = node->htab[i]; elem != NULL; elem = elem->next) {
                    elem->refcount++;
                    eresult->elem_array[eresult->elem_count++] = elem;
                }
            }
            else if (node->htab[i] != NULL) {
                /* Another hash node. Go down */
                stack[cur_depth].idx = i+1;
                push = true;
                node = node->htab[i];
                cur_depth++;
                break;
            }
        }

        /* Scannned everything in this node. Go up. */
        if (i >= SET_HASHTAB_SIZE) {
            cur_depth--;
            if (cur_depth < 0)
                node = NULL; /* done */
            else
                node = stack[cur_depth].node;
        }
    }
    assert(eresult->elem_count == info->ccnt);
}

/* See do_map_elem_traverse_dfs and do_map_elem_link. do_map_elem_traverse_dfs
 * can visit all elements, but only supports get and delete operations.
 * Do something similar and visit all elements.
 */
static void do_map_elem_get_all(map_meta_info *info, elems_result_t *eresult)
{
    assert(eresult->elem_arrsz >= info->ccnt && eresult->elem_count == 0);
    map_hash_node *node;
    map_elem_item *elem;
    int cur_depth, i;
    bool push;

    /* Temporay stack we use to do dfs. Static is ugly but is okay...
     * This function runs with the cache lock acquired.
     */
    static int stack_max = 0;
    static struct {
        map_hash_node *node;
        int idx;
    } *stack = NULL;

    node = info->root;
    cur_depth = 0;
    push = true;
    while (node != NULL) {
        if (push) {
            push = false;
            if (stack_max <= cur_depth) {
                stack_max += 16;
                stack = realloc(stack, sizeof(*stack) * stack_max);
            }
            stack[cur_depth].node = node;
            stack[cur_depth].idx = 0;
        }

        /* Scan the current node */
        for (i = stack[cur_depth].idx; i < MAP_HASHTAB_SIZE; i++) {
            if (node->hcnt[i] >= 0) {
                /* Hash chain.  Insert all elements on the chain into the
                 * to-be-copied list.
                 */
                for (elem = node->htab[i]; elem != NULL; elem = elem->next) {
                    elem->refcount++;
                    eresult->elem_array[eresult->elem_count++] = elem;
                }
            }
            else if (node->htab[i] != NULL) {
                /* Another hash node.  Go down */
                stack[cur_depth].idx = i+1;
                push = true;
                node = node->htab[i];
                cur_depth++;
                break;
            }
        }

        /* Scannned everything in this node.  Go up. */
        if (i >= MAP_HASHTAB_SIZE) {
            cur_depth--;
            if (cur_depth < 0)
                node = NULL; /* done */
            else
                node = stack[cur_depth].node;
        }
    }
    assert(eresult->elem_count == info->ccnt);
}

/* Scan the whole btree with the cache lock acquired.
 * We only build the table of the current elements.
 * See do_btree_elem_delete and do_btree_multi_elem_unlink.
 * They show how to traverse the btree.
 * FIXME. IS THIS STILL RIGHT AFTER THE MERGE? do_btree_multi_elem_unlink is gone.
 */
static void do_btree_elem_get_all(btree_meta_info *info, elems_result_t *eresult)
{
    assert(eresult->elem_arrsz >= info->ccnt && eresult->elem_count == 0);
    btree_elem_item *elem;
    btree_elem_posi  posi;

    elem = do_btree_find_first(info->root, BKEY_RANGE_TYPE_ASC, NULL, &posi, false);
    while (elem != NULL) {
        elem->refcount++;
        eresult->elem_array[eresult->elem_count++] = elem;
        /* Never have to go backward?  FIXME */
        elem = do_btree_find_next(&posi, NULL);
    }
    assert(eresult->elem_count == info->ccnt);
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
        eresult->elem_count = 0;
        if (eresult->elem_arrsz < info->ccnt) {
            uint32_t new_size = GET_ALIGN_SIZE(info->ccnt, 100);
            if (do_coll_eresult_realloc(eresult, new_size) < 0) {
                ret = ENGINE_ENOMEM; break;
            }
        }
        /* get all elements */
        if (IS_LIST_ITEM(it))       do_list_elem_get_all((list_meta_info*)info, eresult);
        else if (IS_SET_ITEM(it))   do_set_elem_get_all((set_meta_info*)info, eresult);
        else if (IS_MAP_ITEM(it))   do_map_elem_get_all((map_meta_info*)info, eresult);
        else if (IS_BTREE_ITEM(it)) do_btree_elem_get_all((btree_meta_info*)info, eresult);
    } while(0);
    if (lock_hold) UNLOCK_CACHE();

    return ret;
}

void coll_elem_release(elems_result_t *eresult, int type)
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
ENGINE_ERROR_CODE item_conf_set_scrub_count(int *count)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    LOCK_CACHE();
    if (SCRUB_MIN_COUNT <= *count &&
        SCRUB_MAX_COUNT >= *count) {
        config->scrub_count = *count;
    } else {
        ret = ENGINE_EBADVALUE;
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE item_conf_set_maxcollsize(const int coll_type, int *maxsize)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    LOCK_CACHE();
    if (*maxsize < 0 || *maxsize > coll_size_limit) {
        *maxsize = coll_size_limit;
    }
    switch (coll_type) {
      case ITEM_TYPE_LIST:
           if (*maxsize <= max_list_size) {
               ret = ENGINE_EBADVALUE;
           } else {
               max_list_size = *maxsize;
               config->max_list_size = *maxsize;
           }
           break;
      case ITEM_TYPE_SET:
           if (*maxsize <= max_set_size) {
               ret = ENGINE_EBADVALUE;
           } else {
               max_set_size = *maxsize;
               config->max_set_size = *maxsize;
           }
           break;
      case ITEM_TYPE_MAP:
           if (*maxsize <= max_map_size) {
               ret = ENGINE_EBADVALUE;
           } else {
               max_map_size = *maxsize;
               config->max_map_size = *maxsize;
           }
           break;
      case ITEM_TYPE_BTREE:
           if (*maxsize <= max_btree_size) {
               ret = ENGINE_EBADVALUE;
           } else {
               max_btree_size = *maxsize;
               config->max_btree_size = *maxsize;
           }
           break;
    }
    UNLOCK_CACHE();
    return ret;
}

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
    char *ret = (void*)(item + 1);
    if (item->iflag & ITEM_WITH_CAS) {
        ret += sizeof(uint64_t);
    }
    return ret;
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

/****
uint8_t item_get_clsid(const hash_item* item)
{
    return 0;
}
****/

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
 * Item and Element size functions
 */
uint32_t item_ntotal(hash_item *item)
{
    return (uint32_t)ITEM_ntotal(item);
}

uint32_t list_elem_ntotal(list_elem_item *elem)
{
    return do_list_elem_ntotal(elem);
}

uint32_t set_elem_ntotal(set_elem_item *elem)
{
    return do_set_elem_ntotal(elem);
}

uint32_t map_elem_ntotal(map_elem_item *elem)
{
    return do_map_elem_ntotal(elem);
}

uint32_t btree_elem_ntotal(btree_elem_item *elem)
{
    return do_btree_elem_ntotal(elem);
}

uint8_t  btree_real_nbkey(uint8_t nbkey)
{
    return (uint8_t)BTREE_REAL_NBKEY(nbkey);
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
    hash_item *item_array[SCRUB_MAX_COUNT];
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
            snprintf(bufptr, 12, " %u\n", it->exptime - mc_curtime);
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

static void *item_dumper_main(void *arg)
{
    struct default_engine *engine = arg;
    struct engine_dumper *dumper = &engine->dumper;
    int        array_size=SCAN_ITEM_ARRAY_SIZE;
    int        item_count;
    hash_item *item_array[SCAN_ITEM_ARRAY_SIZE];
    hash_item *it;
    struct assoc_scan scan;
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

    pthread_mutex_lock(&engine->cache_lock);

    assoc_scan_init(&scan);
    while (true)
    {
        item_count = assoc_scan_next(&scan, item_array, array_size, 0);
        if (item_count < 0) { /* reached to the end */
            break;
        }
        /* Currently, item_count > 0.
         * See the internals of assoc_scan_next(). It does not return 0.
         */
        memc_curtime = svcore->get_current_time();
        for (i = 0; i < item_count; i++) {
            it = item_array[i];
            if ((it->iflag & ITEM_INTERNAL) == 0 && do_item_isvalid(it, memc_curtime)) {
                ITEM_REFCOUNT_INCR(it); /* valid user item */
            } else {
                item_array[i] = NULL;
            }
        }
        pthread_mutex_unlock(&engine->cache_lock);

        /* write key string to buffer */
        memc_curtime = svcore->get_current_time();
        for (i = 0; i < item_count; i++) {
            if ((it = item_array[i]) == NULL) continue;
            dumper->visited++;
            /* check prefix name */
            if (dumper->nprefix >= 0 &&
                !assoc_prefix_issame(it->pfxptr, dumper->prefix, dumper->nprefix)) {
                continue; /* NOT the given prefix */
            }
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

        pthread_mutex_lock(&engine->cache_lock);
        for (i = 0; i < item_count; i++) {
            if (item_array[i] != NULL) {
                do_item_release(item_array[i]);
            }
        }
        if (ret != 0) break;

        if (!engine->initialized || dumper->stop) {
            logger->log(EXTENSION_LOG_INFO, NULL, "Stop the current dump.\n");
            ret = -1; break;
        }
    }
    assoc_scan_final(&scan);

    pthread_mutex_unlock(&engine->cache_lock);

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
                     "{ prefix=%s, count=%"PRIu64", total=%"PRIu64" elapsed=%"PRIu64" }\n",
                     dumper->nprefix > 0 ? dumper->prefix : (dumper->nprefix == 0 ? "<null>" : "<all>"),
                     dumper->dumpped, dumper->visited, (uint64_t)(time(NULL)-dumper->started));
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
    pthread_mutex_lock(&dumper->lock);
    dumper->stopped = time(NULL);
    dumper->running = false;
    pthread_mutex_unlock(&dumper->lock);
    return NULL;
}

ENGINE_ERROR_CODE item_start_dump(struct default_engine *engine,
                                  enum dump_mode mode,
                                  const char *prefix, const int nprefix,
                                  const char *filepath)
{
    struct engine_dumper *dumper = &engine->dumper;
    pthread_t tid;
    pthread_attr_t attr;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

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
        dumper->visited = 0;
        dumper->dumpped = 0;
        dumper->success = false;
        dumper->stop    = false;

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

void item_stop_dump(struct default_engine *engine)
{
    struct engine_dumper *dumper = &engine->dumper;

    pthread_mutex_lock(&dumper->lock);
    if (dumper->running) {
        /* stop the dumper */
        dumper->stop = true;
    }
    pthread_mutex_unlock(&dumper->lock);
}

void item_stats_dump(struct default_engine *engine,
                     ADD_STAT add_stat, const void *cookie)
{
    struct engine_dumper *dumper = &engine->dumper;

    pthread_mutex_lock(&dumper->lock);
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
        char val[256];
        int len;
        if (dumper->mode == DUMP_MODE_KEY) {
            add_stat("dumper:mode", 11, "key", 3, cookie);
        } else if (dumper->mode == DUMP_MODE_ITEM) {
            add_stat("dumper:mode", 11, "item", 4, cookie);
        } else {
            add_stat("dumper:mode", 11, "none", 4, cookie);
        }
        if (dumper->stopped != 0) {
            time_t diff = dumper->stopped - dumper->started;
            len = sprintf(val, "%"PRIu64, (uint64_t)diff);
            add_stat("dumper:last_run", 15, val, len, cookie);
        }
        len = sprintf(val, "%"PRIu64, dumper->visited);
        add_stat("dumper:visited", 14, val, len, cookie);
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
    pthread_mutex_unlock(&dumper->lock);
}

#ifdef ENABLE_PERSISTENCE_01_ITEM_SCAN
/**
 * item scan
 */
/* item scan macros */
#define MAX_ITSCAN_COUNT 10
#define MAX_ITSCAN_ITEMS 128

/* item scan structure */
typedef struct _item_scan_t {
    struct assoc_scan asscan; /* assoc scan */
    const char *prefix;
    int        nprefix;
    bool       by_chkpt;
    bool       is_used;
    struct _item_scan_t *next;
} item_scan_t;

/* static global variables */
static item_scan_t     g_itscan_array[MAX_ITSCAN_COUNT];
static item_scan_t    *g_itscan_free_list;
static uint32_t        g_itscan_free_count;
static bool            g_itscan_init = false;
static pthread_mutex_t g_itscan_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Internal item scan functions
 */
static void do_itscan_prepare(void)
{
    item_scan_t *sp; /* scan pointer */

    for (int i = 0; i < MAX_ITSCAN_COUNT; i++) {
        sp = &g_itscan_array[i];
        sp->is_used = false;
        if (i < (MAX_ITSCAN_COUNT-1)) sp->next = &g_itscan_array[i+1];
        else                          sp->next = NULL;
    }
    g_itscan_free_list = &g_itscan_array[0];
    g_itscan_free_count = MAX_ITSCAN_COUNT;
}

static item_scan_t *do_itscan_alloc(void)
{
    item_scan_t *sp; /* scan pointer */

    pthread_mutex_lock(&g_itscan_lock);
    if (g_itscan_init != true) {
        do_itscan_prepare();
        g_itscan_init = true;
    }
    if ((sp = g_itscan_free_list) != NULL) {
        g_itscan_free_list = sp->next;
        g_itscan_free_count -= 1;
    }
    pthread_mutex_unlock(&g_itscan_lock);
    return sp;
}

static void do_itscan_free(item_scan_t *sp)
{
    pthread_mutex_lock(&g_itscan_lock);
    sp->next = g_itscan_free_list;
    g_itscan_free_list = sp;
    g_itscan_free_count += 1;
    pthread_mutex_unlock(&g_itscan_lock);
}

/*
 * External item scan functions
 */
void *itscan_open(struct default_engine *engine, const char *prefix, const int nprefix, bool chkpt)
{
    item_scan_t *sp = do_itscan_alloc();
    if (sp != NULL) {
        LOCK_CACHE();
        assoc_scan_init(&sp->asscan);
#ifdef ENABLE_PERSISTENCE
        if (chkpt) {
            item_clog_set_scan(&sp->asscan);
        }
#endif
        UNLOCK_CACHE();
        sp->prefix = prefix;
        sp->nprefix = nprefix;
        sp->by_chkpt = chkpt;
        sp->is_used = true;
    }
    return (void*)sp;
}

int itscan_getnext(void *scan, void **item_array, elems_result_t *erst_array, int item_arrsz)
{
    item_scan_t *sp = (item_scan_t *)scan;
    hash_item *it;
    elems_result_t *eresult;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    rel_time_t curtime = svcore->get_current_time();
    int scan_count = item_arrsz < MAX_ITSCAN_ITEMS
                   ? item_arrsz : MAX_ITSCAN_ITEMS;
    int elem_limit = (erst_array ? 1000 : 0);
    int item_count;
    int real_count;

    LOCK_CACHE();
    item_count = assoc_scan_next(&sp->asscan, (hash_item**)item_array, scan_count, elem_limit);
    if (item_count < 0) {
        real_count = -1; /* The end of assoc scan */
    } else {
        /* Currently, item_count > 0.
         * See the internals of assoc_scan_next(). It does not return 0.
         */
        real_count = 0;
        for (int idx = 0; idx < item_count; idx++) {
            it = (hash_item *)item_array[idx];
            /* Is it an internal or invalid item ? */
            if ((it->iflag & ITEM_INTERNAL) != 0 ||
                do_item_isvalid(it, curtime) != true) {
                item_array[idx] = NULL; continue;
            }
            /* Is it not the prefix item ? */
            if (sp->nprefix >= 0) {
                if (!assoc_prefix_issame(it->pfxptr, sp->prefix, sp->nprefix)) {
                    item_array[idx] = NULL; continue;
                }
            }
            /* Found the valid items */
            if (real_count < idx) {
                item_array[real_count] = item_array[idx];
            }
            if (erst_array != NULL) {
                eresult = &erst_array[real_count];
                eresult->elem_count = 0;
                if (IS_COLL_ITEM(it)) {
                    ret = coll_elem_get_all(it, eresult, false);
                    if (ret == ENGINE_ENOMEM) break;
                }
            }
            ITEM_REFCOUNT_INCR(it);
            real_count += 1;
        }
    }
    UNLOCK_CACHE();

    if (ret == ENGINE_ENOMEM) {
        if (real_count > 0) {
            itscan_release(scan, item_array, erst_array, real_count);
        }
        real_count = -2; /* OUT OF MEMORY */
    }
    return real_count;
}

void itscan_release(void *scan, void **item_array, elems_result_t *erst_array, int item_count)
{
    if (erst_array != NULL) {
        for (int idx = 0; idx < item_count; idx++) {
            if (erst_array[idx].elem_count > 0) {
                hash_item *it = (hash_item *)item_array[idx];
                assert(IS_COLL_ITEM(it));
                coll_elem_release(&erst_array[idx], GET_ITEM_TYPE(it));
            }
        }
    }

    LOCK_CACHE();
    for (int idx = 0; idx < item_count; idx++) {
        do_item_release(item_array[idx]);
    }
    UNLOCK_CACHE();
}

void itscan_close(void *scan, bool success)
{
    item_scan_t *sp = (item_scan_t *)scan;

    LOCK_CACHE();
    assoc_scan_final(&sp->asscan);
#ifdef ENABLE_PERSISTENCE
    if (sp->by_chkpt) {
        item_clog_reset_scan(success);
    }
#endif
    UNLOCK_CACHE();

    sp->is_used = false;
    do_itscan_free(sp);
}
#endif

/*
 * MAP collection manangement
 */
static inline int map_hash_eq(const int h1, const void *v1, size_t vlen1,
                              const int h2, const void *v2, size_t vlen2)
{
    return (h1 == h2 && vlen1 == vlen2 && memcmp(v1, v2, vlen1) == 0);
}

#define MAP_GET_HASHIDX(hval, hdepth) \
        (((hval) & (MAP_HASHIDX_MASK << ((hdepth)*4))) >> ((hdepth)*4))

static ENGINE_ERROR_CODE do_map_item_find(const void *key, const uint32_t nkey,
                                          bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_MAP_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static hash_item *do_map_item_alloc(const void *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    uint32_t nbytes = 2; /* "\r\n" */
    uint32_t real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                         + sizeof(map_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, attrp->flags, attrp->exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_MAP;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), "\r\n", nbytes);

        /* initialize map meta information */
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        info->mcnt = do_coll_real_maxcount(it, attrp->maxcount);
        info->ccnt = 0;
        info->ovflact = OVFL_ERROR;
        info->mflags  = 0;
#ifdef ENABLE_STICKY_ITEM
        if (IS_STICKY_EXPTIME(attrp->exptime)) info->mflags |= COLL_META_FLAG_STICKY;
#endif
        if (attrp->readable == 1)              info->mflags |= COLL_META_FLAG_READABLE;
        info->itdist  = (uint16_t)((size_t*)info-(size_t*)it);
        info->stotal  = 0;
        info->root    = NULL;
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);
    }
    return it;
}

static map_hash_node *do_map_node_alloc(uint8_t hash_depth, const void *cookie)
{
    size_t ntotal = sizeof(map_hash_node);

    map_hash_node *node = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (node != NULL) {
        assert(node->slabs_clsid == 0);
        node->slabs_clsid = slabs_clsid(ntotal);
        node->refcount    = 0;
        node->hdepth      = hash_depth;
        node->tot_hash_cnt = 0;
        node->tot_elem_cnt = 0;
        memset(node->hcnt, 0, MAP_HASHTAB_SIZE*sizeof(uint16_t));
        memset(node->htab, 0, MAP_HASHTAB_SIZE*sizeof(void*));
    }
    return node;
}

static void do_map_node_free(map_hash_node *node)
{
    do_item_mem_free(node, sizeof(map_hash_node));
}

static map_elem_item *do_map_elem_alloc(const int nfield,
                                        const uint32_t nbytes, const void *cookie)
{
    size_t ntotal = sizeof(map_elem_item) + nfield + nbytes;

    map_elem_item *elem = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (elem != NULL) {
        assert(elem->slabs_clsid == 0);
        elem->slabs_clsid = slabs_clsid(ntotal);
        elem->refcount    = 0;
        elem->nfield      = (uint8_t)nfield;
        elem->nbytes      = (uint16_t)nbytes;
        elem->next = (map_elem_item *)ADDR_MEANS_UNLINKED; /* Unliked state */
    }
    return elem;
}

static void do_map_elem_free(map_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = do_map_elem_ntotal(elem);
    do_item_mem_free(elem, ntotal);
}

static void do_map_elem_release(map_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->next == (map_elem_item *)ADDR_MEANS_UNLINKED) {
        do_map_elem_free(elem);
    }
}

static void do_map_node_link(map_meta_info *info,
                             map_hash_node *par_node, const int par_hidx,
                             map_hash_node *node)
{
    if (par_node == NULL) {
        info->root = node;
    } else {
        map_elem_item *elem;
        int num_elems = par_node->hcnt[par_hidx];
        int num_found =0;

        while (par_node->htab[par_hidx] != NULL) {
            elem = par_node->htab[par_hidx];
            par_node->htab[par_hidx] = elem->next;

            int hidx = MAP_GET_HASHIDX(elem->hval, node->hdepth);
            elem->next = node->htab[hidx];
            node->htab[hidx] = elem;
            node->hcnt[hidx] += 1;
            num_found ++;
        }
        assert(num_found  == num_elems);
        node->tot_elem_cnt = num_found ;

        par_node->htab[par_hidx] = node;
        par_node->hcnt[par_hidx] = -1; /* child hash node */
        par_node->tot_elem_cnt -= num_found ;
        par_node->tot_hash_cnt += 1;
    }

    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(sizeof(map_hash_node));
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, stotal);
    }
}

static void do_map_node_unlink(map_meta_info *info,
                               map_hash_node *par_node, const int par_hidx)
{
    map_hash_node *node;

    if (par_node == NULL) {
        node = info->root;
        info->root = NULL;
        assert(node->tot_hash_cnt == 0);
        assert(node->tot_elem_cnt == 0);
    } else {
        assert(par_node->hcnt[par_hidx] == -1); /* child hash node */
        map_elem_item *head = NULL;
        map_elem_item *elem;
        int hidx, fcnt = 0;

        node = (map_hash_node *)par_node->htab[par_hidx];
        assert(node->tot_hash_cnt == 0);

        for (hidx = 0; hidx < MAP_HASHTAB_SIZE; hidx++) {
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
        size_t stotal = slabs_space_size(sizeof(map_hash_node));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, stotal);
    }

    /* free the node */
    do_map_node_free(node);
}

static void do_map_elem_replace(map_meta_info *info,
                                map_prev_info *pinfo, map_elem_item *new_elem)
{
    map_elem_item *prev = pinfo->prev;
    map_elem_item *old_elem;
    size_t old_stotal;
    size_t new_stotal;

    if (prev != NULL) {
        old_elem = prev->next;
    } else {
        old_elem = (map_elem_item *)pinfo->node->htab[pinfo->hidx];
    }

    old_stotal = slabs_space_size(do_map_elem_ntotal(old_elem));
    new_stotal = slabs_space_size(do_map_elem_ntotal(new_elem));

    CLOG_MAP_ELEM_INSERT(info, old_elem, new_elem);

    new_elem->next = old_elem->next;
    if (prev != NULL) {
        prev->next = new_elem;
    } else {
        pinfo->node->htab[pinfo->hidx] = new_elem;
    }

    old_elem->next = (map_elem_item *)ADDR_MEANS_UNLINKED;
    if (old_elem->refcount == 0) {
        do_map_elem_free(old_elem);
    }

    if (new_stotal != old_stotal) {
        assert(info->stotal > 0);
        if (new_stotal > old_stotal) {
            do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, (new_stotal-old_stotal));
        } else {
            do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, (old_stotal-new_stotal));
        }
    }
}

static ENGINE_ERROR_CODE do_map_elem_link(map_meta_info *info, map_elem_item *elem,
                                          const bool replace_if_exist, const void *cookie)
{
    assert(info->root != NULL);
    map_hash_node *node = info->root;
    map_elem_item *prev = NULL;
    map_elem_item *find;
    map_prev_info pinfo;
    ENGINE_ERROR_CODE res = ENGINE_SUCCESS;

    int hidx = -1;

    /* map hash value */
    elem->hval = genhash_string_hash(elem->data, elem->nfield);

    while (node != NULL) {
        hidx = MAP_GET_HASHIDX(elem->hval, node->hdepth);
        if (node->hcnt[hidx] >= 0) /* map element hash chain */
            break;
        node = node->htab[hidx];
    }
    assert(node != NULL);
    for (find = node->htab[hidx]; find != NULL; find = find->next) {
        if (map_hash_eq(elem->hval, elem->data, elem->nfield,
                        find->hval, find->data, find->nfield))
            break;
        prev = find;
    }

    if (find != NULL) {
        if (replace_if_exist) {
#ifdef ENABLE_STICKY_ITEM
            /* sticky memory limit check */
            if (IS_STICKY_COLLFLG(info)) {
                if (find->nbytes < elem->nbytes) {
                    if (do_item_sticky_overflowed())
                        return ENGINE_ENOMEM;
                }
            }
#endif
            pinfo.node = node;
            pinfo.prev = prev;
            pinfo.hidx = hidx;
            do_map_elem_replace(info, &pinfo, elem);
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_ELEM_EEXISTS;
        }
    }

#ifdef ENABLE_STICKY_ITEM
    /* sticky memory limit check */
    if (IS_STICKY_COLLFLG(info)) {
        if (do_item_sticky_overflowed())
            return ENGINE_ENOMEM;
    }
#endif

    if (node->hcnt[hidx] >= MAP_MAX_HASHCHAIN_SIZE) {
        map_hash_node *n_node = do_map_node_alloc(node->hdepth+1, cookie);
        if (n_node == NULL) {
            res = ENGINE_ENOMEM;
            return res;
        }
        do_map_node_link(info, node, hidx, n_node);

        node = n_node;
        hidx = MAP_GET_HASHIDX(elem->hval, node->hdepth);
    }

    elem->next = node->htab[hidx];
    node->htab[hidx] = elem;
    node->hcnt[hidx] += 1;
    node->tot_elem_cnt += 1;

    info->ccnt++;

    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(do_map_elem_ntotal(elem));
        do_coll_space_incr((coll_meta_info *)info, ITEM_TYPE_MAP, stotal);
    }

    return res;
}

static void do_map_elem_unlink(map_meta_info *info,
                               map_hash_node *node, const int hidx,
                               map_elem_item *prev, map_elem_item *elem,
                               enum elem_delete_cause cause)
{
    if (prev != NULL) prev->next = elem->next;
    else              node->htab[hidx] = elem->next;
    elem->next = (map_elem_item *)ADDR_MEANS_UNLINKED;
    node->hcnt[hidx] -= 1;
    node->tot_elem_cnt -= 1;
    info->ccnt--;

    CLOG_MAP_ELEM_DELETE(info, elem, cause);

    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(do_map_elem_ntotal(elem));
        do_coll_space_decr((coll_meta_info *)info, ITEM_TYPE_MAP, stotal);
    }

    if (elem->refcount == 0) {
        do_map_elem_free(elem);
    }
}

static bool do_map_elem_traverse_dfs_byfield(map_meta_info *info, map_hash_node *node, const int hval,
                                             const field_t *field, const bool delete,
                                             map_elem_item **elem_array)
{
    bool ret;
    int hidx = MAP_GET_HASHIDX(hval, node->hdepth);

    if (node->hcnt[hidx] == -1) {
        map_hash_node *child_node = node->htab[hidx];
        ret = do_map_elem_traverse_dfs_byfield(info, child_node, hval, field, delete, elem_array);
        if (ret && delete) {
            if (child_node->tot_hash_cnt == 0 &&
                child_node->tot_elem_cnt < (MAP_MAX_HASHCHAIN_SIZE/2)) {
                do_map_node_unlink(info, node, hidx);
            }
        }
    } else {
        ret = false;
        if (node->hcnt[hidx] > 0) {
            map_elem_item *prev = NULL;
            map_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                if (map_hash_eq(hval, field->value, field->length, elem->hval, elem->data, elem->nfield)) {
                    if (elem_array) {
                        elem->refcount++;
                        elem_array[0] = elem;
                    }

                    if (delete) {
                        do_map_elem_unlink(info, node, hidx, prev, elem, ELEM_DELETE_NORMAL);
                    }
                    ret = true;
                    break;
                }
                prev = elem;
                elem = elem->next;
            }
        }
    }
    return ret;
}

static int do_map_elem_traverse_dfs_bycnt(map_meta_info *info, map_hash_node *node,
                                          const uint32_t count, const bool delete,
                                          map_elem_item **elem_array, enum elem_delete_cause cause)
{
    int hidx;
    int fcnt = 0; /* found count */

    if (node->tot_hash_cnt > 0) {
        map_hash_node *child_node;
        int rcnt; /* request count */
        for (hidx = 0; hidx < MAP_HASHTAB_SIZE; hidx++) {
            if (node->hcnt[hidx] == -1) {
                child_node = (map_hash_node *)node->htab[hidx];
                rcnt = (count > 0 ? (count - fcnt) : 0);
                fcnt += do_map_elem_traverse_dfs_bycnt(info, child_node, rcnt, delete,
                                            (elem_array==NULL ? NULL : &elem_array[fcnt]), cause);
                if (delete) {
                    if  (child_node->tot_hash_cnt == 0 &&
                         child_node->tot_elem_cnt < (MAP_MAX_HASHCHAIN_SIZE/2)) {
                         do_map_node_unlink(info, node, hidx);
                     }
                }
                if (count > 0 && fcnt >= count)
                    return fcnt;
            }
        }
    }
    assert(count == 0 || fcnt < count);

    for (hidx = 0; hidx < MAP_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] > 0) {
            map_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                if (elem_array) {
                    elem->refcount++;
                    elem_array[fcnt] = elem;
                }
                fcnt++;
                if (delete) do_map_elem_unlink(info, node, hidx, NULL, elem, cause);
                if (count > 0 && fcnt >= count) break;
                elem = (delete ? node->htab[hidx] : elem->next);
            }
            if (count > 0 && fcnt >= count) break;
        }
    }
    return fcnt;
}

static uint32_t do_map_elem_delete_with_field(map_meta_info *info, const int numfields,
                                              const field_t *flist, enum elem_delete_cause cause)
{
    assert(cause == ELEM_DELETE_NORMAL);
    uint32_t delcnt = 0;

    if (info->root != NULL) {
        if (numfields == 0) {
            delcnt = do_map_elem_traverse_dfs_bycnt(info, info->root, 0, true, NULL, cause);
        } else {
            for (int ii = 0; ii < numfields; ii++) {
                int hval = genhash_string_hash(flist[ii].value, flist[ii].length);
                if (do_map_elem_traverse_dfs_byfield(info, info->root, hval, &flist[ii], true, NULL)) {
                    delcnt++;
                }
            }
        }
        if (info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
            do_map_node_unlink(info, NULL, 0);
        }
    }
    return delcnt;
}

static map_elem_item *do_map_elem_find(map_hash_node *node, const field_t *field, map_prev_info *pinfo)
{
    map_elem_item *elem = NULL;
    map_elem_item *prev = NULL;
    int hval = genhash_string_hash(field->value, field->length);
    int hidx = -1;

    while (node != NULL) {
        hidx = MAP_GET_HASHIDX(hval, node->hdepth);
        if (node->hcnt[hidx] >= 0) /* map element hash chain */
            break;
        node = node->htab[hidx];
    }
    assert(node != NULL);
    for (elem = node->htab[hidx]; elem != NULL; elem = elem->next) {
        if (map_hash_eq(hval, field->value, field->length, elem->hval, elem->data, elem->nfield)) {
            if (pinfo != NULL) {
                pinfo->node = node;
                pinfo->prev = prev;
                pinfo->hidx = hidx;
            }
            break;
        }
        prev = elem;
    }
    return elem;
}

static ENGINE_ERROR_CODE do_map_elem_update(map_meta_info *info,
                                            const field_t *field, const char *value,
                                            const uint32_t nbytes, const void *cookie)
{
    map_prev_info  pinfo;
    map_elem_item *elem;

    if (info->root == NULL) {
        return ENGINE_ELEM_ENOENT;
    }

    elem = do_map_elem_find(info->root, field, &pinfo);
    if (elem == NULL) {
        return ENGINE_ELEM_ENOENT;
    }

    if (elem->refcount == 0 && elem->nbytes == nbytes) {
        /* old body size == new body size */
        /* do in-place update */
        memcpy(elem->data + elem->nfield, value, nbytes);
        CLOG_MAP_ELEM_INSERT(info, elem, elem);
    } else {
        /* old body size != new body size */
#ifdef ENABLE_STICKY_ITEM
        /* sticky memory limit check */
        if (IS_STICKY_COLLFLG(info)) {
            if (elem->nbytes < nbytes) {
                if (do_item_sticky_overflowed())
                    return ENGINE_ENOMEM;
            }
        }
#endif

        map_elem_item *new_elem = do_map_elem_alloc(elem->nfield, nbytes, cookie);
        if (new_elem == NULL) {
            return ENGINE_ENOMEM;
        }

        /* build the new element */
        memcpy(new_elem->data, elem->data, elem->nfield);
        memcpy(new_elem->data + elem->nfield, value, nbytes);
        new_elem->hval = elem->hval;

        /* replace the element */
        do_map_elem_replace(info, &pinfo, new_elem);
    }

    return ENGINE_SUCCESS;
}

static uint32_t do_map_elem_delete(map_meta_info *info, const uint32_t count,
                                   enum elem_delete_cause cause)
{
    assert(cause == ELEM_DELETE_COLL);
    uint32_t fcnt = 0;
    if (info->root != NULL) {
        fcnt = do_map_elem_traverse_dfs_bycnt(info, info->root, count, true, NULL, cause);
        if (info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
            do_map_node_unlink(info, NULL, 0);
        }
    }
    return fcnt;
}

static ENGINE_ERROR_CODE do_map_elem_get(map_meta_info *info, const int numfields, const field_t *flist,
                                         const bool delete, map_elem_item **elem_array, uint32_t *elem_count)
{
    uint32_t array_cnt = 0;

    if (info->root != NULL) {
        if (numfields == 0) {
            array_cnt = do_map_elem_traverse_dfs_bycnt(info, info->root, 0, delete, elem_array, ELEM_DELETE_NORMAL);
        } else {
            for (int ii = 0; ii < numfields; ii++) {
                int hval = genhash_string_hash(flist[ii].value, flist[ii].length);
                if (do_map_elem_traverse_dfs_byfield(info, info->root, hval, &flist[ii],
                                                     delete, &elem_array[array_cnt])) {
                    array_cnt++;
                }
            }
        }
        if (delete && info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
            do_map_node_unlink(info, NULL, 0);
        }
    }

    *elem_count = array_cnt;
    if (array_cnt > 0) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ELEM_ENOENT;
    }
}

static ENGINE_ERROR_CODE do_map_elem_insert(hash_item *it, map_elem_item *elem,
                                            const bool replace_if_exist, const void *cookie)
{
    map_meta_info *info = (map_meta_info *)item_get_meta(it);
    int32_t real_mcnt = (info->mcnt == -1 ? max_map_size : info->mcnt);
    ENGINE_ERROR_CODE ret;

    /* overflow check */
    assert(info->ovflact == OVFL_ERROR);
    if (info->ccnt >= real_mcnt) {
        return ENGINE_EOVERFLOW;
    }

    /* create the root hash node if it does not exist */
    bool new_root_flag = false;
    if (info->root == NULL) { /* empty map */
        map_hash_node *r_node = do_map_node_alloc(0, cookie);
        if (r_node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_map_node_link(info, NULL, 0, r_node);
        new_root_flag = true;
    }

    /* insert the element */
    ret = do_map_elem_link(info, elem, replace_if_exist, cookie);
    if (ret != ENGINE_SUCCESS) {
        if (new_root_flag) {
            do_map_node_unlink(info, NULL, 0);
        }
        return ret;
    }

    CLOG_MAP_ELEM_INSERT(info, NULL, elem);
    return ENGINE_SUCCESS;
}

/*
 * MAP Interface Functions
 */
ENGINE_ERROR_CODE map_struct_create(const char *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "map struct create - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_map_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            do_item_release(it);
        }
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

map_elem_item *map_elem_alloc(const int nfield, const uint32_t nbytes, const void *cookie)
{
    map_elem_item *elem;
    LOCK_CACHE();
    elem = do_map_elem_alloc(nfield, nbytes, cookie);
    UNLOCK_CACHE();
    return elem;
}

void map_elem_free(map_elem_item *elem)
{
    LOCK_CACHE();
    assert(elem->next == (map_elem_item *)ADDR_MEANS_UNLINKED);
    do_map_elem_free(elem);
    UNLOCK_CACHE();
}

void map_elem_release(map_elem_item **elem_array, const int elem_count)
{
    int cnt = 0;
    LOCK_CACHE();
    while (cnt < elem_count) {
        do_map_elem_release(elem_array[cnt++]);
        if ((cnt % 100) == 0 && cnt < elem_count) {
            UNLOCK_CACHE();
            LOCK_CACHE();
        }
    }
    UNLOCK_CACHE();
}

ENGINE_ERROR_CODE map_elem_insert(const char *key, const uint32_t nkey,
                                  map_elem_item *elem, item_attr *attrp,
                                  bool *created, const void *cookie)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    *created = false;

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "map elem insert - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_map_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_KEY_ENOENT && attrp != NULL) {
        it = do_map_item_alloc(key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            if (ret == ENGINE_SUCCESS) {
                *created = true;
            } else {
                /* The item is to be released, below */
            }
        }
    }
    if (ret == ENGINE_SUCCESS) {
        ret = do_map_elem_insert(it, elem, false /* replace_if_exist */, cookie);
        if (ret != ENGINE_SUCCESS && *created) {
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
        }
    }
    if (it != NULL) do_item_release(it);
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE map_elem_update(const char *key, const uint32_t nkey,
                                  const field_t *field, const char *value,
                                  const uint32_t nbytes, const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "map elem update - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_map_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        ret = do_map_elem_update(info, field, value, nbytes, cookie);
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE map_elem_delete(const char *key, const uint32_t nkey,
                                  const int numfields, const field_t *flist,
                                  const bool drop_if_empty,
                                  uint32_t *del_count, bool *dropped,
                                  const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    *dropped = false;

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "map elem delete - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_map_item_find(key, nkey, DONT_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        *del_count = do_map_elem_delete_with_field(info, numfields, flist, ELEM_DELETE_NORMAL);
        if (*del_count > 0) {
            if (info->ccnt == 0 && drop_if_empty) {
                assert(info->root == NULL);
                do_item_unlink(it, ITEM_UNLINK_NORMAL);
                *dropped = true;
            }
        } else {
            ret = ENGINE_ELEM_ENOENT;
        }
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE map_elem_get(const char *key, const uint32_t nkey,
                               const int numfields, const field_t *flist,
                               const bool delete, const bool drop_if_empty,
                               struct elems_result *eresult,
                               const void *cookie)
{
    hash_item *it;
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_PERSISTENCE
    log_waiter_t *waiter = NULL;
#endif

    eresult->elem_array = NULL;
    eresult->elem_count = 0;

    LOCK_CACHE();
#ifdef ENABLE_PERSISTENCE
    if (config->use_persistence && delete) {
        waiter = cmdlog_waiter_alloc();
        if (waiter == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "map elem get - cmdlog waiter alloc fail\n");
            UNLOCK_CACHE();
            return ENGINE_ENOMEM; /* FIXME: define error code */
        }
    }
#endif
    ret = do_map_item_find(key, nkey, DO_UPDATE, &it);
    if (ret == ENGINE_SUCCESS) {
        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        do {
            if ((info->mflags & COLL_META_FLAG_READABLE) == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (numfields == 0 || info->ccnt < numfields) {
                eresult->elem_array = (eitem **)malloc(info->ccnt * sizeof(eitem*));
            } else {
                eresult->elem_array = (eitem **)malloc(numfields * sizeof(eitem*));
            }
            if (eresult->elem_array == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            ret = do_map_elem_get(info, numfields, flist, delete,
                                  (map_elem_item **)eresult->elem_array, &(eresult->elem_count));
            if (ret == ENGINE_SUCCESS) {
                if (info->ccnt == 0 && drop_if_empty) {
                    assert(delete == true);
                    do_item_unlink(it, ITEM_UNLINK_NORMAL);
                    eresult->dropped = true;
                } else {
                    eresult->dropped = false;
                }
                eresult->flags = it->flags;
            } else {
                /* ret = ENGINE_ELEM_ENOENT */
                free(eresult->elem_array);
                eresult->elem_array = NULL;
            }
        } while (0);
        do_item_release(it);
    }
#ifdef ENABLE_PERSISTENCE
    if (waiter != NULL && delete) {
        cmdlog_waiter_free(waiter);
    }
#endif
    UNLOCK_CACHE();
    return ret;
}

#ifdef ENABLE_PERSISTENCE
//#define DEBUG_ITEM_APPLY

/*
 * Apply functions by recovery.
 */
ENGINE_ERROR_CODE
item_apply_kv_link(const char *key, const uint32_t nkey,
                   const uint32_t flags, const rel_time_t exptime,
                   const uint32_t nbytes, const char *value, const uint64_t cas)
{
    ENGINE_ERROR_CODE ret;
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_kv_link."
                " key=%.*s, nbytes=%u\n", nkey, key, nbytes);
#endif

    LOCK_CACHE();
    hash_item *old_it = do_item_get(key, nkey, DONT_UPDATE);
    hash_item *new_it = do_item_alloc(key, nkey, flags, exptime, nbytes, NULL);
    if (new_it) {
        /* Assume data is small, and copying with lock held is okay */
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
            /* Override the cas with the master's cas. */
            item_set_cas(new_it, cas);
        }
        do_item_release(new_it);
    } else {
        ret = ENGINE_ENOMEM;
        if (old_it) { /* Remove inconsistent hash_item */
            do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
            do_item_release(old_it);
        }
    }
    UNLOCK_CACHE();

    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_kv_link failed."
                    " key=%.*s, code=%d\n", nkey, key, ret);
    }
    return ret;
}

ENGINE_ERROR_CODE
item_apply_list_link(const char *key, const uint32_t nkey, item_attr *attrp)
{
    ENGINE_ERROR_CODE ret;
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_list_link. key=%.*s\n", nkey, key);
#endif

    LOCK_CACHE();
    hash_item *old_it = do_item_get(key, nkey, DONT_UPDATE);
    if (old_it) {
        /* Remove the old item first. */
        do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
        do_item_release(old_it);
    }
    hash_item *new_it = do_list_item_alloc(key, nkey, attrp, NULL);
    if (new_it) {
        /* Link the new item into the hash table */
        ret = do_item_link(new_it);
        do_item_release(new_it);
    } else {
        ret = ENGINE_ENOMEM;
    }
    UNLOCK_CACHE();

    if (ret == ENGINE_SUCCESS) {
        /* The caller wants to know if the old item has been replaced.
         * This code still indicates success.
         */
        if (old_it != NULL) {
            ret = ENGINE_KEY_EEXISTS;
        }
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_list_link failed."
                    " key=%.*s, code=%d\n", nkey, key, ret);
    }
    return ret;
}

ENGINE_ERROR_CODE
item_apply_set_link(const char *key, const uint32_t nkey, item_attr *attrp)
{
    ENGINE_ERROR_CODE ret;
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_set_link. key=%.*s\n", nkey, key);
#endif

    LOCK_CACHE();
    hash_item *old_it = do_item_get(key, nkey, DONT_UPDATE);
    if (old_it) {
        /* Remove the old item first. */
        do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
        do_item_release(old_it);
    }
    hash_item *new_it = do_set_item_alloc(key, nkey, attrp, NULL);
    if (new_it) {
        /* Link the new item into the hash table */
        ret = do_item_link(new_it);
        do_item_release(new_it);
    } else {
        ret = ENGINE_ENOMEM;
    }
    UNLOCK_CACHE();

    if (ret == ENGINE_SUCCESS) {
        /* The caller wants to know if the old item has been replaced.
         * This code still indicates success.
         */
        if (old_it != NULL) {
            ret = ENGINE_KEY_EEXISTS;
        }
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_set_link failed."
                    " key=%.*s, code=%d\n", nkey, key, ret);
    }
    return ret;
}

ENGINE_ERROR_CODE
item_apply_map_link(const char *key, const uint32_t nkey, item_attr *attrp)
{
    ENGINE_ERROR_CODE ret;
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_map_link. key=%.*s\n", nkey, key);
#endif

    LOCK_CACHE();
    hash_item *old_it = do_item_get(key, nkey, DONT_UPDATE);
    if (old_it) {
        /* Remove the old item first. */
        do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
        do_item_release(old_it);
    }
    hash_item *new_it = do_map_item_alloc(key, nkey, attrp, NULL);
    if (new_it) {
        /* Link the new item into the hash table */
        ret = do_item_link(new_it);
        do_item_release(new_it);
    } else {
        ret = ENGINE_ENOMEM;
    }
    UNLOCK_CACHE();

    if (ret == ENGINE_SUCCESS) {
        /* The caller wants to know if the old item has been replaced.
         * This code still indicates success.
         */
        if (old_it != NULL) {
            ret = ENGINE_KEY_EEXISTS;
        }
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_map_link failed."
                    " key=%.*s, code=%d\n", nkey, key, ret);
    }
    return ret;
}

ENGINE_ERROR_CODE
item_apply_btree_link(const char *key, const uint32_t nkey, item_attr *attrp)
{
    ENGINE_ERROR_CODE ret;
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_btree_link. key=%.*s\n", nkey, key);
#endif

    LOCK_CACHE();
    hash_item *old_it = do_item_get(key, nkey, DONT_UPDATE);
    if (old_it) {
        /* Remove the old item first. */
        do_item_unlink(old_it, ITEM_UNLINK_NORMAL);
        do_item_release(old_it);
    }
    hash_item *new_it = do_btree_item_alloc(key, nkey, attrp, NULL);
    if (new_it) {
        /* Copy relevent fields in meta info */
        btree_meta_info *info = (btree_meta_info*)item_get_meta(new_it);
        if (attrp->trimmed) {
            info->mflags |= COLL_META_FLAG_TRIMMED; // set trimmed
        }
        if (attrp->maxbkeyrange.len != BKEY_NULL) {
            info->maxbkeyrange = attrp->maxbkeyrange;
        }
        /* Link the new item into the hash table */
        ret = do_item_link(new_it);
        do_item_release(new_it);
    } else {
        ret = ENGINE_ENOMEM;
    }
    UNLOCK_CACHE();

    if (ret == ENGINE_SUCCESS) {
        /* The caller wants to know if the old item has been replaced.
         * This code still indicates success.
         */
        if (old_it != NULL) {
            ret = ENGINE_KEY_EEXISTS;
        }
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_btree_link failed."
                    " key=%.*s, code=%d\n", nkey, key, ret);
    }
    return ret;
}

ENGINE_ERROR_CODE
item_apply_unlink(const char *key, const uint32_t nkey)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_unlink. key=%.*s\n", nkey, key);
#endif

    LOCK_CACHE();
    hash_item *it = do_item_get(key, nkey, DONT_UPDATE);
    if (it) {
        do_item_unlink(it, ITEM_UNLINK_NORMAL); /* must unlink first. */
        do_item_release(it);
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_unlink failed."
                    " not found key=%.*s", nkey, key);
        ret = ENGINE_KEY_ENOENT;
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE
item_apply_list_elem_insert(hash_item *it, const int nelems, const int index,
                            const char *value, const uint32_t nbytes)
{
    ENGINE_ERROR_CODE ret;
    const char *key = item_get_key(it);
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_list_elem_insert. key=%.*s "
                "nelems=%d index=%d.\n", it->nkey, key, nelems, index);
#endif

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_list_elem_insert failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        list_meta_info *info = (list_meta_info*)item_get_meta(it);
        if (nelems != -1 && info->ccnt != nelems) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_list_elem_insert failed."
                        " element count mismatch. ecnt(%d) != nelems(%d)\n",
                        info->ccnt, nelems);
            ret = ENGINE_EINVAL; break;
        }

        list_elem_item *elem = do_list_elem_alloc(nbytes, NULL);
        if (elem == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_list_elem_insert failed."
                        " element alloc failed. nbytes=%d\n", nbytes);
            ret = ENGINE_ENOMEM; break;
        }
        memcpy(elem->value, value, nbytes);

        ret = do_list_elem_insert(it, index, elem, NULL);
        if (ret != ENGINE_SUCCESS) {
            do_list_elem_free(elem);
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_list_elem_insert failed."
                        " key=%.*s index=%d code=%d\n",
                        it->nkey, key, index, ret);
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent hash_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE
item_apply_list_elem_delete(hash_item *it, const int nelems,
                            const int index, const int count)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    const char *key = item_get_key(it);
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_list_elem_delete. key=%.*s "
                "index=%d, count=%d\n", it->nkey, key, index, count);
#endif

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_list_elem_delete failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        list_meta_info *info = (list_meta_info*)item_get_meta(it);
        if (info->ccnt != nelems) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_list_elem_delete failed."
                        "element count mismatch. ecnt(%d) != nelems(%d)",
                        info->ccnt, nelems);
            ret = ENGINE_EINVAL; break;
        }

        uint32_t del_count = do_list_elem_delete(info, index, count, ELEM_DELETE_NORMAL);
        if (del_count == 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_list_elem_delete failed."
                        " no element deleted. key=%.*s index=%d count=%d\n",
                        it->nkey, key, index, count);
            ret = ENGINE_FAILED; break;
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent hash_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE
item_apply_set_elem_insert(hash_item *it, const char *value, const uint32_t nbytes)
{
    ENGINE_ERROR_CODE ret;
    const char *key = item_get_key(it);
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_set_elem_insert. key=%.*s\n", it->nkey, key);
#endif

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_set_elem_insert failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        set_elem_item *elem = do_set_elem_alloc(nbytes, NULL);
        if (elem == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_set_elem_insert failed."
                        " element alloc failed. nbytes=%d\n", nbytes);
            ret = ENGINE_ENOMEM; break;
        }
        memcpy(elem->value, value, nbytes);

        ret = do_set_elem_insert(it, elem, NULL);
        if (ret != ENGINE_SUCCESS) {
            do_set_elem_free(elem);
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_set_elem_insert failed."
                        " key=%.*s code=%d\n",
                        it->nkey, key, ret);
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent hash_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE
item_apply_set_elem_delete(hash_item *it, const char *value, const uint32_t nbytes)
{
    ENGINE_ERROR_CODE ret;
    const char *key = item_get_key(it);
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_set_elem_delete. key=%.*s\n", it->nkey, key);
#endif

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_set_elem_delete failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        ret = do_set_elem_delete_with_value(info, value, nbytes,
                                            ELEM_DELETE_NORMAL);
        if (ret == ENGINE_ELEM_ENOENT) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_set_elem_delete failed."
                        " no element deleted. key=%.*s\n", it->nkey, key);
            break;
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent hash_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE
item_apply_map_elem_insert(hash_item *it, const char *data, const uint32_t nfield, const uint32_t nbytes)
{
    ENGINE_ERROR_CODE ret;
    const char *key = item_get_key(it);
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_map_elem_insert. key=%.*s "
                "nfield=%u field=%.*s\n", it->nkey, key, nfield, nfield, data);
#endif

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_map_elem_insert failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        map_elem_item *elem = do_map_elem_alloc(nfield, nbytes, NULL);
        if (elem == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_map_elem_insert failed."
                        " element alloc failed. nfield=%d nbytes=%d\n", nfield, nbytes);
            ret = ENGINE_ENOMEM; break;
        }
        memcpy(elem->data, data, nfield + nbytes);

        ret = do_map_elem_insert(it, elem, true /* replace_if_exist */,  NULL);
        if (ret != ENGINE_SUCCESS) {
            do_map_elem_free(elem);
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_map_elem_insert failed."
                        " key=%.*s nfield=%d field=%.*s code=%d\n",
                        it->nkey, key, nfield, nfield, data + it->nkey, ret);
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent has_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE
item_apply_map_elem_delete(hash_item *it, const char *field, const uint32_t nfield)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    const char *key = item_get_key(it);
    field_t flist;
    flist.value = (char*)field;
    flist.length = nfield;
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_map_elem_delete. key=%.*s field=%.*s\n",
                it->nkey, key, nfield, field);
#endif

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_map_elem_delete failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        map_meta_info *info = (map_meta_info *)item_get_meta(it);
        if (info->ccnt == 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_map_elem_delete failed."
                        " no element.\n");
            ret = ENGINE_ELEM_ENOENT; break;
        }

        uint32_t del_count = do_map_elem_delete_with_field(info, 1, &flist, ELEM_DELETE_NORMAL);
        if (del_count == 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_map_elem_delete failed."
                        " no element deleted. key=%.*s field=%.*s\n",
                        it->nkey, key, nfield, field);
            ret = ENGINE_FAILED; break;
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent hash_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE
item_apply_btree_elem_insert(hash_item *it, const char *data, const uint32_t nbkey,
                             const uint32_t neflag, const uint32_t nbytes)
{
    ENGINE_ERROR_CODE ret;
    bool replaced;
    const char *key = item_get_key(it);
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_btree_elem_insert. key=%.*s "
                "nbkey=%u bkey=%.*s\n", it->nkey, key, nbkey, nbkey, data);
#endif

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_btree_elem_insert failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        btree_elem_item *elem = do_btree_elem_alloc(nbkey, neflag, nbytes, NULL);
        if (elem == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_btree_elem_insert failed."
                        " element alloc failed. nbkey=%d neflag=%d nbytes=%d\n", nbkey, neflag, nbytes);
            ret = ENGINE_ENOMEM; break;
        }
        memcpy(elem->data, data, BTREE_REAL_NBKEY(nbkey) + neflag + nbytes);

        ret = do_btree_elem_insert(it, elem, true /* replace_if_exist */,
                                   &replaced, NULL, NULL, NULL);
        if (ret != ENGINE_SUCCESS) {
            do_btree_elem_free(elem);
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_btree_elem_insert failed."
                        " key=%.*s nbkey=%d bkey=%.*s code=%d\n",
                        it->nkey, key, nbkey, nbkey, (data + it->nkey), ret);
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent has_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE
item_apply_btree_elem_delete(hash_item *it, const char *bkey, const uint32_t nbkey)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    const char *key = item_get_key(it);

    bkey_range bkrange;
    /* one-element range */
    memcpy(bkrange.from_bkey, bkey, BTREE_REAL_NBKEY(nbkey));
    bkrange.from_nbkey = nbkey;
    /* bkey_range.to_bkey */
    bkrange.to_nbkey = BKEY_NULL;
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_btree_elem_delete. key=%.*s bkey=%.*s\n",
                it->nkey, key, nbkey, bkey);
#endif

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_btree_elem_delete failed."
                        " invalid item.\n");
            ret = ENGINE_KEY_ENOENT; break;
        }

        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        if (info->ccnt == 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_btree_elem_delete failed."
                        " no element.\n");
            ret = ENGINE_ELEM_ENOENT; break;
        }
        if ((info->bktype == BKEY_TYPE_UINT64 && bkrange.from_nbkey > 0) ||
            (info->bktype == BKEY_TYPE_BINARY && bkrange.from_nbkey == 0)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_btree_elem_delete failed."
                        " bkey mismatch. key=%.*s bkey=%.*s\n", it->nkey, key, nbkey, bkey);
            ret = ENGINE_EBADBKEY; break;
        }

        uint32_t del_count = do_btree_elem_delete(info, BKEY_RANGE_TYPE_SIN, &bkrange,
                                                  NULL, 0, NULL, ELEM_DELETE_NORMAL);
        if (del_count == 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_btree_elem_delete failed."
                        " no element deleted. key=%.*s bkey=%.*s", it->nkey, key, nbkey, bkey);
            ret = ENGINE_FAILED; break;
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent has_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    return ret;
}

ENGINE_ERROR_CODE
item_apply_setattr_exptime(const char *key, const uint32_t nkey, rel_time_t exptime)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_setattr_exptime. key=%.*s\n", nkey, key);
#endif

    LOCK_CACHE();
    hash_item *it = do_item_get(key, nkey, DONT_UPDATE);
    if (it) {
        it->exptime = exptime;
        do_item_release(it);
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_setattr_exptime failed."
                    " not found key=%.*s\n", nkey, key);
        ret = ENGINE_KEY_ENOENT;
    }
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE
item_apply_setattr_meta_info(hash_item *it, const uint8_t ovflact, const uint8_t mflags,
                             rel_time_t exptime, const int32_t mcnt, bkey_t *maxbkeyrange)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    const char *key = item_get_key(it);
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_setattr_meta_info. key=%.*s\n", it->nkey, key);
#endif

    LOCK_CACHE();
    do {
        if (!item_is_valid(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_setattr_meta_info failed."
                        " invalid item. key=%.*s\n", it->nkey, key);
            ret = ENGINE_KEY_ENOENT; break;
        }
        if (!IS_COLL_ITEM(it)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_setattr_meta_info failed."
                        " The item is not a collection. key=%.*s\n", it->nkey, key);
            ret = ENGINE_EBADTYPE; break;
        }
        if (maxbkeyrange != NULL && (it->iflag & ITEM_IFLAG_BTREE) == 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item_apply_setattr_meta_info failed."
                        " The item is not a btree. key=%.*s\n", it->nkey, key);
            ret = ENGINE_EBADTYPE; break;
        }
        coll_meta_info *info = (coll_meta_info*)item_get_meta(it);
        info->mcnt = mcnt;
        info->ovflact = ovflact;
        info->mflags = mflags;
        it->exptime = exptime;
        if (maxbkeyrange) {
            ((btree_meta_info*)info)->maxbkeyrange = *maxbkeyrange;
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) { /* Remove inconsistent has_item */
        do_item_unlink(it, ITEM_UNLINK_NORMAL);
        do_item_release(it);
    }
    UNLOCK_CACHE();

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE
item_apply_flush(const char *prefix, const int nprefix)
{
    ENGINE_ERROR_CODE ret;
#ifdef DEBUG_ITEM_APPLY
    logger->log(EXTENSION_LOG_INFO, NULL, "item_apply_flush. prefix=%s nprefix=%d\n",
                prefix ? prefix : "<null>", nprefix);
#endif

    LOCK_CACHE();
    ret = do_item_flush_expired(prefix, nprefix, 0 /* right now */, NULL);
    UNLOCK_CACHE();
    return ret;
}

#endif
