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
#include <sys/time.h> /* gettimeofday() */

/* Dummy PERSISTENCE_ACTION Macros */
#define PERSISTENCE_ACTION_BEGIN(a, b)
#define PERSISTENCE_ACTION_END(a)

#include "default_engine.h"
#include "item_clog.h"

#ifdef REORGANIZE_ITEM_BASE
#else
/* Forward Declarations */
static void do_item_unlink(hash_item *it, enum item_unlink_cause cause);
#endif

#ifdef REORGANIZE_ITEM_COLL // SET, MAP
#else
/* used by set and map collection */
extern int genhash_string_hash(const void* p, size_t nkey);
#endif

#ifdef REORGANIZE_ITEM_BASE
#else
/* We only reposition items in the LRU queue if they haven't been repositioned
 * in this many seconds. That saves us from churning on frequently-accessed
 * items.
 */
#define ITEM_UPDATE_INTERVAL 60

/* How long an object can reasonably be assumed to be locked before
 * harvesting it on a low memory condition.
 */
#define TAIL_REPAIR_TIME (3 * 3600)
#endif

#ifdef REORGANIZE_ITEM_COLL // BTREE
#else
/* bkey type */
#define BKEY_TYPE_UNKNOWN 0
#define BKEY_TYPE_UINT64  1
#define BKEY_TYPE_BINARY  2

/* get bkey real size */
#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))

/* bkey range type */
#define BKEY_RANGE_TYPE_SIN 1 /* single bkey */
#define BKEY_RANGE_TYPE_ASC 2 /* ascending bkey range */
#define BKEY_RANGE_TYPE_DSC 3 /* descending bkey range */

/* btree item status */
#define BTREE_ITEM_STATUS_USED   2
#define BTREE_ITEM_STATUS_UNLINK 1
#define BTREE_ITEM_STATUS_FREE   0

/* overflow type */
#define OVFL_TYPE_NONE  0
#define OVFL_TYPE_COUNT 1
#define OVFL_TYPE_RANGE 2

/* btree scan direction */
#define BTREE_DIRECTION_PREV 2
#define BTREE_DIRECTION_NEXT 1
#define BTREE_DIRECTION_NONE 0

/* btree element item or btree node item */
#define BTREE_GET_ELEM_ITEM(node, indx) ((btree_elem_item *)((node)->item[indx]))
#define BTREE_GET_NODE_ITEM(node, indx) ((btree_indx_node *)((node)->item[indx]))

/* btree element position */
typedef struct _btree_elem_posi {
    btree_indx_node *node;
    uint16_t         indx;
    /* It is used temporarily in order to check
     * if the found bkey is equal to from_bkey or to_bkey of given bkey range
     * in the do_btree_find_first/next/prev functions.
     */
    bool             bkeq;
} btree_elem_posi;

/* btree scan structure */
typedef struct _btree_scan_info {
    hash_item       *it;
    btree_elem_posi  posi;
    uint32_t         kidx; /* An index in the given key array as a parameter */
    int32_t          next; /* for free scan link */
} btree_scan_info;

/* btree position debugging */
static bool btree_position_debug = false;

/* bkey min & max value */
static uint64_t      bkey_uint64_min;
static uint64_t      bkey_uint64_max;
static unsigned char bkey_binary_min[MIN_BKEY_LENG];
static unsigned char bkey_binary_max[MAX_BKEY_LENG];
#endif

#ifdef REORGANIZE_ITEM_BASE
#else
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
#endif

static struct default_engine *engine=NULL;
static struct engine_config *config=NULL; // engine config
static struct items         *itemsp=NULL;
#ifdef REORGANIZE_ITEM_BASE
#else
static struct engine_stats  *statsp=NULL;
static SERVER_STAT_API      *svstat=NULL; // server stat api
#endif
static SERVER_CORE_API      *svcore=NULL; // server core api
static EXTENSION_LOGGER_DESCRIPTOR *logger;

#ifdef REORGANIZE_ITEM_COLL // BTREE
#else
/* Temporary Facility
 * forced btree overflow action
 */
static char    forced_action_prefix[256];
static int32_t forced_action_pfxlen = 0;
static uint8_t forced_btree_ovflact = 0;

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

static void _setif_forced_btree_overflow_action(btree_meta_info *info,
                                                const void *key, const uint32_t nkey)
{
    if (forced_btree_ovflact != 0 && forced_action_pfxlen < nkey &&
        memcmp(forced_action_prefix, key, forced_action_pfxlen) == 0) {
        info->ovflact = forced_btree_ovflact;
    }
}
#endif

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

#ifdef REORGANIZE_ITEM_BASE
#else
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

#ifdef ENABLE_STICKY_ITEM
static inline bool do_item_sticky_overflowed(void)
{
    if (statsp->sticky_bytes < config->sticky_limit) {
        return false;
    }
    return true;
}
#endif

static void do_coll_space_incr(coll_meta_info *info, ENGINE_ITEM_TYPE item_type,
                               const size_t nspace)
{
    info->stotal += nspace;

    hash_item *it = (hash_item*)COLL_GET_HASH_ITEM(info);
    do_item_stat_bytes_incr(it, nspace);
    prefix_bytes_incr(it->pfxptr, item_type, nspace);
}

static void do_coll_space_decr(coll_meta_info *info, ENGINE_ITEM_TYPE item_type,
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

static void do_item_mem_free(void *item, size_t ntotal)
{
    hash_item *it = (hash_item *)item;
    unsigned int clsid = it->slabs_clsid;
    it->slabs_clsid = 0; /* to notify the item memory is freed */
    slabs_free(it, ntotal, clsid);
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

static ENGINE_ERROR_CODE do_item_link(hash_item *it)
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

static void do_item_replace(hash_item *old_it, hash_item *new_it)
{
    MEMCACHED_ITEM_REPLACE(item_get_key(old_it), old_it->nkey, old_it->nbytes,
                           item_get_key(new_it), new_it->nkey, new_it->nbytes);

    assert((old_it->iflag & ITEM_LINKED) != 0);

    CLOG_ITEM_UNLINK(old_it, ITEM_UNLINK_REPLACE);

    /* unlink the item from LUR list */
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
#endif

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

#ifdef REORGANIZE_ITEM_COLL // LIST
#else
/*
 * LIST collection management
 */
static inline uint32_t do_list_elem_ntotal(list_elem_item *elem)
{
    return sizeof(list_elem_item) + elem->nbytes;
}

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

static int32_t do_list_real_maxcount(int32_t maxcount)
{
    int32_t real_maxcount = maxcount;

    if (maxcount < 0) {
        /* It has the max_list_size that can be increased in the future */
        real_maxcount = -1;
    } else if (maxcount == 0) {
        real_maxcount = DEFAULT_LIST_SIZE;
    } else if (maxcount > config->max_list_size) {
        real_maxcount = config->max_list_size;
    }
    return real_maxcount;
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
        info->mcnt = do_list_real_maxcount(attrp->maxcount);
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

static uint32_t do_list_elem_get(list_meta_info *info,
                                 const int index, const uint32_t count,
                                 const bool forward, const bool delete,
                                 list_elem_item **elem_array)
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

    return fcnt;
}

static ENGINE_ERROR_CODE do_list_elem_insert(hash_item *it,
                                             int index, list_elem_item *elem,
                                             const void *cookie)
{
    list_meta_info *info = (list_meta_info *)item_get_meta(it);
    uint32_t real_mcnt = (info->mcnt > 0 ? info->mcnt : config->max_list_size);
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
#endif

#ifdef REORGANIZE_ITEM_COLL // SET
#else
/*
 * SET collection manangement
 */
//#define SET_DELETE_NO_MERGE

#define SET_GET_HASHIDX(hval, hdepth) \
        (((hval) & (SET_HASHIDX_MASK << ((hdepth)*4))) >> ((hdepth)*4))

static inline int set_hash_eq(const int h1, const void *v1, size_t vlen1,
                              const int h2, const void *v2, size_t vlen2)
{
    return (h1 == h2 && vlen1 == vlen2 && memcmp(v1, v2, vlen1) == 0);
}

static inline uint32_t do_set_elem_ntotal(set_elem_item *elem)
{
    return sizeof(set_elem_item) + elem->nbytes;
}

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

static int32_t do_set_real_maxcount(int32_t maxcount)
{
    int32_t real_maxcount = maxcount;

    if (maxcount < 0) {
        /* It has the max_set_size that can be increased in the future */
        real_maxcount = -1;
    } else if (maxcount == 0) {
        real_maxcount = DEFAULT_SET_SIZE;
    } else if (maxcount > config->max_set_size) {
        real_maxcount = config->max_set_size;
    }
    return real_maxcount;
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
        info->mcnt = do_set_real_maxcount(attrp->maxcount);
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

static uint32_t do_set_elem_get(set_meta_info *info,
                                const uint32_t count, const bool delete,
                                set_elem_item **elem_array)
{
    assert(info->root);
    uint32_t fcnt;

    if (delete) {
        CLOG_ELEM_DELETE_BEGIN((coll_meta_info*)info, count, ELEM_DELETE_NORMAL);
    }
    fcnt = do_set_elem_traverse_dfs(info, info->root, count, delete, elem_array);
    if (delete && info->root->tot_hash_cnt == 0 && info->root->tot_elem_cnt == 0) {
        do_set_node_unlink(info, NULL, 0);
    }
    if (delete) {
        CLOG_ELEM_DELETE_END((coll_meta_info*)info, ELEM_DELETE_NORMAL);
    }
    return fcnt;
}

static ENGINE_ERROR_CODE do_set_elem_insert(hash_item *it, set_elem_item *elem,
                                            const void *cookie)
{
    set_meta_info *info = (set_meta_info *)item_get_meta(it);
    uint32_t real_mcnt = (info->mcnt > 0 ? info->mcnt : config->max_set_size);
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
#endif

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
                if (nprefix < 0 || prefix_issame(iter->pfxptr, prefix, nprefix)) {
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
                if (nprefix < 0 || prefix_issame(iter->pfxptr, prefix, nprefix)) {
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

    eresult->elem_count = 0;

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
            if (sp->nprefix >= 0 && !prefix_issame(it->pfxptr, sp->prefix, sp->nprefix)) {
                item_array[i] = NULL; continue;
            }
            /* Found the valid item */
            if (erst_array != NULL && IS_COLL_ITEM(it)) {
                ret = coll_elem_get_all(it, &erst_array[nfound], false);
                if (ret  == ENGINE_ENOMEM) break;
            }
            ITEM_REFCOUNT_INCR(it);
            if (nfound < i) {
                item_array[nfound] = it;
            }
            nfound += 1;
        }
        item_count = nfound;
    } else {
        /* item_count == 0: not found element */
        /* item_count <  0: the end of scan */
    }
    UNLOCK_CACHE();

    if (ret != ENGINE_SUCCESS) {
        if (item_count > 0)
            item_scan_release(sp, item_array, erst_array, item_count);
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
        item_stop_dump(engine);
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
