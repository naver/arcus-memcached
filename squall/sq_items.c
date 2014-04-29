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
#include <pthread.h>

#include "trace.h"
#include <memcached/util.h>
#include "squall_config.h"
#include "sq_buffer.h"
#include "sq_prefix.h"
#include "sq_assoc.h"
#include "sq_items.h"
#include "sq_log.h"

#if 1 // ENABLE_SQUALL_ENGINE
/*** Item Internal Flags: 16 bits ***
 * the lower 8 bits is reserved for the core server,
 * the upper 8 bits is reserved for engine implementation.
 ************************************/
#define ITEM_WITH_CAS       1
#define ITEM_IFLAG_LIST     2   /* list item */
#define ITEM_IFLAG_SET      4   /* set item */
#define ITEM_IFLAG_BTREE    8   /* b+tree item */
#define ITEM_IFLAG_COLL    14   /* collection item: list/set/b+tree */

#if 0
#define ITEM_LINKED     (1<<8)
#define ITEM_SLABBED    (2<<8)
#define ITEM_DELMARK    (4<<8)
#endif

/* collection meta data */
#define META_OFFSET_IN_ITEM(nkey,nbytes) ((((nkey)+(nbytes)-1)/8+1)*8)

/* item write status */
enum item_wrstat {
    ITEM_WRSTAT_NONE    = 0,
    ITEM_WRSTAT_ALLOC   = 1,
    ITEM_WRSTAT_LINK    = 2,
    ITEM_WRSTAT_UNLINK  = 3,
    ITEM_WRSTAT_UPDDATA = 4,
    ITEM_WRSTAT_UPDATTR = 5,
    ITEM_WRSTAT_DELMARK = 6,
    ITEM_WRSTAT_MAX
};
#endif

/*
 * Internal data structures and defines
 */

#if 0 // ENABLE_SQUALL_ENGINE
typedef struct _btree_elem_item_fixed {
    unsigned short refcount;     /* reference count */
    uint8_t  slabs_clsid;        /* which slab class we're in */
    uint8_t  status;             /* 3(used), 2(insert mark), 1(delete_mark), or 0(free) */
    uint8_t  nbkey;              /* length of bkey */
    uint8_t  neflag;             /* length of element flag */
    uint16_t nbytes;             /**< The total size of the data (in bytes) */
} btree_elem_item_fixed;

typedef struct _list_meta_info {
    int32_t  mcnt;      /* maximum count */
    int32_t  ccnt;      /* current count */
    uint8_t  ovflact;   /* overflow action */
    uint8_t  sticky;    /* sticky collection */
    uint8_t  readable;  /* readable collection */
    uint8_t  reserved;
    uint32_t stotal;    /* total space */
    void    *prefix;
    list_elem_item *head;
    list_elem_item *tail;
} list_meta_info;

#define SET_HASHTAB_SIZE 16
#define SET_HASHIDX_MASK 0x0000000F
#define SET_MAX_HASHCHAIN_SIZE 64

typedef struct _set_hash_node {
    unsigned short refcount;      /* reference count */
    uint8_t  slabs_clsid;         /* which slab class we're in */
    uint8_t  hdepth;
    uint16_t tot_elem_cnt;
    uint16_t tot_hash_cnt;
    int16_t  hcnt[SET_HASHTAB_SIZE];
    void    *htab[SET_HASHTAB_SIZE];
} set_hash_node;

typedef struct _set_meta_info {
    int32_t  mcnt;      /* maximum count */
    int32_t  ccnt;      /* current count */
    uint8_t  ovflact;   /* overflow action */
    uint8_t  sticky;    /* sticky collection */
    uint8_t  readable;  /* readable collection */
    uint8_t  reserved;
    uint32_t stotal;    /* total space */
    void    *prefix;
    set_hash_node *root;
} set_meta_info;

#define BTREE_MAX_DEPTH  5
#define BTREE_ITEM_COUNT 32 /* Recommend BTREE_ITEM_COUNT >= 8 */
typedef struct _btree_indx_node {
    unsigned short refcount;   /* reference count */
    uint8_t  slabs_clsid;      /* which slab class we're in */
    uint8_t  ndepth;
    uint16_t used_count;
    uint16_t reserved;
    struct _btree_indx_node *prev;
    struct _btree_indx_node *next;
    void *item[BTREE_ITEM_COUNT];
} btree_indx_node;

typedef struct _btree_meta_info {
    int32_t  mcnt;      /* maximum count */
    int32_t  ccnt;      /* current count */
    uint8_t  ovflact;   /* overflow action */
    uint8_t  sticky;    /* sticky collection */
    uint8_t  readable;  /* readable collection */
    uint8_t  bktype;    /* bkey type : BKEY_TYPE_UINT64 or BKEY_TYPE_BINARY */
    uint32_t stotal;    /* total space */
    void    *prefix;
    bkey_t   maxbkeyrange;
    btree_indx_node *root;
} btree_meta_info;

typedef struct _btree_elem_posi {
    btree_indx_node *node;
    uint16_t         indx;
} btree_elem_posi;

typedef struct _btree_scan_info {
    hash_item       *it;
    btree_elem_posi  posi;
    uint32_t         kidx; /* An index in the given key array as a parameter */
} btree_scan_info;

/* common meta info of list and set */
typedef struct _coll_meta_info {
    int32_t  mcnt;      /* maximum count */
    int32_t  ccnt;      /* current count */
    uint8_t  ovflact;   /* overflow action */
    uint8_t  sticky;    /* sticky collection */
    uint8_t  readable;  /* readable collection */
    uint8_t  reserved;
    uint32_t stotal;    /* total space */
    void    *prefix;
} coll_meta_info;
#endif

/* Forward Declarations */

#define ENABLE_FLUSH_LOGGING

extern int genhash_string_hash(const void* p, size_t nkey);

/* collection type check */
#define IS_LIST_ITEM(it)  (((it)->iflag & ITEM_IFLAG_LIST) != 0)
#define IS_SET_ITEM(it)   (((it)->iflag & ITEM_IFLAG_SET) != 0)
#define IS_BTREE_ITEM(it) (((it)->iflag & ITEM_IFLAG_BTREE) != 0)
#define IS_COLL_ITEM(it)  (((it)->iflag & ITEM_IFLAG_COLL) != 0)

/* address for unlinked */
#define ADDR_MEANS_UNLINKED  1

/* BTREE RELATED DEFINES */
#define BTREE_ITEM_STATUS_USED   2
#define BTREE_ITEM_STATUS_UNLINK 1
#define BTREE_ITEM_STATUS_FREE   0

#define BKEY_TYPE_UNKNOWN 0
#define BKEY_TYPE_UINT64  1
#define BKEY_TYPE_BINARY  2

#define BKEY_MIN_BINARY_LENG 1
#define BKEY_MAX_BINARY_LENG MAX_BKEY_LENG

#define BTREE_UINT64_MIN_BKEY 0
#define BTREE_UINT64_MAX_BKEY (uint64_t)((int64_t)-1) /* need check */

#define BTREE_DIRECTION_PREV 2
#define BTREE_DIRECTION_NEXT 1
#define BTREE_DIRECTION_NONE 0

#define BTREE_GET_ELEM_ITEM(node, indx) \
        ((btree_elem_item *)((node)->item[indx]))
#define BTREE_GET_NODE_ITEM(node, indx) \
        ((btree_indx_node *)((node)->item[indx]))

#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))
#define BTREE_ELEM_SIZE(elem) \
        (sizeof(btree_elem_item_fixed) + BTREE_REAL_NBKEY(elem->nbkey) + elem->neflag + elem->nbytes)

#define OVFL_TYPE_NONE  0
#define OVFL_TYPE_COUNT 1
#define OVFL_TYPE_RANGE 2

#if 0
static uint64_t      btree_uint64_min_bkey = BTREE_UINT64_MIN_BKEY;
static uint64_t      btree_uint64_max_bkey = BTREE_UINT64_MAX_BKEY;
static unsigned char btree_binary_min_bkey[BKEY_MIN_BINARY_LENG] = { 0x00 };
static unsigned char btree_binary_max_bkey[BKEY_MAX_BINARY_LENG] = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                                                     0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                                                     0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                                                     0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };
#endif

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

#define ITEM_TYPE_ANY ITEM_TYPE_UNKNOWN

#define HASH_ITEM_CHUNK_SIZE 128
#define PAGE_ITEM_CHUNK_SIZE 128

typedef struct _hitm_bucket {
    pthread_mutex_t     lock;
    hash_item          *h_next; /* single linked list of hash items */
} hitm_BUCKET;

typedef struct _hitm_chunk {
    struct _hitm_chunk *c_next;
    hash_item           item[HASH_ITEM_CHUNK_SIZE];
} hitm_CHUNK;

typedef struct _hitm_free_list {
    pthread_mutex_t lock;
    hash_item      *itm_head;   /* free hash item list */
    hitm_CHUNK     *chn_head;   /* hash item chunk list */
    uint32_t        itm_count;  /* # of free hash items */
    uint32_t        chn_count;  /* # of hash item chunks */
} hitm_FREE_LIST;

typedef struct _item_del_queue {
    hash_item      *head;
    hash_item      *tail;
    uint32_t        size;
} item_DEL_QUEUE;

typedef struct _item_gc_thread {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    bool            sleep;
    bool            start;
    bool            init; /* start or stop reuqest */
} item_GC_THREAD;

typedef struct _item_stat_wrap {
   pthread_mutex_t  lock;
   struct item_stat stat;
} item_STAT_WARP;

typedef struct _item_scrubber {
   pthread_mutex_t   lock;
   struct item_scrub stat;
} item_SCRUBBER;

struct item_global {
    hitm_BUCKET    *hash_tab;
    hitm_FREE_LIST  hitm_free_list;
    item_DEL_QUEUE  del_queue;
    item_GC_THREAD  gc_thread;
    item_STAT_WARP  stats;
    item_SCRUBBER   scrubber;
    uint32_t        hash_size;
    uint32_t        page_body_size;
    uint32_t        max_base_slot_size;
    uint32_t        max_ovfl_data_size;
    bool            use_item_cache;
    volatile bool   initialized;
};

static struct config    *config = NULL;
static SERVER_HANDLE_V1 *server = NULL;
static EXTENSION_LOGGER_DESCRIPTOR *logger = NULL;
static struct item_global item_gl; /* item global data */

/* Get the next CAS id for a new item. */
static uint64_t get_cas_id(void) {
    static uint64_t cas_id = 0;
    return ++cas_id;
}

static int do_item_type_get(const data_item *item)
{
    if (! IS_COLL_ITEM(item))     return ITEM_TYPE_KV;
    else if (IS_LIST_ITEM(item))  return ITEM_TYPE_LIST;
    else if (IS_SET_ITEM(item))   return ITEM_TYPE_LIST;
    else if (IS_BTREE_ITEM(item)) return ITEM_TYPE_BTREE;
    else                          return ITEM_TYPE_UNKNOWN;
}

/* warning: don't use these macros with a function, as it evals its arg twice */
#if 1 // ENABLE_SQUALL_ENGINE
static inline size_t ITEM_stotal(const data_item *item, const bool inclusive)
{
    size_t ntotal = sizeof(data_item) + GET_8_ALIGN_SIZE(item->nkey + item->ndata);
    if (item->iflag & ITEM_WITH_CAS) {
        ntotal += sizeof(uint64_t);
    }
    if (item->novfl > 0) {
        ntotal += (item->novfl * sizeof(ItemID));
        if (inclusive)
            ntotal += (item->novfl * config->sq.data_page_size);
    }
    return ntotal;
}

static inline size_t ITEM_ovfl_stotal(const ovfl_item *item)
{
    return GET_8_ALIGN_SIZE(sizeof(ovfl_item) + item->ndata + 1);
}
#else
static inline size_t ITEM_ntotal(struct squall_engine *engine,
                                 const hash_item *item) {
    size_t ret;
    if (IS_COLL_ITEM(item)) {
        ret = sizeof(*item) + META_OFFSET_IN_ITEM(item->nkey,item->nbytes);
        if (IS_LIST_ITEM(item))
            ret += sizeof(list_meta_info);
        else if (IS_SET_ITEM(item))
            ret += sizeof(set_meta_info);
        else /* IS_BTREE_ITEM(item)) */
            ret += sizeof(btree_meta_info);
    } else {
        ret = sizeof(*item) + item->nkey + item->nbytes;
    }
    if (engine->config.use_cas) {
        ret += sizeof(uint64_t);
    }

    return ret;
}

static inline size_t ITEM_stotal(struct squall_engine *engine,
                                 const hash_item *item, const bool inclusive) {
    size_t ntotal = ITEM_ntotal(engine, item);
    size_t stotal = slabs_space_size(engine, ntotal);
    if (inclusive && IS_COLL_ITEM(item)) {
        coll_meta_info *info = (coll_meta_info *)item_get_meta(item);
        stotal += info->stotal;
    }
    return stotal;
}
#endif

#if 1 // ENABLE_SQUALL_ENGINE
#else
static void increase_collection_space(struct squall_engine *engine,
                                      coll_meta_info *info, const size_t inc_space)
{
    info->stotal += inc_space;
    /* Currently, stats.lock is useless since global cache lock is held. */
    //pthread_mutex_lock(&engine->stats.lock);
    if (info->sticky) engine->stats.sticky_bytes += inc_space;
    assoc_prefix_update_size(info->prefix, inc_space, PLUS_SIZE);
    engine->stats.curr_bytes += inc_space;
    //pthread_mutex_unlock(&engine->stats.lock);
}

static void decrease_collection_space(struct squall_engine *engine,
                                      coll_meta_info *info, const size_t dec_space)
{
    info->stotal -= dec_space;
    /* Currently, stats.lock is useless since global cache lock is held. */
    //pthread_mutex_lock(&engine->stats.lock);
    if (info->sticky) engine->stats.sticky_bytes -= dec_space;
    assoc_prefix_update_size(info->prefix, dec_space, MINUS_SIZE);
    engine->stats.curr_bytes -= dec_space;
    //pthread_mutex_unlock(&engine->stats.lock);
}
#endif

#if 1 // ENABLE_SQUALL_ENGINE
#if 0 // will be used later
static bool do_item_isvalid(ItemDesc *itdesc, rel_time_t current_time)
{
    data_item *dit = itdesc->ptr;
    assert(dit != NULL);

    /* expiration time check */
    if (dit->exptime != 0 && dit->exptime <= current_time) {
        return false; /* expired */
    }
    return true;
}
#endif
#else
static bool do_item_isvalid(struct squall_engine *engine, hash_item *it,
                            rel_time_t current_time) {
    assert(it != NULL);

    /* expiration time check */
    /* The sticky item has an exptime((rel_tiem_t)(-1)) larger than current_item */
    if (it->exptime != 0 && it->exptime <= current_time) {
        return false; /* expired */
    }
    if (engine->config.junk_item_time != 0) {
        if ((current_time - it->time) > engine->config.junk_item_time)
            return false; /* Not accessed for a very long time => junk item */
    }
    /* flush check */
    if (engine->config.oldest_live != 0 && engine->config.oldest_live <= current_time
        && it->time <= engine->config.oldest_live) {
        return false; /* flushed by flush_all */
    }
    return true; /* Yes, it's a valid item */
}
#endif

#if 1 // ENABLE_SQUALL_ENGINE
#if 0 // will be used later
static bool do_item_isstale(ItemDesc *itdesc)
{
    data_item *dit = itdesc->ptr;
    assert(dit != NULL);
#ifdef ENABLE_CLUSTER_AWARE
    if (! server->core->is_my_key(item_get_key(dit), dit->nkey)) {
        return true; /* stale data */
    }
#endif
    return false; /* not-stale data */
}
#endif
#else
static bool do_item_isstale(struct squall_engine *engine, hash_item *it)
{
    assert(it != NULL);
#ifdef ENABLE_CLUSTER_AWARE
    if (! engine->server.core->is_my_key(item_get_key(it),it->nkey)) {
        return true; /* stale data */
    }
#endif
    return false; /* not-stale data */
}
#endif

/*@null@*/

#if 1 // ENABLE_SQUALL_ENGINE
static void *do_item_temp_space_alloc(size_t size)
{
    return malloc(size);
}

static void do_item_temp_space_free(void *ptr)
{
    free(ptr);
}
#endif

static ENGINE_ERROR_CODE do_item_alloc(const void *key, const size_t nkey, const size_t nbytes,
                                       const uint32_t flags, const rel_time_t exptime,
                                       ItemDesc *itdesc)
//                                       ENGINE_ITEM_TYPE type, ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    int         pfxid;
    bool        use_cas_flag;
    uint32_t    nbase, novfl, nrest, ndata;
    uint32_t    base_slot_size;
    ItemID     *ovfl_iids;

    pfxid = pfx_open(key, pfx_get_length(key, nkey), true);
    if (pfxid == -1) { /* SEVERE error: prefix creation fail */
        return ENGINE_PREFIX_ENOENT;
    }
    use_cas_flag = pfx_use_cas(pfxid);

    /* compute data slot size and ovfl slot count */
    nbase = sizeof(data_item) + (use_cas_flag ? sizeof(uint64_t) : 0) + nkey;
    novfl = nbytes / item_gl.max_ovfl_data_size;
    ndata = nbytes % item_gl.max_ovfl_data_size;
    nrest = 0;
    if ((nbase + ndata + (novfl * sizeof(ItemID))) > item_gl.max_base_slot_size) {
        novfl += 1;
        nrest = ndata;
        ndata = 0;
    }
    base_slot_size = GET_8_ALIGN_SIZE(nbase + ndata + (novfl * sizeof(ItemID)));
    if (base_slot_size > item_gl.max_base_slot_size) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item alloc - too big size\n");
        pfx_close(pfxid);
        return ENGINE_E2BIG;
    }

    /* allocate data slot and ovfl slots */
    do {
        ret = pfx_data_slot_alloc(pfxid, base_slot_size, itdesc);
        if (ret != ENGINE_SUCCESS) { /* ENGINE_ENODISK | ENGINE_FAILED */
            logger->log(EXTENSION_LOG_WARNING, NULL, "item alloc - data slot alloc fail\n");
            break;
        }
        if (novfl > 0) {
            itdesc->ofl = (ovfl_item**)do_item_temp_space_alloc(sizeof(void*) * novfl);
            if (itdesc->ofl == NULL) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "item alloc - ovfl memory alloc fail\n");
                pfx_data_slot_free(pfxid, itdesc);
                ret = ENGINE_ENOMEM; break;
            }
            ovfl_iids = (ItemID*)((char*)itdesc->ptr + GET_8_ALIGN_SIZE(nbase + ndata));
            ret = pfx_ovfl_slot_alloc(pfxid, novfl, ovfl_iids, (void**)itdesc->ofl);
            if (ret != ENGINE_SUCCESS) { /* ENGINE_ENODISK | ENGINE_FAILED */
                logger->log(EXTENSION_LOG_WARNING, NULL, "item alloc - ovfl slot alloc fail\n");
                do_item_temp_space_free((void*)itdesc->ofl);
                pfx_data_slot_free(pfxid, itdesc);
                break;
            }
        } else {
            itdesc->ofl = NULL;
        }
    } while(0);

    if (ret != ENGINE_SUCCESS) {
        pfx_close(pfxid);
    } else {
        /* initialize the allocated ovfl slots */
        if (novfl > 0) {
            for (int i = 0; i < novfl; i++) {
                if (i < (novfl-1) || nrest == 0)
                    itdesc->ofl[i]->ndata = item_gl.max_ovfl_data_size;
                else
                    itdesc->ofl[i]->ndata = nrest;
            }
        }

        /* initialize the allocated data slot */
        itdesc->ptr->flags   = flags;
        itdesc->ptr->exptime = exptime;
        itdesc->ptr->nbytes  = nbytes;
        itdesc->ptr->nkey    = nkey;
        itdesc->ptr->iflag   = (use_cas_flag ? ITEM_WITH_CAS : 0);
        itdesc->ptr->pfxid   = pfxid;
        itdesc->ptr->novfl   = novfl;
        itdesc->ptr->ndata   = ndata;

        /* how to increase refcount ?? */
        itdesc->ptr->refcount = 1;  /* the caller will have a reference */
        DEBUG_REFCNT(it, '*');

        memcpy((void*)item_get_key(itdesc->ptr), key, nkey);
        /* slot allocation LOGGING only */
        /* content LOGGING is delayed */

        /* item write status */
        itdesc->wrstat = (uint8_t)ITEM_WRSTAT_ALLOC;
    }
    return ret;
}

static void do_item_free(ItemDesc *itdesc)
{
    data_item *dit = itdesc->ptr;
    int pfxid = PREFIXID_FROM_ITEMID(&itdesc->iid);
    assert(dit->refcount == 0 && (dit->iflag & ITEM_LINKED) == 0);

    DEBUG_REFCNT(dit, 'F');

    /* free ovfl data slots */
    if (dit->novfl > 0) {
        ItemID *ovfl_iids = (ItemID*)item_get_meta(dit);
        pfx_ovfl_slot_free(pfxid, dit->novfl, ovfl_iids, (void**)itdesc->ofl);
        if (itdesc->ofl != NULL) {
            do_item_temp_space_free((void*)itdesc->ofl);
            itdesc->ofl = NULL;
        }
#ifdef ENABLE_RECOVERY
        if (config->sq.use_recovery) { /* TRANSACTION */
            ITOvfDelLog itovfdel_log;
            lrec_it_ovfdel_init((LogRec*)&itovfdel_log);
            itovfdel_log.osids = (void*)ovfl_iids;
            itovfdel_log.header.length = dit->novfl * sizeof(ItemID);
            log_record_write((LogRec*)&itovfdel_log, (Trans*)itdesc->trx);
        }
#endif
    }
    /* free base data slot */
    pfx_data_slot_free(pfxid, itdesc);
}

static void do_item_delmark(ItemDesc *itdesc)
{
    data_item *dit = itdesc->ptr;
    assert((dit->iflag & ITEM_LINKED) == 0); /* unlinked state */

    dit->iflag |= ITEM_DELMARK;

    Page *pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(dit, config->sq.data_page_size);
#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        ITDelMarkLog itdelmark_log;
        lrec_it_delmark_init((LogRec*)&itdelmark_log);
        itdelmark_log.body.itmid = itdesc->iid;
        itdelmark_log.header.length = sizeof(ITDelMarkData);

        /* write log record */
        log_record_write((LogRec*)&itdelmark_log, (Trans*)itdesc->trx);
        /* update PageLSN */
        buf_set_lsn(pgptr, itdelmark_log.header.self_lsn);
    }
#endif
    /* set buffer page dirty */
    buf_set_dirty(pgptr);
}

static ENGINE_ERROR_CODE do_item_link(ItemDesc *itdesc)
{
    data_item *dit = itdesc->ptr;
    assert((dit->iflag & ITEM_LINKED) == 0 && dit->nbytes < (1024 * 1024));  /* 1MB max size */

    MEMCACHED_ITEM_LINK(item_get_key(dit), dit->nkey, dit->nbytes);

    ENGINE_ERROR_CODE ret = assoc_insert(dit->pfxid, itdesc);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item link - assoc insert fail (ret=%x)\n", ret);
        /* ENGINE_SUCCESS | ENGINE_KEY_EEXISTS | ENGINE_FAILED */
        return ret;
    }

    dit->iflag |= ITEM_LINKED;       /* set ITEM_LINKED flag */
    item_set_cas(dit, get_cas_id()); /* set a new CAS ID on linked item. */

    Page *pgptr;
    uint64_t items_cnt, items_len;
    int stotal = ITEM_stotal(dit, true);
    int ittype = do_item_type_get(dit);
    assert(ittype != ITEM_TYPE_UNKNOWN);

    /* update prefix item stats */
    pfx_item_stats_arith(dit->pfxid, ittype, 1, stotal, &items_cnt, &items_len);

    if (itdesc->ofl != NULL) {
        assert(itdesc->ptr->novfl > 0);
        for (int i = 0; i < dit->novfl; i++) {
            pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(itdesc->ofl[i], config->sq.data_page_size);
#ifdef ENABLE_RECOVERY
            if (config->sq.use_recovery) { /* TRANSACTION */
                ITOvfAddLog itovfadd_log;
                lrec_it_ovfadd_init((LogRec*)&itovfadd_log);
                itovfadd_log.body.pgid = pgptr->header.self_pgid;
                itovfadd_log.itptr = (char*)itdesc->ofl[i];
                itovfadd_log.header.length = offsetof(ITOvfAddData, data) + ITEM_ovfl_stotal(itdesc->ofl[i]);

                log_record_write((LogRec*)&itovfadd_log, (Trans*)itdesc->trx);
                /* update PageLSN */
                buf_set_lsn(pgptr, itovfadd_log.header.self_lsn);
            }
#endif
            /* set buffer page dirty */
            buf_set_dirty(pgptr);
        }
    }
    pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(itdesc->ptr, config->sq.data_page_size);
#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        ITLinkLog itlnk_log; /* commit type log */
        lrec_it_link_init((LogRec*)&itlnk_log);
        itlnk_log.body.itmid = itdesc->iid;
        itlnk_log.body.items_cnt = items_cnt;
        itlnk_log.body.items_len = items_len;
        itlnk_log.itptr = (void *)itdesc->ptr;
        itlnk_log.header.length = offsetof(ITLinkData, data) + ITEM_stotal(dit, false);

        buf_relatch_only(pgptr, BUF_MODE_WRITE);
        log_record_write((LogRec*)&itlnk_log, (Trans*)itdesc->trx);
        /* update PageLSN */
        buf_set_lsn(pgptr, itlnk_log.header.self_lsn);
        buf_unlatch_only(pgptr);
    }
#endif
    /* set buffer page dirty */
    buf_set_dirty(pgptr);

    /* update global item stats */
    pthread_mutex_lock(&item_gl.stats.lock);
    item_gl.stats.stat.curr_bytes += stotal;
    item_gl.stats.stat.curr_items += 1;
    item_gl.stats.stat.total_items += 1;
    pthread_mutex_unlock(&item_gl.stats.lock);

    /* item write status */
    assert(itdesc->wrstat == (uint8_t)ITEM_WRSTAT_ALLOC);
    itdesc->wrstat = (uint8_t)ITEM_WRSTAT_LINK;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_item_unlink(ItemDesc *itdesc)
{
    data_item *dit = itdesc->ptr;
    assert((dit->iflag & ITEM_LINKED) != 0);

    MEMCACHED_ITEM_UNLINK(item_get_key(dit), dit->nkey, dit->nbytes);

    ENGINE_ERROR_CODE ret = assoc_delete(dit->pfxid, itdesc);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item unlink - assoc delete fail (ret=%x)\n", ret);
        /* ENGINE_SUCCESS | ENGINE_KEY_ENOENT | ENGINE_FAILED */
        return ret;
    }

    dit->iflag &= ~ITEM_LINKED; /* clear ITEM_LINKED flag */

    Page *pgptr;
    uint64_t items_cnt, items_len;
    int stotal = ITEM_stotal(dit, true);
    int ittype = do_item_type_get(dit);
    assert(ittype != ITEM_TYPE_UNKNOWN);

    /* update prefix item stats */
    pfx_item_stats_arith(dit->pfxid, ittype, -1, (-stotal), &items_cnt, &items_len);

    pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(itdesc->ptr, config->sq.data_page_size);
#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        ITUnlinkLog itulk_log; /* commit type log */
        lrec_it_unlink_init((LogRec*)&itulk_log);
        itulk_log.body.itmid = itdesc->iid;
        itulk_log.body.items_cnt = items_cnt;
        itulk_log.body.items_len = items_len;
        itulk_log.body.ittype = ittype;
        itulk_log.header.length = GET_8_ALIGN_SIZE(sizeof(ITUnlinkData));

        buf_relatch_only(pgptr, BUF_MODE_WRITE);
        log_record_write((LogRec*)&itulk_log, (Trans*)itdesc->trx);
        /* update PageLSN */
        buf_set_lsn(pgptr, itulk_log.header.self_lsn);
        buf_unlatch_only(pgptr);
    }
#endif
    /* set buffer page dirty */
    buf_set_dirty(pgptr);

    /* update global item stats */
    pthread_mutex_lock(&item_gl.stats.lock);
    item_gl.stats.stat.curr_bytes -= stotal;
    item_gl.stats.stat.curr_items -= 1;
    pthread_mutex_unlock(&item_gl.stats.lock);

    /* item write status */
    assert(itdesc->wrstat == (uint8_t)ITEM_WRSTAT_NONE);
    itdesc->wrstat = (uint8_t)ITEM_WRSTAT_UNLINK;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_item_replace(ItemDesc *old_itdesc, ItemDesc *new_itdesc)
{
    Page *old_pgptr;
    Page *new_pgptr;
    data_item *old_dit = old_itdesc->ptr;
    data_item *new_dit = new_itdesc->ptr;
    assert((old_dit->iflag & ITEM_LINKED) != 0);
    assert((new_dit->iflag & ITEM_LINKED) == 0);

    MEMCACHED_ITEM_REPLACE(item_get_key(old_dit), old_dit->nkey, old_dit->nbytes,
                           item_get_key(new_dit), new_dit->nkey, new_dit->nbytes);

    ENGINE_ERROR_CODE ret = assoc_toggle(old_dit->pfxid, old_itdesc, new_itdesc);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item replace - assoc toggle fail (ret=%x)\n", ret);
        /* ENGINE_SUCCESS | ENGINE_KEY_ENOENT | ENGINE_FAILED */
        return ret;
    }

    old_dit->iflag &= ~ITEM_LINKED; /* clear ITEM_LINKED flag */
    new_dit->iflag |= ITEM_LINKED;  /* set ITEM_LINKED flag */
    item_set_cas(new_dit, get_cas_id()); /* set a new CAS ID on linked item. */

    /* get each item size */
    int old_stotal = ITEM_stotal(old_dit, true);
    int new_stotal = ITEM_stotal(new_dit, true);
    uint64_t items_cnt = 0;
    uint64_t items_len = 0;

    /* update prefix item stats */
    if (old_stotal != new_stotal) {
        assert(do_item_type_get(old_dit) == ITEM_TYPE_KV);
        assert(do_item_type_get(new_dit) == ITEM_TYPE_KV);
        pfx_item_stats_arith(old_dit->pfxid, ITEM_TYPE_KV, 0, (new_stotal-old_stotal), &items_cnt, &items_len);
    }

    if (new_itdesc->ofl != NULL) {
        assert(new_dit->novfl > 0);
        for (int i = 0; i < new_dit->novfl; i++) {
            new_pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(new_itdesc->ofl[i], config->sq.data_page_size);
#ifdef ENABLE_RECOVERY
            if (config->sq.use_recovery) { /* TRANSACTION */
                ITOvfAddLog itovfadd_log;
                lrec_it_ovfadd_init((LogRec*)&itovfadd_log);
                itovfadd_log.body.pgid = new_pgptr->header.self_pgid;
                itovfadd_log.itptr = (char*)new_itdesc->ofl[i];
                itovfadd_log.header.length = offsetof(ITOvfAddData,data) + ITEM_ovfl_stotal(new_itdesc->ofl[i]);

                log_record_write((LogRec*)&itovfadd_log, (Trans*)new_itdesc->trx);
                /* update PageLSN */
                buf_set_lsn(new_pgptr, itovfadd_log.header.self_lsn);
            }
#endif
            /* set buffer page dirty */
            buf_set_dirty(new_pgptr);
        }
    }
    old_pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(old_dit, config->sq.data_page_size);
    new_pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(new_dit, config->sq.data_page_size);
#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        ITReplaceLog itrpl_log; /* commit type log */
        lrec_it_replace_init((LogRec*)&itrpl_log);
        itrpl_log.body.old_itmid = old_itdesc->iid;
        itrpl_log.body.new_itmid = new_itdesc->iid;
        itrpl_log.body.items_cnt = items_cnt;
        itrpl_log.body.items_len = items_len;
        itrpl_log.body.new_itlen = ITEM_stotal(new_dit, false);
        itrpl_log.itptr = (void*)new_dit;
        itrpl_log.header.length = GET_8_ALIGN_SIZE(offsetof(ITReplaceData,data) + itrpl_log.body.new_itlen);

        if (old_pgptr == new_pgptr) {
            buf_relatch_only(old_pgptr, BUF_MODE_WRITE);
        } else {
            /* let's keep the latch order in order to avoid deadlock */
            assert(PAGEID_IS_NE(&old_pgptr->header.self_pgid, &new_pgptr->header.self_pgid));
            if (PAGEID_IS_LT(&old_pgptr->header.self_pgid, &new_pgptr->header.self_pgid)) {
                buf_relatch_only(old_pgptr, BUF_MODE_WRITE);
                buf_relatch_only(new_pgptr, BUF_MODE_WRITE);
            } else {
                buf_relatch_only(new_pgptr, BUF_MODE_WRITE);
                buf_relatch_only(old_pgptr, BUF_MODE_WRITE);
            }
        }
        log_record_write((LogRec*)&itrpl_log, (Trans*)new_itdesc->trx);
        /* update PageLSN */
        if (old_pgptr == new_pgptr) {
            buf_set_lsn(old_pgptr, itrpl_log.header.self_lsn);
            buf_unlatch_only(old_pgptr);
        } else {
            buf_set_lsn(old_pgptr, itrpl_log.header.self_lsn);
            buf_set_lsn(new_pgptr, itrpl_log.header.self_lsn);
            buf_unlatch_only(old_pgptr);
            buf_unlatch_only(new_pgptr);
        }
    }
#endif
    /* set buffer page dirty */
    buf_set_dirty(old_pgptr);
    buf_set_dirty(new_pgptr);

    /* update global item stats */
    pthread_mutex_lock(&item_gl.stats.lock);
    if (old_stotal != new_stotal) {
        item_gl.stats.stat.curr_bytes += (new_stotal - old_stotal);
    }
    item_gl.stats.stat.total_items += 1;
    pthread_mutex_unlock(&item_gl.stats.lock);

    /* item write status */
    assert(old_itdesc->wrstat == (uint8_t)ITEM_WRSTAT_NONE);
    old_itdesc->wrstat = (uint8_t)ITEM_WRSTAT_UNLINK;
    assert(new_itdesc->wrstat == (uint8_t)ITEM_WRSTAT_ALLOC);
    new_itdesc->wrstat = (uint8_t)ITEM_WRSTAT_LINK;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_item_ovfl_get(ItemDesc *itdesc)
{
    data_item *dit = itdesc->ptr;
    ItemID    *ovfl_iids;
    Page      *pgptr;
    PageID     pgid;
    uint32_t   slotid, i;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    itdesc->ofl = (ovfl_item**)do_item_temp_space_alloc(dit->novfl * sizeof(void*));
    if (itdesc->ofl == NULL) {
        return ENGINE_ENOMEM; /* no more memory */
    }

    ovfl_iids = (ItemID*)item_get_meta(dit);
    for (i = 0; i < dit->novfl; i++) {
        GET_PAGEID_SLOTID_FROM_ITEMID(&ovfl_iids[i], &pgid, &slotid);
        /* ovfl pages are pertained to only one data item.
         * So, do not need to latch the page buffer.
         */
        pgptr = buf_pin_only(pgid, false, false);
        if (pgptr == NULL) { /* buf pin fail */
            logger->log(EXTENSION_LOG_WARNING, NULL, "item ovfl get - pin ovfl page fail\n");
            ret = ENGINE_FAILED; break;
        }
        itdesc->ofl[i] = opage_find_slot(pgptr, slotid);
    }
    if (ret != ENGINE_SUCCESS) { /* unpin all the pinned pages */
        for (int j = i-1; j >= 0; j--) {
            pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(itdesc->ofl[j], config->sq.data_page_size);
            buf_unpin_only(pgptr);
        }
        do_item_temp_space_free((void*)itdesc->ofl);
        itdesc->ofl = NULL;
    }
    return ret;
}

static void do_item_unpin_buffer(ItemDesc *itdesc)
{
    Page *pgptr;

    /* unpin ovfl pages from page buffer */
    if (itdesc->ofl != NULL) {
        assert(itdesc->ptr->novfl > 0);
        for (int i = 0; i < itdesc->ptr->novfl; i++) {
            pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(itdesc->ofl[i], config->sq.data_page_size);
#if 0 // USELESS_BUFFER_SET_DRITY
            if (itdesc->wrstat != (uint8_t)ITEM_WRSTAT_NONE)
                buf_set_dirty(pgptr);
#endif
            buf_unpin_only(pgptr);
        }
        do_item_temp_space_free((void*)itdesc->ofl);
        itdesc->ofl = NULL;
    }
    /* unpin data page from page buffer */
    pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(itdesc->ptr, config->sq.data_page_size);
#if 0 // USELESS_BUFFER_SET_DRITY
    if (itdesc->wrstat != (uint8_t)ITEM_WRSTAT_NONE)
        buf_set_dirty(pgptr);
#endif
    buf_unpin_only(pgptr);
}

static ENGINE_ERROR_CODE do_item_delete_expired(ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret;
#ifdef ENABLE_RECOVERY
    void *saved_trx = NULL;

    if (config->sq.use_recovery) { /* TRANSACTION */
        saved_trx = itdesc->trx;
        itdesc->trx = (void*)tran_alloc(true); /* implicit transaction */
        if (itdesc->trx == NULL) {
            itdesc->trx = saved_trx;
            logger->log(EXTENSION_LOG_WARNING, NULL, "item delete expired - tran alloc fail\n");
            return ENGINE_ETRANS;
        }
    }
#endif

    if ((ret = do_item_unlink(itdesc)) == ENGINE_SUCCESS) {
        if (itdesc->ptr->refcount == 0) {
            /* No one is now accessing it, so it must be freed */
            /* free the item (with unpinning the buffer pages) */
            do_item_free(itdesc);
        } else {
            /* other process will free the item when release it */
            do_item_delmark(itdesc);
            do_item_unpin_buffer(itdesc);
        }
    }

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        tran_free(itdesc->trx, (ret==ENGINE_SUCCESS ? TRAN_COM_COMMIT : TRAN_COM_ABORT));
        itdesc->trx = saved_trx;
    }
#endif
    return ret;
}

/** wrapper around assoc_find which does the lazy expiration logic */
static ENGINE_ERROR_CODE do_item_get(const char *key, const size_t nkey, const int type, const bool readonly,
                                     ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret;

    int pfxid = pfx_open(key, pfx_get_length(key, nkey), false);
    if (pfxid == -1) {
        ret = ENGINE_KEY_ENOENT; /* instead of ENGINE_PREFIX_ENOENT */
    } else {
        /* Following execution order must be preserved */
        do {
            ret = assoc_find(pfxid, key, nkey, itdesc);
            if (ret != ENGINE_SUCCESS) { /* ENGINE_KEY_ENOENT | ENGINE_XXXX_FAILED */
                itdesc->ptr = NULL; break;
            }
            itdesc->ofl = NULL;

            /* check expiration time */
            if (itdesc->ptr->exptime != 0 && itdesc->ptr->exptime <= server->core->get_current_time()) {
                /* [NOTE] Should we do push the item into item delete queue ??
                 * It's a little awkward for the get process to unlink and free the item.
                 * Because, the get process writes log records even if its only purpose is read.
                 * But, the expired item must be removed from index(hash table).
                 * We chose to push the item into the delete queue in readonly case.
                 * In other cases, unlink the item and free it immediately.
                 */
                ret = do_item_delete_expired(itdesc);
                if (ret != ENGINE_SUCCESS) {
                    logger->log(EXTENSION_LOG_INFO, NULL, "delete expired item fail\n");
                    ret = ENGINE_FAILED;
                } else {
                    itdesc->ptr = NULL;
                    ret = ENGINE_KEY_ENOENT;
                }
                break;
            }

            /* check item type */
            if (type != ITEM_TYPE_ANY) {
                if (do_item_type_get(itdesc->ptr) != type) {
                    ret = ENGINE_EBADTYPE; break;
                }
            }
        } while(0);

        if (ret == ENGINE_SUCCESS) {
            itdesc->ptr->refcount++;
            DEBUG_REFCNT(itdesc->ptr, '+');
        } else {
            /* unpin the data page */
            if (itdesc->ptr != NULL) {
                do_item_unpin_buffer(itdesc);
            }
            /* close the prefix */
            pfx_close(pfxid);
        }
    }

    /* return values:
     *   ENGINE_SUCCESS
     *   ENGINE_KEY_ENOENT (including ENGINE_PREFIX_ENOENT)
     *   ENGINE_EBADTYPE
     *   ENGINE_FAIELD
     */
    if (config->verbose > 2) {
        switch (ret) {
          case ENGINE_SUCCESS:
               fprintf(stderr, "> FOUND KEY %s\n", key); break;
          case ENGINE_KEY_ENOENT:
          case ENGINE_EBADTYPE:
               fprintf(stderr, "> NOT FOUND %s\n", key); break;
          default:
               fprintf(stderr, "> INTERNAL ERROR IN FINDING %s\n", key);
        }
    }
    return ret;
}

/* item release */

static void do_item_release(ItemDesc *itdesc)
{
    data_item *dit = itdesc->ptr;
    int pfxid = dit->pfxid;

    MEMCACHED_ITEM_REMOVE(item_get_key(dit), dit->nkey, dit->nbytes);

    /* [NOTE] check the refcount of given item */
    assert(dit->refcount > 0);

    /* decrement refcount */
    if (dit->refcount != 0) {
        dit->refcount--;
        DEBUG_REFCNT(dit, '-');
    }

    if (dit->refcount > 0) {
        /* It's still referenced by others, now. */
        if (itdesc->wrstat == (uint8_t)ITEM_WRSTAT_UNLINK) {
            /* set delele mark */
            do_item_delmark(itdesc);
        }
        do_item_unpin_buffer(itdesc);
    } else { /* dit->refcount == 0 */
        if ((dit->iflag & ITEM_LINKED) != 0) {
            /* possible item write status
             *   ITEM_WRSTAT_NONE
             *   ITEM_WRSTAT_LINK | ITEM_WRSTAT_UPDDATA | ITEM_WRSTAT_UPDATTR
             */
            assert(itdesc->wrstat != (uint8_t)ITEM_WRSTAT_ALLOC &&
                   itdesc->wrstat != (uint8_t)ITEM_WRSTAT_UNLINK);
            if (itdesc->wrstat != (uint8_t)ITEM_WRSTAT_NONE) {
                /* logging is needed */
            }
            do_item_unpin_buffer(itdesc);
        } else { /* (dit->iflag & ITEM_LINKED) == 0: UNLINKED */
            if (itdesc->wrstat == (uint8_t)ITEM_WRSTAT_NONE) {
                 /* An retrieved item was unlinked by deletion or expiration */
                 /* Just set delete mark. It must be free by item gc thread, later */
                 assert((dit->iflag & ITEM_DELMARK) != 0);
                 itdesc->wrstat = (uint8_t)ITEM_WRSTAT_DELMARK;
            } else {
                 assert(itdesc->wrstat == (uint8_t)ITEM_WRSTAT_UNLINK ||
                        itdesc->wrstat == (uint8_t)ITEM_WRSTAT_ALLOC);
                 /* Some explanation on write status of itdesc
                  * Possible write status.
                  *  1) ITEM_WRSTAT_UNLINK
                  *     item delete (get => unlink => release)
                  *     just free the item - normal action with logging
                  *  2) ITEM_WRSTAT_ALLOC
                  *     A newly allocated item wasn't linked into key index (alloc => relelase)
                  *     do rollback - free the item.
                  * Other cases cannot be happened.
                  *  1) ITEM_WRSTAT_LINK
                  *     A newly allocated/linked item mighe be deleted by other thread.
                  *     This case was avoided by releasing the item immediately after storing it.
                  *  2) Nothing..
                  */
                do_item_free(itdesc);
            }
        }
    }

    /* close the prefix */
    pfx_close(pfxid);
}

#if 0
static void do_item_store_release(ItemDesc *itdesc)
{
    data_item *dit = itdesc->ptr;
    if ((dit->iflag & ITEM_LINKED) == 0) { /* unlinked */
        assert(itdesc->wrstat == (uint8_t)ITEM_WRSTAT_ALLOC);
        /* A newly allocated item wasn't linked into key index (alloc => relelase) */
        /* do nothing - rollback function will free the item */
        dit->refcount -= 1;
        do_item_free(itdesc);
        dit->refcount += 1;
        itdesc->wrstat = (uint8_t)ITEM_WRSTAT_NONE;
    } else {
        assert(itdesc->wrstat == (uint8_t)ITEM_WRSTAT_LINK);
        /* do nothing */
    }
}
#endif

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns the state of storage.
 */
static ENGINE_ERROR_CODE do_item_store_set(ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret;
    ItemDesc old_itdbuf;

    /* initialize item write status */
    old_itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;

#ifdef ENABLE_RECOVERY
    /* initialize transaction entry pointer */
    old_itdbuf.trx = NULL;
    if (config->sq.use_recovery) { /* TRANSACTION */
        assert(itdesc->trx != NULL);
        old_itdbuf.trx = itdesc->trx;
    }
#endif

    ret = do_item_get(item_get_key(itdesc->ptr), itdesc->ptr->nkey, ITEM_TYPE_KV, false, &old_itdbuf);
    if (ret == ENGINE_SUCCESS) {
        ret = do_item_replace(&old_itdbuf, itdesc);
        /* return code:
         *   ENGINE_SUCCESS | ENGINE_KEY_ENOENT | ENGINE_FAILED
         */
        do_item_release(&old_itdbuf);
    } else {
        if (ret == ENGINE_KEY_ENOENT) {
            ret = do_item_link(itdesc);
            /* return code:
             *   ENGINE_SUCCESS | ENGINE_KEY_EEXISTS | ENGINE_FAILED
             */
        }
    }
    return ret;
}

static ENGINE_ERROR_CODE do_item_store_add(ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret;
    ItemDesc old_itdbuf;

    /* initialize item write status */
    old_itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;

#ifdef ENABLE_RECOVERY
    /* initialize transaction entry pointer */
    old_itdbuf.trx = NULL;
    if (config->sq.use_recovery) { /* TRANSACTION */
        assert(itdesc->trx != NULL);
        old_itdbuf.trx = itdesc->trx;
    }
#endif

    ret = do_item_get(item_get_key(itdesc->ptr), itdesc->ptr->nkey, ITEM_TYPE_KV, false, &old_itdbuf);
    if (ret == ENGINE_SUCCESS) {
        do_item_release(&old_itdbuf);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        if (ret == ENGINE_KEY_ENOENT) {
            ret = do_item_link(itdesc);
            /* return code:
             *   ENGINE_SUCCESS | ENGINE_KEY_EEXISTS | ENGINE_FAILED
             */
        }
    }
    /* [NOTE] return the same error code that default engine return (when the key has existed) */
    if (ret == ENGINE_KEY_EEXISTS) {
        ret = ENGINE_NOT_STORED;
    }
    return ret;
}

static ENGINE_ERROR_CODE do_item_store_replace(ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret;
    ItemDesc old_itdbuf;

    /* initialize item write status */
    old_itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;

#ifdef ENABLE_RECOVERY
    /* initialize transaction entry pointer */
    old_itdbuf.trx = NULL;
    if (config->sq.use_recovery) { /* TRANSACTION */
        assert(itdesc->trx != NULL);
        old_itdbuf.trx = itdesc->trx;
    }
#endif

    ret = do_item_get(item_get_key(itdesc->ptr), itdesc->ptr->nkey, ITEM_TYPE_KV, false, &old_itdbuf);
    if (ret == ENGINE_SUCCESS) {
        ret = do_item_replace(&old_itdbuf, itdesc);
        /* return code:
         *   ENGINE_SUCCESS | ENGINE_KEY_ENOENT | ENGINE_FAILED
         */
        do_item_release(&old_itdbuf);
    }
    /* [NOTE] return the same error code that default engine return (when the key hasn't found) */
    if (ret == ENGINE_KEY_ENOENT) {
        ret = ENGINE_NOT_STORED;
    }
    return ret;
}

static ENGINE_ERROR_CODE do_item_store_attach(ItemDesc *itdesc, ENGINE_STORE_OPERATION attach_op)
{
    ENGINE_ERROR_CODE ret;
    ItemDesc old_itdbuf;
    ItemDesc new_itdbuf;

    /* initialize item write status */
    old_itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;
    new_itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;

#ifdef ENABLE_RECOVERY
    /* initialize transaction entry pointer */
    old_itdbuf.trx = NULL;
    new_itdbuf.trx = NULL;
    if (config->sq.use_recovery) { /* TRANSACTION */
        assert(itdesc->trx != NULL);
        old_itdbuf.trx = itdesc->trx;
        new_itdbuf.trx = itdesc->trx;
    }
#endif

    ret = do_item_get(item_get_key(itdesc->ptr), itdesc->ptr->nkey, ITEM_TYPE_KV, false, &old_itdbuf);
    if (ret == ENGINE_SUCCESS) {
        do {
            if (item_get_cas(itdesc->ptr) != 0 &&
                item_get_cas(itdesc->ptr) != item_get_cas(old_itdbuf.ptr)) {
                /* CAS much be equal */
                ret = ENGINE_KEY_EEXISTS; break;
            }
            if (old_itdbuf.ptr->novfl > 0) {
                ret = do_item_ovfl_get(&old_itdbuf);
                if (ret != ENGINE_SUCCESS) {
                    /* ENGINE_ENOMEM | ENGINE_FAILED */
                    break;
                }
            }
            /* we have it and old_it here - alloc memory to hold both */
            ret = do_item_alloc(item_get_key(itdesc->ptr), itdesc->ptr->nkey,
                                itdesc->ptr->nbytes + old_itdbuf.ptr->nbytes - 2 /* CRLF */,
                                old_itdbuf.ptr->flags, old_itdbuf.ptr->exptime, &new_itdbuf);
            if (ret != ENGINE_SUCCESS) {
                /* ENGINE_PREFIX_ENOENT | ENGINE_ENOMEM | ENGINE_ENODISK | ENGINE_FAILED */
                break;
            }

            /* copy data from it and old_it to new_it */
            if (attach_op == OPERATION_APPEND) {
                memcpy(item_get_data(new_itdbuf.ptr),
                       item_get_data(old_itdbuf.ptr), old_itdbuf.ptr->nbytes);
                memcpy(item_get_data(new_itdbuf.ptr) + old_itdbuf.ptr->nbytes - 2 /* CRLF */,
                       item_get_data(itdesc->ptr), itdesc->ptr->nbytes);
            } else { /* OPERATION_PREPEND */
                memcpy(item_get_data(new_itdbuf.ptr),
                       item_get_data(itdesc->ptr), itdesc->ptr->nbytes);
                memcpy(item_get_data(new_itdbuf.ptr) + itdesc->ptr->nbytes - 2 /* CRLF */,
                       item_get_data(old_itdbuf.ptr), old_itdbuf.ptr->nbytes);
            }

            /* replace old item with new item */
            ret = do_item_replace(&old_itdbuf, &new_itdbuf);
            /* return code:
             *   ENGINE_SUCCESS | ENGINE_KEY_ENOENT | ENGINE_FAILED
             */
            do_item_release(&new_itdbuf);
        } while(0);

        do_item_release(&old_itdbuf);
    }
    return ret;
}

static ENGINE_ERROR_CODE do_item_store_cas(ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret;
    ItemDesc old_itdbuf;

    /* initialize item write status */
    old_itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;

#ifdef ENABLE_RECOVERY
    /* initialize transaction entry pointer */
    old_itdbuf.trx = NULL;
    if (config->sq.use_recovery) { /* TRANSACTION */
        assert(itdesc->trx != NULL);
        old_itdbuf.trx = itdesc->trx;
    }
#endif

    ret = do_item_get(item_get_key(itdesc->ptr), itdesc->ptr->nkey, ITEM_TYPE_KV, false, &old_itdbuf);
    if (ret == ENGINE_SUCCESS) {
        if (item_get_cas(itdesc->ptr) == item_get_cas(old_itdbuf.ptr)) {
            ret = do_item_replace(&old_itdbuf, itdesc);
            /* return code:
             *   ENGINE_SUCCESS | ENGINE_KEY_ENOENT | ENGINE_FAILED
             */
        } else {
            if (config->verbose > 1) {
                logger->log(EXTENSION_LOG_INFO, NULL, "CAS failure: expected %"PRIu64", got %"PRIu64"\n",
                            item_get_cas(old_itdbuf.ptr), item_get_cas(itdesc->ptr));
            }
            ret = ENGINE_KEY_EEXISTS;
        }
        do_item_release(&old_itdbuf);
    }
    return ret;
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
static ENGINE_ERROR_CODE do_add_delta(ItemDesc *itdesc, const bool incr, const int64_t delta,
                                      uint64_t *rcas, uint64_t *result)
{
    data_item *dit = itdesc->ptr;
    const char *vptr;
    uint64_t   value;
    char       buf[80];
    int        len;
    ENGINE_ERROR_CODE ret;

    vptr = item_get_data(dit);
    if (!safe_strtoull(vptr, &value)) {
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

    if ((len = snprintf(buf, sizeof(buf), "%"PRIu64"\r\n", value)) == -1) {
        return ENGINE_EINVAL;
    }

    if (dit->refcount == 1 && GET_8_ALIGN_SIZE(dit->nkey + dit->nbytes) == GET_8_ALIGN_SIZE(dit->nkey + len)) {
        Page *pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(itdesc->ptr, config->sq.data_page_size);
        buf_relatch_only(pgptr, BUF_MODE_WRITE);

        memcpy(item_get_data(dit), buf, len);
        if (dit->nbytes != len)
            dit->nbytes = len;

        item_set_cas(dit, get_cas_id()); /* set a new CAS ID on the item */

#ifdef ENABLE_RECOVERY
        if (config->sq.use_recovery) { /* TRANSACTION */
            /* commit type log */
            ITUpdateLog itupd_log;
            lrec_it_update_init((LogRec*)&itupd_log);
            itupd_log.body.itmid = itdesc->iid;
            itupd_log.body.cas = item_get_cas(dit);
            itupd_log.body.nbytes = len;
            itupd_log.value = (char*)item_get_data(dit);
            itupd_log.header.length = GET_8_ALIGN_SIZE(offsetof(ITUpdateData,data) + len);

            /* write log record */
            log_record_write((LogRec*)&itupd_log, (Trans*)itdesc->trx);
            /* update PageLSN */
            buf_set_lsn(pgptr, itupd_log.header.self_lsn);
        }
#endif

        /* set buffer page dirty */
        buf_set_dirty(pgptr);
        buf_unlatch_only(pgptr);

        /* item write status */
        itdesc->wrstat = (uint8_t)ITEM_WRSTAT_UPDDATA;

        *rcas = item_get_cas(dit);
        ret = ENGINE_SUCCESS;
    } else {
        ItemDesc new_itdbuf;

        /* initialize item write status */
        new_itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;

#ifdef ENABLE_RECOVERY
        /* initialize transaction entry pointer */
        new_itdbuf.trx = NULL;
        if (config->sq.use_recovery) { /* TRANSACTION */
            assert(itdesc->trx != NULL);
            new_itdbuf.trx = itdesc->trx;
        }
#endif

        ret = do_item_alloc(item_get_key(dit), dit->nkey, len, dit->flags, dit->exptime, &new_itdbuf);
        if (ret == ENGINE_SUCCESS) {
            memcpy(item_get_data(new_itdbuf.ptr), buf, len);
            ret = do_item_replace(itdesc, &new_itdbuf);
            if (ret == ENGINE_SUCCESS) {
                *rcas = item_get_cas(new_itdbuf.ptr);
            } else {
                /* ENGINE_KEY_ENOENT | ENGINE_FAILED */
            }
            do_item_release(&new_itdbuf);
        } else {
            /* ENGINE_PREFIX_ENOENT | ENGINE_ENOMEM | ENGINE_ENODISK | ENGINE_FAILED */
        }
    }
    return ret;
}

/* Item Stats */

static void do_item_stats_main(struct item_stat *stats)
{
    pthread_mutex_lock(&item_gl.stats.lock);
    *(struct item_stat*)(stats) = item_gl.stats.stat;
    pthread_mutex_unlock(&item_gl.stats.lock);
}

/** dumps out a list of objects of each size, with granularity of 32 bytes */
/*@null@*/
static void do_item_stats_size(struct item_size *stats)
{
    // JOON: DONOT_SUPPORT_STATS_SIZES
    stats->num_buckets = 0;
}

#if 0 // ENABLE_SQUALL_ENGINE
/* Common functions for collection management */
static void do_mem_slot_free(struct squall_engine *engine, void *data, size_t ntotal) {
    /* so slab size changer can tell later if item is already free or not */
    hash_item *it = (hash_item *)data;
    unsigned int clsid = it->slabs_clsid;;
    it->slabs_clsid = 0;
    slabs_free(engine, it, ntotal, clsid);
}
#endif

#if 0 // ENABLE_SQUALL_ENGINE
static ENGINE_ERROR_CODE do_list_item_find(struct squall_engine *engine,
                                           const void *key, const size_t nkey,
                                           bool LRU_reposition, hash_item **item) {
    *item = NULL
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

static hash_item *do_list_item_alloc(struct squall_engine *engine,
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
        info->sticky  = ((attrp->exptime == (rel_time_t)(-1)) ? 1 : 0);
#endif
        info->readable = attrp->readable;
        info->stotal  = 0;
        info->prefix  = NULL;
        info->head = info->tail = NULL;
    }
    return it;
}

static list_elem_item *do_list_elem_alloc(struct squall_engine *engine,
                                          const int nbytes, const void *cookie) {
    size_t ntotal = sizeof(list_elem_item) + nbytes;

    list_elem_item *elem = item_alloc_internal(engine, ntotal, LRU_CLSID_FOR_LIST, cookie);
    if (elem != NULL) {
        assert(elem->slabs_clsid == 0);
        elem->slabs_clsid = slabs_clsid(engine, ntotal);
        elem->refcount    = 1;
        elem->nbytes      = nbytes;
        elem->prev = elem->next = (list_elem_item *)ADDR_MEANS_UNLINKED; /* Unliked state */
    }
    return elem;
}

static void do_list_elem_free(struct squall_engine *engine, list_elem_item *elem) {
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = sizeof(list_elem_item) + elem->nbytes;
    do_mem_slot_free(engine, elem, ntotal);
}

static void do_list_elem_release(struct squall_engine *engine, list_elem_item *elem) {
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->next == (list_elem_item *)ADDR_MEANS_UNLINKED) {
        do_list_elem_free(engine, elem);
    }
}

static list_elem_item *do_list_elem_find(list_meta_info *info, int index) {
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

static ENGINE_ERROR_CODE do_list_elem_link(struct squall_engine *engine,
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
        increase_collection_space(engine, (coll_meta_info *)info, stotal);
    }
    return ENGINE_SUCCESS;
}

static void do_list_elem_unlink(struct squall_engine *engine,
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
            decrease_collection_space(engine, (coll_meta_info *)info, stotal);
        }

        if (elem->refcount == 0) {
            do_list_elem_free(engine, elem);
        }
    }
}

static uint32_t do_list_elem_delete(struct squall_engine *engine,
                                    list_meta_info *info,
                                    const int index, const uint32_t count) {
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

static uint32_t do_list_elem_get(struct squall_engine *engine,
                                 list_meta_info *info,
                                 const int index, const uint32_t count,
                                 const bool forward, const bool delete,
                                 list_elem_item **elem_array) {
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
#endif

#if 0 // ENABLE_SQUALL_ENGINE
static inline int set_hash_eq(const int h1, const void *v1, size_t vlen1,
                              const int h2, const void *v2, size_t vlen2) {
    return (h1 == h2 && vlen1 == vlen2 && memcmp(v1, v2, vlen1) == 0);
}

#define SET_GET_HASHIDX(hval, hdepth) \
        (((hval) & (SET_HASHIDX_MASK << ((hdepth)*4))) >> ((hdepth)*4))

static ENGINE_ERROR_CODE do_set_item_find(struct squall_engine *engine,
                                          const void *key, const size_t nkey,
                                          bool LRU_reposition, hash_item **item) {
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

static hash_item *do_set_item_alloc(struct squall_engine *engine,
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
        info->sticky  = ((attrp->exptime == (rel_time_t)(-1)) ? 1 : 0);
#endif
        info->readable = attrp->readable;
        info->stotal  = 0;
        info->prefix  = NULL;
        info->root    = NULL;
    }
    return it;
}

static set_hash_node *do_set_node_alloc(struct squall_engine *engine,
                                        uint8_t hash_depth, const void *cookie)
{
    size_t ntotal = sizeof(set_hash_node);

    set_hash_node *node = item_alloc_internal(engine, ntotal, LRU_CLSID_FOR_SET, cookie);
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

static void do_set_node_free(struct squall_engine *engine, set_hash_node *node)
{
    do_mem_slot_free(engine, node, sizeof(set_hash_node));
}

static set_elem_item *do_set_elem_alloc(struct squall_engine *engine,
                                        const int nbytes, const void *cookie) {
    size_t ntotal = sizeof(set_elem_item) + nbytes;

    set_elem_item *elem = item_alloc_internal(engine, ntotal, LRU_CLSID_FOR_SET, cookie);
    if (elem != NULL) {
        assert(elem->slabs_clsid == 0);
        elem->slabs_clsid = slabs_clsid(engine, ntotal);
        elem->refcount    = 1;
        elem->nbytes      = nbytes;
        elem->next = (set_elem_item *)ADDR_MEANS_UNLINKED; /* Unliked state */
    }
    return elem;
}

static void do_set_elem_free(struct squall_engine *engine, set_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = sizeof(set_elem_item) + elem->nbytes;
    do_mem_slot_free(engine, elem, ntotal);
}

static void do_set_elem_release(struct squall_engine *engine, set_elem_item *elem)
{
    if (elem->refcount != 0) {
        elem->refcount--;
    }
    if (elem->refcount == 0 && elem->next == (set_elem_item *)ADDR_MEANS_UNLINKED) {
        do_set_elem_free(engine, elem);
    }
}

static void do_set_node_link(struct squall_engine *engine,
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
        increase_collection_space(engine, (coll_meta_info *)info, stotal);
    }
}

static void do_set_node_unlink(struct squall_engine *engine,
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
        decrease_collection_space(engine, (coll_meta_info *)info, stotal);
    }

    /* free the node */
    do_set_node_free(engine, node);
}

static ENGINE_ERROR_CODE do_set_elem_link(struct squall_engine *engine,
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
        increase_collection_space(engine, (coll_meta_info *)info, stotal);
    }

    return ENGINE_SUCCESS;
}

static void do_set_elem_unlink(struct squall_engine *engine,
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
        decrease_collection_space(engine, (coll_meta_info *)info, stotal);
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

static ENGINE_ERROR_CODE do_set_elem_traverse_delete(struct squall_engine *engine,
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

static ENGINE_ERROR_CODE do_set_elem_delete_with_value(struct squall_engine *engine,
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

static int do_set_elem_traverse_dfs(struct squall_engine *engine,
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

static uint32_t do_set_elem_delete(struct squall_engine *engine,
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

static uint32_t do_set_elem_get(struct squall_engine *engine,
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
#endif

#if 0 // ENABLE_SQUALL_ENGINE
static ENGINE_ERROR_CODE do_btree_item_find(struct squall_engine *engine,
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

static hash_item *do_btree_item_alloc(struct squall_engine *engine,
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
        info->sticky  = ((attrp->exptime == (rel_time_t)(-1)) ? 1 : 0);
#endif
        info->readable = attrp->readable;
        info->bktype  = BKEY_TYPE_UNKNOWN;
        info->stotal  = 0;
        info->prefix  = NULL;
        info->maxbkeyrange.len = BKEY_NULL;
        info->root    = NULL;
    }
    return it;
}

static btree_indx_node *do_btree_node_alloc(struct squall_engine *engine,
                                            const uint8_t node_depth, const void *cookie)
{
    size_t ntotal = sizeof(btree_indx_node);

    btree_indx_node *node = item_alloc_internal(engine, ntotal, LRU_CLSID_FOR_BTREE, cookie);
    if (node != NULL) {
        assert(node->slabs_clsid == 0);
        node->slabs_clsid = slabs_clsid(engine, ntotal);
        node->refcount    = 0;
        node->ndepth      = node_depth;
        node->used_count  = 0;
        node->prev = node->next = NULL;
        memset(node->item, 0, BTREE_ITEM_COUNT*sizeof(void*));
    }
    return node;
}

static void do_btree_node_free(struct squall_engine *engine, btree_indx_node *node)
{
    do_mem_slot_free(engine, node, sizeof(btree_indx_node));
}

static btree_elem_item *do_btree_elem_alloc(struct squall_engine *engine,
                                            const int nbkey, const int neflag, const int nbytes,
                                            const void *cookie)
{
    size_t ntotal = sizeof(btree_elem_item_fixed) + BTREE_REAL_NBKEY(nbkey) + neflag + nbytes;

    btree_elem_item *elem = item_alloc_internal(engine, ntotal, LRU_CLSID_FOR_BTREE, cookie);
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

static void do_btree_elem_free(struct squall_engine *engine, btree_elem_item *elem)
{
    assert(elem->refcount == 0);
    assert(elem->slabs_clsid != 0);
    size_t ntotal = BTREE_ELEM_SIZE(elem);
    do_mem_slot_free(engine, elem, ntotal);
}

static void do_btree_elem_release(struct squall_engine *engine, btree_elem_item *elem)
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


static btree_indx_node *do_btree_find_leaf(btree_indx_node *root,
                                           const unsigned char *bkey, const int nbkey,
                                           btree_elem_posi *path,
                                           btree_elem_item **found_elem)
{
    btree_indx_node *node = root;
    btree_elem_item *elem;
    int16_t mid, left, right, comp;

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
            *found_elem = elem;
            if (path) {
                path[node->ndepth].node = node;
                path[node->ndepth].indx = mid;
            }
            node = do_btree_get_first_leaf((btree_indx_node *)(node->item[mid]), path);
            assert(node->ndepth == 0);
            return node;
        }

        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = right;
        }
        node = (btree_indx_node *)(node->item[right]);
    }
    assert(node->ndepth == 0);
    return node;
}

static ENGINE_ERROR_CODE do_btree_find_insposi(btree_meta_info *info,
                                               const unsigned char *ins_bkey, const int ins_nbkey,
                                               btree_elem_posi *path)
{
    assert(info->root != NULL);
    btree_elem_item *elem = NULL;
    btree_indx_node *node;

    node = do_btree_find_leaf(info->root, ins_bkey, ins_nbkey, path, &elem);
    if (elem != NULL) { /* the same bkey is found */
        path[0].node = node;
        path[0].indx = 0;
        return ENGINE_ELEM_EEXISTS;
    } else {
        int mid, left, right, comp;

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

        if (left <= right) { /* The same bkey is found */
            path[0].node = node;
            path[0].indx = mid;
            return ENGINE_ELEM_EEXISTS;
        } else {
            path[0].node = node;
            path[0].indx = left;
            return ENGINE_SUCCESS;
        }
    }
}

static btree_elem_item *do_btree_find_first(btree_meta_info *info, const bkey_range *bkrange,
                                            btree_elem_posi *path, const bool path_flag, bool *bkey_isequal)
{
    assert(info->root != NULL);
    btree_elem_item *elem = NULL;
    btree_indx_node *node;

    *bkey_isequal = false;

    node = do_btree_find_leaf(info->root, bkrange->from_bkey, bkrange->from_nbkey,
                              (path_flag ? path : NULL), &elem);
    if (elem != NULL) { /* the same bkey is found */
        path[0].node = node;
        path[0].indx = 0;
        *bkey_isequal = true;
    } else {
        int mid, left, right, comp;

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

        if (left <= right) { /* the same bkey is found. */
            path[0].node = node;
            path[0].indx = mid;
            *bkey_isequal = true;
        } else {
            if (bkrange->to_nbkey == BKEY_NULL) {
                comp = 0;
            } else {
                comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey, bkrange->to_bkey, bkrange->to_nbkey);
            }
            if (comp == 0) {
                /* save the position info even if NOT FOUND */
                path[0].node = node;
                path[0].indx = left;
                if (left >= node->used_count) {
                    path[0].node = node->next; /* migit NULL */
                    path[0].indx = 0;
                } else if (left <= 0) {
                    path[0].node = node->prev;
                    if (node->prev != NULL)
                        path[0].indx = node->prev->used_count-1;
                    else
                        path[0].indx = BTREE_ITEM_COUNT;
                }
                return NULL;
            }
            if (comp < 0) {
                /* ascending range order */
                if (left < node->used_count) {
                    path[0].node = node;
                    path[0].indx = left;
                } else {
                    /* save the position info even if NOT FOUND */
                    path[0].node = node->next;
                    path[0].indx = 0;
                    if (node->next == NULL) return NULL;
                }
                elem = BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
                if (BKEY_ISGT(elem->data, elem->nbkey, bkrange->to_bkey, bkrange->to_nbkey))
                    return NULL;
            } else {
                /* descending range order */
                if (right >= 0) {
                    path[0].node = node;
                    path[0].indx = right;
                } else {
                    /* save the position info even if NOT FOUND */
                    path[0].node = node->prev;
                    if (node->prev != NULL)
                        path[0].indx = node->prev->used_count-1;
                    else
                        path[0].indx = BTREE_ITEM_COUNT;
                    if (node->prev == NULL) return NULL;
                }
                elem = BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
                if (BKEY_ISLT(elem->data, elem->nbkey, bkrange->to_bkey, bkrange->to_nbkey))
                    return NULL;
            }
        }
    }
    return elem;
}

static inline void do_btree_incr_posi(btree_elem_posi *posi)
{
    if (posi->indx < (posi->node->used_count-1)) {
        posi->indx += 1;
    } else {
        /* save the position info even if NOT FOUND */
        posi->node = posi->node->next;
        posi->indx = 0;
    }
}

static inline void do_btree_decr_posi(btree_elem_posi *posi)
{
    if (posi->indx > 0) {
        posi->indx -= 1;
    } else {
        /* save the position info even if NOT FOUND */
        posi->node = posi->node->prev;
        if (posi->node != NULL)
            posi->indx = posi->node->used_count-1;
        else
            posi->indx = BTREE_ITEM_COUNT;
    }
}

static btree_elem_item *do_btree_find_next(btree_elem_posi *posi,
                                           const unsigned char *to_bkey, const int to_nbkey)
{
    do_btree_incr_posi(posi);
    if (posi->node == NULL) {
        return NULL;
    } else {
        btree_elem_item *elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
        if (BKEY_ISLE(elem->data, elem->nbkey, to_bkey, to_nbkey))
            return elem;
        else
            return NULL;
    }
}

static btree_elem_item *do_btree_find_prev(btree_elem_posi *posi,
                                           const unsigned char *to_bkey, const int to_nbkey)
{
    do_btree_decr_posi(posi);
    if (posi->node == NULL) {
        return NULL;
    } else {
        btree_elem_item *elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
        if (BKEY_ISGE(elem->data, elem->nbkey, to_bkey, to_nbkey))
            return elem;
        else
            return NULL;
    }
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

    return (*BINARY_COMPARE_OP[efilter->compop])(operand, efilter->ncompval,
                                                 efilter->compval, efilter->ncompval);
}

static void do_btree_node_item_move(btree_indx_node *c_node, /* current node */
                                    btree_indx_node *n_node, /* neighbor node */
                                    int direction, int move_count) {
    assert(move_count > 0);
    int i;
    if (direction == BTREE_DIRECTION_NEXT) {
        for (i = (n_node->used_count-1); i >= 0; i--) {
             n_node->item[move_count+i] = n_node->item[i];
        }
        for (i = 0; i < move_count; i++) {
             n_node->item[i] = c_node->item[c_node->used_count-move_count+i];
        }
    } else { /* BTREE_DIRECTION_PREV */
        for (i = 0; i < move_count; i++) {
             n_node->item[n_node->used_count+i] = c_node->item[i];
        }
        for (i = move_count; i < c_node->used_count; i++) {
             c_node->item[i-move_count] = c_node->item[i];
        }
    }
    n_node->used_count += move_count;
    c_node->used_count -= move_count;
}

static void do_btree_node_sbalance(btree_indx_node *node, btree_elem_posi *posi)
{
    btree_indx_node *n_node; /* neighber node */
    int direction;
    int move_count;

    /* balance the number of elements with neighber node */
    if (node->next != NULL && node->prev != NULL) {
        direction = (node->next->used_count < node->prev->used_count ?
                     BTREE_DIRECTION_NEXT : BTREE_DIRECTION_PREV);
    } else {
        direction = (node->next != NULL ?
                     BTREE_DIRECTION_NEXT : BTREE_DIRECTION_PREV);
    }
    n_node = (direction == BTREE_DIRECTION_NEXT ? node->next : node->prev);

    if (n_node->used_count > 0) {
        move_count = (node->used_count - n_node->used_count) / 2;
    } else {
        if (n_node->prev != NULL && n_node->next != NULL)
            move_count = node->used_count / 2;
        else
            move_count = node->used_count / 10;
    }
    if (move_count == 0) move_count = 1;

    do_btree_node_item_move(node, n_node, direction, move_count);

    /* adjust posi information */
    if (direction == BTREE_DIRECTION_NEXT) {
        if (posi->indx >= node->used_count) {
            posi->node = n_node;
            posi->indx -= node->used_count;
        }
    } else {
        if (posi->indx < move_count) {
            posi->node = n_node;
            posi->indx += (n_node->used_count-move_count);
        } else {
            posi->indx -= move_count;
        }
    }
}

static void do_btree_node_link(struct squall_engine *engine,
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
        }
        p_node->item[p_posi->indx] = node;
        p_node->used_count++;
    }

    if (1) { /* apply memory space */
        size_t stotal = slabs_space_size(engine, sizeof(btree_indx_node));
        increase_collection_space(engine, (coll_meta_info *)info, stotal);
    }
}

static ENGINE_ERROR_CODE do_btree_node_split(struct squall_engine *engine,
                                             btree_meta_info *info, btree_elem_posi *path,
                                             const void *cookie)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    btree_indx_node *s_node;
    btree_indx_node *n_node[BTREE_MAX_DEPTH]; /* neighber nodes */
    int     i, direction;
    uint8_t btree_depth = 0;

    s_node = path[btree_depth].node;
    do {
        if ((s_node->next != NULL && s_node->next->used_count < (BTREE_ITEM_COUNT/2)) ||
            (s_node->prev != NULL && s_node->prev->used_count < (BTREE_ITEM_COUNT/2))) {
            do_btree_node_sbalance(s_node, &path[btree_depth]);
            break;
        }

        n_node[btree_depth] = do_btree_node_alloc(engine, btree_depth, cookie);
        if (n_node[btree_depth] == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        btree_depth += 1;
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
        btree_elem_posi p_posi;
        for (i = btree_depth-1; i >= 0; i--) {
            s_node = path[i].node;
            if (s_node->prev == NULL && s_node->next == NULL) {
                direction = (path[i].indx < (BTREE_ITEM_COUNT/2) ?
                             BTREE_DIRECTION_PREV : BTREE_DIRECTION_NEXT);
            } else {
                direction = (s_node->prev == NULL ?
                             BTREE_DIRECTION_PREV : BTREE_DIRECTION_NEXT);
            }
            if (direction == BTREE_DIRECTION_PREV) {
                p_posi = path[i+1];
            } else {
                path[i+1].indx += 1;
                p_posi = path[i+1];
            }
            do_btree_node_link(engine, info, n_node[i], &p_posi);
            do_btree_node_sbalance(s_node, &path[i]);
        }
    } else {
        for (i = 0; i < btree_depth; i++) {
            do_btree_node_free(engine, n_node[i]);
        }
    }
    return ret;
}

/* merge check */
static bool do_btree_node_mbalance(btree_indx_node *node)
{
    if ((node->prev != NULL && node->prev->used_count < (BTREE_ITEM_COUNT/2)) ||
        (node->next != NULL && node->next->used_count < (BTREE_ITEM_COUNT/2))) {
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
        return true;
    }
    return false;
}

static void do_btree_node_unlink(struct squall_engine *engine,
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
        for (int i = p_posi->indx+1; i < p_node->used_count; i++) {
            p_node->item[i-1] = p_node->item[i];
        }
        p_node->used_count--;
    }

    /* free the node */
    do_btree_node_free(engine, node);

    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(engine, sizeof(btree_indx_node));
        decrease_collection_space(engine, (coll_meta_info *)info, stotal);
    }
}

static void do_btree_node_detach(struct squall_engine *engine, btree_indx_node *node)
{
    /* unlink the given node from b+tree */
    if (node->prev != NULL) node->prev->next = node->next;
    if (node->next != NULL) node->next->prev = node->prev;
    node->prev = node->next = NULL;

    /* free the node */
    do_btree_node_free(engine, node);
}

static inline void do_btree_node_remove_null_items(btree_elem_posi *posi, const bool forward, const int null_count)
{
    /* null_count > 0 */
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
                node->item[f++] = node->item[i];
                node->item[i] = NULL;
            } else {
                rem_count++;
            }
        }
        assert(rem_count == null_count);
    }
    node->used_count -= null_count;
}

static int do_btree_multi_node_unlink(struct squall_engine *engine, btree_meta_info *info,
                                      btree_elem_posi *p_posi, const bool forward, const int count)
{
    btree_elem_posi c_posi = *p_posi; /* current posi */
    btree_elem_posi s_posi = c_posi;  /* start posi */
    btree_indx_node *node;
    int tot_unlink_cnt = 0;
    int cur_unlink_cnt = 0;
    int upper_node_cnt = 1;

    /* unlink non-leaf nodes */
    for (int i = 0; i < count; i++) {
        node = BTREE_GET_NODE_ITEM(c_posi.node, c_posi.indx);
        assert(node != NULL);

        if ((node->used_count == 0) ||
            (node->used_count < (BTREE_ITEM_COUNT/2) && do_btree_node_mbalance(node)==true)) {
            do_btree_node_detach(engine, node);
            c_posi.node->item[c_posi.indx] = NULL;
            //BTREE_GET_NODE_ITEM(c_posi.node, c_posi.indx) = NULL;
            cur_unlink_cnt++;
        }

        if (forward) do_btree_incr_posi(&c_posi);
        else         do_btree_decr_posi(&c_posi);

        if (c_posi.node == NULL) break;

        if (s_posi.node != c_posi.node) {
            if (cur_unlink_cnt > 0) {
                do_btree_node_remove_null_items(&s_posi, forward, cur_unlink_cnt);
                tot_unlink_cnt += cur_unlink_cnt; cur_unlink_cnt = 0;
            }
            s_posi = c_posi;
            upper_node_cnt += 1;
        }
    }

    if (cur_unlink_cnt > 0) {
        do_btree_node_remove_null_items(&s_posi, forward, cur_unlink_cnt);
        tot_unlink_cnt += cur_unlink_cnt; cur_unlink_cnt = 0;
    }

    if (tot_unlink_cnt > 0 && info->stotal > 0) { /* apply memory space */
        size_t stotal = tot_unlink_cnt * slabs_space_size(engine, sizeof(btree_indx_node));
        decrease_collection_space(engine, (coll_meta_info *)info, stotal);
    }

    return upper_node_cnt;
}

static void do_btree_node_merge(struct squall_engine *engine,
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
            break;
        }

        if (cur_node_count == 1) {
            if ((node->used_count == 0) ||
                (node->used_count < (BTREE_ITEM_COUNT/2) && do_btree_node_mbalance(node)==true)) {
                do_btree_node_unlink(engine, info, node, &path[btree_depth+1]);
                par_node_count = 1;
            } else {
                par_node_count = 0;
            }
        } else { /* cur_node_count >= 2 */
            par_node_count = do_btree_multi_node_unlink(engine, info,
                                                        &path[btree_depth+1], forward, cur_node_count);
        }

        btree_depth += 1;
        cur_node_count = par_node_count;
        par_node_count = 0;
    }
}

static void do_btree_elem_unlink(struct squall_engine *engine,
                                 btree_meta_info *info, btree_elem_posi *path)
{
    btree_elem_posi *posi = &path[0];
    btree_elem_item *elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    if (info->stotal > 0) { /* apply memory space */
        size_t stotal = slabs_space_size(engine, BTREE_ELEM_SIZE(elem));
        decrease_collection_space(engine, (coll_meta_info *)info, stotal);
    }

    if (elem->refcount > 0) {
        elem->status = BTREE_ITEM_STATUS_UNLINK;
    } else  {
        elem->status = BTREE_ITEM_STATUS_FREE;
        do_btree_elem_free(engine, elem);
    }

    /* remove the element from the leaf node */
    btree_indx_node *node = posi->node;
    for (int i = posi->indx+1; i < node->used_count; i++) {
        node->item[i-1] = node->item[i];
    }
    node->used_count--;
    info->ccnt--;

    if (node->used_count < (BTREE_ITEM_COUNT/2)) {
        do_btree_node_merge(engine, info, path, true, 1);
    }
}

static int do_btree_multi_elem_unlink(struct squall_engine *engine,
                                      btree_meta_info *info, btree_elem_posi *path,
                                      const bkey_range *bkrange, const eflag_filter *efilter,
                                      const int count)
{
    assert(bkrange->to_nbkey != BKEY_NULL);
    int    tot_fcnt = 0;
    int    cur_fcnt = 0;
    int    node_cnt = 1;
    size_t stotal   = 0;
    bool   forward  = (BKEY_ISLE(bkrange->from_bkey, bkrange->from_nbkey,
                                 bkrange->to_bkey,   bkrange->to_nbkey) ? true : false);

    btree_elem_posi c_posi = path[0];
    btree_elem_posi s_posi = c_posi;
    btree_elem_item *elem = BTREE_GET_ELEM_ITEM(c_posi.node, c_posi.indx);
    assert(elem != NULL);

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
            //BTREE_GET_ELEM_ITEM(c_posi.node, c_posi.indx) = NULL;

            cur_fcnt++;
            if (count > 0 && (tot_fcnt+cur_fcnt) >= count) break;
        }

        elem = (forward ? do_btree_find_next(&c_posi, bkrange->to_bkey, bkrange->to_nbkey)
                        : do_btree_find_prev(&c_posi, bkrange->to_bkey, bkrange->to_nbkey));
        if (elem == NULL) break;

        if (s_posi.node != c_posi.node) {
            if (cur_fcnt > 0) {
                do_btree_node_remove_null_items(&s_posi, forward, cur_fcnt);
                tot_fcnt += cur_fcnt; cur_fcnt  = 0;
            }
            s_posi = c_posi;
            node_cnt += 1;
        }
    } while (elem != NULL);

    if (cur_fcnt > 0) {
        do_btree_node_remove_null_items(&s_posi, forward, cur_fcnt);
        tot_fcnt += cur_fcnt; cur_fcnt  = 0;
    }

    info->ccnt -= tot_fcnt;

    if (stotal > 0 && info->stotal > 0) { /* apply memory space */
        decrease_collection_space(engine, (coll_meta_info *)info, stotal);
    }

    do_btree_node_merge(engine, info, path, forward, node_cnt);
    return tot_fcnt;
}

static ENGINE_ERROR_CODE do_btree_elem_replace(struct squall_engine *engine, btree_meta_info *info,
                                               btree_elem_posi *posi, btree_elem_item *new_elem)
{
    btree_elem_item *old_elem = BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    size_t old_stotal = slabs_space_size(engine, BTREE_ELEM_SIZE(old_elem));
    size_t new_stotal = slabs_space_size(engine, BTREE_ELEM_SIZE(new_elem));

#ifdef ENABLE_STICKY_ITEM
    if (new_stotal > old_stotal) {
        /* sticky memory limit check */
        if (info->sticky && engine->stats.sticky_bytes >= engine->config.sticky_limit) {
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

    if (new_stotal != old_stotal) {
        assert(info->stotal > 0);
        if (new_stotal > old_stotal)
            increase_collection_space(engine, (coll_meta_info *)info, (new_stotal-old_stotal));
        else
            decrease_collection_space(engine, (coll_meta_info *)info, (old_stotal-new_stotal));
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE do_btree_elem_update(struct squall_engine *engine, btree_meta_info *info,
                                              const bkey_range *bkrange, const eflag_update *eupdate,
                                              const char *value, const int nbytes, const void *cookie)
{
    ENGINE_ERROR_CODE ret = ENGINE_ELEM_ENOENT;
    btree_elem_posi  posi;
    btree_elem_item *elem;
    unsigned char *ptr;
    int real_nbkey;
    int new_neflag;
    int new_nbytes;
    bool bkey_isequal;

    if (info->root == NULL) return ret;

    elem = do_btree_find_first(info, bkrange, &posi, false, &bkey_isequal);
    if (elem != NULL) {
        assert(bkey_isequal == true);

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

static uint32_t do_btree_elem_delete(struct squall_engine *engine, btree_meta_info *info,
                                     const bkey_range *bkrange, const eflag_filter *efilter,
                                     const uint32_t count)
{
    uint32_t tot_fcnt = 0; /* found count */

    if (info->root != NULL) {
        btree_elem_posi path[BTREE_MAX_DEPTH];
        bool bkey_isequal;
        btree_elem_item *elem = do_btree_find_first(info, bkrange, path, true, &bkey_isequal);
        if (elem != NULL) {
            if (bkrange->to_nbkey == BKEY_NULL ||
                BKEY_ISEQ(bkrange->from_bkey, bkrange->from_nbkey, bkrange->to_bkey, bkrange->to_nbkey)) {
                assert(bkey_isequal == true);
                if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                    do_btree_elem_unlink(engine, info, path);
                    tot_fcnt = 1;
                }
            } else {
                tot_fcnt = do_btree_multi_elem_unlink(engine, info, path, bkrange, efilter, count);
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
            *(uint64_t*)bkrange->from_bkey = btree_uint64_min_bkey;
            *(uint64_t*)bkrange->to_bkey   = btree_uint64_max_bkey;
        } else {
            *(uint64_t*)bkrange->from_bkey = btree_uint64_max_bkey;
            *(uint64_t*)bkrange->to_bkey   = btree_uint64_min_bkey;
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
                if (info->ovflact == OVFL_LARGEST_TRIM)
                    *overflow_type = OVFL_TYPE_RANGE;
                else /* OVFL_SMALLEST_TRIM or OVFL_ERROR */
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
                if (info->ovflact == OVFL_SMALLEST_TRIM)
                    *overflow_type = OVFL_TYPE_RANGE;
                else /* OVFL_LARGEST_TRIM or OVFL_ERROR */
                    return ENGINE_EBKEYOOR;
            }
        }
    }

    /* step 2: overflow check on max element count */
    if (info->ccnt >= info->mcnt && *overflow_type == OVFL_TYPE_NONE) {
        if (info->ovflact == OVFL_ERROR) {
            return ENGINE_EOVERFLOW;
        }
        if (info->ovflact == OVFL_SMALLEST_TRIM) {
            if (min_bkey_elem == NULL)
                min_bkey_elem = do_btree_get_first_elem(info->root);
            if (BKEY_ISLT(elem->data, elem->nbkey, min_bkey_elem->data, min_bkey_elem->nbkey))
                return ENGINE_EBKEYOOR;
        } else { /* OVFL_LARGEST_TRIM */
            if (max_bkey_elem == NULL)
                max_bkey_elem = do_btree_get_last_elem(info->root);
            if (BKEY_ISGT(elem->data, elem->nbkey, max_bkey_elem->data, max_bkey_elem->nbkey))
                return ENGINE_EBKEYOOR;
        }
        *overflow_type = OVFL_TYPE_COUNT;
    }

    return ENGINE_SUCCESS;
}

static void do_btree_overflow_trim(struct squall_engine *engine, btree_meta_info *info,
                                   btree_elem_item *elem, const int overflow_type)
{
    assert(info->ovflact == OVFL_SMALLEST_TRIM || info->ovflact == OVFL_LARGEST_TRIM);

    if (overflow_type == OVFL_TYPE_RANGE) {
        btree_elem_item *edge_elem;
        uint32_t del_count;
        bkey_range bkrange_space;
        if (info->ovflact == OVFL_SMALLEST_TRIM) {
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
        del_count = do_btree_elem_delete(engine, info, &bkrange_space, NULL, 0);
        assert(del_count > 0);
        assert(info->ccnt > 0);
    } else { /* overflow_type == OVFL_TYPE_COUNT */
        assert(overflow_type == OVFL_TYPE_COUNT);
        assert((info->ccnt-1) == info->mcnt);

        btree_elem_posi delpath[BTREE_MAX_DEPTH];
        if (info->ovflact == OVFL_SMALLEST_TRIM) {
            delpath[0].node = do_btree_get_first_leaf(info->root, delpath);
            delpath[0].indx = 0;
        } else { /* info->ovflact == OVFL_LARGEST_TRIM */
            delpath[0].node = do_btree_get_last_leaf(info->root, delpath);
            delpath[0].indx = delpath[0].node->used_count - 1;
        }
        do_btree_elem_unlink(engine, info, delpath);
    }
}

static ENGINE_ERROR_CODE do_btree_elem_link(struct squall_engine *engine,
                                            btree_meta_info *info, btree_elem_item *elem,
                                            const bool replace_if_exist, bool *replaced,
                                            const void *cookie) {
    assert(info->root != NULL);
    ENGINE_ERROR_CODE res;
    btree_elem_posi path[BTREE_MAX_DEPTH];

    *replaced = false;

    res = do_btree_find_insposi(info, elem->data, elem->nbkey, path);
    if (res == ENGINE_SUCCESS) {
        int ovfl_type = OVFL_TYPE_NONE;

#ifdef ENABLE_STICKY_ITEM
        /* sticky memory limit check */
        if (info->sticky && engine->stats.sticky_bytes >= engine->config.sticky_limit) {
            return ENGINE_ENOMEM;
        }
#endif

        /* overflow check */
        if (info->ccnt > 0) {
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
        if (path[0].node->used_count > 0) {
            for (int i = (path[0].node->used_count-1); i >= path[0].indx; i--) {
                path[0].node->item[i+1] = path[0].node->item[i];
            }
        }
        path[0].node->item[path[0].indx] = elem;
        path[0].node->used_count++;
        info->ccnt++;

        if (info->ccnt == 1) {
            info->bktype = (elem->nbkey==0 ? BKEY_TYPE_UINT64 : BKEY_TYPE_BINARY);
        }

        if (1) { /* apply memory space */
            size_t stotal = slabs_space_size(engine, BTREE_ELEM_SIZE(elem));
            increase_collection_space(engine, (coll_meta_info *)info, stotal);
        }

        if (ovfl_type != OVFL_TYPE_NONE) {
            do_btree_overflow_trim(engine, info, elem, ovfl_type);
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
                                                   btree_elem_posi *posi, const bool ascend)
{
    assert(info->ccnt == info->mcnt);
    bool is_overlapped = false;

    if (posi->node == NULL) {
        /* check the not found elem position */
        if (info->ovflact == OVFL_SMALLEST_TRIM) {
           if (posi->indx == BTREE_ITEM_COUNT)
               is_overlapped = true;
        } else if (info->ovflact == OVFL_LARGEST_TRIM) {
           if (posi->indx == 0)
               is_overlapped = true;
        }
    } else {
        /* check the first found elem position */
        /* assume the bkey of the first found elem isn't same with the from_bkey of bkey range */
        assert(posi->node->ndepth == 0); /* leaf node */
        if (info->ovflact == OVFL_SMALLEST_TRIM) {
           if (ascend == true  &&
               posi->node->prev == NULL && posi->indx == 0) /* the first element */
               is_overlapped = true;
        } else if (info->ovflact == OVFL_LARGEST_TRIM) {
           if (ascend == false &&
               posi->node->next == NULL && posi->indx == (posi->node->used_count-1)) /* the last element */
               is_overlapped = true;

        }
    }
    return is_overlapped;
}

static uint32_t do_btree_elem_get(struct squall_engine *engine, btree_meta_info *info,
                                  const bkey_range *bkrange, const eflag_filter *efilter,
                                  const uint32_t offset, const uint32_t count, const bool delete,
                                  btree_elem_item **elem_array, bool *potentialbkeytrim)
{
    uint32_t tot_fcnt = 0; /* found count */

    *potentialbkeytrim = false;

    if (info->root != NULL) {
        btree_elem_posi path[BTREE_MAX_DEPTH];
        bool bkey_isequal;
        btree_elem_item *elem = do_btree_find_first(info, bkrange, path, delete, &bkey_isequal);
        if (elem != NULL) {
            if (bkrange->to_nbkey == BKEY_NULL ||
                BKEY_ISEQ(bkrange->from_bkey, bkrange->from_nbkey, bkrange->to_bkey, bkrange->to_nbkey)) {
                assert(bkey_isequal == true);
                if (offset == 0) {
                    if (efilter == NULL || do_btree_elem_filter(elem, efilter)) {
                        elem->refcount++;
                        elem_array[tot_fcnt++] = elem;
                        if (delete) do_btree_elem_unlink(engine, info, path);
                    }
                }
            } else {
                bool forward  = (BKEY_ISLE(bkrange->from_bkey, bkrange->from_nbkey,
                                           bkrange->to_bkey,   bkrange->to_nbkey) ? true : false);
                int  cur_fcnt = 0;
                int  skip_cnt = 0;
                size_t stotal = 0;
                int  node_cnt = 1;
                btree_elem_posi c_posi = path[0];
                btree_elem_posi s_posi = c_posi;

                /* check if start position might be trimmed */
                if (bkey_isequal == false && info->ccnt == info->mcnt) {
                    if (do_btree_overlapped_with_trimmed_space(info, &c_posi, forward)) {
                        *potentialbkeytrim = true;
                    }
                }

                /* get <count> elements */
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
                                //BTREE_GET_ELEM_ITEM(c_posi.node, c_posi.indx) = NULL;
                            }
                            cur_fcnt++;
                            if (count > 0 && (tot_fcnt+cur_fcnt) >= count) break;
                        }
                    }

                    elem = (forward ? do_btree_find_next(&c_posi, bkrange->to_bkey, bkrange->to_nbkey)
                                    : do_btree_find_prev(&c_posi, bkrange->to_bkey, bkrange->to_nbkey));
                    if (elem == NULL) break;

                    if (s_posi.node != c_posi.node) {
                        if (cur_fcnt > 0) {
                            if (delete) do_btree_node_remove_null_items(&s_posi, forward, cur_fcnt);
                            tot_fcnt += cur_fcnt; cur_fcnt  = 0;
                        }
                        s_posi = c_posi;
                        node_cnt += 1;
                    }
                } while (elem != NULL);

                /* check if end position might be trimmed */
                if (elem == NULL) {
                    if (c_posi.node == NULL && info->ccnt == info->mcnt) {
                        if (do_btree_overlapped_with_trimmed_space(info, &c_posi, forward)) {
                            *potentialbkeytrim = true;
                        }
                    }
                }

                if (cur_fcnt > 0) {
                    if (delete) do_btree_node_remove_null_items(&s_posi, forward, cur_fcnt);
                    tot_fcnt += cur_fcnt; cur_fcnt  = 0;
                }

                if (delete)
                    info->ccnt -= tot_fcnt;

                if (stotal > 0) { /* apply memory space */
                    assert(info->stotal > 0);
                    decrease_collection_space(engine, (coll_meta_info *)info, stotal);
                    do_btree_node_merge(engine, info, path, forward, node_cnt);
                }
            }
        } else {
            if (info->ccnt == info->mcnt) {
                bool forward = true;
                if (path[0].node != NULL && bkrange->to_nbkey != BKEY_NULL) {
                    forward = (BKEY_ISLE(bkrange->from_bkey, bkrange->from_nbkey,
                                         bkrange->to_bkey,   bkrange->to_nbkey) ? true : false);
                }
                if (do_btree_overlapped_with_trimmed_space(info, &path[0], forward)) {
                    *potentialbkeytrim = true;
                }
            }
        }
    }
    return tot_fcnt;
}

static uint32_t do_btree_elem_count(struct squall_engine *engine, btree_meta_info *info,
                                    const bkey_range *bkrange, const eflag_filter *efilter)
{
    uint32_t tot_fcnt = 0; /* found count */

    if (info->root != NULL) {
        btree_elem_posi  posi;
        bool bkey_isequal;
        btree_elem_item *elem = do_btree_find_first(info, bkrange, &posi, false, &bkey_isequal);
        if (elem != NULL) {
            if (bkrange->to_nbkey == BKEY_NULL ||
                BKEY_ISEQ(bkrange->from_bkey, bkrange->from_nbkey, bkrange->to_bkey, bkrange->to_nbkey)) {
                assert(bkey_isequal == true);
                if (efilter == NULL || do_btree_elem_filter(elem, efilter))
                    tot_fcnt++;
            } else { /* range */
                bool forward = (BKEY_ISLE(bkrange->from_bkey, bkrange->from_nbkey,
                                          bkrange->to_bkey,   bkrange->to_nbkey) ? true : false);
                do {
                    if (efilter == NULL || do_btree_elem_filter(elem, efilter))
                        tot_fcnt++;

                    elem = (forward ? do_btree_find_next(&posi, bkrange->to_bkey, bkrange->to_nbkey)
                                    : do_btree_find_prev(&posi, bkrange->to_bkey, bkrange->to_nbkey));
                } while (elem != NULL);
            }
        }
    }
    return tot_fcnt;
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
static ENGINE_ERROR_CODE do_btree_smget_scan_sort(struct squall_engine *engine,
                                    token_t *key_array, const int key_count,
                                    const bkey_range *bkrange, const eflag_filter *efilter,
                                    const uint32_t req_count, btree_scan_info *btree_scan_buf,
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
    bool ascending = (BKEY_ISLE(bkrange->from_bkey, bkrange->from_nbkey,
                                bkrange->to_bkey,   bkrange->to_nbkey) ? true : false);
    bool bkey_isequal;
    bkey_t   maxbkeyrange;
    int32_t  maxelemcount = 0;
    uint8_t  overflowactn = OVFL_SMALLEST_TRIM;

    *missed_key_count = 0;

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
        if (info->readable == 0) { /* unreadable collection */
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

        elem = do_btree_find_first(info, bkrange, &posi, false, &bkey_isequal);
        if (elem == NULL) { /* No elements within the bkey range */
            do_item_release(engine, it);
            if (info->ccnt == info->mcnt) {
                /* Some elements weren't cached because of overflow trim */
                if (do_btree_overlapped_with_trimmed_space(info, &posi, ascending)) {
                    ret = ENGINE_EBKEYOOR; break;
                }
            }
            continue;
        }

        if (bkey_isequal == false && info->ccnt == info->mcnt) {
            /* Some elements weren't cached because of overflow trim */
            if (do_btree_overlapped_with_trimmed_space(info, &posi, ascending)) {
                do_item_release(engine, it);
                ret = ENGINE_EBKEYOOR; break;
            }
        }

        do {
           if (efilter == NULL || do_btree_elem_filter(elem, efilter))
               break;

           elem = (ascending ? do_btree_find_next(&posi, bkrange->to_bkey, bkrange->to_nbkey)
                             : do_btree_find_prev(&posi, bkrange->to_bkey, bkrange->to_nbkey));
        } while (elem != NULL);

        if (elem == NULL) {
            do_item_release(engine, it);
            if (posi.node == NULL && info->ccnt == info->mcnt) {
                if (do_btree_overlapped_with_trimmed_space(info, &posi, ascending)) {
                    ret = ENGINE_EBKEYOOR; break;
                }
            }
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
                                    const bkey_range *bkrange, const eflag_filter *efilter,
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
    bool ascending = (BKEY_ISLE(bkrange->from_bkey, bkrange->from_nbkey,
                                bkrange->to_bkey,   bkrange->to_nbkey) ? true : false);

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
            elem = (ascending ? do_btree_find_next(&btree_scan_buf[curr_idx].posi,
                                                   bkrange->to_bkey, bkrange->to_nbkey)
                              : do_btree_find_prev(&btree_scan_buf[curr_idx].posi,
                                                   bkrange->to_bkey, bkrange->to_nbkey));
            if (elem != NULL) {
                if (efilter == NULL || do_btree_elem_filter(elem, efilter))
                    break;
            }
        } while (elem != NULL);

        if (elem == NULL) {
            if (btree_scan_buf[curr_idx].posi.node == NULL) {
                /* reached to the end of b+tree scan */
                info = (btree_meta_info *)item_get_meta(btree_scan_buf[curr_idx].it);
                if (info->ccnt == info->mcnt) {
                    if (do_btree_overlapped_with_trimmed_space(info, &btree_scan_buf[curr_idx].posi, ascending)) {
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
#endif

static hitm_CHUNK *do_item_hash_chunk_alloc()
{
    hitm_CHUNK *chunk = malloc(sizeof(hitm_CHUNK));
    if (chunk != NULL) {
        /* initialize the hash item chunk */
        for (int i = 0; i < HASH_ITEM_CHUNK_SIZE; i++) {
            if (i < (HASH_ITEM_CHUNK_SIZE-1)) {
                chunk->item[i].next = &chunk->item[i+1];
            } else {
                chunk->item[i].next = NULL;
            }
            /* item write status */
            chunk->item[i].desc.trx = NULL;
            chunk->item[i].desc.wrstat = (uint8_t)ITEM_WRSTAT_NONE;
            chunk->item[i].desc.cached = false;
            chunk->item[i].desc.inuse = false;
        }
    }
    return chunk;
}

#if 0 // USELESS FUNCTION
static void do_item_hash_chunk_free(hitm_CHUNK *chunk)
{
    free((void*)chunk);
}
#endif

static hash_item *do_item_hash_entry_alloc()
{
    hash_item *hitm = NULL;
    hitm_FREE_LIST *frlist = &item_gl.hitm_free_list;

    pthread_mutex_lock(&frlist->lock);
    if (frlist->itm_head == NULL) {
        hitm_CHUNK *chunk = do_item_hash_chunk_alloc();
        if (chunk != NULL) {
            chunk->c_next = frlist->chn_head;
            frlist->chn_head = chunk;
            frlist->chn_count += 1;

            frlist->itm_head = &chunk->item[0];
            frlist->itm_count += HASH_ITEM_CHUNK_SIZE;
        }
    }
    if (frlist->itm_head != NULL) {
        hitm = frlist->itm_head;
        frlist->itm_head = hitm->next;
        frlist->itm_count -= 1;
    }
    pthread_mutex_unlock(&frlist->lock);
    if (hitm != NULL) {
        hitm->desc.inuse = true;
    }
    return hitm;
}

static void do_item_hash_entry_free(hash_item *hitm)
{
    hitm_FREE_LIST *frlist = &item_gl.hitm_free_list;

    assert(hitm->desc.cached == false);

    /* item write status */
    hitm->desc.trx = NULL;
    hitm->desc.wrstat = (uint8_t)ITEM_WRSTAT_NONE;
    hitm->desc.inuse = false;

    pthread_mutex_lock(&frlist->lock);
    hitm->next = frlist->itm_head;
    frlist->itm_head = hitm;
    frlist->itm_count += 1;
    pthread_mutex_unlock(&frlist->lock);
}

static void do_item_push_del_queue(hash_item *it)
{
    it->next = NULL;
    pthread_mutex_lock(&item_gl.gc_thread.lock);
    if (item_gl.del_queue.head == NULL) {
        item_gl.del_queue.head = it;
    } else {
        item_gl.del_queue.tail->next = it;
    }
    item_gl.del_queue.tail = it;
    item_gl.del_queue.size += 1;
    if (item_gl.gc_thread.sleep == true) {
        /* wake up item delete thead */
        pthread_cond_signal(&item_gl.gc_thread.cond);
    }
    pthread_mutex_unlock(&item_gl.gc_thread.lock);
}

static hash_item *do_item_pop_del_queue(void)
{
    hash_item *it;
    pthread_mutex_lock(&item_gl.gc_thread.lock);
    it = item_gl.del_queue.head;
    if (it != NULL) {
        assert(it->desc.ptr->refcount == 0);
        item_gl.del_queue.head = it->next;
        if (item_gl.del_queue.head == NULL) {
            item_gl.del_queue.tail = NULL;
        }
        item_gl.del_queue.size -= 1;
    }
    pthread_mutex_unlock(&item_gl.gc_thread.lock);
    return it;
}

static void do_item_gc_thread_sleep(void)
{
    struct timeval  tv;
    struct timespec to;

    pthread_mutex_lock(&item_gl.gc_thread.lock);
    if (item_gl.del_queue.head == NULL) {
        /* 1 second sleep */
        gettimeofday(&tv, NULL);
        to.tv_sec = tv.tv_sec + 1;
        to.tv_nsec = tv.tv_usec * 1000;
        item_gl.gc_thread.sleep = true;
        pthread_cond_timedwait(&item_gl.gc_thread.cond,
                               &item_gl.gc_thread.lock, &to);
        item_gl.gc_thread.sleep = false;
    }
    pthread_mutex_unlock(&item_gl.gc_thread.lock);
}

static void do_item_gc_thread_wakeup(void)
{
    pthread_mutex_lock(&item_gl.gc_thread.lock);
    if (item_gl.gc_thread.sleep == true) {
        /* wake up item delete thead */
        pthread_cond_signal(&item_gl.gc_thread.cond);
    }
    pthread_mutex_unlock(&item_gl.gc_thread.lock);
}

#if 0 /* will be used later */
static void do_item_GC_operation(void)
{
    PageID       pgid;
    Page        *pgptr;
    data_item   *itptr;
    int          pfxid;

    /**** item GC operations ****
     * 1. remove expired items
     * 2. compact data pages if possible
     ****************************/

    pfxid = pfx_open_first();
    while (pfxid != -1)
    {
        pgptr = pfx_data_page_get_first(pfxid, &pgid);
        while (pgptr != NULL) {
            /* remove expired items */
            for (i = 0; i < pgptr->header.totl_nslots; i++) {
                itptr = dpage_find_slot(pgptr, i);
                if (itptr == NULL) continue;
                if (itptr->exptime != 0 && itptr->exptime <= server->core->get_current_time()) {
                    /* the expired item */
                }
            }
            pfx_data_page_release(pfxid, pgptr);
            pgptr = pfx_data_get_next_page(pfxid, pgid, &pgid);
        }
        pfx_close(pfxid);
        pfxid = pfx_open_next(pfxid);
    }
}
#endif

static void *item_gc_thread(void *arg)
{
    hash_item *it;

    assert(item_gl.gc_thread.init == true);

    item_gl.gc_thread.start = true;
    //fprintf(stderr, "item gc thread started\n");
    while (1)
    {
        it = do_item_pop_del_queue();
        if (it != NULL) {
            assert((it->desc.ptr->iflag & ITEM_LINKED) == 0);
            assert((it->desc.ptr->iflag & ITEM_DELMARK) != 0);
#ifdef ENABLE_RECOVERY
            if (config->sq.use_recovery) { /* TRANSACTION */
                it->desc.trx = (void*)tran_alloc(true); /* implicit transaction */
                if (it->desc.trx == NULL) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "item gc thread - tran alloc fail\n");
                }
                assert(it->desc.trx != NULL);
            }
#endif
            do_item_free(&it->desc);
#ifdef ENABLE_RECOVERY
            if (config->sq.use_recovery) { /* TRANSACTION */
                tran_free(it->desc.trx, TRAN_COM_COMMIT);
                it->desc.trx = NULL;
            }
#endif
            do_item_hash_entry_free(it);
            continue;
        }

        if (item_gl.gc_thread.init == false) {
            assert(item_gl.del_queue.head == NULL);
            break;
        }
        do_item_gc_thread_sleep();
    }
    //fprintf(stderr, "item gc thread terminated\n");
    item_gl.gc_thread.start = false;
    return NULL;
}

/********************************* ITEM ACCESS *******************************/

/*
 * Allocates a new item.
 */
ENGINE_ERROR_CODE item_alloc(const void *key, size_t nkey, size_t nbytes, uint32_t flags,
                             rel_time_t exptime, hash_item **item)
{
    ENGINE_ERROR_CODE ret;
    hash_item *it = do_item_hash_entry_alloc();
    if (it == NULL) {
        return ENGINE_ENOMEM;
    }
#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        it->desc.trx = (void*)tran_alloc(false);
        if (it->desc.trx == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item alloc - tran alloc fail\n");
            do_item_hash_entry_free(it);
            return ENGINE_ETRANS;
        }
    }
#endif
    ret = do_item_alloc(key, nkey, nbytes, flags, exptime, &it->desc);
    if (ret == ENGINE_SUCCESS) {
        it->desc.hsh = server->core->hash(key, nkey, 0);
        *item = it;
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "item alloc - item alloc fail\n");
        /* ENGINE_PREFIX_ENOENT | ENGINE_ENOMEM | ENGINE_ENODISK | ENGINE_FAILED */
#ifdef ENABLE_RECOVERY
        if (config->sq.use_recovery) { /* TRANSACTION */
            tran_free(it->desc.trx, TRAN_COM_ABORT); it->desc.trx = NULL;
        }
#endif
        do_item_hash_entry_free(it);
    }
    return ret;
}

/*
 * Returns an item if it hasn't been marked as expired,
 * lazy-expiring as needed.
 */
#if 1 // ENABLE_SQUALL_ENGINE
/* Item Cache Operations */
static hash_item *do_item_cache_get(int hidx, const void *key, const size_t nkey)
{
    hash_item *it = item_gl.hash_tab[hidx].h_next;
    while (it != NULL) {
        if ((nkey == it->desc.ptr->nkey) &&
            (memcmp(key, item_get_key(it->desc.ptr), nkey) == 0) &&
            ((it->desc.ptr->iflag & ITEM_LINKED) != 0))
            break; /* found */
        it = it->next;
    }
    if (it != NULL) {
        it->desc.ptr->refcount++;
        DEBUG_REFCNT(it->desc.ptr, '+');
        //pfx_open_with_id(PREFIXID_FROM_ITEMID(&it->desc.iid));
    }
    return it;
}

static void do_item_cache_release(hash_item *it)
{
    assert(it->desc.cached == true);
    it->desc.ptr->refcount--;
    DEBUG_REFCNT(it->desc.ptr, '-');
}

static void do_item_cache_link(int hidx, hash_item *it)
{
    it->desc.cached = true;
    it->next = item_gl.hash_tab[hidx].h_next;
    item_gl.hash_tab[hidx].h_next = it;
}

static void do_item_cache_unlink(int hidx, hash_item *it)
{
    hash_item *prev = NULL;
    hash_item *curr = item_gl.hash_tab[hidx].h_next;
    while (curr != NULL) {
        if (curr == it) break; /* found */
        prev = curr; curr = curr->next;
    }
    assert(curr != NULL);
    if (prev != NULL) prev->next = it->next;
    else              item_gl.hash_tab[hidx].h_next = it->next;
    it->desc.cached = false;
}
#endif

ENGINE_ERROR_CODE item_get(const void *key, const size_t nkey, hash_item **item)
{
    ENGINE_ERROR_CODE ret;
    hash_item *it;
    uint32_t hval = server->core->hash(key, nkey, 0);
    int      hidx = hval % item_gl.hash_size;

    if (item_gl.use_item_cache) {
        pthread_mutex_lock(&item_gl.hash_tab[hidx].lock);
        it = do_item_cache_get(hidx, key, nkey);
        pthread_mutex_unlock(&item_gl.hash_tab[hidx].lock);
        if (it != NULL) {
            *item = it;
            return ENGINE_SUCCESS;
        }
    }

    if ((it = do_item_hash_entry_alloc()) == NULL) {
        return ENGINE_ENOMEM;
    }
    it->desc.hsh = hval;

    pthread_mutex_lock(&item_gl.hash_tab[hidx].lock);
    do {
        ret = do_item_get(key, nkey, ITEM_TYPE_KV, true, &it->desc); // READONLY
        if (ret != ENGINE_SUCCESS) {
            /* ENGINE_KEY_ENOENT | ENGINE_EBADTYPE | ENGINE_FAILED */
            break;
        }
        if (it->desc.ptr->novfl > 0) {
            ret = do_item_ovfl_get(&it->desc);
            if (ret != ENGINE_SUCCESS) {
                /* ENGINE_ENOMEM | ENGINE_FAILED */
                do_item_release(&it->desc);
                break;
            }
        }
        if (item_gl.use_item_cache) {
            do_item_cache_link(hidx, it);
        }
    } while(0);
    pthread_mutex_unlock(&item_gl.hash_tab[hidx].lock);

    if (ret == ENGINE_SUCCESS) {
        *item = it;
    } else {
        do_item_hash_entry_free(it);
    }
    return ret;
}

/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void item_release(hash_item *item)
{
    /* two cases
     *  1. item_alloc => item_store => item_release
     *  2. item_get => item_release
     */
    int hidx = item->desc.hsh % item_gl.hash_size;

    if (item_gl.use_item_cache && item->desc.cached == true) {
        pthread_mutex_lock(&item_gl.hash_tab[hidx].lock);
        if (item->desc.ptr->refcount > 1) {
            do_item_cache_release(item);
            item = NULL;
        } else { /* item->desc.ptr->refcount == 1 */
            do_item_cache_unlink(hidx, item);
            do_item_release(&item->desc);
        }
        pthread_mutex_unlock(&item_gl.hash_tab[hidx].lock);
    } else {
        pthread_mutex_lock(&item_gl.hash_tab[hidx].lock);
        /* NOTE) write status of item descriptor
         * There might be a write status of ITEM_WRSTAT_ALLOC.
         * In that case, the item was allocated
         * but it is now released without storing.
         */
        do_item_release(&item->desc);
        pthread_mutex_unlock(&item_gl.hash_tab[hidx].lock);
    }
    if (item != NULL) {
#ifdef ENABLE_RECOVERY
        if (config->sq.use_recovery && item->desc.trx != NULL) { /* TRANSACTION */
            assert(item->desc.wrstat == (uint8_t)ITEM_WRSTAT_ALLOC);
            tran_free(item->desc.trx, TRAN_COM_ABORT); item->desc.trx = NULL;
        }
#endif
        if (item->desc.wrstat == (uint8_t)ITEM_WRSTAT_DELMARK)
            do_item_push_del_queue(item);
        else
            do_item_hash_entry_free(item);
    }
}

/*
 * Delete an item
 */
ENGINE_ERROR_CODE item_delete(const void* key, const size_t nkey, uint64_t cas)
{
    ENGINE_ERROR_CODE ret;
    ItemDesc itdbuf; /* item descriptor buffer */
    int      hidx;

    /* initialize item write status */
    itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;

#ifdef ENABLE_RECOVERY
    /* initialize transaction entry pointer */
    itdbuf.trx = NULL;
    if (config->sq.use_recovery) { /* TRANSACTION */
        itdbuf.trx = (void*)tran_alloc(false);
        if (itdbuf.trx == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item delete - tran alloc fail\n");
            return ENGINE_ETRANS;
        }
    }
#endif

    itdbuf.hsh = server->core->hash(key, nkey, 0);
    hidx = itdbuf.hsh % item_gl.hash_size;

    pthread_mutex_lock(&item_gl.hash_tab[hidx].lock);
    ret = do_item_get(key, nkey, ITEM_TYPE_ANY, false, &itdbuf);
    if (ret == ENGINE_SUCCESS) {
        if (cas == 0 || cas == item_get_cas(itdbuf.ptr)) {
            ret = do_item_unlink(&itdbuf);
            /* return code:
             *   ENGINE_SUCCESS | ENGINE_KEY_ENOENT | ENGINE_FAILED
             */
        } else {
            ret = ENGINE_KEY_EEXISTS;
        }
        do_item_release(&itdbuf);
    }
    pthread_mutex_unlock(&item_gl.hash_tab[hidx].lock);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        tran_free(itdbuf.trx, (ret==ENGINE_SUCCESS ? TRAN_COM_COMMIT : TRAN_COM_ABORT));
    }
#endif
    return ret;
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
ENGINE_ERROR_CODE item_store(hash_item *item, uint64_t *cas, ENGINE_STORE_OPERATION operation)
{
    ENGINE_ERROR_CODE ret;
    int hidx = item->desc.hsh % item_gl.hash_size;

    pthread_mutex_lock(&item_gl.hash_tab[hidx].lock);
    switch (operation) {
      case OPERATION_SET:
           ret = do_item_store_set(&item->desc);
           break;
      case OPERATION_ADD:
           ret = do_item_store_add(&item->desc);
           break;
      case OPERATION_REPLACE:
           ret = do_item_store_replace(&item->desc);
           break;
      case OPERATION_CAS:
           ret = do_item_store_cas(&item->desc);
           break;
      case OPERATION_PREPEND:
      case OPERATION_APPEND:
           ret = do_item_store_attach(&item->desc, operation);
           break;
      default:
           ret = ENGINE_NOT_STORED;
    }
    //do_item_store_release(&item->desc);
    pthread_mutex_unlock(&item_gl.hash_tab[hidx].lock);

    if (ret == ENGINE_SUCCESS) {
        assert(item->desc.wrstat == (uint8_t)ITEM_WRSTAT_LINK);
#ifdef ENABLE_RECOVERY
        if (config->sq.use_recovery) { /* TRANSACTION */
            tran_free(item->desc.trx, TRAN_COM_COMMIT); item->desc.trx = NULL;
        }
#endif
        item->desc.wrstat = (uint8_t)ITEM_WRSTAT_NONE; /* reset write status */
        /* get cas value */
        *cas = item_get_cas(item->desc.ptr);
    }
    return ret;
}

/*
 * Increment/Decrement the value of an item
 */
ENGINE_ERROR_CODE item_arithmetic(const void* key, const int nkey, const bool incr, const uint64_t delta,
                                  const bool create, const uint64_t initial, const int flags,
                                  const rel_time_t exptime, uint64_t *cas, uint64_t *result)
{
    ENGINE_ERROR_CODE ret;
    ItemDesc itdbuf; /* item descriptor buffer */
    int      hidx;

    /* initialize item write status */
    itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;

#ifdef ENABLE_RECOVERY
    /* initialize transaction entry pointer */
    itdbuf.trx = NULL;
    if (config->sq.use_recovery) { /* TRANSACTION */
        itdbuf.trx = (void*)tran_alloc(false);
        if (itdbuf.trx == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item arithmetic - tran alloc fail\n");
            return ENGINE_ETRANS;
        }
    }
#endif

    itdbuf.hsh = server->core->hash(key, nkey, 0);
    hidx = itdbuf.hsh % item_gl.hash_size;

    pthread_mutex_lock(&item_gl.hash_tab[hidx].lock);
    ret = do_item_get(key, nkey, ITEM_TYPE_KV, false, &itdbuf);
    if (ret == ENGINE_SUCCESS) {
        ret = do_add_delta(&itdbuf, incr, delta, cas, result);
        do_item_release(&itdbuf);
    } else {
        /* ENGINE_KEY_ENOENT | ENGINE_EBADTYPE | ENGINE_FAILED */
        if (ret == ENGINE_KEY_ENOENT && create) {
            char buffer[128];
            int len = snprintf(buffer, sizeof(buffer), "%"PRIu64"\r\n", (uint64_t)initial);

            ret = do_item_alloc(key, nkey, len, flags, exptime, &itdbuf);
            if (ret == ENGINE_SUCCESS) {
                memcpy((void*)item_get_data(itdbuf.ptr), buffer, len);
                ret = do_item_link(&itdbuf);
                if (ret == ENGINE_SUCCESS) {
                    *cas = item_get_cas(itdbuf.ptr);
                    *result = initial;
                } else {
                    /* ENGINE_KEY_EEXISTS | ENGINE_FAILED */
                }
                do_item_release(&itdbuf);
            } else {
                /* ENGINE_PREFIX_ENOENT | ENGINE_ENOMEM | ENGINE_ENODISK | ENGINE_FAILED */
            }
        }
    }
    pthread_mutex_unlock(&item_gl.hash_tab[hidx].lock);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        tran_free(itdbuf.trx, (ret==ENGINE_SUCCESS ? TRAN_COM_COMMIT : TRAN_COM_ABORT));
    }
#endif
    return ret;
}

bool item_info_get(hash_item *item, item_info *info)
{
    data_item *dit = item->desc.ptr;
    struct iovec *vector;
    uint16_t real_valcnt;

    if (info->nvalue < 1) return false;

    info->cas = item_get_cas(dit);
    info->flags = dit->flags;
    info->exptime = dit->exptime;
    info->nbytes = dit->nbytes;
    info->clsid = 0; /* useless information */
    info->nkey = dit->nkey;
    info->key = item_get_key(dit);

    if (dit->novfl == 0) {
        real_valcnt = 1;
    } else {
        real_valcnt = (dit->ndata > 0 ? 1 : 0) + dit->novfl;
    }
    if (info->nvalue < real_valcnt) {
        if ((vector = malloc(sizeof(struct iovec) * real_valcnt)) == NULL) {
            return false; /* no more memory */
        }
        info->value[0].iov_base = vector;
        info->value[0].iov_len = 0;
    } else {
        vector = info->value;
    }
    info->nvalue = real_valcnt;

    if (dit->novfl == 0) {
        vector[0].iov_base = item_get_data(dit);
        vector[0].iov_len  = dit->nbytes;
    } else { /* dit->novfl > 0 */
        ovfl_item *ovfl_slot;
        int i, vcnt = 0;
        if (dit->ndata > 0) {
            vector[vcnt].iov_base = item_get_data(dit);
            vector[vcnt].iov_len  = dit->ndata;
            vcnt++;
        }
        for (i = 0; i < dit->novfl; i++) {
            ovfl_slot = (ovfl_item*)item->desc.ofl[i];
            vector[vcnt].iov_base = (char*)ovfl_slot + sizeof(ovfl_item);
            vector[vcnt].iov_len  = ovfl_slot->ndata;
            vcnt++;
        }
    }
    return true;
}

/*
 * Flushes expired items after a flush_all call
 */
ENGINE_ERROR_CODE item_flush_prefix(const char *prefix, const int nprefix, time_t when)
{
    /* The 'when' argument is ignored */
    ENGINE_ERROR_CODE ret = pfx_delete(prefix, nprefix);
    if (config->verbose) {
        if (nprefix >= 0) {
            logger->log(EXTENSION_LOG_INFO, NULL, "flush prefix(%s): %s",
                        (prefix==NULL ? "null" : prefix), (ret==ENGINE_SUCCESS ? "sucess" : "not exist"));
        } else {
            logger->log(EXTENSION_LOG_INFO, NULL, "flush all");
        }
    }
    return ret;
}

/*
 * Dumps part of the cache
 */
char *item_cachedump(const char *prefix, const int nprefix, const bool forward,
                     unsigned int offset, unsigned int limit, unsigned int *bytes)
{
    /* implemention will be here */
    char *buffer;
    unsigned int memlimit = 1024;
    unsigned int bufcurr;

    buffer = malloc((size_t)memlimit);
    if (buffer != NULL) {
        bufcurr = 0;

        memcpy(buffer + bufcurr, "END\r\n", 6);
        bufcurr += 5;

        *bytes = bufcurr;
    }
    return buffer;
}

#if 1 // ENABLE_RECOVERY
void item_undo_free(ItemDesc *itdesc)
{
    do_item_free(itdesc);
}
#endif

static ENGINE_ERROR_CODE do_item_data_check(int pfxid, uint64_t *item_count)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    PageID       pgid;
    Page        *pgptr;
    data_item   *itptr;
    uint64_t    tot_itm_siz = 0;
    uint64_t    tot_itm_cnt = 0;
    uint64_t    tot_dpg_cnt = 0;
    uint64_t    tot_opg_cnt = 0;
    int         err_slots, i;
    bool        exist;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    pgid = pfx_data_page_get_first(pfxid);
    while (!PAGEID_IS_NULL(&pgid))
    {
        tot_dpg_cnt += 1;
        if ((tot_dpg_cnt % 100000) == 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "PREFIX [%s] checking data pages (count=%"PRIu64")\n",
                        (pfdesc->nlen == 0 ? "NULL" : pfdesc->name), tot_dpg_cnt);
        }

        pgptr = buf_fix(pgid, BUF_MODE_READ, false, false);
        if (pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "data page check - fix fail\n");
            ret = ENGINE_BUFF_FAILED; break;
        }
        if (dpage_check_consistency(pgptr) == false) {
            buf_unfix(pgptr);
            logger->log(EXTENSION_LOG_WARNING, NULL, "data page check - consistency fail\n");
            ret = ENGINE_PAGE_FAILED; break;
        }
        err_slots = 0;
        for (i = 0; i < pgptr->header.totl_nslots; i++) {
            itptr = dpage_find_slot(pgptr, i);
            if (itptr == NULL) continue;

            ret = assoc_exist(pfxid, item_get_key(itptr), itptr->nkey, &exist);
            if (ret != ENGINE_SUCCESS) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "data item check - assoc exist fail\n");
                break;
            }
            if (exist == false) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "data item check - no index key fail\n");
                err_slots += 1;
            }
            else if (itptr->refcount != 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "data item check - refcount(%d) fail\n",
                            itptr->refcount);
                err_slots += 1;
            }
            else if ((itptr->iflag & ITEM_LINKED) == 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "data item check - iflag(%X) fail\n",
                            itptr->iflag);
                err_slots += 1;
            }
            tot_opg_cnt += itptr->novfl;
            tot_itm_siz += ITEM_stotal(itptr, true);
        }
        tot_itm_cnt += pgptr->header.used_nslots;
        buf_unfix(pgptr);

        if (ret != ENGINE_SUCCESS) break;
        if (err_slots > 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "data item check - error slots(%d) found in page(%u|%u|%u)\n",
                        err_slots, pgid.pfxid, pgid.flnum, pgid.pgnum);
            ret = ENGINE_EINCONSISTENT; break;
        }
        pgid = pfx_data_page_get_next(pfxid, pgid);
    }

    if (ret == ENGINE_SUCCESS) { /* check item stats of prefix */
        uint64_t stat_itm_siz = 0;
        uint64_t stat_itm_cnt = 0;
        for (i = 0; i < ITEM_TYPE_MAX; i++) {
            stat_itm_siz += pfdesc->d.stats.siz_items[i];
            stat_itm_cnt += pfdesc->d.stats.num_items[i];
        }
        do {
            if (stat_itm_siz != tot_itm_siz) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "data item size mismatch - stats(%"PRIu64") != check(%"PRIu64")\n",
                            stat_itm_siz, tot_itm_siz);
                ret = ENGINE_EINCONSISTENT; break;
            }
            if (stat_itm_cnt != tot_itm_cnt) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "data item count mismatch - stats(%"PRIu64") != check(%"PRIu64")\n",
                            stat_itm_cnt, tot_itm_cnt);
                ret = ENGINE_EINCONSISTENT; break;
            }
            *item_count = tot_itm_cnt;
        } while(0);
    }

    if (ret == ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "PREFIX [%s] data_item_count=%"PRIu64" data_page_count=%"PRIu64" ovfl_page_count=%"PRIu64"\n",
                    (pfdesc->nlen == 0 ? "NULL" : pfdesc->name), tot_itm_cnt, tot_dpg_cnt, tot_opg_cnt);
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "PREFIX [%s] data inconsistent\n",
                    (pfdesc->nlen == 0 ? "NULL" : pfdesc->name));
    }
    return ret;
}

ENGINE_ERROR_CODE item_dbcheck(void)
{
    prefix_desc *pfdesc;
    uint64_t ditem_count;
    uint64_t xitem_count;
    ENGINE_ERROR_CODE ret;

    fprintf(stderr, "-----------------------------------\n"
                    "squall db consistency check: start\n");

    do {
        /* step 1) check prefix spaces */
        ret = pfx_space_dbcheck();
        if (ret != ENGINE_SUCCESS) {
            fprintf(stderr, "prefix space consistency check fail\n");
            break;
        }
        /* step 2) check prefixes */
        pfdesc = pfx_desc_get_first();
        while (pfdesc != NULL) {
            ret = assoc_dbcheck(pfdesc->pfxid, &xitem_count);
            if (ret != ENGINE_SUCCESS) {
                fprintf(stderr, "prefix(%d) index consistency check fail\n", pfdesc->pfxid);
                break;
            }
            ret = do_item_data_check(pfdesc->pfxid, &ditem_count);
            if (ret != ENGINE_SUCCESS) {
                fprintf(stderr, "prefix(%d) data consistency check fail\n", pfdesc->pfxid);
                break;
            }
            if (ditem_count != xitem_count) {
                fprintf(stderr, "prefix(%d) item & key count mismatch - items(%"PRIu64") != keys(%"PRIu64")\n",
                        pfdesc->pfxid, ditem_count, xitem_count);
                ret = ENGINE_EINCONSISTENT; break;
            }
            pfdesc = pfx_desc_get_next(pfdesc->pfxid);
        }
    } while(0);

    fprintf(stderr, "squall db consistency check: %s\n"
                    "-----------------------------------\n",
            (ret == ENGINE_SUCCESS ? "success" : "fail"));
    return ret;
}

void item_stats_get(int target, void *stats)
{
    /* target: ITEM_STAT_MAIN | ITEM_STAT_SIZE */
    switch (target) {
      case ITEM_STATS_MAIN:
        do_item_stats_main((struct item_stat*)stats);
        break;
      case ITEM_STATS_SIZE:
        do_item_stats_size((struct item_size*)stats);
        break;
    }
}

void item_stats_reset(void)
{
    pthread_mutex_lock(&item_gl.stats.lock);
    item_gl.stats.stat.evictions = 0;
    item_gl.stats.stat.reclaimed = 0;
    item_gl.stats.stat.total_items = 0;
    pthread_mutex_unlock(&item_gl.stats.lock);
}

static void do_item_stats_init(void)
{
    prefix_desc *pfdesc;

    /* initialize global item stats */
    pthread_mutex_init(&item_gl.stats.lock, NULL);
    item_gl.stats.stat.evictions = 0;
    item_gl.stats.stat.reclaimed = 0;
    item_gl.stats.stat.curr_bytes = 0;
    item_gl.stats.stat.curr_items = 0;
    item_gl.stats.stat.total_items = 0;

    pfdesc = pfx_desc_get_first();
    while (pfdesc != NULL) {
        for (int i = 0; i < ITEM_TYPE_MAX; i++) {
            item_gl.stats.stat.curr_items += pfdesc->d.stats.num_items[i];
            item_gl.stats.stat.curr_bytes += pfdesc->d.stats.siz_items[i];
        }
        pfdesc = pfx_desc_get_next(pfdesc->pfxid);
    }
    item_gl.stats.stat.total_items = item_gl.stats.stat.curr_items;
}

static void do_item_scrubber_init(void)
{
    pthread_mutex_init(&item_gl.scrubber.lock, NULL);
    item_gl.scrubber.stat.visited = 0;
    item_gl.scrubber.stat.cleaned = 0;
    item_gl.scrubber.stat.started = 0;
    item_gl.scrubber.stat.stopped = 0;
    item_gl.scrubber.stat.runmode = SCRUB_MODE_STOP;
    item_gl.scrubber.stat.running = false;
}

static void do_item_del_queue_init(void)
{
    /* initialize item delete queue */
    item_gl.del_queue.head = NULL;
    item_gl.del_queue.tail = NULL;
    item_gl.del_queue.size = 0;
}

static void do_item_gc_thread_init(void)
{
    pthread_mutex_init(&item_gl.gc_thread.lock, NULL);
    pthread_cond_init(&item_gl.gc_thread.cond, NULL);
    item_gl.gc_thread.sleep = false;
    item_gl.gc_thread.start = false;
    item_gl.gc_thread.init = false;
}

static int do_item_gc_thread_start(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.
    pthread_t tid;
    int       ret;

    /* start request */
    item_gl.gc_thread.init = true;

    ret = pthread_create(&tid, NULL, item_gc_thread, NULL);
    if (ret != 0) {
        item_gl.gc_thread.init = false;
        fprintf(stderr, "Can't create item gc thread: %s\n", strerror(ret));
        return -1;
    }
    /* wait until item gc thread starts */
    while (item_gl.gc_thread.start == false) {
        nanosleep(&sleep_time, NULL);
    }
    return 0;
}

static void do_item_gc_thread_stop(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.

    if (item_gl.gc_thread.init == true) {
        /* stop request */
        pthread_mutex_lock(&item_gl.gc_thread.lock);
        item_gl.gc_thread.init = false;
        pthread_mutex_unlock(&item_gl.gc_thread.lock);

        /* wait until item gc thread finishes its task */
        while (item_gl.gc_thread.start == true) {
            if (item_gl.gc_thread.sleep == true) {
                do_item_gc_thread_wakeup();
            }
            nanosleep(&sleep_time, NULL);
        }
    }
}

static void do_item_global_init(void)
{
    /* do not use item cache */
    item_gl.use_item_cache = false;

    /* page sise and slot sizes */
    item_gl.page_body_size = config->sq.data_page_size - sizeof(PGHDR);
    item_gl.max_base_slot_size = item_gl.page_body_size - SLOT_ALIGN_BYTES;  /* consider slot structure */
    item_gl.max_ovfl_data_size = item_gl.page_body_size - (sizeof(ovfl_item)+1); /* consider ovfl item header and trailer */
}

static void do_item_struct_final(void)
{
    hitm_FREE_LIST *frlist = &item_gl.hitm_free_list;
    hitm_CHUNK     *chunk;

    if (frlist->itm_count != (frlist->chn_count * HASH_ITEM_CHUNK_SIZE)) {
        fprintf(stderr, "hash item free list: chn_count=%u itm_count=%u (chn_size=%d)\n",
                frlist->chn_count, frlist->itm_count, HASH_ITEM_CHUNK_SIZE);
    }
    while (frlist->chn_head != NULL) {
        chunk = frlist->chn_head;
        frlist->chn_head = chunk->c_next;
        free((void*)chunk);
    }
    if (item_gl.hash_tab != NULL) {
        free(item_gl.hash_tab);
    }
}

static int do_item_struct_init(void)
{
    int ret = 0;

    item_gl.hash_size = config->num_threads * 1024;
    item_gl.hash_tab = NULL;
    item_gl.hitm_free_list.chn_head = NULL;

    do {
        /* initialize hash table */
        item_gl.hash_tab = malloc(sizeof(hitm_BUCKET) * item_gl.hash_size);
        if (item_gl.hash_tab == NULL) {
            fprintf(stderr, "item manager - hash table alloc error\n");
            ret = -1; break;
        }
        for (int i = 0; i < item_gl.hash_size; i++) {
            pthread_mutex_init(&item_gl.hash_tab[i].lock, NULL);
            item_gl.hash_tab[i].h_next = NULL;
        }

        /* initialize hash item free list */
        hitm_CHUNK *chunk = do_item_hash_chunk_alloc();
        if (chunk == NULL) {
            fprintf(stderr, "item manager - hash item chunk alloc error\n");
            ret = -1; break;
        }
        chunk->c_next = item_gl.hitm_free_list.chn_head;
        item_gl.hitm_free_list.chn_head = chunk;
        item_gl.hitm_free_list.chn_count = 1;
        item_gl.hitm_free_list.itm_head  = &item_gl.hitm_free_list.chn_head->item[0];
        item_gl.hitm_free_list.itm_count = HASH_ITEM_CHUNK_SIZE;
        pthread_mutex_init(&item_gl.hitm_free_list.lock, NULL);
    } while(0);

    if (ret != 0) {
        do_item_struct_final();
    }
    return ret;
}

int item_mgr_init(void *conf, void *srvr)
{
    config = (struct config *)conf;
    server = (SERVER_HANDLE_V1 *)srvr;
    logger = server->log->get_logger();

    item_gl.initialized = false;

    do_item_global_init();
    if (do_item_struct_init() != 0) {
        return -1;
    }

    do_item_stats_init();
    do_item_scrubber_init();

    do_item_del_queue_init();
    do_item_gc_thread_init();

    item_gl.initialized = true;
    return 0;
}

void item_mgr_release(void)
{
    /* hash item release */
    hitm_FREE_LIST *frlist = &item_gl.hitm_free_list;
    hitm_CHUNK     *chunk;
    hash_item      *hitem;

    if (frlist->itm_count == (frlist->chn_count * HASH_ITEM_CHUNK_SIZE)) {
        return; /* no hash items in use */
    }

    chunk = frlist->chn_head;
    while (chunk != NULL) {
        for (int i = 0; i < HASH_ITEM_CHUNK_SIZE; i++) {
            hitem = &chunk->item[i];
            if (hitem->desc.inuse == true) {
                do_item_release(&hitem->desc);
#ifdef ENABLE_RECOVERY
                if (config->sq.use_recovery && hitem->desc.trx != NULL) { /* TRANSACTION */
                    assert(hitem->desc.wrstat == (uint8_t)ITEM_WRSTAT_ALLOC);
                    tran_free(hitem->desc.trx, TRAN_COM_ABORT); hitem->desc.trx = NULL;
                }
#endif
                do_item_hash_entry_free(hitem);
            }
        }
        chunk = chunk->c_next;
    }
}

void item_mgr_final(void)
{
    /* release item resources */
    do_item_struct_final();
}

int item_mgr_gc_thread_start(void)
{
    return do_item_gc_thread_start();
}

void item_mgr_gc_thread_stop(void)
{
    do_item_gc_thread_stop();
}

/*
 * List Interface Functions
 */
#if 1 // ENABLE_SQUALL_ENGINE
#if 0 // sample code
ENGINE_ERROR_CODE list_struct_create(const char *key, const size_t nkey, item_attr *attrp)
{
    ENGINE_ERROR_CODE ret;
    ItemDesc itdbuf;
    uint32_t hval = server->core->hash(key, nkey, 0);
    int      hidx = hval % item_gl.hash_size;

    pthread_mutex_lock(&item_gl.hash_tab[hidx].lock);
    ret = do_item_get(key, nkey, ITEM_TYPE_LIST, &itdbuf);
    if (ret == ENGINE_SUCCESS) {
        do_item_release(&itdbuf);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        ret = do_list_item_alloc(key, nkey, attrp, &itdbuf);
        if (ret == ENGINE_SUCCESS) {
            ret = do_item_link(&itdbuf);
            do_item_release(engine, it);
        }
    }
    pthread_mutex_unlock(&item_gl.hash_tab[hidx].lock);
    return ret;
}
#endif
#else
ENGINE_ERROR_CODE list_struct_create(struct squall_engine *engine,
                                     const char *key, const size_t nkey,
                                     item_attr *attrp, const void *cookie) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
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
            if (do_item_link(engine, it) != 1) {
                ret = ENGINE_ENOMEM;
            }
            do_item_release(engine, it);
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

list_elem_item *list_elem_alloc(struct squall_engine *engine,
                                const int nbytes, const void *cookie) {
    list_elem_item *elem;

    pthread_mutex_lock(&engine->cache_lock);
    elem = do_list_elem_alloc(engine, nbytes, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return elem;
}

void list_elem_release(struct squall_engine *engine,
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

ENGINE_ERROR_CODE list_elem_insert(struct squall_engine *engine,
                                   const char *key, const size_t nkey,
                                   int index, list_elem_item *elem,
                                   item_attr *attrp,
                                   bool *created, const void *cookie) {
    ENGINE_ERROR_CODE ret;
    hash_item *it;
    list_meta_info *info = NULL;

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
            if (info->sticky && engine->stats.sticky_bytes >= engine->config.sticky_limit) {
                ret = ENGINE_ENOMEM; break;
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
                if (do_item_link(engine, it) != 1) {
                    /* The list item is to be released in the outside of do-while loop */
                    ret = ENGINE_ENOMEM; break;
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

static int adjust_list_range(int num_elems, int *from_index, int *to_index) {
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

ENGINE_ERROR_CODE list_elem_delete(struct squall_engine *engine,
                                   const char *key, const size_t nkey,
                                   int from_index, int to_index,
                                   const bool drop_if_empty,
                                   uint32_t *del_count, bool *dropped) {
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

ENGINE_ERROR_CODE list_elem_get(struct squall_engine *engine,
                                const char *key, const size_t nkey,
                                int from_index, int to_index, const bool delete,
                                const bool drop_if_empty,
                                list_elem_item **elem_array, uint32_t *elem_count,
                                uint32_t *flags, bool *dropped) {
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_list_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        list_meta_info *info = (list_meta_info *)item_get_meta(it);
        do {
            if (info->readable == 0) {
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
#endif

/*
 * Set Interface Functions
 */
#if 1 // ENABLE_SQUALL_ENGINE
/*
 * NOT Yet Implemented
 */
#else
ENGINE_ERROR_CODE set_struct_create(struct squall_engine *engine,
                                    const char *key, const size_t nkey,
                                    item_attr *attrp, const void *cookie) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
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
            if (do_item_link(engine, it) != 1) {
                ret = ENGINE_ENOMEM;
            }
            do_item_release(engine, it);
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

set_elem_item *set_elem_alloc(struct squall_engine *engine,
                              const int nbytes, const void *cookie) {
    set_elem_item *elem;

    pthread_mutex_lock(&engine->cache_lock);
    elem = do_set_elem_alloc(engine, nbytes, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return elem;
}

void set_elem_release(struct squall_engine *engine,
                      set_elem_item **elem_array, const int elem_count)
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

ENGINE_ERROR_CODE set_elem_insert(struct squall_engine *engine,
                                  const char *key, const size_t nkey,
                                  set_elem_item *elem,
                                  item_attr *attrp,
                                  bool *created, const void *cookie) {
    ENGINE_ERROR_CODE ret;
    hash_item *it;
    set_meta_info *info = NULL;

    *created = false;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_set_item_find(engine, key, nkey, false, &it);
    do {
        if (ret == ENGINE_SUCCESS) { /* it != NULL */
            info = (set_meta_info *)item_get_meta(it);
#ifdef ENABLE_STICKY_ITEM
            /* sticky memory limit check */
            if (info->sticky && engine->stats.sticky_bytes >= engine->config.sticky_limit) {
                ret = ENGINE_ENOMEM; break;
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
                if (do_item_link(engine, it) != 1) {
                    /* The set item is to be released in the outside of do-while loop */
                    ret = ENGINE_ENOMEM; break;
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

ENGINE_ERROR_CODE set_elem_delete(struct squall_engine *engine,
                                  const char *key, const size_t nkey,
                                  const char *value, const size_t nbytes,
                                  const bool drop_if_empty,
                                  bool *dropped) {
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    *dropped = false;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_set_item_find(engine, key, nkey, false, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
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

ENGINE_ERROR_CODE set_elem_exist(struct squall_engine *engine,
                                 const char *key, const size_t nkey,
                                 const char *value, const size_t nbytes,
                                 bool *exist) {
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_set_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        do {
            if (info->readable == 0) {
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

ENGINE_ERROR_CODE set_elem_get(struct squall_engine *engine,
                               const char *key, const size_t nkey,
                               const uint32_t count, const bool delete,
                               const bool drop_if_empty,
                               set_elem_item **elem_array, uint32_t *elem_count,
                               uint32_t *flags, bool *dropped) {
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_set_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        set_meta_info *info = (set_meta_info *)item_get_meta(it);
        do {
            if (info->readable == 0) {
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
                ret = ENGINE_ELEM_ENOENT;
            }
        } while (0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}
#endif

/*
 * B+Tree Interface Functions
 */
#if 1 // ENABLE_SQUALL_ENGINE
/*
 * NOT Yet Implemented.
 */
#else
ENGINE_ERROR_CODE btree_struct_create(struct squall_engine *engine,
                                      const char *key, const size_t nkey,
                                      item_attr *attrp, const void *cookie)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    hash_item *it;

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
            if (do_item_link(engine, it) != 1) {
                ret = ENGINE_ENOMEM;
            }
            do_item_release(engine, it);
        }
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

btree_elem_item *btree_elem_alloc(struct squall_engine *engine,
                                  const int nbkey, const int neflag, const int nbytes,
                                  const void *cookie) {
    btree_elem_item *elem;

    pthread_mutex_lock(&engine->cache_lock);
    elem = do_btree_elem_alloc(engine, nbkey, neflag, nbytes, cookie);
    pthread_mutex_unlock(&engine->cache_lock);
    return elem;
}

void btree_elem_release(struct squall_engine *engine,
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

ENGINE_ERROR_CODE btree_elem_insert(struct squall_engine *engine,
                                    const char *key, const size_t nkey, btree_elem_item *elem,
                                    const bool replace_if_exist, item_attr *attrp,
                                    bool *replaced, bool *created, const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    hash_item *it;
    btree_meta_info *info = NULL;

    *created = false;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, false, &it);
    do {
        if (ret == ENGINE_SUCCESS) { /* it != NULL */
            info = (btree_meta_info *)item_get_meta(it);
            /* bkey type check */
            if (info->ccnt > 0) {
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
                if (do_item_link(engine, it) != 1) {
                    /* The btree item is to be released in the outside of do-while loop */
                    ret = ENGINE_ENOMEM; break;
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
            ret = do_btree_elem_link(engine, info, elem, replace_if_exist, replaced, cookie);
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
        } else {
            /* ENGINE_KEY_ENOENT | ENGINE_EBADTYPE */
        }
    } while(0);

    if (it != NULL) do_item_release(engine, it);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_update(struct squall_engine *engine,
                                    const char *key, const size_t nkey, const bkey_range *bkrange,
                                    const eflag_update *eupdate, const char *value, const int nbytes,
                                    const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, false, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            /* check bkey type */
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            ret = do_btree_elem_update(engine, info, bkrange, eupdate, value, nbytes, cookie);
        } while(0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

ENGINE_ERROR_CODE btree_elem_delete(struct squall_engine *engine,
                                    const char *key, const size_t nkey,
                                    const bkey_range *bkrange, const eflag_filter *efilter,
                                    const uint32_t req_count, const bool drop_if_empty,
                                    uint32_t *del_count, bool *dropped) {
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, false, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            /* check bkey type */
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *del_count = do_btree_elem_delete(engine, info, bkrange, efilter, req_count);
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

ENGINE_ERROR_CODE btree_elem_get(struct squall_engine *engine,
                                 const char *key, const size_t nkey,
                                 const bkey_range *bkrange, const eflag_filter *efilter,
                                 const uint32_t offset, const uint32_t req_count,
                                 const bool delete, const bool drop_if_empty,
                                 btree_elem_item **elem_array, uint32_t *elem_count,
                                 uint32_t *flags, bool *dropped_trimmed)
{
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            bool potentialbkeytrim;
            if (info->readable == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            if (info->ccnt == 0) {
                ret = ENGINE_ELEM_ENOENT; break;
            }
            /* check bkey type */
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *elem_count = do_btree_elem_get(engine, info, bkrange, efilter, offset, req_count,
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

ENGINE_ERROR_CODE btree_elem_count(struct squall_engine *engine,
                                   const char *key, const size_t nkey,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   uint32_t *elem_count, uint32_t *flags) {
    ENGINE_ERROR_CODE ret;
    hash_item *it;

    pthread_mutex_lock(&engine->cache_lock);
    ret = do_btree_item_find(engine, key, nkey, true, &it);
    if (ret == ENGINE_SUCCESS) { /* it != NULL */
        btree_meta_info *info = (btree_meta_info *)item_get_meta(it);
        do {
            if (info->readable == 0) {
                ret = ENGINE_UNREADABLE; break;
            }
            /* check bkey type */
            if ((info->bktype == BKEY_TYPE_UINT64 && bkrange->from_nbkey >  0) ||
                (info->bktype == BKEY_TYPE_BINARY && bkrange->from_nbkey == 0)) {
                ret = ENGINE_EBADBKEY; break;
            }
            *elem_count = do_btree_elem_count(engine, info, bkrange, efilter);
            *flags = it->flags;
        } while (0);
        do_item_release(engine, it);
    }
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}

#ifdef SUPPORT_BOP_SMGET
ENGINE_ERROR_CODE btree_elem_smget(struct squall_engine *engine,
                                   token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   btree_elem_item **elem_array, uint32_t *kfnd_array,
                                   uint32_t *flag_array, uint32_t *elem_count,
                                   uint32_t *missed_key_array, uint32_t *missed_key_count,
                                   bool *trimmed, bool *duplicated)
{
    assert(key_count > 0);
    assert(count > 0);
    assert(offset+count <= MAX_SMGET_REQ_COUNT);
    btree_scan_info btree_scan_buf[offset+count+1];
    uint16_t        sort_sindx_buf[offset+count]; /* sorted scan index buffer */
    uint32_t        sort_sindx_cnt, i;
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
                                   bkrange, efilter, (offset+count),
                                   btree_scan_buf, sort_sindx_buf, &sort_sindx_cnt,
                                   missed_key_array, missed_key_count, duplicated);
    if (ret == ENGINE_SUCCESS) {
        /* the 2nd phase: get the sorted elems */
        *elem_count = do_btree_smget_elem_sort(btree_scan_buf, sort_sindx_buf, sort_sindx_cnt,
                                               bkrange, efilter, offset, count,
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
#endif

static void do_item_getattr_base(data_item *dit, item_attr *attr_data)
{
    /* flags */
    attr_data->flags = dit->flags;

    /* exptime - human readable form */
    rel_time_t current_time = server->core->get_current_time();
    if (dit->exptime == 0) {
        attr_data->exptime = dit->exptime;
#ifdef ENABLE_STICKY_ITEM
    } else if(dit->exptime == (rel_time_t)-1) {
        attr_data->exptime = dit->exptime;
#endif
    } else if (dit->exptime <= current_time) {
        attr_data->exptime = (rel_time_t)-2;
    } else {
        attr_data->exptime = dit->exptime - current_time;
    }

    /* item type */
    attr_data->type = (uint8_t)do_item_type_get(dit);
}

static void do_item_getattr_coll(data_item *dit, item_attr *attr_data)
{
#if 0
    coll_meta_info *info = (coll_meta_info *)item_get_meta(dit);

    attr_data->count      = info->ccnt;
    attr_data->maxcount   = info->mcnt;
    attr_data->ovflaction = info->ovflact;
    attr_data->readable   = info->readable;

    if (attr_data->type == ITEM_TYPE_BTREE) {
        btree_meta_info *binfo = (btree_meta_info *)info;
        attr_data->maxbkeyrange = binfo->maxbkeyrange;
    }
#endif
}

ENGINE_ERROR_CODE item_getattr(const void* key, const int nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data)
{
    ENGINE_ERROR_CODE ret;
    ItemDesc itdbuf;
    uint32_t hval = server->core->hash(key, nkey, 0);
    int      hidx = hval % item_gl.hash_size;

    /* initialize item write status */
    itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;

    /* set hash value of the given key */
    itdbuf.hsh = hval;

    pthread_mutex_lock(&item_gl.hash_tab[hidx].lock);
    ret = do_item_get(key, nkey, ITEM_TYPE_ANY, true, &itdbuf); // READONLY
    if (ret == ENGINE_SUCCESS) {
        /* get base attributes */
        do_item_getattr_base(itdbuf.ptr, attr_data);

        if (attr_data->type == ITEM_TYPE_KV) {
            /* validation check on other attribute requests */
            for (int i = 0; i < attr_count; i++) {
                if (attr_ids[i] == ATTR_COUNT      || attr_ids[i] == ATTR_MAXCOUNT ||
                    attr_ids[i] == ATTR_OVFLACTION || attr_ids[i] == ATTR_READABLE ||
                    attr_ids[i] == ATTR_MAXBKEYRANGE)
                {
                    ret = ENGINE_EBADATTR; break;
                }
            }
        } else {
            /* validation check on other attribute requests */
            if (attr_data->type != ITEM_TYPE_BTREE) {
                for (int i = 0; i < attr_count; i++) {
                     if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
                        ret = ENGINE_EBADATTR; break;
                     }
                }
            }
            if (ret == ENGINE_SUCCESS) {
                /* get collection attributes */
                do_item_getattr_coll(itdbuf.ptr, attr_data);
            }
        }
        do_item_release(&itdbuf);
    }
    pthread_mutex_unlock(&item_gl.hash_tab[hidx].lock);
    return ret;
}

static bool do_item_chkattr_exptime(data_item *dit, rel_time_t exptime)
{
    /* do not allow sticky toggling */
    if ((dit->exptime == (rel_time_t)-1 && exptime != (rel_time_t)-1) ||
        (dit->exptime != (rel_time_t)-1 && exptime == (rel_time_t)-1)) {
        return false;
    }
    return true;
}

static bool do_item_chkattr_maxcount(data_item *dit, uint32_t maxcount)
{
    return true;
#if 0
    coll_meta_info *info = (coll_meta_info *)item_get_data(dit);
    if (info->ccnt > maxcount)
        return false;
    else
        return true;
#endif
}

static bool do_item_chkattr_ovflaction(data_item *dit, uint8_t ovflaction)
{
    return true;
#if 0
    coll_meta_info *info = (coll_meta_info *)item_get_data(dit);
    switch (ovflaction) {
      case OVFL_ERROR:
           return true;
      case OVFL_HEAD_TRIM:
      case OVFL_TAIL_TRIM:
           if (IS_LIST_ITEM(dit)) return true;
           else                   return false;
      case OVFL_SMALLEST_TRIM:
      case OVFL_LARGEST_TRIM:
           if (IS_BTREE_ITEM(dit)) return true;
           else                    return false;
      default:
           return false;
    }
#endif
}

static bool do_item_chkattr_readable(data_item *dit, uint8_t readable)
{
    if (readable == 1) return true;
    else               return false;
}

static bool do_item_chkattr_maxbkeyrange(data_item *dit, bkey_t *maxbkeyrange)
{
    return true;
#if 0
    btree_meta_info *binfo = (btree_meta_info *)item_get_data(dit);

    if (maxbkeyrange->len == BKEY_NULL) return true;

    if (binfo->ccnt == 0) {
        /* Even if the bkey type of maxbkeyrange is different each other,
         * it can be changed given that there is no element in the btree.
         */
        return true;
    } else { /* binfo->ccnt > 0 */
        /* check bkey type of maxbkeyrange */
        if ((binfo->bktype == BKEY_TYPE_UINT64 && maxbkeyrange->len >  0) ||
            (binfo->bktype == BKEY_TYPE_BINARY && maxbkeyrange->len == 0)) {
            return false;
        }
        if (binfo->ccnt <= 1) return true;
    }
    /* binfo->ccnt >= 2 */
    /* If the new maxbkeyrange is different from the original maxbkeyrange,
     * we must ensure that the new maxbkeyrange can cover the bkeys of current elemtents.
     */
    if (maxbkeyrange->len != binfo->maxbkeyrange.len ||
        BKEY_ISNE(maxbkeyrange->val, maxbkeyrange->len, binfo->maxbkeyrange.val, binfo->maxbkeyrange.len)) {
        /* check current bkeyrange with the given maxbkeyrange */
        bkey_t curbkeyrange;
        btree_elem_item *min_elem = do_btree_get_first_elem(binfo->root);
        btree_elem_item *max_elem = do_btree_get_last_elem(binfo->root);
        curbkeyrange.len = maxbkeyrange->len;
        BKEY_DIFF(max_elem->data, max_elem->nbkey, min_elem->data, min_elem->nbkey,
                  curbkeyrange.len, curbkeyrange.val);
        if (BKEY_ISGT(curbkeyrange.val, curbkeyrange.len, maxbkeyrange->val, maxbkeyrange->len)) {
            return false;
        }
    }
    return true;
#endif
}

static bool do_item_setattr_check(ItemDesc *itdesc, ENGINE_ITEM_ATTR *attr_ids,
                                  const uint32_t attr_count, item_attr *attr_data)
{
    data_item *dit = itdesc->ptr;
    int ittype = do_item_type_get(dit);
    bool check = true;

    if (ittype == ITEM_TYPE_UNKNOWN) {
        return false;
    }
    for (int i = 0; i < attr_count; i++) {
        /* common attributes */
        if (attr_ids[i] == ATTR_EXPIRETIME) {
            if (do_item_chkattr_exptime(dit, attr_data->exptime) != true) {
                check = false; break;
            }
            continue;
        }
        /* collection related attributes */
        if (ittype == ITEM_TYPE_KV) {
            check = false; break;
        }
        if (attr_ids[i] == ATTR_MAXCOUNT) {
            /* adjust the given maxcount value if it is too large */
            if (ittype == ITEM_TYPE_LIST) {
                if (attr_data->maxcount > MAX_LIST_SIZE)
                    attr_data->maxcount = MAX_LIST_SIZE;
            } else if (ittype == ITEM_TYPE_SET) {
                if (attr_data->maxcount > MAX_SET_SIZE)
                    attr_data->maxcount = MAX_SET_SIZE;
            } else { /* ittype == ITEM_TYPE_BTREE */
                if (attr_data->maxcount > MAX_BTREE_SIZE)
                    attr_data->maxcount = MAX_BTREE_SIZE;
            }
            if (do_item_chkattr_maxcount(dit, attr_data->maxcount) != true) {
                check = false; break;
            }
        } else if (attr_ids[i] == ATTR_OVFLACTION) {
            if (do_item_chkattr_ovflaction(dit, attr_data->ovflaction) != true) {
                check = false; break;
            }
        } else if (attr_ids[i] == ATTR_READABLE) {
            if (do_item_chkattr_readable(dit, attr_data->ovflaction) != true) {
                check = false; break;
            }
        } else if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
            /* btree only attribute */
            if (ittype != ITEM_TYPE_BTREE) {
                check = false; break;
            }
            if (do_item_chkattr_maxbkeyrange(dit, &attr_data->maxbkeyrange) != true) {
                check = false; break;
            }
        }
    }
    return check;
}

static void do_item_setattr_exec(ItemDesc *itdesc, ENGINE_ITEM_ATTR *attr_ids,
                                 const uint32_t attr_count, item_attr *attr_data)
{
    data_item *dit = itdesc->ptr;
    bool item_attr_update = false;
    bool coll_attr_update = false;

    for (int i = 0; i < attr_count; i++) {
        if (attr_ids[i] == ATTR_EXPIRETIME) {
            if (dit->exptime != attr_data->exptime) {
                dit->exptime = attr_data->exptime;
                item_attr_update = true;
            }
        }
#if 0 // will be used, later
        else if (attr_ids[i] == ATTR_MAXCOUNT) {
            coll_meta_info *info = (coll_meta_info *)item_get_meta(dit);
            if (info->mcnt != attr_data->maxcount) {
                info->mcnt = attr_data->maxcount;
            }
        }
        else if (attr_ids[i] == ATTR_OVFLACTION) {
            coll_meta_info *info = (coll_meta_info *)item_get_meta(dit);
            if (info->ovflact != attr_data->ovflaction) {
                info->ovflact = attr_data->ovflaction;
            }
#if 0 // is it needed ?? maybe not..
            if (attr_data->ovflaction == OVFL_ERROR) {
               /* In case of set collection, do not need to set that.
                * Because, set collection can have OVFL_ERROR only.
                */
               if (IS_LIST_ITEM(dit) || IS_BTREE_ITEM(dit)) {
                   info->ovflact = attr_data->ovflaction;
               }
            } else {
               info->ovflact = attr_data->ovflaction;
            }
#endif
        }
        else if (attr_ids[i] == ATTR_READABLE) {
            coll_meta_info *info = (coll_meta_info *)item_get_meta(dit);
            if (info->readable != attr_data->readable) {
                info->readable != attr_data->readable;
            }
        }
        else if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
            btree_meta_info *binfo = (btree_meta_info *)item_get_meta(dit);
            if (binfo->maxbkeyrange.len != attr_data->maxbkeyrange.len ||
                BKEY_ISNE(binfo->maxbkeyrange.val, binfo->maxbkeyrange.len,
                          attr_data->maxbkeyrange.val, attr_data->maxbkeyrange.len)) {
                binfo->maxbkeyrange = attr_data->maxbkeyrange;
        }
#endif
    }

    if (item_attr_update || coll_attr_update) {
        Page *pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(dit, config->sq.data_page_size);
#ifdef ENABLE_RECOVERY
        if (config->sq.use_recovery) { /* TRANSACTION */
            ITAttrLog itattr_log;
            lrec_it_attr_init((LogRec*)&itattr_log);
            itattr_log.body.itmid = itdesc->iid;
            if (item_attr_update) {
                itattr_log.body.itemat_offset = (char*)&dit->exptime - (char*)dit;
                itattr_log.body.itemat_length = (char*)&dit->exptime - (char*)dit;
            } else {
                itattr_log.body.itemat_offset = 0;
                itattr_log.body.itemat_length = 0;
            }
            assert(coll_attr_update == false);
            itattr_log.body.collat_offset = 0;
            itattr_log.body.collat_length = 0;
            itattr_log.itptr = (void*)dit;
            itattr_log.header.length = GET_8_ALIGN_SIZE(offsetof(ITAttrData, data)
                                                        + itattr_log.body.itemat_length
                                                        + itattr_log.body.collat_length);
            log_record_write((LogRec*)&itattr_log, (Trans*)itdesc->trx);
            /* update PageLSN */
            buf_set_lsn(pgptr, itattr_log.header.self_lsn);
        }
#endif
        /* set buffer page dirty */
        buf_set_dirty(pgptr);
    }
}

ENGINE_ERROR_CODE item_setattr(const void* key, const int nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data)
{
    ENGINE_ERROR_CODE ret;
    ItemDesc itdbuf; /* item descriptor buffer */
    int      hidx;

    /* initialize item write status */
    itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;

#ifdef ENABLE_RECOVERY
    /* initialize transaction entry pointer */
    itdbuf.trx = NULL;
    if (config->sq.use_recovery) { /* TRANSACTION */
        itdbuf.trx = (void*)tran_alloc(false);
        if (itdbuf.trx == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "item setattr - tran alloc fail\n");
            return ENGINE_ETRANS;
        }
    }
#endif

    /* set hash value of the given key */
    itdbuf.hsh = server->core->hash(key, nkey, 0);
    hidx = itdbuf.hsh % item_gl.hash_size;

    pthread_mutex_lock(&item_gl.hash_tab[hidx].lock);
    ret = do_item_get(key, nkey, ITEM_TYPE_ANY, false, &itdbuf);
    if (ret == ENGINE_SUCCESS) {
        if (do_item_setattr_check(&itdbuf, attr_ids, attr_count, attr_data) == true) {
            do_item_setattr_exec(&itdbuf, attr_ids, attr_count, attr_data);

            /* item write status */
            itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_UPDATTR;
        } else {
            ret = ENGINE_EBADATTR;
        }
        do_item_release(&itdbuf);
    }
    pthread_mutex_unlock(&item_gl.hash_tab[hidx].lock);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        tran_free(itdbuf.trx, (ret==ENGINE_SUCCESS ? TRAN_COM_COMMIT : TRAN_COM_ABORT));
    }
#endif
    return ret;
}

/* scrub command handling */

static void *item_scrubber_main(void *arg)
{
    item_SCRUBBER *scrubber = arg;

#if 0 // will be implemented later */
    struct timespec sleep_time = {0, 1000};
    rel_time_t      current_time = engine->server.core->get_current_time();
    uint64_t        total_npages;
    PageID          pgid;
    Page           *pgptr;
    ItemDesc        itdbuf;
    int             pfxid;

    pfxid = pfx_open_first();
    while (pfxid != -1) {
        total_npages = 0;
        pgptr = pfx_data_page_get_first(pfxid, &pgid);
        while (pgptr != NULL) {
            for (int i = 0; i < pgptr->header.totl_nslots; i++) {
                itdbuf.ptr = dpage_find_slot(pgptr, i);
                if (itdbuf.ptr == NULL) continue;

                scrubber->stat.visited++;
                if (scrubber->stat.runmode == SCRUB_MODE_NORMAL) {
                    if (itdbuf.ptr->refcount == 0 && do_item_isvalid(&itdbuf, current_time) == false) {
                        itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;
                        do_item_unlink(&itdbuf);
                        scrubber->stat.cleaned++;
                    }
                } else { /* SCRUB_MODE_STALE */
                    if (do_item_isstale(&itdbuf) == true) {
                        itdbuf.wrstat = (uint8_t)ITEM_WRSTAT_NONE;
                        do_item_unlink(&itdbuf);
                        scrubber->stat.cleaned++;
                    }
                }
            }
            pfx_data_page_release(pfxid, pgptr);

            if ((++total_npages % 10) == 0) {
                nanosleep(&sleep_time, NULL);
            }
            pgptr = pfx_data_get_next_page(pfxid, pgid, &pgid);
        }
        pfx_close(pfxid);

        nanosleep(&sleep_time, NULL);
        pfxid = pfx_open_next(pfxid);
    }
#endif

    pthread_mutex_lock(&scrubber->lock);
    scrubber->stat.stopped = time(NULL);
    scrubber->stat.running = false;
    pthread_mutex_unlock(&scrubber->lock);

    return NULL;
}

bool item_scrub_start(int mode)
{
    item_SCRUBBER *scrubber = &item_gl.scrubber;
    bool ret = false;

    pthread_mutex_lock(&scrubber->lock);
    if (!scrubber->stat.running) {
        scrubber->stat.started = time(NULL);
        scrubber->stat.stopped = 0;
        scrubber->stat.visited = 0;
        scrubber->stat.cleaned = 0;
        scrubber->stat.runmode = (enum scrub_mode)mode;
        scrubber->stat.running = true;

        pthread_t t;
        pthread_attr_t attr;

        if (pthread_attr_init(&attr) != 0 ||
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
            pthread_create(&t, &attr, item_scrubber_main, scrubber) != 0)
        {
            scrubber->stat.running = false;
        } else {
            ret = true;
        }
    }
    pthread_mutex_unlock(&scrubber->lock);

    return ret;
}

void item_scrub_get(struct item_scrub *stat)
{
    item_SCRUBBER *scrubber = &item_gl.scrubber;

    pthread_mutex_lock(&scrubber->lock);
    memcpy((void*)stat, (void*)&scrubber->stat, sizeof(struct item_scrub));
    pthread_mutex_unlock(&scrubber->lock);
}

#if 1 // ENABLE_SQUALL_ENGINE
uint64_t item_get_cas(const data_item* item)
{
    if (item->iflag & ITEM_WITH_CAS) {
        return *(uint64_t*)((char*)item + sizeof(data_item));
    } else {
        return 0;
    }
}

void item_set_cas(data_item* item, uint64_t val)
{
    if (item->iflag & ITEM_WITH_CAS) {
        *(uint64_t*)((char*)item + sizeof(data_item)) = val;
    }
}

char* item_get_key(const data_item* item)
{
    char *ret = (char*)item + sizeof(data_item);
    if (item->iflag & ITEM_WITH_CAS) {
        ret += sizeof(uint64_t);
    }
    return ret;
}

char* item_get_data(const data_item* item)
{
    return ((char*)item_get_key(item)) + item->nkey;
}

char* item_get_meta(const data_item* item)
{
    //return ((char*)item_get_key(item)) + META_OFFSET_IN_ITEM(item->nkey, item->nbytes);
    return ((char*)item_get_key(item)) + GET_8_ALIGN_SIZE(item->nkey+item->ndata);
}

int item_get_type(const data_item *item)
{
    return do_item_type_get(item);
}
#endif

