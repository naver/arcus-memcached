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
#ifndef ITEM_BASE_H
#define ITEM_BASE_H

#include <memcached/engine.h>
#include <memcached/util.h>
#include <memcached/visibility.h>

#ifdef REORGANIZE_ITEM_BASE
/* max collection size */
#define MINIMUM_MAX_COLL_SIZE  10000
#define MAXIMUM_MAX_COLL_SIZE  1000000
#define DEFAULT_MAX_LIST_SIZE  50000
#define DEFAULT_MAX_SET_SIZE   50000
#define DEFAULT_MAX_MAP_SIZE   50000
#define DEFAULT_MAX_BTREE_SIZE 50000

/* default collection size */
#define DEFAULT_LIST_SIZE  4000
#define DEFAULT_SET_SIZE   4000
#define DEFAULT_MAP_SIZE   4000
#define DEFAULT_BTREE_SIZE 4000

/* max element bytes */
#define MINIMUM_MAX_ELEMENT_BYTES 1024
#define MAXIMUM_MAX_ELEMENT_BYTES (32*1024)
#define DEFAULT_MAX_ELEMENT_BYTES (16*1024)

/* item scrub count */
#define MINIMUM_SCRUB_COUNT 16
#define MAXIMUM_SCRUB_COUNT 320
#define DEFAULT_SCRUB_COUNT 96

/* update type */
enum upd_type {
    /* key value command */
    UPD_STORE = 0,
    UPD_DELETE,
    UPD_SETATTR_EXPTIME,
    UPD_SETATTR_EXPTIME_INFO,
    UPD_SETATTR_EXPTIME_INFO_BKEY,
    UPD_FLUSH,
    /* list command */
    UPD_LIST_CREATE,
    UPD_LIST_ELEM_INSERT,
    UPD_LIST_ELEM_DELETE,
    UPD_LIST_ELEM_DELETE_DROP,
    /* set command */
    UPD_SET_CREATE,
    UPD_SET_ELEM_INSERT,
    UPD_SET_ELEM_DELETE,
    UPD_SET_ELEM_DELETE_DROP,
    /* map command */
    UPD_MAP_CREATE,
    UPD_MAP_ELEM_INSERT,
    UPD_MAP_ELEM_DELETE,
    UPD_MAP_ELEM_DELETE_DROP,
    /* btree command */
    UPD_BT_CREATE,
    UPD_BT_ELEM_INSERT,
    UPD_BT_ELEM_DELETE,
    UPD_BT_ELEM_DELETE_DROP,
    /* not command */
    UPD_NONE
};

/* item unlink cause */
enum item_unlink_cause {
    ITEM_UNLINK_NORMAL = 1, /* unlink by normal request */
    ITEM_UNLINK_EVICT,      /* unlink by eviction */
    ITEM_UNLINK_INVALID,    /* unlink by invalidation such like expiration/flush */
    ITEM_UNLINK_REPLACE,    /* unlink by replacement of set/replace command */
    ITEM_UNLINK_STALE       /* unlink by staleness */
};

/* element delete cause */
enum elem_delete_cause {
    ELEM_DELETE_NORMAL = 1, /* delete by normal request */
    ELEM_DELETE_COLL,       /* delete by collection deletion */
    ELEM_DELETE_TRIM        /* delete by overflow trim */
};

/* get hash item address from collection info address */
#define COLL_GET_HASH_ITEM(info) ((size_t*)(info) - (info)->itdist)

/* Item internal flag (1 byte) : item type and flag */
/* 1) item type: increasing order (See ENGINE_ITEM_TYPE) */
#define ITEM_IFLAG_LIST  1   /* list item */
#define ITEM_IFLAG_SET   2   /* set item */
#define ITEM_IFLAG_MAP   3   /* map item */
#define ITEM_IFLAG_BTREE 4   /* b+tree item */
#define ITEM_IFLAG_COLL  7   /* collection item: list/set/map/b+tree */
/* 2) item flag: decreasing order */
#define ITEM_LINKED      32  /* linked to assoc hash table */
#define ITEM_INTERNAL    64  /* internal cache item */
#define ITEM_WITH_CAS    128 /* having CAS value */

/* Macros for checking item type */
#define GET_ITEM_TYPE(it) ((it)->iflag & ITEM_IFLAG_COLL)
#define IS_LIST_ITEM(it)  (((it)->iflag & ITEM_IFLAG_COLL) == ITEM_IFLAG_LIST)
#define IS_SET_ITEM(it)   (((it)->iflag & ITEM_IFLAG_COLL) == ITEM_IFLAG_SET)
#define IS_MAP_ITEM(it)   (((it)->iflag & ITEM_IFLAG_COLL) == ITEM_IFLAG_MAP)
#define IS_BTREE_ITEM(it) (((it)->iflag & ITEM_IFLAG_COLL) == ITEM_IFLAG_BTREE)
#define IS_COLL_ITEM(it)  (((it)->iflag & ITEM_IFLAG_COLL) != 0)

/* collection meta flag */
#define COLL_META_FLAG_READABLE 2
#define COLL_META_FLAG_STICKY   4
#define COLL_META_FLAG_TRIMMED  8

/* LRU id of small memory items */
#define LRU_CLSID_FOR_SMALL 0

/* A do_update argument value representing that
 * we should check and reposition items in the LRU list.
 */
#define DO_UPDATE true
#define DONT_UPDATE false

#ifdef ENABLE_STICKY_ITEM
/* macros for identifying sticky items */
#define IS_STICKY_EXPTIME(e) ((e) == (rel_time_t)(-1))
#define IS_STICKY_COLLFLG(i) (((i)->mflags & COLL_META_FLAG_STICKY) != 0)
#endif

/* collection meta info offset */
#define META_OFFSET_IN_ITEM(nkey,nbytes) ((((nkey)+(nbytes)-1)/8+1)*8)

/* special address for representing unlinked status */
#define ADDR_MEANS_UNLINKED  1

/* hash item strtucture */
typedef struct _hash_item {
    uint16_t refcount;  /* reference count */
    uint8_t  slabs_clsid;/* which slab class we're in */
    uint8_t  refchunk;  /* reference chunk */
    uint32_t flags;     /* Flags associated with the item (in network byte order) */
    struct _hash_item *next;   /* LRU chain next */
    struct _hash_item *prev;   /* LRU chain prev */
    struct _hash_item *h_next; /* hash chain next */
    rel_time_t time;    /* least recent access */
    rel_time_t exptime; /* When the item will expire (relative to process startup) */
    uint8_t  iflag;     /* Internal flags: item type and flag */
    uint16_t nkey;      /* The total length of the key (in bytes) */
    uint32_t nbytes;    /* The total length of the data (in bytes) */
    /* Following fields are used to trade off memory space for performance */
    uint32_t khash;     /* The hash value of key string */
    void    *pfxptr;    /* pointer to prefix structure */
} hash_item;

/* list element */
typedef struct _list_elem_item {
    uint16_t refcount;
    uint8_t  slabs_clsid;         /* which slab class we're in */
    uint32_t dummy;
    struct _list_elem_item *next; /* next chain in double linked list */
    struct _list_elem_item *prev; /* prev chain in double linked list */
    uint32_t nbytes;              /**< The total size of the data (in bytes) */
    char     value[1];            /**< the data itself */
} list_elem_item;

/* set element */
typedef struct _set_elem_item {
    uint16_t refcount;
    uint8_t  slabs_clsid;         /* which slab class we're in */
    uint32_t hval;                /* hash value */
    struct _set_elem_item *next;  /* hash chain next */
    uint32_t nbytes;              /**< The total size of the data (in bytes) */
    char     value[1];            /**< the data itself */
} set_elem_item;

/* map element */
typedef struct _map_elem_item {
    uint16_t refcount;
    uint8_t  slabs_clsid;         /* which slab class we're in */
    uint32_t hval;                /* hash value */
    struct _map_elem_item *next;  /* hash chain next */
    uint8_t nfield;               /**< The total size of the field (in bytes) */
    uint16_t nbytes;              /**< The total size of the data (in bytes) */
    unsigned char data[1];        /* data: <field, value> */
} map_elem_item;

/* btree element */
typedef struct _btree_elem_item_fixed {
    uint16_t refcount;
    uint8_t  slabs_clsid;        /* which slab class we're in */
    uint8_t  status;             /* 3(used), 2(insert mark), 1(delete_mark), or 0(free) */
    uint8_t  nbkey;              /* length of bkey */
    uint8_t  neflag;             /* length of element flag */
    uint16_t nbytes;             /**< The total size of the data (in bytes) */
} btree_elem_item_fixed;

typedef struct _btree_elem_item {
    uint16_t refcount;
    uint8_t  slabs_clsid;        /* which slab class we're in */
    uint8_t  status;             /* 3(used), 2(insert mark), 1(delete_mark), or 0(free) */
    uint8_t  nbkey;              /* length of bkey */
    uint8_t  neflag;             /* length of element flag */
    uint16_t nbytes;             /**< The total size of the data (in bytes) */
    unsigned char data[1];       /* data: <bkey, [eflag,] value> */
} btree_elem_item;

/* list meta info */
typedef struct _list_meta_info {
    int32_t  mcnt;      /* maximum count */
    int32_t  ccnt;      /* current count */
    uint8_t  ovflact;   /* overflow action */
    uint8_t  mflags;    /* sticky, readable flags */
    uint16_t itdist;    /* distance from hash item (unit: sizeof(size_t)) */
    uint32_t stotal;    /* total space */
    list_elem_item *head;
    list_elem_item *tail;
} list_meta_info;

/* set meta info */
#define SET_HASHTAB_SIZE 16
#define SET_HASHIDX_MASK 0x0000000F
#define SET_MAX_HASHCHAIN_SIZE 64

typedef struct _set_hash_node {
    uint16_t refcount;
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
    uint8_t  mflags;    /* sticky, readable flags */
    uint16_t itdist;    /* distance from hash item (unit: sizeof(size_t)) */
    uint32_t stotal;    /* total space */
    set_hash_node *root;
} set_meta_info;

/* map meta info */
#define MAP_HASHTAB_SIZE 16
#define MAP_HASHIDX_MASK 0x0000000F
#define MAP_MAX_HASHCHAIN_SIZE 64

typedef struct _map_hash_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;         /* which slab class we're in */
    uint8_t  hdepth;
    uint16_t tot_elem_cnt;
    uint16_t tot_hash_cnt;
    int16_t  hcnt[MAP_HASHTAB_SIZE];
    void    *htab[MAP_HASHTAB_SIZE];
} map_hash_node;

typedef struct _map_meta_info {
    int32_t  mcnt;      /* maximum count */
    int32_t  ccnt;      /* current count */
    uint8_t  ovflact;   /* overflow action */
    uint8_t  mflags;    /* sticky, readable flags */
    uint16_t itdist;    /* distance from hash item (unit: sizeof(size_t)) */
    uint32_t stotal;    /* total space */
    map_hash_node *root;
} map_meta_info;

/* btree meta info */
#define BTREE_MAX_DEPTH  7
#define BTREE_ITEM_COUNT 32 /* Recommend BTREE_ITEM_COUNT >= 8 */

typedef struct _btree_leaf_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;      /* which slab class we're in */
    uint8_t  ndepth;
    uint16_t used_count;
    uint16_t reserved;
    struct _btree_indx_node *prev;
    struct _btree_indx_node *next;
    void    *item[BTREE_ITEM_COUNT];
} btree_leaf_node;

typedef struct _btree_indx_node {
    uint16_t refcount;
    uint8_t  slabs_clsid;      /* which slab class we're in */
    uint8_t  ndepth;
    uint16_t used_count;
    uint16_t reserved;
    struct _btree_indx_node *prev;
    struct _btree_indx_node *next;
    void    *item[BTREE_ITEM_COUNT];
    uint32_t ecnt[BTREE_ITEM_COUNT];
} btree_indx_node;

typedef struct _btree_meta_info {
    int32_t  mcnt;      /* maximum count */
    int32_t  ccnt;      /* current count */
    uint8_t  ovflact;   /* overflow action */
    uint8_t  mflags;    /* sticky, readable, trimmed flags */
    uint16_t itdist;    /* distance from hash item (unit: sizeof(size_t)) */
    uint32_t stotal;    /* total space */
    uint8_t  bktype;    /* bkey type : BKEY_TYPE_UINT64 or BKEY_TYPE_BINARY */
    uint8_t  dummy[7];  /* reserved space */
    bkey_t   maxbkeyrange;
    btree_indx_node *root;
} btree_meta_info;

/* common meta info of list and set */
typedef struct _coll_meta_info {
    int32_t  mcnt;      /* maximum count */
    int32_t  ccnt;      /* current count */
    uint8_t  ovflact;   /* overflow action */
    uint8_t  mflags;    /* sticky, readable flags */
    uint16_t itdist;    /* distance from hash item (unit: sizeof(size_t)) */
    uint32_t stotal;    /* total space */
} coll_meta_info;

/* item stats */
typedef struct {
    unsigned int evicted;
    unsigned int evicted_nonzero;
    rel_time_t   evicted_time;
    unsigned int outofmemory;
    unsigned int tailrepairs;
    unsigned int reclaimed;
} itemstats_t;

/* item global */
struct items {
   hash_item   *heads[MAX_SLAB_CLASSES];
   hash_item   *tails[MAX_SLAB_CLASSES];
   hash_item   *lowMK[MAX_SLAB_CLASSES]; /* low mark for invalidation(expire/flush) check */
   hash_item   *curMK[MAX_SLAB_CLASSES]; /* cur mark for invalidation(expire/flush) check */
   hash_item   *sticky_heads[MAX_SLAB_CLASSES];
   hash_item   *sticky_tails[MAX_SLAB_CLASSES];
   hash_item   *sticky_curMK[MAX_SLAB_CLASSES]; /* cur mark for invalidation(expire/flush) check */
   unsigned int sizes[MAX_SLAB_CLASSES];
   unsigned int sticky_sizes[MAX_SLAB_CLASSES];
   itemstats_t  itemstats[MAX_SLAB_CLASSES];
};
#endif

#ifdef REORGANIZE_ITEM_BASE
/* cache locking & unlocking */
void LOCK_CACHE(void);
void UNLOCK_CACHE(void);
void TRYLOCK_CACHE(int ntries);

void ITEM_REFCOUNT_INCR(hash_item *it);
void ITEM_REFCOUNT_DECR(hash_item *it);

/* stats functions */
void do_item_stat_get(ADD_STAT add_stat, const void *cookie);
void do_item_stat_reset(void);
#ifdef ENABLE_STICKY_ITEM
bool do_item_sticky_overflowed(void);
#endif

void do_coll_space_incr(coll_meta_info *info, ENGINE_ITEM_TYPE item_type,
                        const size_t nspace);
void do_coll_space_decr(coll_meta_info *info, ENGINE_ITEM_TYPE item_type,
                        const size_t nspace);

/* item functions */
bool do_item_isvalid(hash_item *it, rel_time_t current_time);

void *do_item_mem_alloc(const size_t ntotal, const unsigned int clsid,
                        const void *cookie);
void  do_item_mem_free(void *item, size_t ntotal);

hash_item *do_item_alloc(const void *key, const uint32_t nkey,
                         const uint32_t flags, const rel_time_t exptime,
                         const uint32_t nbytes, const void *cookie);
void       do_item_free(hash_item *it);

ENGINE_ERROR_CODE do_item_link(hash_item *it);
void              do_item_unlink(hash_item *it, enum item_unlink_cause cause);
void              do_item_replace(hash_item *old_it, hash_item *new_it);
void              do_item_update(hash_item *it, bool force);

hash_item *do_item_get(const char *key, const uint32_t nkey, bool do_update);
void       do_item_release(hash_item *it);

#endif

#ifdef REORGANIZE_ITEM_BASE
void coll_del_thread_wakeup(void);
#endif

#ifdef REORGANIZE_ITEM_BASE
/*
 * Item access functions
 */
uint64_t    item_get_cas(const hash_item* item);
void        item_set_cas(const hash_item* item, uint64_t val);
const void* item_get_key(const hash_item* item);
char*       item_get_data(const hash_item* item);
const void* item_get_meta(const hash_item* item);

/*
 * Item size functions
 */
uint32_t item_ntotal(hash_item *item);

/*
 * Check item validity
 */
bool item_is_valid(hash_item* item);
#endif

#ifdef REORGANIZE_ITEM_BASE // APPLY
/* Item Apply Macros */
//#define ITEM_APPLY_LOG_LEVEL EXTENSION_LOG_INFO
#define ITEM_APPLY_LOG_LEVEL EXTENSION_LOG_DEBUG
#define PRINT_NKEY(nkey) ((nkey) < 250 ? (nkey) : 250)
#endif

#ifdef REORGANIZE_ITEM_BASE
/* item base module: init & final functions */
int  item_base_init(void *engine_ptr);
void item_base_final(void *engine_ptr);
#endif

#endif
