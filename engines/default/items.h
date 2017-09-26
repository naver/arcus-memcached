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
#ifndef ITEMS_H
#define ITEMS_H

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

typedef struct _prefix_t prefix_t;

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
    uint8_t  iflag;     /* Intermal flags: item type and flag */
    uint16_t nkey;      /* The total length of the key (in bytes) */
    uint32_t nbytes;    /* The total length of the data (in bytes) */
    /* Following fields are used to trade off memory space for performance */
    uint32_t khash;     /* The hash value of key string */
    prefix_t *pfxptr;   /* pointer to prefix structure */
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
} btree_scan_info;

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

/* item queue */
typedef struct {
   hash_item   *head;
   hash_item   *tail;
   unsigned int size;
} item_queue;

/*
 * You should not try to aquire any of the item locks before calling these
 * functions.
 */

/**
 * Allocate and initialize a new item structure
 * @param engine handle to the storage engine
 * @param key the key for the new item
 * @param nkey the number of bytes in the key
 * @param flags the flags in the new item
 * @param exptime when the object should expire
 * @param nbytes the number of bytes in the body for the item
 * @return a pointer to an item on success NULL otherwise
 */
hash_item *item_alloc(struct default_engine *engine,
                      const void *key, size_t nkey, int flags,
                      rel_time_t exptime, int nbytes, const void *cookie);

/**
 * Get an item from the cache
 *
 * @param engine handle to the storage engine
 * @param key the key for the item to get
 * @param nkey the number of bytes in the key
 * @return pointer to the item if it exists or NULL otherwise
 */
hash_item *item_get(struct default_engine *engine, const void *key, const size_t nkey);

/**
 * Reset the item statistics
 * @param engine handle to the storage engine
 */
void item_stats_reset(struct default_engine *engine);

/**
 * Get item statitistics
 * @param engine handle to the storage engine
 * @param add_stat callback provided by the core used to
 *                 push statistics into the response
 * @param cookie cookie provided by the core to identify the client
 */
void item_stats(struct default_engine *engine, ADD_STAT add_stat, const void *cookie);

/**
 * Get detaild item statitistics
 * @param engine handle to the storage engine
 * @param add_stat callback provided by the core used to
 *                 push statistics into the response
 * @param cookie cookie provided by the core to identify the client
 */
void item_stats_sizes(struct default_engine *engine, ADD_STAT add_stat, const void *cookie);

/**
 * Dump items from the cache
 * @param engine handle to the storage engine
 * @param slabs_clsid the slab class to get items from
 * @param limit the maximum number of items to receive
 * @param bytes the number of bytes in the return message (OUT)
 * @return pointer to a string containint the data
 *
 * @todo we need to rewrite this to use callbacks!!!! currently disabled
 */
char *item_cachedump(struct default_engine *engine, const unsigned int slabs_clsid,
                     const unsigned int limit, const bool forward, const bool sticky,
                     unsigned int *bytes);

/**
 * Flush expired items from the cache
 * @param engine handle to the storage engine
 * @prefix prefix string
 * @nprefix prefix string length: -1(all prefixes), 0(null prefix)
 * @param when when the items should be flushed
 */
ENGINE_ERROR_CODE item_flush_expired(struct default_engine *engine,
                                     const char *prefix, const int nprefix,
                                     time_t when, const void* cookie);

/**
 * Release our reference to the current item
 * @param engine handle to the storage engine
 * @param it the item to release
 */
void item_release(struct default_engine *engine, hash_item *it);

/**
 * Store an item in the cache
 * @param engine handle to the storage engine
 * @param item the item to store
 * @param cas the cas value (OUT)
 * @param operation what kind of store operation is this (ADD/SET etc)
 * @return ENGINE_SUCCESS on success
 *
 * @todo should we refactor this into hash_item ** and remove the cas
 *       there so that we can get it from the item instead?
 */
ENGINE_ERROR_CODE store_item(struct default_engine *engine, hash_item *item,
                             uint64_t *cas, ENGINE_STORE_OPERATION operation,
                             const void *cookie);

ENGINE_ERROR_CODE arithmetic(struct default_engine *engine, const void* cookie,
                             const void* key, const int nkey, const bool increment,
                             const bool create, const uint64_t delta, const uint64_t initial,
                             const int flags, const rel_time_t exptime, uint64_t *cas,
                             uint64_t *result);

/**
 * Delete an item of the given key.
 * @param engine handle to the storage engine
 * @param key the key to delete
 * @param nkey the number of bytes in the key
 * @param cas the cas value
 */
ENGINE_ERROR_CODE item_delete(struct default_engine *engine,
                              const void* key, const size_t nkey,
                              uint64_t cas);

void coll_del_thread_wakeup(void);

ENGINE_ERROR_CODE item_init(struct default_engine *engine);

void              item_final(struct default_engine *engine);

ENGINE_ERROR_CODE list_struct_create(struct default_engine *engine,
                                     const char *key, const size_t nkey,
                                     item_attr *attrp, const void *cookie);

list_elem_item *list_elem_alloc(struct default_engine *engine,
                                const int nbytes, const void *cookie);

void list_elem_release(struct default_engine *engine,
#ifdef USE_EBLOCK_RESULT
                       eitem *eitem, EITEM_TYPE type);
#else
                       list_elem_item **elem_array, const int elem_count);
#endif

ENGINE_ERROR_CODE list_elem_insert(struct default_engine *engine,
                                   const char *key, const size_t nkey,
                                   int index, list_elem_item *elem,
                                   item_attr *attrp,
                                   bool *created, const void *cookie);

ENGINE_ERROR_CODE list_elem_delete(struct default_engine *engine,
                                   const char *key, const size_t nkey,
                                   int from_index, int to_index,
                                   const bool drop_if_empty,
                                   uint32_t *del_count, bool *dropped);

ENGINE_ERROR_CODE list_elem_get(struct default_engine *engine,
                                const char *key, const size_t nkey,
                                int from_index, int to_index,
                                const bool delete, const bool drop_if_empty,
#ifdef USE_EBLOCK_RESULT
                                eblock_result_t *eblk_ret,
#else
                                list_elem_item **elem_array, uint32_t *elem_count,
#endif
                                uint32_t *flags, bool *dropped);

ENGINE_ERROR_CODE set_struct_create(struct default_engine *engine,
                                    const char *key, const size_t nkey,
                                    item_attr *attrp, const void *cookie);

set_elem_item *set_elem_alloc(struct default_engine *engine,
                              const int nbytes, const void *cookie);

void set_elem_release(struct default_engine *engine,
#ifdef USE_EBLOCK_RESULT
                      eitem *eitem, EITEM_TYPE type);
#else
                      set_elem_item **elem_array, const int elem_count);
#endif

ENGINE_ERROR_CODE set_elem_insert(struct default_engine *engine,
                                  const char *key, const size_t nkey,
                                  set_elem_item *elem,
                                  item_attr *attrp,
                                  bool *created, const void *cookie);

ENGINE_ERROR_CODE set_elem_delete(struct default_engine *engine,
                                  const char *key, const size_t nkey,
                                  const char *value, const size_t nbytes,
                                  const bool drop_if_empty,
                                  bool *dropped);

ENGINE_ERROR_CODE set_elem_exist(struct default_engine *engine,
                                 const char *key, const size_t nkey,
                                 const char *value, const size_t nbytes,
                                 bool *exist);

ENGINE_ERROR_CODE set_elem_get(struct default_engine *engine,
                               const char *key, const size_t nkey, const uint32_t count,
                               const bool delete, const bool drop_if_empty,
#ifdef USE_EBLOCK_RESULT
                               eblock_result_t *eblk_ret,
#else
                               set_elem_item **elem_array, uint32_t *elem_count,
#endif
                               uint32_t *flags, bool *dropped);

ENGINE_ERROR_CODE map_struct_create(struct default_engine *engine,
                                    const char *key, const size_t nkey,
                                    item_attr *attrp, const void *cookie);

map_elem_item *map_elem_alloc(struct default_engine *engine, const int nfield,
                              const int nbytes, const void *cookie);

void map_elem_release(struct default_engine *engine,
#ifdef USE_EBLOCK_RESULT
                      eitem *eitem, EITEM_TYPE type);
#else
                      map_elem_item **elem_array, const int elem_count);
#endif

ENGINE_ERROR_CODE map_elem_insert(struct default_engine *engine,
                                  const char *key, const size_t nkey,
                                  map_elem_item *elem,
                                  item_attr *attrp,
                                  bool *created, const void *cookie);

ENGINE_ERROR_CODE map_elem_update(struct default_engine *engine,
                                  const char *key, const size_t nkey,
                                  const field_t *field,
                                  const char *value, const int nbytes,
                                  const void *cookie);

ENGINE_ERROR_CODE map_elem_delete(struct default_engine *engine,
                                  const char *key, const size_t nkey,
                                  const int numfields, const field_t *flist,
                                  const bool drop_if_empty, uint32_t *del_count,
                                  bool *dropped);

ENGINE_ERROR_CODE map_elem_get(struct default_engine *engine,
                               const char *key, const size_t nkey,
                               const int numfields, const field_t *flist, const bool delete,
#ifdef USE_EBLOCK_RESULT
                               const bool drop_if_empty, eblock_result_t *eblk_ret,
                               uint32_t *flags, bool *dropped);
#else
                               const bool drop_if_empty, map_elem_item **elem_array,
                               uint32_t *elem_count, uint32_t *flags, bool *dropped);
#endif

ENGINE_ERROR_CODE btree_struct_create(struct default_engine *engine,
                                      const char *key, const size_t nkey,
                                      item_attr *attrp, const void *cookie);

btree_elem_item *btree_elem_alloc(struct default_engine *engine,
                                  const int nbkey, const int neflag, const int nbytes,
                                  const void *cookie);

void btree_elem_release(struct default_engine *engine,
#ifdef USE_EBLOCK_RESULT
                        eitem *eitem, EITEM_TYPE type);
#else
                        btree_elem_item **elem_array, const int elem_count);
#endif

ENGINE_ERROR_CODE btree_elem_insert(struct default_engine *engine,
                                    const char *key, const size_t nkey,
                                    btree_elem_item *elem, const bool replace_if_exist, item_attr *attrp,
                                    bool *replaced, bool *created, btree_elem_item **trimmed_elems,
                                    uint32_t *trimmed_count, uint32_t *trimmed_flags, const void *cookie);

ENGINE_ERROR_CODE btree_elem_update(struct default_engine *engine,
                                    const char *key, const size_t nkey, const bkey_range *bkrange,
                                    const eflag_update *eupdate, const char *value, const int nbytes,
                                    const void *cookie);

ENGINE_ERROR_CODE btree_elem_delete(struct default_engine *engine,
                                    const char *key, const size_t nkey,
                                    const bkey_range *bkrange, const eflag_filter *efilter,
                                    const uint32_t req_count, const bool drop_if_empty,
                                    uint32_t *del_count, uint32_t *access_count, bool *dropped);

ENGINE_ERROR_CODE btree_elem_arithmetic(struct default_engine *engine,
                                        const char* key, const size_t nkey,
                                        const bkey_range *bkrange,
                                        const bool increment, const bool create,
                                        const uint64_t delta, const uint64_t initial,
                                        const eflag_t *eflagp,
                                        uint64_t *result, const void* cookie);

ENGINE_ERROR_CODE btree_elem_get(struct default_engine *engine,
                                 const char *key, const size_t nkey,
                                 const bkey_range *bkrange, const eflag_filter *efilter,
                                 const uint32_t offset, const uint32_t req_count,
                                 const bool delete, const bool drop_if_empty,
#ifdef USE_EBLOCK_RESULT
                                 eblock_result_t *eblk_ret,
#else
                                 btree_elem_item **elem_array, uint32_t *elem_count,
#endif
                                 uint32_t *access_count,
                                 uint32_t *flags, bool *dropped_trimmed);

ENGINE_ERROR_CODE btree_elem_count(struct default_engine *engine,
                                   const char *key, const size_t nkey,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   uint32_t *elem_count, uint32_t *access_count);

ENGINE_ERROR_CODE btree_posi_find(struct default_engine *engine,
                                  const char *key, const size_t nkey, const bkey_range *bkrange,
                                  ENGINE_BTREE_ORDER order, int *position);

ENGINE_ERROR_CODE btree_posi_find_with_get(struct default_engine *engine,
                                           const char *key, const size_t nkey,
                                           const bkey_range *bkrange, ENGINE_BTREE_ORDER order,
                                           const int count, int *position,
#ifdef USE_EBLOCK_RESULT
                                           eblock_result_t *eblk_ret,
#else
                                           btree_elem_item **elem_array, uint32_t *elem_count,
#endif
                                           uint32_t *elem_index, uint32_t *flags);

ENGINE_ERROR_CODE btree_elem_get_by_posi(struct default_engine *engine,
                                  const char *key, const size_t nkey,
                                  ENGINE_BTREE_ORDER order, uint32_t from_posi, uint32_t to_posi,
#ifdef USE_EBLOCK_RESULT
                                  eblock_result_t *eblk_ret, uint32_t *flags);
#else
                                  btree_elem_item **elem_array, uint32_t *elem_count, uint32_t *flags);
#endif

#ifdef SUPPORT_BOP_SMGET
#ifdef JHPARK_OLD_SMGET_INTERFACE
/* smget old interface */
ENGINE_ERROR_CODE btree_elem_smget_old(struct default_engine *engine,
                                   token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
#ifdef USE_EBLOCK_RESULT
                                   eblock_result_t *eblk_ret, uint32_t *kfnd_array,
                                   uint32_t *flag_array,
#else
                                   btree_elem_item **elem_array, uint32_t *kfnd_array,
                                   uint32_t *flag_array, uint32_t *elem_count,
#endif
                                   uint32_t *missed_key_array, uint32_t *missed_key_count,
                                   bool *trimmed, bool *duplicated);
#endif

/* smget new interface */
ENGINE_ERROR_CODE btree_elem_smget(struct default_engine *engine,
                                   token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   const bool unique,
                                   smget_result_t *result);
#endif

ENGINE_ERROR_CODE item_getattr(struct default_engine *engine,
                               const void* key, const int nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data);

ENGINE_ERROR_CODE item_setattr(struct default_engine *engine,
                               const void* key, const int nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data);

/*
 * Item config functions
 */
#ifdef CONFIG_MAX_COLLECTION_SIZE
ENGINE_ERROR_CODE item_conf_set_maxcollsize(struct default_engine *engine,
                                            const int coll_type, int *maxsize);
#endif
bool item_conf_get_evict_to_free(struct default_engine *engine);
void item_conf_set_evict_to_free(struct default_engine *engine, bool value);

/*
 * Item access functions
 */
uint64_t    item_get_cas(const hash_item* item);
void        item_set_cas(const hash_item* item, uint64_t val);
const void* item_get_key(const hash_item* item);
char*       item_get_data(const hash_item* item);
const void* item_get_meta(const hash_item* item);

/*
 * Check linked status
 */
bool item_is_valid(struct default_engine *engine, hash_item *item);
bool item_is_linked(const hash_item* item);
bool list_elem_is_linked(list_elem_item *elem);
bool set_elem_is_linked(set_elem_item *elem);
bool map_elem_is_linked(map_elem_item *elem);
bool btree_elem_is_linked(btree_elem_item *elem);

/*
 * Item and Element size functions
 */
uint32_t item_ntotal(struct default_engine *engine, hash_item *item);
uint32_t list_elem_ntotal(list_elem_item *elem);
uint32_t set_elem_ntotal(set_elem_item *elem);
uint32_t map_elem_ntotal(map_elem_item *elem);
uint32_t btree_elem_ntotal(btree_elem_item *elem);
uint8_t  btree_real_nbkey(uint8_t nbkey);

/**
 * Item scrubber
 */
bool item_start_scrub(struct default_engine *engine, int mode);
bool item_onoff_scrub(struct default_engine *engine, bool val);
void item_stats_scrub(struct default_engine *engine,
                      ADD_STAT add_stat, const void *cookie);

#ifdef JHPARK_KEY_DUMP
/**
 * Item dumpper
 */
enum dump_mode {
    DUMP_MODE_NONE = 0,
    DUMP_MODE_KEY  = 1, /* key string only */
    DUMP_MODE_ITEM = 2  /* key string & item value */
};
int  item_start_dump(struct default_engine *engine,
                     enum dump_mode mode,
                     const char *prefix, const int nprefix,
                     const char *filepath);
void item_stop_dump(struct default_engine *engine);
void item_stats_dump(struct default_engine *engine,
                     ADD_STAT add_stat, const void *cookie);
#endif

#endif
