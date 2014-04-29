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
#ifndef SQ_ITEMS_H
#define SQ_ITEMS_H

#include "memcached/engine.h"
#include "sq_types.h"

/* item stats type */
#define ITEM_STATS_MAIN 1
#define ITEM_STATS_SIZE 2
#define ITEM_SIZES_NBUCKET 1024

#define ITEM_LINKED     (1<<8)
#define ITEM_SLABBED    (2<<8)
#define ITEM_DELMARK    (4<<8)

struct item_conf {
    uint32_t    (*hash_func)(const void *data, size_t size, uint32_t seed);
    uint32_t    max_threads;
    uint32_t    page_size;
};

struct item_stat {
   uint64_t     evictions;
   uint64_t     reclaimed;
   uint64_t     curr_bytes;
   uint64_t     curr_items;
   uint64_t     total_items;
};

struct item_size {
   uint32_t     num_buckets;
   uint32_t     histogram[ITEM_SIZES_NBUCKET];
};

/* scrub mode */
enum scrub_mode {
    SCRUB_MODE_STOP   = 0,
    SCRUB_MODE_NORMAL = 1,
    SCRUB_MODE_STALE  = 2
};

struct item_scrub {
   uint64_t        visited;
   uint64_t        cleaned;
   time_t          started;
   time_t          stopped;
   enum scrub_mode runmode;
   bool            running;
};

typedef struct _free_item {
    uint16_t    offset;
    uint16_t    length;
} free_item;

typedef struct _data_item {
    uint32_t    flags;      /* Flags associated with the item (in network byte order) */
    rel_time_t  exptime;    /* when the item will expire (relative to process startup) */
    uint32_t    nbytes;     /* The total size of the data (in bytes) */
    uint16_t    nkey;       /* The total size of the key (in bytes) */
    uint16_t    iflag;      /* Internal flags: cas, linked, item type */
    uint16_t    pfxid;      /* prefix id */
    uint16_t    refcount;
    uint16_t    ndata;      /* length of data part */
    uint16_t    novfl;
} data_item;

typedef struct _ovfl_item {
    uint16_t    ndata;
} ovfl_item;

typedef struct _item_desc {
    ItemID      iid;    /* item id */
    data_item  *ptr;    /* data item ptr */
    ovfl_item **ofl;    /* ovfl item ptr array */
    void       *trx;    /* transaction entry pointer */
    uint32_t    hsh;    /* key hash value */
    uint8_t     wrstat; /* item write status */
    bool        cached; /* item cached ? true or false */
    bool        inuse;  /* true or false */
} ItemDesc;

typedef struct _hash_item {
    struct _hash_item *next;
    ItemDesc           desc;
} hash_item;

#if 0 // version 2
typedef struct _data_item {
    uint16_t    ndata;
    uint8_t     status;     /* data | ovfl | free item */
    uint8_t     hasnext;
    uint16_t    refcount;
    uint16_t    pfxid;      /* prefix id */
    uint32_t    flags;      /* Flags associated with the item (in network byte order) */
    rel_time_t  exptime;    /* when the item will expire (relative to process startup) */
    uint32_t    nbytes;     /* The total size of the data (in bytes) */
    uint16_t    nkey;       /* The total size of the key (in bytes) */
    uint16_t    iflag;      /* Internal flags: cas, linked, item type */
} data_item;

typedef struct _ovfl_item {
    uint16_t    ndata;
    uint8_t     status;     /* data | ovfl | free item */
    uint8_t     hasnext;
} ovfl_item;
#endif

#if 0 // version 1
typedef struct _data_item {
    //uint16_t    length;     /* physical item length */
    uint16_t    ndata;      /* data(or value) size */
    uint16_t    novfls;     /* # of ovfl items */
    uint16_t    refcount;   /* reference count */
    uint32_t    flags;      /* Flags associated with the item (in network byte order) */
    rel_time_t  exptime;    /* when the item will expire (relative to process startup) */
    uint32_t    nbytes;     /* The total size of the data (in bytes) */
    uint8_t     nkey;       /* The total size of the key (in bytes) */
    uint8_t     iflag;      /* Internal flags: cas, linked, item type */
    uint16_t    pfxid;      /* prefix id */
} data_item;

typedef struct _ovfl_item {
    uint16_t    ndata;      /* data(or value) size */
    uint8_t     phytype;
} ovfl_item;
#endif

#define DATA_ITEM_HDR_SIZE  (sizeof(data_item))
#define OVFL_ITEM_HDR_SIZE  (2*sizeof(uint16_t))

#if 0 // version 0
typedef struct _data_item {
    uint16_t    length;     /* physical item length */
    uint8_t     status;     /* used or free or delmark, if ovfl exist */
    uint8_t     ittype;     /* data item or ovfl item */
    uint32_t    flags;      /* Flags associated with the item (in network byte order) */
    rel_time_t  exptime;    /* When the item will expire (relative to process startup) */
    uint32_t    nbytes;     /* The total size of the data (in bytes) */
//    uint16_t    nval;       /* The partial size of the data contained in this item */
    uint16_t    nkey;       /* The total size of the key (in bytes) */
    uint16_t    iflag;      /* Intermal flags.
                               the lower 8 bits is reserved for the core server,
                               the upper 8 bits is reserved for engine implementation. */
    uint16_t    refcount;
} data_item;

typedef struct _ovfl_item {
    uint16_t    length;     /* physical item length */
    uint8_t     status;     /* used or free or delmark */
    uint8_t     ittype;     /* data item or ovfl item, if next item exist */
}
#endif

#if 0 // OLD VERSION
typedef struct _data_item {
    /* non-volatile information */
    uint32_t    flags;      /* Flags associated with the item (in network byte order) */
    rel_time_t  exptime;    /* When the item will expire (relative to process startup) */
    uint32_t    nbytes;     /* The total size of the data (in bytes) */
    uint16_t    nval;       /* The partial size of the data contained in this item */
    uint16_t    nkey;       /* The total size of the key (in bytes) */
    uint16_t    iflag;      /* Intermal flags.
                               the lower 8 bits is reserved for the core server,
                               the upper 8 bits is reserved for engine implementation. */
} data_item;

typedef struct _ovfl_item {
    ItemID      next_itmid;
    uint16_t    nval;
    char        val[1];
} ovfl_item;
#endif

#if 0 // ENABLE_SQUALL_ENGINE
typedef struct _list_elem_item {
    unsigned short refcount;      /* reference count */
    uint8_t  slabs_clsid;         /* which slab class we're in */
    uint32_t dummy;
    struct _list_elem_item *next; /* next chain in double linked list */
    struct _list_elem_item *prev; /* prev chain in double linked list */
    uint32_t nbytes;              /**< The total size of the data (in bytes) */
    char     value[1];            /**< the data itself */
} list_elem_item;

typedef struct _set_elem_item {
    unsigned short refcount;      /* reference count */
    uint8_t  slabs_clsid;         /* which slab class we're in */
    uint32_t hval;                /* hash value */
    struct _set_elem_item *next;  /* hash chain next */
    uint32_t nbytes;              /**< The total size of the data (in bytes) */
    char     value[1];            /**< the data itself */
} set_elem_item;

typedef struct _btree_elem_item {
    unsigned short refcount;     /* reference count */
    uint8_t  slabs_clsid;        /* which slab class we're in */
    uint8_t  status;             /* 3(used), 2(insert mark), 1(delete_mark), or 0(free) */
    uint8_t  nbkey;              /* length of bkey */
    uint8_t  neflag;             /* length of element flag */
    uint16_t nbytes;             /**< The total size of the data (in bytes) */
    unsigned char data[1];       /* data: <bkey, [eflag,] value> */
} btree_elem_item;
#endif

/*
 * You should not try to aquire any of the item locks
 * before calling these functions.
 */

/**
 * Allocate and initialize a new item structure
 * @param key the key for the new item
 * @param nkey the number of bytes in the key
 * @param nbytes the number of bytes in the body for the item
 * @param flags the flags in the new item
 * @param exptime when the object should expire
 * @param a pointer to an item on success or NULL otherwise
 * @return ENGINE_SUCCESS on success
 */
ENGINE_ERROR_CODE item_alloc(const void *key, size_t nkey, size_t nbytes,
                             uint32_t flags, rel_time_t exptime, hash_item **item);

/**
 * Release our reference to the current item
 * @param item the item to release
 */
void item_release(hash_item *item);

/**
 * Delete an item from the cache (make it inaccessible)
 * @param key the key for the new item
 * @param nkey the number of bytes in the key
 * @param cas the cas value (IN)
 * @return ENGINE_SUCCESS on success
 */
ENGINE_ERROR_CODE item_delete(const void* key, const size_t nkey, uint64_t cas);

/**
 * Store an item in the cache
 * @param item the item to store
 * @param cas the cas value (OUT)
 * @param operation what kind of store operation is this (ADD/SET etc)
 * @return ENGINE_SUCCESS on success
 *
 * @todo should we refactor this into hash_item ** and remove the cas
 *       there so that we can get it from the item instead?
 */
ENGINE_ERROR_CODE item_store(hash_item *item, uint64_t *cas, ENGINE_STORE_OPERATION operation);


/**
 * Do arithmetic operation (incrment/decrment) on an item
 * @param key the key for the item to operate on
 * @param nkey the number of bytes in the key
 * @param incrment true if we want to increment, false for decrement
 * @param delta the amount to incr/decr
 * @param initial the initial value to set when the key isn't found.
 * @param exptime the expiration time to set when the key isn't found.
 * @param cas the new cas value (OUT)
 * @param result the new value for the item (OUT)
 * @return ENGINE_SUCCESS on success
 *
 * @todo perhaps we should do the same refactor as suggested for store_item
 */
ENGINE_ERROR_CODE item_arithmetic(const void* key, const int nkey, const bool incr, const uint64_t delta,
                                  const bool create, const uint64_t initial, const int flags,
                                  const rel_time_t exptime, uint64_t *cas, uint64_t *result);

/**
 * Get an item from the storage
 * @param key the key for the item to get
 * @param nkey the number of bytes in the key
 * @param pointer to the item if it exists or NULL otherwise
 * @return ENGINE_SUCCESS on success
 */
ENGINE_ERROR_CODE item_get(const void *key, const size_t nkey, hash_item **item);

/**
 * Get an item info from the storage
 * @param item the item to get info
 * @param info the item information
 * @return true or false
 */
bool item_info_get(hash_item *item, item_info *info);

/**
 * Get item statitistics
 * @param target the stats type to get
 * @param stats  the stats information
 */
void item_stats_get(int target, void *stats);

/**
 * Reset the item statistics
 * @param engine handle to the storage engine
 */
void item_stats_reset(void);

/**
 * Dump items from the cache
 * @param prefix prefix string (NULL means null prefix)
 * @param nprefix prefix string length (it must be larger than or equal to 0)
 * @param offset position to dump items.
 * @param limit the maximum number of items to receive
 * @param bytes the number of bytes in the return message (OUT)
 * @return pointer to a string containing the data
 *
 * @todo we need to rewrite this to use callbacks!!!! currently disabled
 */
char *item_cachedump(const char *prefix, const int nprefix, const bool forward,
                     unsigned int offset, unsigned int limit, unsigned int *bytes);

/**
 * Flush expired items from the cache
 * @param prefix prefix string (NULL means either null prefix or all prefixes)
 * @param nprefix prefix string length (0 means null prefix, -1 means all prefixes)
 * @param when when the items should be flushed
 */
ENGINE_ERROR_CODE item_flush_prefix(const char *prefix, const int nprefix, time_t when);


/**
 * Start the item scrubber
 */
bool item_scrub_start(int mode);
void item_scrub_get(struct item_scrub *stat);

#if 1 // ENABLE_RECOVERY
void item_undo_free(ItemDesc *itdesc);
#endif

ENGINE_ERROR_CODE item_dbcheck(void);

/*
 * Collection: Initialize API
 */
int  item_mgr_init(void *conf, void *srvr);
void item_mgr_release(void);
void item_mgr_final(void);
int  item_mgr_gc_thread_start(void);
void item_mgr_gc_thread_stop(void);

/*
 * Squall - new API
 */
uint64_t item_get_cas(const data_item* item);
void     item_set_cas(data_item* item, uint64_t val);
char*    item_get_key(const data_item* item);
char*    item_get_data(const data_item* item);
char*    item_get_meta(const data_item* item);
int      item_get_type(const data_item* item);

/*
 * Collection: Attribute API
 */
ENGINE_ERROR_CODE item_getattr(const void* key, const int nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data);
ENGINE_ERROR_CODE item_setattr(const void* key, const int nkey,
                               ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                               item_attr *attr_data);

#if 0 // ENABLE_SQUALL_ENGINE
/*
 * Collection: List API
 */
ENGINE_ERROR_CODE list_struct_create(struct squall_engine *engine,
                                     const char *key, const size_t nkey,
                                     item_attr *attrp, const void *cookie);
list_elem_item *  list_elem_alloc(struct squall_engine *engine,
                                  const int nbytes, const void *cookie);
void              list_elem_release(struct squall_engine *engine,
                                    list_elem_item **elem_array, const int elem_count);
ENGINE_ERROR_CODE list_elem_insert(struct squall_engine *engine,
                                   const char *key, const size_t nkey, int index,
                                   list_elem_item *elem, item_attr *attrp,
                                   bool *created, const void *cookie);
ENGINE_ERROR_CODE list_elem_delete(struct squall_engine *engine,
                                   const char *key, const size_t nkey,
                                   int from_index, int to_index, const bool drop_if_empty,
                                   uint32_t *del_count, bool *dropped);
ENGINE_ERROR_CODE list_elem_get(struct squall_engine *engine,
                                const char *key, const size_t nkey,
                                int from_index, int to_index,
                                const bool delete, const bool drop_if_empty,
                                list_elem_item **elem_array, uint32_t *elem_count,
                                uint32_t *flags, bool *dropped);

/*
 * Collection: Set API
 */
ENGINE_ERROR_CODE set_struct_create(struct squall_engine *engine,
                                    const char *key, const size_t nkey,
                                    item_attr *attrp, const void *cookie);
set_elem_item *   set_elem_alloc(struct squall_engine *engine,
                                 const int nbytes, const void *cookie);
void              set_elem_release(struct squall_engine *engine,
                                   set_elem_item **elem_array, const int elem_count);
ENGINE_ERROR_CODE set_elem_insert(struct squall_engine *engine,
                                  const char *key, const size_t nkey,
                                  set_elem_item *elem, item_attr *attrp,
                                  bool *created, const void *cookie);
ENGINE_ERROR_CODE set_elem_delete(struct squall_engine *engine,
                                  const char *key, const size_t nkey,
                                  const char *value, const size_t nbytes,
                                  const bool drop_if_empty, bool *dropped);
ENGINE_ERROR_CODE set_elem_exist(struct squall_engine *engine,
                                 const char *key, const size_t nkey,
                                 const char *value, const size_t nbytes, bool *exist);
ENGINE_ERROR_CODE set_elem_get(struct squall_engine *engine,
                               const char *key, const size_t nkey, const uint32_t count,
                               const bool delete, const bool drop_if_empty,
                               set_elem_item **elem_array, uint32_t *elem_count,
                               uint32_t *flags, bool *dropped);

/*
 * Collection: B+Tree API
 */
ENGINE_ERROR_CODE btree_struct_create(struct squall_engine *engine,
                                      const char *key, const size_t nkey,
                                      item_attr *attrp, const void *cookie);
btree_elem_item * btree_elem_alloc(struct squall_engine *engine,
                                   const int nbkey, const int neflag, const int nbytes,
                                   const void *cookie);
void              btree_elem_release(struct squall_engine *engine,
                                     btree_elem_item **elem_array, const int elem_count);
ENGINE_ERROR_CODE btree_elem_insert(struct squall_engine *engine,
                                    const char *key, const size_t nkey,
                                    btree_elem_item *elem,
                                    const bool replace_if_exist,
                                    item_attr *attrp,
                                    bool *replaced,
                                    bool *created, const void *cookie);
ENGINE_ERROR_CODE btree_elem_update(struct squall_engine *engine,
                                    const char *key, const size_t nkey,
                                    const bkey_range *bkrange, const eflag_update *eupdate,
                                    const char *value, const int nbytes,
                                    const void *cookie);
ENGINE_ERROR_CODE btree_elem_delete(struct squall_engine *engine,
                                    const char *key, const size_t nkey,
                                    const bkey_range *bkrange, const eflag_filter *efilter,
                                    const uint32_t req_count, const bool drop_if_empty,
                                    uint32_t *del_count, bool *dropped);
ENGINE_ERROR_CODE btree_elem_get(struct squall_engine *engine,
                                 const char *key, const size_t nkey,
                                 const bkey_range *bkrange, const eflag_filter *efilter,
                                 const uint32_t offset, const uint32_t req_count,
                                 const bool delete, const bool drop_if_empty,
                                 btree_elem_item **elem_array, uint32_t *elem_count,
                                 uint32_t *flags, bool *dropped_trimmed);
ENGINE_ERROR_CODE btree_elem_count(struct squall_engine *engine,
                                   const char *key, const size_t nkey,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   uint32_t *elem_count, uint32_t *flags);
#ifdef SUPPORT_BOP_SMGET
ENGINE_ERROR_CODE btree_elem_smget(struct squall_engine *engine,
                                   token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   btree_elem_item **elem_array, uint32_t *kfnd_array,
                                   uint32_t *flag_array, uint32_t *elem_count,
                                   uint32_t *missed_key_array, uint32_t *missed_key_count,
                                   bool *trimmed, bool *duplicated);
#endif
#endif

#endif
