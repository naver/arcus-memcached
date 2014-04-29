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
#ifndef SQ_PREFIX_H
#define SQ_PREFIX_H

#include "sq_items.h"

#define SQUALL_MAGIC_CODELEN   16
#define PREFIX_MAX_NAMELEN    127

/***** NOTE *****
   This define must be moved to include/memcached/types.h
   Refer to ENGINE_ITEM_TYPE.
*****************/
#define ITEM_TYPE_MAX           4

#define HASH_INDEX_MAX_DEPTH    32

#define USE_WARM_FREE_PAGE

#ifdef USE_WARM_FREE_PAGE
/* The warm pool of partial free pages */
//#define WARM_PAGE_POOL_SIZE 32
#define WARM_PAGE_POOL_SIZE 16
#define WPPS WARM_PAGE_POOL_SIZE
#define WARM_PAGE_CLASS_SIZE 8
#define WCLS WARM_PAGE_CLASS_SIZE
#endif

typedef uint16_t PrefixID;

/* prefix configuration */
struct prefix_conf {
    int         dbidx;     /* 0 or 1 */
    uint32_t    max_prefixes;
    uint32_t    file_size; /* unit: MB */
    uint32_t    page_size; /* unit: KB */
    char        delimiter;
    char       *data_path;
    uint32_t (*hash_func)(const void *data, size_t size, uint32_t seed);
};

/* prefix stats */
struct prefix_stat {
    char        name[PREFIX_MAX_NAMELEN+1]; /* prefix name: '\0' terminated */
    uint64_t    num_items[ITEM_TYPE_MAX];   /* total number of items for each item type */
    uint64_t    siz_items[ITEM_TYPE_MAX];   /* total size   of items for each item type */
    time_t      create_time;                /* prefix creation time */
};

#if 0 // NOT_USED_STRUCTURES
/* used page list: doubly linked list */
typedef struct page_used_list {
    PageID      head;
    PageID      tail;
    uint64_t    count;
} PageUsedList;

/* (total) free page list: single linked list */
typedef struct page_free_list {
    PageID      head;
    uint64_t    count;
} PageFreeList;

/* (partial) free page list: multiple single linked list */
#define PAL 4 /* parellel allocation level */
typedef struct page_free_list {
    PageID      head[PAL]; /* N free page lists */
    uint32_t    size[PAL]; /* free page list size */
    uint32_t    max_lcnt;  /* max # of free page lists */
    uint32_t    cur_lcnt;  /* cur # of free page lists: NOT logged */
    uint32_t    cur_lidx;  /* cur free page list index: NOT logged */
    uint32_t    dummy;
} PageFreeList;

/* total free page pool */
typedef struct page_free_pool {
    PageID      warm[256];
    uint16_t    size; /* max size of warm buffer: 256 */
    uint16_t    wcnt; /* warm buffer having free page ids */
    uint16_t    head; /* head index in warm buffer */
    uint16_t    tail; /* tail index in warm buffer */
} PageFreePool;
#endif

/* disk info of prefix space : 128 bytes */
typedef struct _prefix_disk_space {
    LogSN       crtlsn;             /* create LSN */
    LogSN       updlsn;             /* update LSN */
    uint32_t    disk_file_size;     /* unit: MB */
    uint32_t    disk_page_size;     /* unit: Bytes */
    uint16_t    space_id;           /* space id */
    uint16_t    space_xx;           /* NOT USED */
    uint32_t    data_total_flcnt;
    uint32_t    ovfl_total_flcnt;
    uint32_t    indx_total_flcnt;
    uint64_t    data_total_pgcnt;   /* data: total # of pages */
    uint64_t    ovfl_total_pgcnt;   /* ovfl: total # of pages */
    uint64_t    indx_total_pgcnt;   /* indx: total # of pages */
    uint64_t    disk_quota_pgcnt;   /* disk quota: # of pages */
    uint8_t     rsvd8[56];          /* reserved space */
} prefix_disk_space;

/* disk info of prefix descriptor : 512 bytes */
typedef struct _prefix_disk_desc {
    /* prefix common information */
    LogSN       crtlsn;              /* create LSN */
    LogSN       updlsn;              /* update LSN */
    uint16_t    prefix_id;          /* prefix id */
    uint16_t    space_id;           /* prefix space id */
    uint32_t    hash_val;           /* prefix hash value */
    uint8_t     use_cas;            /* use cas ? true or false */
    uint8_t     explicit_create;    /* explicitly created ? true or false */
    uint8_t     delete_request;     /* deleted ? true or false */
    uint8_t     private_space;      /* 1 or 0 */
    uint8_t     rsvd8[4];

    /* btree index management */
    int32_t     bndx_global_depth;
    int32_t     bndx_dummy;
    PageID      bndx_root_pgid;

    /* hash index management */
    int32_t     hndx_global_depth;
    int32_t     hndx_dummy;
    uint32_t    hndx_npage_per_depth[HASH_INDEX_MAX_DEPTH];

    /* extents information : NOT USED */
    PageID      data_extmap;
    PageID      ovfl_extmap;
    PageID      indx_extmap;

    /* used page count : NOT USED */
    uint64_t    data_used_pgcnt;
    uint64_t    ovfl_used_pgcnt;
    uint64_t    indx_used_pgcnt;

    uint64_t    rsvd64[10]; /* reserved 80 bytes */

    /* prefix statistics */
    struct prefix_stat stats;
} prefix_disk_desc;

#if 0 // OLD CODE
#define FREE_PAGE_CLASS_COUNT 2
#define TOTAL_FREE_PAGE       0
#define PARTIAL_FREE_PAGE     1

#define FINFO_TABLE_SIZE 64 /* initial size */
#define FINFO_CHUNK_SIZE 64
#define FILE_TABLE_SIZE 256
#define FILE_CHUNK_SIZE 256
#endif

#ifdef USE_WARM_FREE_PAGE
typedef struct _wmpg_entry {
    PageID      pgid;
    uint32_t    frsz; /* contiguous free size [HINT] */
    int16_t     wcls; /* wmpg space class id */
    int16_t     next; /* the next wmpg entry index */
} wmpg_entry;

typedef struct _warm_frpg_info {
    wmpg_entry  went[WPPS];
    int16_t     used[WCLS]; /* warm page class in respective of free space size */
    int16_t     free;       /* the free index in warm entries */
    uint16_t    rcnt;       /* # of referenced warm entries */
    uint16_t    ccnt;       /* # of used warm entries */
    uint16_t    mcnt;       /* # of total warm entries */
} warm_frpg_info;
#endif

/* pmap page header */
/* The header size must be multiple of 64 bytes
 * to make 1MB grow space to be represented by one pmap page.
 */
/* Refer to generic page header structure */
typedef struct _pmap_hdr {
    LogSN       map_lsn;    /* map page lsn */
    PageID      map_pgid;   /* curr map pgid */
    PageID      map_prev;   /* (NOT_USED) prev map pgid */
    PageID      map_next;   /* (NOT_USED) next map pgid */
    uint8_t     pgtype;     /* SQUALL_PAGE_TYPE_PMAP */
    uint8_t     pbitcnt;    /* partial bit count : additional # of bits for each partial free page */
    uint16_t    poffset;    /* partial bit offset: start offset of partial page bits in the pmap body */
    uint32_t    tot_pgs;    /* # of pages represented by this pmap page */
    uint32_t    tfr_pgs;    /* # of (total) free pages */
    int32_t     tfr_min;    /* (total) free min position */
    int32_t     tfr_max;    /* (total) free max position */
    uint32_t    pfr_pgs;    /* # of (partial) free pages */
    int32_t     pfr_min;    /* (partial) free min position */
    int32_t     pfr_max;    /* (partial) free max position */
} pmap_hdr;

/* pmap page */
typedef struct _pmap_page {
    pmap_hdr    hdr;
    uint8_t     body[1];
} pmap_page;

/* free file anchor */
typedef struct _frfl_anch {
    uint16_t    fhead; /* file head */
    uint16_t    ftail; /* file tail */
    uint32_t    pgcnt; /* # of free pages in all files */
} frfl_anch;

/* free file item */
typedef struct _frfl_item {
    uint16_t    fprev; /* [NOT USED] file prev */
    uint16_t    fnext; /* file next */
    uint32_t    pgcnt; /* # of free pages in each file */
} frfl_item;

/* file descriptor */
typedef struct _file_desc {
    int         fd;
    uint16_t    flnum;
    uint32_t    npage;  /* current # of pages used */
    pmap_page **pmaps;  /* pmap page pointer array */
    uint32_t   *pmtfs;  /* # of total free pages in each pmap */
    uint32_t   *pmpfs;  /* # of partial free pages in each pmap */
    frfl_item   tfree;  /* total free info   : all pages */
    frfl_item   pfree;  /* partial free info : data page only */
} file_desc;

/* file management */
typedef struct _file_mgmt {
    file_desc **fldsc_tab;
    uint32_t    fldsc_siz;
    uint32_t    fldsc_cnt;
    frfl_anch   tfree;       /* total free info   : all pages */
    frfl_anch   pfree;       /* partial free info : data page only */
    uint16_t    spcid;       /* space id */
    uint16_t    ftype;       /* file type */
    uint32_t    npage_pbyte; /* # of pages per byte */
    uint32_t    npage_ppmap; /* # of pages per pmap */
    uint32_t    npmap_pfile; /* # of pmaps per file */
} file_mgmt;

/* prefix space */
typedef struct _prefix_space {
    prefix_disk_space   d;

    //struct _prefix_space *next;     /* next prefix in a hash chain */
    uint16_t            spcid;      /* space id which is an index on space structure array */
    uint16_t            pfxcount;   /* # of prefixes shared by this prefix space */
    uint8_t             used;       /* 1(used) or 0(free) */

    /* file space management */
    file_mgmt           data_flmgt;
    file_mgmt           ovfl_flmgt;
    file_mgmt           indx_flmgt;

    pthread_mutex_t     data_lock;
    pthread_mutex_t     ovfl_lock;
    pthread_mutex_t     indx_lock; /* to serialize index page alloc & free */
} prefix_space;

/* prefix descriptor */
typedef struct _prefix_desc {
    /* 1. non-volatile information */
    prefix_disk_desc    d;

    /* 2. volatile information */
    /* basic information */
    struct _prefix_desc *next;      /* next prefix in a hash chain */
    char               *name;       /* prefix name */
    uint16_t            nlen;       /* prefix name length */
    uint16_t            tlen;       /* prefix name length + delimiter length if exist */
    uint32_t            hval;       /* prefix hash value : to find hash entry */
    uint16_t            refcount;   /* reference count */
    uint16_t            pfxid;      /* prefix id which is an index on prefix structure array */
    uint8_t             used;       /* 1(used) or 0(free) */
    uint8_t             lnked;      /* 1(linked) or 0(unlinked) */

    /* file space management */
    prefix_space       *space;
#ifdef USE_WARM_FREE_PAGE
    warm_frpg_info      wfrpg_info;
#endif
    /* btree index management */
    //Page           *bt_root;    /* btree root page */

    //pthread_mutex_t     lock;
    pthread_mutex_t     root_lock; /* to serialize index root building task */
#ifdef USE_WARM_FREE_PAGE
    pthread_mutex_t     warm_lock;
#endif
    pthread_mutex_t     stat_lock;
} prefix_desc;

/* External Functions */


int               pfx_mgr_init(void *conf, void *srvr);
void              pfx_mgr_final(void);
int               pfx_mgr_gc_thread_start(void);
void              pfx_mgr_gc_thread_stop(void);

prefix_desc      *pfx_find(const char *prefix, const size_t nprefix);
int               pfx_open(const char *prefix, const size_t nprefix, bool create);
void              pfx_open_with_id(int pfxid);
int               pfx_open_first(void);
int               pfx_open_next(int pfxid);
void              pfx_close(int pfxid);

ENGINE_ERROR_CODE pfx_create(const char *prefix, const size_t nprefix, int *pfxid);
ENGINE_ERROR_CODE pfx_delete(const char *prefix, const size_t nprefix);

void              pfx_data_anchor_flush(void);

ENGINE_ERROR_CODE pfx_space_dbcheck(void);

prefix_desc      *pfx_desc_get(int pfxid);
prefix_desc      *pfx_desc_get_first(void);
prefix_desc      *pfx_desc_get_next(int pfxid);

size_t            pfx_get_length(const char *key, const size_t nkey);
int               pfx_get_stats(const char *prefix, const int nprefix, struct prefix_stat *stats);
bool              pfx_use_cas(int pfxid);
void              pfx_item_stats_arith(int pfxid, int type, int count, int size, uint64_t *tot_count, uint64_t *tot_size);

ENGINE_ERROR_CODE pfx_data_slot_alloc(int pfxid, uint32_t size, ItemDesc *itdesc);
void              pfx_data_slot_free(int pfxid, ItemDesc *itdesc);

ENGINE_ERROR_CODE pfx_ovfl_slot_alloc(int pfxid, uint32_t count, ItemID *osids, void **osptr);
void              pfx_ovfl_slot_free(int pfxid, uint32_t count, ItemID *osids, void **osptr);

ENGINE_ERROR_CODE pfx_bndx_page_alloc(int pfxid, PageID *pgid);
void              pfx_bndx_page_free(int pfxid, PageID pgid);

ENGINE_ERROR_CODE pfx_hndx_page_set_used(prefix_desc *pfdesc, PageID pgid);
void              pfx_hndx_page_set_free(prefix_desc *pfdesc, PageID pgid);
bool              pfx_hndx_page_is_used(prefix_desc *pfdesc, PageID pgid);

PageID            pfx_data_page_get_first(int pfxid);
PageID            pfx_data_page_get_next(int pfxid, PageID pgid);

bool              pfx_page_check_before_write(Page *pgptr);
bool              pfx_page_check_after_read(Page *pgptr);
int               pfx_get_fd_from_pgid(PageID pgid);

#if 1 // ENABLE_RECOVERY
int               pfx_redo_create(const char *prefix, const size_t nprefix, int pfxid, int spcid, bool exp_crt, LogSN lsn);
int               pfx_redo_delete(const char *prefix, const size_t nprefix, LogSN lsn);
int               pfx_redo_flgrow(int spcid, int ftype, int flnum, int pgnum, int grow_pgcnt, LogSN lsn);
int               pfx_redo_pgalloc(PageID pgid, int pgspace, LogSN lsn);
int               pfx_redo_pgfree(PageID pgid, LogSN lsn);
int               pfx_redo_pgpartial(PageID pgid, LogSN lsn);
int               pfx_redo_pgalloc_v2(PageID *pgids, int pgcnt, bool partial, LogSN lsn);
int               pfx_redo_pgfree_v2(PageID *pgids, int pgcnt, bool partial, LogSN lsn);
void              pfx_redo_item_stats(int pfxid, int type, uint64_t tot_count, uint64_t tot_size);
int               mpage_redo_alloc_page(pmap_page *pmapg, int *pbidx_list, int count);
int               mpage_redo_free_page(pmap_page *pmapg, int *pbidx_list, int count);
int               mpage_redo_get_partial_page(pmap_page *pmapg, int pbidx);
int               mpage_redo_set_partial_page(pmap_page *pmapg, int pbidx);
#endif
#endif
