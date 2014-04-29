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
#ifndef SQ_PAGE_H
#define SQ_PAGE_H

#include "sq_types.h"

/* page configuration */
struct page_conf {
    uint32_t page_size;
};

/* page type */
typedef enum page_type {
    PAGE_TYPE_META = 0, /* meta data page */
    PAGE_TYPE_DATA,     /* data base page */
    PAGE_TYPE_OVFL,     /* data overflow page */
    PAGE_TYPE_INDX      /* index page (btree or hash) */
} SQUALL_PAGE_TYPE;

/* page space usage */
typedef enum page_space {
    PAGE_SPACE_FREE = 0, /* no space is used */
    PAGE_SPACE_HALF,     /* half space is used */
    PAGE_SPACE_FULL      /* full space is used */
} SQUALL_PAGE_SPACE;

/* page slot structure */
typedef struct page_slot {
    uint16_t offset;    /* the distance from page header: 0 (free), >= page header size (used) */
    uint16_t length;    /* the length of the slot item */
} PGSLOT;

#if 0 /* when several slot status need to be maintained */
enum slot_status {
    SLOT_STATUS_FREE = 0,
    SLOT_STATUS_USED,
    SLOT_STATUS_DELMARK
};
typedef struct page_slot {
    uint32_t offset:14; /* unit: SLOT_ALIGN_BYTES(8 bytes) */
    uint32_t length:14; /* unit: SLOT_ALIGN_BYTES(8 bytes) */
    uint32_t status:4;
} PGSLOT;
#endif

#if 0 /* page structure according to page type */
typedef struct xpghdr {
    LogSN       pglsn;          /* page LSN */
    PageID      self_pgid;      /* self pageid */
    PageID      prev_pgid;      /* prev pageid */
    PageID      next_pgid;      /* next pageid */
    uint16_t    slot_size;      /* 0: variable slot size, positive integer: fixed slot size */
    uint16_t    used_nslot;     /* used # of slots */
    uint16_t    page_depth;     /* index page depth */
    uint16_t    total_nfree;    /* total free size : variable length slot page only */
    free_item   scat_fslot;     /* scattered free slot list */
    free_item   cont_fslot;     /* contiguous free slot: vairable length slot page only */
} XPGHDR;

typedef struct xpage {
    struct xpghdr header;
    char          body[1];
} XPage;

typedef struct dpghdr {
    LogSN       pglsn;          /* page LSN */
    PageID      self_pgid;      /* self pageid */
    PageID      prev_pgid;      /* prev pageid */
    PageID      next_pgid;      /* next pageid */
    uint16_t    slot_size;      /* 0: variable slot size, positive integer: fixed slot size */
    uint16_t    used_nslot;     /* used # of slots */
    uint16_t    total_nslot;    /* total # of slots */
    uint16_t    total_nfree;    /* total free size : variable length slot page only */
    free_item   scat_fslot;     /* scattered free slot list */
    free_item   cont_fslot;     /* contiguous free slot: vairable length slot page only */
} DPGHDR;

typedef struct dpage {
    struct dpghdr header;
    char         body[1];
} DPage;
#endif

/*** Page Structure *********************************
 * INDX PAGE : (must)   variable length slot page
 * DATA PAGE : (prefer) variable length slot page
 * OVFL PAGE : (prefer) fixed    length slot page
 ****************************************************/

/* Page Header Structure: 64 bytes in 64-bit environment */
typedef struct pghdr {
    LogSN       pglsn;          /* page LSN */
    PageID      self_pgid;      /* self pageid */
    PageID      prev_pgid;      /* prev pageid */
    PageID      next_pgid;      /* next pageid */
    uint8_t     pgtype;         /* page type: refer to enum squall_page_type */
    int8_t      pgdepth;        /* page depth: -1(data/ovfl page), 0 or positive value(index page) */
    uint16_t    pgsltsz;        /* page slot size: 0(variable length slot), positive value(fixed length slot) */
    uint32_t    pgdummy;        /* [NOT USED] */
    uint16_t    used_nslots;    /* used # of slots */
    uint16_t    totl_nslots;    /* (DATA PAGE ONLY) total # of slots */
    uint16_t    totl_frsize;    /* (VARIABLE LENGTH SLOT PAGE ONLY) total free size */
    uint16_t    cont_offset;    /* (VARIABLE LENGTH SLOT PAGE ONLY) the offset of the last free space */
    PGSLOT      scat_frslot;    /* (VARIABLE LENGTH SLOT PAGE ONLY) scattered free slot list */
    uint32_t    rsvd32;         /* reserved 4 bytes */
    uint64_t    rsvd64;         /* reserved 8 bytes */
} PGHDR;

typedef struct page {
    struct pghdr header;
    char         body[1];
} Page;


/*****************************************************/
/* index page management : variable length slot page */
/*****************************************************/
void     xpage_init(Page *pgptr, PageID pgid, int pgdepth);
void     xpage_clear_space(Page *pgptr);
void    *xpage_find_slot(Page *pgptr, uint32_t slotid);
void    *xpage_alloc_slot(Page *pgptr, uint32_t slotid, uint32_t size);
void    *xpage_realloc_slot(Page *pgptr, uint32_t slotid, uint32_t size);
int      xpage_free_slot(Page *pgptr, uint32_t slotid);
bool     xpage_check_consistency(Page *pgptr, int pgdepth);
int      xpage_get_depth(Page *pgptr);
void     xpage_set_depth(Page *pgptr, int pgdepth);
bool     xpage_is_empty(Page *pgptr);
bool     xpage_has_free_space(Page *pgptr, uint32_t size);

void     xpage_move_slots(Page *dst_pgptr, Page *src_pgptr, uint32_t slotid, uint32_t count);
void     xpage_copy_slots(Page *dst_pgptr, Page *src_pgptr, uint32_t slotid, uint32_t count);
void     xpage_truncate_slots(Page *pgptr, uint32_t slotid, uint32_t count);

#if 1 // ENABLE_RECOVERY
void     xpage_redo_build(Page *pgptr, char *logdata, uint32_t dataleng, uint32_t slotleng);
void     xpage_alloc_slot_redo(Page *pgptr, uint32_t slotid, uint16_t offset, uint16_t length);
void     xpage_free_slot_redo(Page *pgptr, uint32_t slotid);
#endif

/****************************************************/
/* data page management : variable length slot page */
/****************************************************/
void     dpage_init(Page *pgptr, PageID pgid);
void     dpage_clear_space(Page *pgptr);
void    *dpage_find_slot(Page *pgptr, uint32_t slotid);
void    *dpage_alloc_slot(Page *pgptr, uint32_t size, ItemID *itmid);
int      dpage_free_slot(Page *pgptr, uint32_t slotid);
int      dpage_delmark_slot(Page *pgptr, uint32_t slotid);
bool     dpage_check_consistency(Page *pgptr);
bool     dpage_is_empty(Page *pgptr);
int      dpage_compact_space(Page *pgptr);

/*****************************************************/
/* overflow page management : fixed length slot page */
/*****************************************************/
void     opage_init(Page *pgptr, PageID pgid, uint32_t slot_size);
void    *opage_find_slot(Page *pgptr, uint32_t slotid);
void    *opage_alloc_slot(Page *pgptr, uint32_t size, ItemID *itmid);
int      opage_free_slot(Page *pgptr, uint32_t slotid);
bool     opage_check_consistency(Page *pgptr);

/**************************/
/* common page management */
/**************************/
PageID   page_get_pageid(Page *pgptr);
uint32_t page_get_total_used_size(Page *pgptr);
uint32_t page_get_total_free_size(Page *pgptr);
uint32_t page_get_cntig_free_size(Page *pgptr);
uint32_t page_get_max_varslot_size(void);
uint32_t page_get_body_size(void);
uint32_t page_get_total_slot_count(Page *pgptr);
uint32_t page_get_used_slot_count(Page *pgptr);
int      page_get_type(Page *pgptr);
#if 1 // ENABLE_RECOVERY
void     page_set_lsn(Page *pgptr, LogSN lsn);
LogSN    page_get_lsn(Page *pgptr);
#endif
PageID   page_get_self_pageid(Page *pgptr);
void     page_set_self_pageid(Page *pgptr, PageID self_pgid);
PageID   page_get_next_pageid(Page *pgptr);
void     page_set_next_pageid(Page *pgptr, PageID next_pgid);
PageID   page_get_prev_pageid(Page *pgptr);
void     page_set_prev_pageid(Page *pgptr, PageID prev_pgid);

/****************/
/* page manager */
/****************/
void     page_mgr_init(void *conf, void *srvr);
void     page_mgr_final(void);
#endif
