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
#ifndef SQ_LOGREC_H
#define SQ_LOGREC_H

#include <stddef.h> /* offsetof */
#include "sq_types.h"

/************************/
/* Log Record Structure */
/************************/

typedef struct loghdr {
    LogSN       self_lsn;
    //LogSN       prev_lsn;
    LogSN       unxt_lsn; /* undo next lsn */
    //PageID      pageid;
    uint64_t    tranid;
    //ItemID      itmid;
    uint32_t    length;  /* log body length */
    uint8_t     logtype; /* log type */
    uint8_t     acttype; /* action type */
    uint8_t     cmttype; /* commit type */
    uint8_t     dummy;
} LOGHDR;

typedef struct logrec {
    struct loghdr header;
    char          body[1];
} LogRec;


/***********************/
/* Specific Log Record */
/***********************/

/* Prefix Create Log Record */
typedef struct _pfcreate_data {
    uint16_t    pfxid;
    uint16_t    spcid;
    uint8_t     use_cas; /* use cas */
    uint8_t     exp_crt; /* explicitly created */
    uint16_t    nprefix;
    char        data[1];
} PFCreateData;

typedef struct _pfcreate_log {
    LOGHDR       header;
    PFCreateData body;
    void        *prefix;
} PFCreateLog;

/* Prefix Drop Log Record */
typedef struct _pfdrop_data {
    uint16_t    pfxid;
    uint16_t    spcid;
    uint16_t    nprefix;
    char        data[1];
} PFDropData;

typedef struct _pfdrop_log {
    LOGHDR      header;
    PFDropData  body;
    void       *prefix;
} PFDropLog;

/* File Grow Log Record */
typedef struct _flgrow_data {
    uint16_t    spcid;
    uint16_t    ftype;
    uint32_t    flnum;
    uint32_t    pgnum;
    uint32_t    pgcnt;
} FLGrowData;

typedef struct _flgrow_log {
    LOGHDR      header;
    FLGrowData  body;
    void        *image;
} FLGrowLog;

/* Page Compaction Log Record */
typedef struct _pgcompact_data {
    PageID      pgid;
    uint16_t    doffset;
    uint16_t    dlength;
    uint16_t    soffset;
    uint16_t    slength;
    char        data[1];
} PGCompactData;

typedef struct _pgcompact_log {
    LOGHDR      header;
    PGCompactData body;
    void       *pgptr;
} PGCompactLog;

/* Page Alloc Log Record */
typedef struct _pgalloc_data {
    uint32_t    pgcnt;
    uint8_t     pgtype;
    uint8_t     partial;
    PageID      pgid[1];
} PGAllocData;

typedef struct _pgalloc_log {
    LOGHDR      header;
    PGAllocData body;
} PGAllocLog;

/* Page Free Log Record */
typedef struct _pgfree_data {
    uint32_t    pgcnt;
    uint8_t     pgtype;
    uint8_t     partial;
    PageID      pgid[1];
} PGFreeData;

typedef struct _pgfree_log {
    LOGHDR      header;
    PGFreeData  body;
} PGFreeLog;

/* Item Alloc Log Record */
typedef struct _italloc_data {
    ItemID      itmid;
    uint32_t    size;
    uint16_t    method;  /* 0: warm free page, 1: partial free page, 2: total free page */
    uint16_t    pgspace; /* PAGE_SPACE_HALF or PAGE_SPACE_FULL */
} ITAllocData;

typedef struct _italloc_log {
    LOGHDR      header;
    ITAllocData body;
} ITAllocLog;

/* Item Overflow Add Log Record */
typedef struct _itovfadd_data {
    PageID      pgid;    /* overflow pageid */
    char        data[1]; /* overflow slot (or item) data itself */
} ITOvfAddData;

typedef struct _itovfadd_log {
    LOGHDR       header;
    ITOvfAddData body;
    void        *itptr;
} ITOvfAddLog;

/* Item Overflow Delete Log Record */
typedef struct _itovfdel_data {
    char        data[1]; /* overflow itemid array */
} ITOvfDelData;

typedef struct _itovfdel_log {
    LOGHDR       header;
    ITOvfDelData body;
    void        *osids;
} ITOvfDelLog;

/* Item Free Log Record */
typedef struct _itfree_data {
    ItemID      itmid;      /* item id */
    uint8_t     empty;
    uint8_t     free;
    uint8_t     compact;
    uint8_t     partial;
    uint8_t     dummy[4];
} ITFreeData;

typedef struct _itfree_log {
    LOGHDR      header;
    ITFreeData  body;
} ITFreeLog;

/* Item Link Log Record */
typedef struct _itlink_data {
    ItemID      itmid;      /* item id */
    uint64_t    items_cnt;  /* total cnt of items of given type in prefix */
    uint64_t    items_len;  /* total len of items of given type in prefix */
    char        data[1];    /* item itself */
} ITLinkData;

typedef struct _itlink_log {
    LOGHDR      header;
    ITLinkData  body;
    void       *itptr;      /* item pointer */
} ITLinkLog;

/* Item Unlink Log Record */
typedef struct _itunlink_data {
    ItemID      itmid;      /* item id */
    uint64_t    items_cnt;  /* total cnt of items of given type in prefix */
    uint64_t    items_len;  /* total len of items of given type in prefix */
    uint32_t    ittype;
    uint32_t    dummy;
} ITUnlinkData;

typedef struct _itunlink_log {
    LOGHDR       header;
    ITUnlinkData body;
} ITUnlinkLog;

/* Item Replace Log Record */
typedef struct _itreplace_data {
    ItemID      old_itmid;  /* old item id */
    ItemID      new_itmid;  /* new item id */
    uint64_t    items_cnt;  /* total cnt of items of given type in prefix */
    uint64_t    items_len;  /* total len of items of given type in prefix */
    uint32_t    new_itlen;
    ItemID      data[1];
} ITReplaceData;

typedef struct _itreplace_log {
    LOGHDR       header;
    ITReplaceData body;
    void        *itptr;     /* new item pointer */
} ITReplaceLog;

/* Item DelMark Log Record */
typedef struct _itdelmark_data {
    ItemID      itmid;  /* item id */
} ITDelMarkData;

typedef struct _itdelmark_log {
    LOGHDR        header;
    ITDelMarkData body;
} ITDelMarkLog;

/* Item Update Log Record (by add_delta operation) */
typedef struct _itupdate_data {
    ItemID      itmid;       /* item id */
    uint64_t    cas;         /* cas value */
    uint32_t    nbytes;      /* value length */
    char        data[1];     /* value itself */
} ITUpdateData;

typedef struct _itupdate_log {
    LOGHDR       header;
    ITUpdateData body;
    void        *value;
} ITUpdateLog;

/* Item Attribute Log Record */
typedef struct _itattr_data {
    ItemID      itmid;      /* item id */
    uint16_t    itemat_offset;
    uint16_t    itemat_length;
    uint16_t    collat_offset;
    uint16_t    collat_length;
    ItemID      data[1];
} ITAttrData;

typedef struct _itattr_log {
    LOGHDR      header;
    ITAttrData  body;
    void       *itptr;
} ITAttrLog;

/* BTree Root Build Log Record */
typedef struct _btrtbld_data {
    PageID      pgid; /* new root pageid */
} BTRtBldData;

typedef struct _btrtbld_log {
    LOGHDR      header;
    BTRtBldData body;
} BTRtBldLog;

/* BTree Root Split Log Record */
typedef struct _btrtspl_data {
    PageID      spl_pgid;   /* root pageid */
    PageID      lft_pgid;   /* left  child pageid */
    PageID      rht_pgid;   /* right child pageid */
    uint16_t    spl_dleng;  /* size of data items in split(root) page */
    uint16_t    spl_sleng;  /* size of data items in split(root) page */
    uint16_t    lft_dleng;  /* size of data items in left  child page */
    uint16_t    lft_sleng;  /* size of slot items in left  child page */
    uint16_t    rht_dleng;  /* size of data items in right child page */
    uint16_t    rht_sleng;  /* size of slot items in right child page */
    uint16_t    spl_depth;  /* page depth of the split(root) page */
    uint16_t    rsvd[1];
    char        data[1];
} BTRtSplData;

typedef struct _btrtspl_log {
    LOGHDR      header;
    BTRtSplData body;
    void       *spl_pgptr;
    void       *lft_pgptr;
    void       *rht_pgptr;
} BTRtSplLog;

/* BTree Page Split Log Record */
typedef struct _btpgspl_data {
    PageID      upp_pgid;   /* upper pageid */
    PageID      spl_pgid;   /* split pageid */
    PageID      rht_pgid;   /* new right pageid */
    PageID      nxt_pgid;   /* old right pageid */
    uint16_t    rht_dleng;
    uint16_t    rht_sleng;
    uint16_t    upp_slotid;
    uint16_t    upp_itemsz;
    uint16_t    spl_depth;  /* page depth of the split page */
    uint16_t    rsvd[3];
    char        data[1];
} BTPgSplData;

typedef struct _btpgspl_log {
    LOGHDR      header;
    BTPgSplData body;
    void       *rht_pgptr; /* right page pointer */
    void       *upp_itptr; /* upper item pointer */
} BTPgSplLog;

/* BTree Root Merge Log Record */
typedef struct _btrtmrg_data {
    PageID      mrg_pgid;
    PageID      chd_pgid;
    uint16_t    mrg_dleng;  /* size of data items in the merged root page */
    uint16_t    mrg_sleng;  /* size of slot items in the merged root page */
    uint16_t    mrg_depth;  /* page depth of the merged(root) page */
    uint16_t    rsvd[1];
    char        data[1];
} BTRtMrgData;

typedef struct _btrtmrg_log {
    LOGHDR      header;
    BTRtMrgData body;
    void       *mrg_pgptr;
} BTRtMrgLog;

/* BTree Page Merge Log Record */
typedef enum _btpgmrg_type {
    BT_PGMRG_RHT_REBAL = 1, /* rebalance with right page */
    BT_PGMRG_RHT_MERGE,     /* merge     with right page */
    BT_PGMRG_LFT_REBAL,     /* rebalance with left page */
    BT_PGMRG_LFT_MERGE      /* merge     with left page */
} BTPgMrgType;

typedef struct _btpgmrg_data {
    PageID      upp_pgid;   /* parent pageid */
    PageID      mrg_pgid;   /* merge pageid */
    PageID      oth_pgid;   /* other pageid */
    PageID      opp_pgid;   /* opposite pageid */
    uint16_t    mrg_type;
    uint16_t    mrg_depth;  /* page depth of the merged page */
    uint16_t    mrg_dleng;  /* size of data items in the merged page */
    uint16_t    mrg_sleng;  /* size of slot items in the merged page */
    uint16_t    oth_slotid; /* 0: right page, otherwise: left page */
    uint16_t    upp_slotid;
    uint16_t    idx_itemsz; /* depends on ?? */
    uint16_t    rsvd[1];
    char        data[1];
} BTPgMrgData;

typedef struct _btpgmrg_log {
    LOGHDR      header;
    BTPgMrgData body;
    void       *mrg_pgptr;
    void       *idx_itptr;
} BTPgMrgLog;

/* BTree Insert Log Record */
/* physiological redo & logical undo logging : assumes atomic page write */
typedef struct _btinsert_data {
    ItemID      xitmid;     /* index item id */
    uint16_t    length;     /* length of the index item */
    char        data[1];    /* index item itself */
} BTInsertData;
#if 0 // physical redo logging
typedef struct _btinsert_data {
    PageID      pgid;       /* btree leaf pageid */
    uint16_t    head_oset;
    uint16_t    head_leng;
    uint16_t    item_oset;
    uint16_t    item_leng;  /* <itmid, nkey, key> : logical undo information */
    uint16_t    slot_oset;
    uint16_t    slot_leng;
} BTInsertData;
#endif

typedef struct _btinsert_log {
    LOGHDR       header;
    BTInsertData body;
    void        *itptr;
} BTInsertLog;

#if 0
typedef struct _btinsclr_data {
    PageID      pgid;       /* btree leaf pageid */
    uint16_t    slotid;
} BTInsCLRData;

typedef struct _btinsclr_log {
    LOGHDR       header;
    BTInsCLRData body;
} BTInsCLRLog;
#endif

/* BTree Delete Log Record */
/* physiological redo & logical undo logging : assumes atomic page write */
typedef struct _btdelete_data {
    ItemID      xitmid;      /* index item id */
    ItemID      ditmid;      /* data item id */
    uint16_t    length;      /* length of subkey */
    char        data[1];     /* subkey string */
} BTDeleteData;

typedef struct _btdelete_log {
    LOGHDR       header;
    BTDeleteData body;
    void        *key;
} BTDeleteLog;

/* BTree Toggle Log Record */
typedef struct _bttoggle_data {
    ItemID      xitmid;      /* index item id */
    ItemID      old_ditmid;  /* old data item id */
    ItemID      new_ditmid;  /* new data item id */
    uint16_t    length;      /* length of subkey */
    char        data[1];     /* subkey string */
} BTToggleData;

typedef struct _bttoggle_log {
    LOGHDR       header;
    BTToggleData body;
    void        *key;
} BTToggleLog;

/* Checkpoint Log Record */

typedef struct _chkptbgn_log {
    LOGHDR       header;
} ChkptBgnLog;

typedef struct _chkptend_data {
    uint64_t     nxt_tranid;
    LogSN        min_trans_lsn;
    LogSN        min_dirty_lsn;
} ChkptEndData;

typedef struct _chkptend_log {
    LOGHDR       header;
    ChkptEndData body;
} ChkptEndLog;


/* log record function structure */
typedef struct _logrec_func {
    void (*write)(LogRec *logrec, char *bufptr);
    int  (*redo)(LogRec *logrec);
    int  (*undo)(LogRec *logrec, void *tran);
    void (*print)(LogRec *logrec);
} LOGREC_FUNC;

extern LOGREC_FUNC logrec_func[];


/*********************/
/* External Funtions */
/*********************/

/* Prefix Create Log Record */
void lrec_pf_create_init(LogRec *logrec);
void lrec_pf_create_write(LogRec *logrec, char *bufptr);
int  lrec_pf_create_redo(LogRec *logrec);
void lrec_pf_create_print(LogRec *logrec);
/* Prefix Drop Log Record */
void lrec_pf_drop_init(LogRec *logrec);
void lrec_pf_drop_write(LogRec *logrec, char *bufptr);
int  lrec_pf_drop_redo(LogRec *logrec);
void lrec_pf_drop_print(LogRec *logrec);
/* File Grow Log Record */
void lrec_fl_grow_init(LogRec *logrec);
void lrec_fl_grow_write(LogRec *logrec, char *bufptr);
int  lrec_fl_grow_redo(LogRec *logrec);
void lrec_fl_grow_print(LogRec *logrec);
/* Page Compaction Log Record */
void lrec_pg_compact_init(LogRec *logrec);
void lrec_pg_compact_write(LogRec *logrec, char *bufptr);
int  lrec_pg_compact_redo(LogRec *logrec);
void lrec_pg_compact_print(LogRec *logrec);
/* Page Alloc Log Record */
void lrec_pg_alloc_init(LogRec *logrec);
void lrec_pg_alloc_write(LogRec *logrec, char *bufptr);
int  lrec_pg_alloc_redo(LogRec *logrec);
int  lrec_pg_alloc_undo(LogRec *logrec, void *tran);
void lrec_pg_alloc_print(LogRec *logrec);
/* Page Free Log Record */
void lrec_pg_free_init(LogRec *logrec);
void lrec_pg_free_write(LogRec *logrec, char *bufptr);
int  lrec_pg_free_redo(LogRec *logrec);
void lrec_pg_free_print(LogRec *logrec);
/* Item Alloc Log Record */
void lrec_it_alloc_init(LogRec *logrec);
void lrec_it_alloc_write(LogRec *logrec, char *bufptr);
int  lrec_it_alloc_redo(LogRec *logrec);
int  lrec_it_alloc_undo(LogRec *logrec, void *tran);
void lrec_it_alloc_print(LogRec *logrec);
/* Item Overflow Add Log Record */
void lrec_it_ovfadd_init(LogRec *logrec);
void lrec_it_ovfadd_write(LogRec *logrec, char *bufptr);
int  lrec_it_ovfadd_redo(LogRec *logrec);
void lrec_it_ovfadd_print(LogRec *logrec);
/* Item Overflow Delete Log Record */
void lrec_it_ovfdel_init(LogRec *logrec);
void lrec_it_ovfdel_write(LogRec *logrec, char *bufptr);
int  lrec_it_ovfdel_redo(LogRec *logrec);
void lrec_it_ovfdel_print(LogRec *logrec);
/* Item Free Log Record */
void lrec_it_free_init(LogRec *logrec);
void lrec_it_free_write(LogRec *logrec, char *bufptr);
int  lrec_it_free_redo(LogRec *logrec);
void lrec_it_free_print(LogRec *logrec);
/* Item Link Log Record */
void lrec_it_link_init(LogRec *logrec);
void lrec_it_link_write(LogRec *logrec, char *bufptr);
int  lrec_it_link_redo(LogRec *logrec);
void lrec_it_link_print(LogRec *logrec);
/* Item Unlink Log Record */
void lrec_it_unlink_init(LogRec *logrec);
void lrec_it_unlink_write(LogRec *logrec, char *bufptr);
int  lrec_it_unlink_redo(LogRec *logrec);
void lrec_it_unlink_print(LogRec *logrec);
/* Item Replace Log Record */
void lrec_it_replace_init(LogRec *logrec);
void lrec_it_replace_write(LogRec *logrec, char *bufptr);
int  lrec_it_replace_redo(LogRec *logrec);
void lrec_it_replace_print(LogRec *logrec);
/* Item Replace Log Record */
void lrec_it_delmark_init(LogRec *logrec);
void lrec_it_delmark_write(LogRec *logrec, char *bufptr);
int  lrec_it_delmark_redo(LogRec *logrec);
void lrec_it_delmark_print(LogRec *logrec);
/* Item Update Log Record */
void lrec_it_update_init(LogRec *logrec);
void lrec_it_update_write(LogRec *logrec, char *bufptr);
int  lrec_it_update_redo(LogRec *logrec);
void lrec_it_update_print(LogRec *logrec);
/* Item Attribute Log Record */
void lrec_it_attr_init(LogRec *logrec);
void lrec_it_attr_write(LogRec *logrec, char *bufptr);
int  lrec_it_attr_redo(LogRec *logrec);
void lrec_it_attr_print(LogRec *logrec);
/* BTree Root Build Log Record */
void lrec_bt_rtbuild_init(LogRec *logrec);
void lrec_bt_rtbuild_write(LogRec *logrec, char *bufptr);
int  lrec_bt_rtbuild_redo(LogRec *logrec);
void lrec_bt_rtbuild_print(LogRec *logrec);
/* BTree Root Split Log Record */
void lrec_bt_rtsplit_init(LogRec *logrec);
void lrec_bt_rtsplit_write(LogRec *logrec, char *bufptr);
int  lrec_bt_rtsplit_redo(LogRec *logrec);
void lrec_bt_rtsplit_print(LogRec *logrec);
/* BTree Page Split Log Record */
void lrec_bt_pgsplit_init(LogRec *logrec);
void lrec_bt_pgsplit_write(LogRec *logrec, char *bufptr);
int  lrec_bt_pgsplit_redo(LogRec *logrec);
void lrec_bt_pgsplit_print(LogRec *logrec);
/* BTree Root Merge Log Record */
void lrec_bt_rtmerge_init(LogRec *logrec);
void lrec_bt_rtmerge_write(LogRec *logrec, char *bufptr);
int  lrec_bt_rtmerge_redo(LogRec *logrec);
void lrec_bt_rtmerge_print(LogRec *logrec);
/* BTree Page Merge Log Record */
void lrec_bt_pgmerge_init(LogRec *logrec);
void lrec_bt_pgmerge_write(LogRec *logrec, char *bufptr);
int  lrec_bt_pgmerge_redo(LogRec *logrec);
void lrec_bt_pgmerge_print(LogRec *logrec);
/* BTree Insert Log Record */
void lrec_bt_insert_init(LogRec *logrec);
void lrec_bt_insert_write(LogRec *logrec, char *bufptr);
int  lrec_bt_insert_redo(LogRec *logrec);
int  lrec_bt_insert_undo(LogRec *logrec, void *tran);
void lrec_bt_insert_print(LogRec *logrec);
/* BTree Delete Log Record */
void lrec_bt_delete_init(LogRec *logrec);
void lrec_bt_delete_write(LogRec *logrec, char *bufptr);
int  lrec_bt_delete_redo(LogRec *logrec);
int  lrec_bt_delete_undo(LogRec *logrec, void *tran);
void lrec_bt_delete_print(LogRec *logrec);
/* BTree Toggle Log Record */
void lrec_bt_toggle_init(LogRec *logrec);
void lrec_bt_toggle_write(LogRec *logrec, char *bufptr);
int  lrec_bt_toggle_redo(LogRec *logrec);
int  lrec_bt_toggle_undo(LogRec *logrec, void *tran);
void lrec_bt_toggle_print(LogRec *logrec);
/* Checkpoint Begin Log Record */
void lrec_chkpt_bgn_init(LogRec *logrec);
void lrec_chkpt_bgn_write(LogRec *logrec, char *bufptr);
int  lrec_chkpt_bgn_redo(LogRec *logrec);
void lrec_chkpt_bgn_print(LogRec *logrec);
/* Checkpoint End Log Record */
void lrec_chkpt_end_init(LogRec *logrec);
void lrec_chkpt_end_write(LogRec *logrec, char *bufptr);
int  lrec_chkpt_end_redo(LogRec *logrec);
void lrec_chkpt_end_print(LogRec *logrec);

/* Log Record Manager Function */
void lrec_mgr_init(void *conf, void *srvr);
void lrec_mgr_final(void);
#endif
