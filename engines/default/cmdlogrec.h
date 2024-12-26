/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2019 JaM2in Co., Ltd.
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
#ifndef CMDLOGREC_H
#define CMDLOGREC_H

#ifdef ENABLE_PERSISTENCE
/*
 * LSN(log sequence number)
 */

/* Change the type of roffset from uint32_t to uint64_t.
 * 1. There is no guarantee that the checkpoint will always succeed,
 *    the cmdlog record offset can exceed 4GB during retries.
 * 2. Depending on the memlimit, a checkpoint can occur when offset exceeds 4GB.
 */
typedef struct logsn {
    uint32_t filenum;  /* cmdlog file number : 1, 2, ... */
    uint32_t rsvd32;   /* reserved 4 bytes */
    uint64_t roffset;  /* cmdlog record offset */
} LogSN;

/* LogSN : SET_NULL */
#define LOGSN_SET_NULL(lsn) \
        do { (lsn)->filenum = 0; (lsn)->roffset = 0; } while(0)

/* LogSN comparison macros */
#define LOGSN_IS_EQ(lsn1, lsn2) ((lsn1)->filenum == (lsn2)->filenum && \
                                 (lsn1)->roffset == (lsn2)->roffset)
#define LOGSN_IS_NE(lsn1, lsn2) ((lsn1)->filenum != (lsn2)->filenum || \
                                 (lsn1)->roffset != (lsn2)->roffset)
#define LOGSN_IS_LT(lsn1, lsn2) (((lsn1)->filenum <  (lsn2)->filenum) || \
                                 ((lsn1)->filenum == (lsn2)->filenum && \
                                  (lsn1)->roffset <  (lsn2)->roffset))
#define LOGSN_IS_LE(lsn1, lsn2) (((lsn1)->filenum <  (lsn2)->filenum) || \
                                 ((lsn1)->filenum == (lsn2)->filenum && \
                                  (lsn1)->roffset <= (lsn2)->roffset))
#define LOGSN_IS_GT(lsn1, lsn2) (((lsn1)->filenum >  (lsn2)->filenum) || \
                                 ((lsn1)->filenum == (lsn2)->filenum && \
                                  (lsn1)->roffset >  (lsn2)->roffset))
#define LOGSN_IS_GE(lsn1, lsn2) (((lsn1)->filenum >  (lsn2)->filenum) || \
                                 ((lsn1)->filenum == (lsn2)->filenum && \
                                  (lsn1)->roffset >= (lsn2)->roffset))

/* thread running state (FIXME: proper position) */
typedef enum {
    RUNNING_UNSTARTED,
    RUNNING_STARTED,
    RUNNING_STOPPED
} THREAD_RUNNING_STATE;

/*
 * Log Record Structures
 */

/* max log record size */
#define MAX_LOG_RECORD_SIZE (2 * 1024 * 1024)

/* log record type */
enum log_type {
    LOG_IT_LINK = 0,
    LOG_IT_UNLINK,
    LOG_IT_SETATTR,
    LOG_IT_FLUSH,
    LOG_LIST_ELEM_INSERT,
    LOG_LIST_ELEM_DELETE,
    LOG_SET_ELEM_INSERT,
    LOG_SET_ELEM_DELETE,
    LOG_MAP_ELEM_INSERT,
    LOG_MAP_ELEM_DELETE,
    LOG_BT_ELEM_INSERT,
    LOG_BT_ELEM_DELETE,
    LOG_BT_ELEM_DELETE_LOGICAL,
    LOG_OPERATION_BEGIN,
    LOG_OPERATION_END,
    LOG_SNAPSHOT_ELEM,
    LOG_SNAPSHOT_DONE
};

/* update type
 * is defined at default_engine.h
 */

/* log header structure */
typedef struct _loghdr {
    uint8_t     logtype;        /* Log type */
    uint8_t     updtype;        /* update command type */
    uint8_t     reserved_8[2];
    uint32_t    body_length;    /* LogRec body length */
} LogHdr;

/* log record structure */
typedef struct _logrec {
    LogHdr      header;
    char        *body;          /* specific log record data */
} LogRec;

/* Specific Log Record Structure */

/* key hash item common */
struct lrec_item_common {
    uint8_t     ittype;         /* item type */
    uint8_t     reserved_8[1];
    uint16_t    keylen;         /* key length */
    uint32_t    vallen;         /* value length */
    uint32_t    flags;          /* item flags */
    uint32_t    exptime;        /* expire time */
};

/* collection meta data */
struct lrec_coll_meta {
    uint8_t     ovflact;        /* overflow action */
    uint8_t     mflags;         /* sticky, readable, trimmed flags */
    uint8_t     maxbkrlen;      /* maxbkeyrange length */
    uint8_t     reserved_8[1];
    int32_t     mcnt;           /* maximum element count */
};

/* Item Link Log Record */
typedef struct _IT_link_data {
    struct lrec_item_common cm;
    union {
        uint64_t cas;
        struct lrec_coll_meta meta;
    } ptr;
    char data[1];
} ITLinkData;

typedef struct _IT_link_log {
    LogHdr      header;
    ITLinkData  body;
    char        *keyptr;
    unsigned char *maxbkrptr;    /* maxbkeyrange value */
} ITLinkLog;

/* Item Unlink Log Record */
typedef struct _IT_unlink_data {
    uint16_t keylen;         /* key length */
    char     data[1];
} ITUnlinkData;

typedef struct _IT_unlink_log {
    LogHdr       header;
    ITUnlinkData body;
    char         *keyptr;
} ITUnlinkLog;

/* Item Flush Log Record */
typedef struct _IT_flush_data {
    uint8_t nprefix;        /* prefix length */
    char    data[1];
} ITFlushData;

typedef struct _IT_flush_log {
    LogHdr      header;
    ITFlushData body;
    char        *prefixptr;
} ITFlushLog;

/* Item SetAttr Log Record */
typedef struct _IT_setattr_data {
    uint16_t keylen;         /* key length */
    uint16_t reserved_16[1];
    uint32_t exptime;        /* expire time */
    uint8_t  ovflact;        /* overflow action */
    uint8_t  mflags;         /* sticky, readable, trimmed flags */
    uint8_t  maxbkrlen;      /* maxbkeyrange length */
    uint8_t  reserved_8[1];
    int32_t  mcnt;           /* maximum element count */
    char     data[1];
} ITSetAttrData;

typedef struct _IT_setattr_log {
    LogHdr        header;
    ITSetAttrData body;
    char          *keyptr;
    unsigned char *maxbkrptr;    /* maxbkeyrange value */
} ITSetAttrLog;

/* Snapshot Element Log Record */
typedef struct _snapshot_elem_data {
    uint32_t nbytes;
    uint8_t  nekey;          /* nbkey(btree), nfield(map) */
    uint8_t  neflag;         /* neflag(btree) */
    char     data[1];
} SnapshotElemData;

typedef struct _snapshot_elem_log {
    LogHdr           header;
    SnapshotElemData body;
    char             *valptr;
    hash_item        *it;
} SnapshotElemLog;

/* List Elem Insert Log Record */
typedef struct _lrec_attr_info {
    uint32_t flags;        /* flags */
    uint32_t exptime;      /* expire time */
    int32_t  maxcount;     /* collection max count */
    uint8_t  reserved_8[2];
    uint8_t  ovflaction;   /* overflow action */
    uint8_t  mflags;       /* sticky, readable, trimmedflags */
} lrec_attr_info;

typedef struct _List_elem_insert_data {
    uint16_t keylen;  /* key length */
    uint8_t  create;  /* create flag */
    uint8_t  reserved_8[1];
    uint32_t vallen;  /* value length */
    uint32_t totcnt;  /* total element count */
    int32_t  eindex;  /* element index */
    char     data[1];
} ListElemInsData;

typedef struct _List_elem_insert_log {
    LogHdr          header;
    ListElemInsData body;
    char            *keyptr;
    char            *valptr;
    lrec_attr_info  *attrp;
} ListElemInsLog;

/* List Elem Delete Log Record */
typedef struct _List_elem_delete_data {
    uint16_t keylen;  /* key length */
    uint8_t  drop;    /* drop if empty */
    uint8_t  reserved_8[1];
    uint32_t totcnt;  /* total element count */
    int32_t  eindex;  /* element index */
    uint32_t delcnt;  /* delete count */
    char     data[1];
} ListElemDelData;

typedef struct _List_elem_delete_log {
    LogHdr          header;
    ListElemDelData body;
    char            *keyptr;
} ListElemDelLog;

/* Map Elem Insert Log Record */
typedef struct _Map_elem_insert_data {
    uint16_t keylen;  /* key length */
    uint8_t  create;  /* create flag */
    uint8_t  fldlen;  /* field length */
    uint32_t vallen;  /* value length */
    char     data[1];
} MapElemInsData;

typedef struct _Map_elem_insert_log {
    LogHdr         header;
    MapElemInsData body;
    char           *keyptr;
    char           *datptr;
    lrec_attr_info *attrp;
} MapElemInsLog;

/* Map Elem Delete Log Record */
typedef struct _Map_elem_delete_data {
    uint16_t keylen;  /* key length */
    uint8_t  drop;    /* drop if empty */
    uint8_t  fldlen;  /* field length */
    char     data[1];
} MapElemDelData;

typedef struct _Map_elem_delete_log {
    LogHdr         header;
    MapElemDelData body;
    char           *keyptr;
    char           *datptr;
} MapElemDelLog;

/* Set Elem Insert Log Record */
typedef struct _Set_elem_insert_data {
    uint16_t keylen;  /* key length */
    uint8_t  create;  /* create flag */
    uint8_t  reserved_8[1];
    uint32_t vallen;  /* value length */
    char     data[1];
} SetElemInsData;

typedef struct _Set_elem_insert_log {
    LogHdr         header;
    SetElemInsData body;
    char           *keyptr;
    char           *valptr;
    lrec_attr_info *attrp;
} SetElemInsLog;

/* Set Elem Delete Log Record */
typedef struct _Set_elem_delete_data {
    uint16_t keylen;  /* key length */
    uint8_t  drop;    /* drop if empty */
    uint8_t  reserved_8[1];
    uint32_t vallen;  /* value length */
    char     data[1];
} SetElemDelData;

typedef struct _Set_elem_delete_log {
    LogHdr         header;
    SetElemDelData body;
    char           *keyptr;
    char           *valptr;
} SetElemDelLog;

/* Btree Elem Insert Log Record */
typedef struct _Btree_elem_insert_data {
    uint16_t keylen;  /* key length */
    uint8_t  create;  /* create flag */
    uint8_t  nbkey;   /* bkey length */
    uint8_t  neflag;  /* eflag length */
    uint8_t  reserved_8[3];
    uint32_t vallen;  /* value length */
    char     data[1];
} BtreeElemInsData;

typedef struct _Btree_elem_insert_log {
    LogHdr           header;
    BtreeElemInsData body;
    char             *keyptr;
    char             *datptr;
    lrec_attr_info   *attrp;
} BtreeElemInsLog;

/* Btree Elem Delete Log Record */
typedef struct _Btree_elem_delete_data {
    uint16_t keylen;  /* key length */
    uint8_t  drop;    /* drop if empty */
    uint8_t  nbkey;   /* bkey length */
    char     data[1];
} BtreeElemDelData;

typedef struct _Btree_elem_delete_log {
    LogHdr           header;
    BtreeElemDelData body;
    char             *keyptr;
    char             *datptr;
} BtreeElemDelLog;

/* Btree Elem Delete Logical Log Record */
typedef struct _Btree_elem_delete_logical_data {
    uint16_t keylen;     /* key length */
    uint8_t  drop;       /* drop if empty */
    uint8_t  from_nbkey; /* fbkey length */
    uint8_t  to_nbkey;   /* tbkey length */
    uint8_t  filtering;  /* enable eflag filter */
    uint8_t  nbitwval;   /* bitwise value length */
    uint8_t  ncompval;   /* compare value length */
    uint8_t  compvcnt;   /* # of compare values */
    uint8_t  f_offset;   /* filter offset */
    uint8_t  bitwop;     /* bitwise operation */
    uint8_t  compop;     /* compare operation */
    uint32_t offset;     /* element offset */
    uint32_t reqcount;   /* request count */
    char     data[1];
} BtreeElemDelLgcData;

typedef struct _Btree_elem_delete_logical_log {
    LogHdr              header;
    BtreeElemDelLgcData body;
    char                *keyptr;
    bkey_range          *bkrangep;
    eflag_filter        *efilterp;
} BtreeElemDelLgcLog;

/* Operation Range Log Record */
typedef struct _operation_range_log {
    LogHdr header;
} OperationRangeLog;

/* Snapshot Done Log Record */
typedef struct _snapshot_done_data {
    char             engine_name[32];
    uint32_t         persistence_major_version;
    uint32_t         persistence_minor_version;
} SnapshotDoneData;

typedef struct _snapshot_done_log {
    LogHdr            header;
    SnapshotDoneData  body;
} SnapshotDoneLog;

/* Function to initialize log record manager */
void cmdlog_rec_init(struct default_engine *engine_ptr);

/* Construct Log Record Functions */
int lrec_construct_snapshot_done(LogRec *logrec);
int lrec_construct_snapshot_elem(LogRec *logrec, hash_item *it, void *elem);
int lrec_construct_link_item(LogRec *logrec, hash_item *it);
int lrec_construct_unlink_item(LogRec *logrec, hash_item *it);
int lrec_construct_flush_item(LogRec *logrec, const char *prefix, const int nprefix);
int lrec_construct_setattr(LogRec *logrec, hash_item *it, uint8_t updtype);
int lrec_construct_list_elem_insert(LogRec *logrec, hash_item *it,
                                    uint32_t totcnt, int eindex, list_elem_item *elem,
                                    bool create, lrec_attr_info *attr);
int lrec_construct_list_elem_delete(LogRec *logrec, hash_item *it,
                                    uint32_t totcnt, int eindex, uint32_t delcnt,
                                    bool drop);
int lrec_construct_map_elem_insert(LogRec *logrec, hash_item *it, map_elem_item *elem,
                                   bool create, lrec_attr_info *attr);
int lrec_construct_map_elem_delete(LogRec *logrec, hash_item *it, map_elem_item *elem,
                                   bool drop);
int lrec_construct_set_elem_insert(LogRec *logrec, hash_item *it, set_elem_item *elem,
                                   bool create, lrec_attr_info *attr);
int lrec_construct_set_elem_delete(LogRec *logrec, hash_item *it, set_elem_item *elem,
                                   bool drop);
int lrec_construct_btree_elem_insert(LogRec *logrec, hash_item *it, btree_elem_item *elem,
                                     bool create, lrec_attr_info *attr);
int lrec_construct_btree_elem_delete(LogRec *logrec, hash_item *it, btree_elem_item *elem,
                                     bool drop);
int lrec_construct_btree_elem_delete_logical(LogRec *logrec, hash_item *it,
                                             const bkey_range *bkrange,
                                             const eflag_filter *efilter,
                                             uint32_t offset, uint32_t reqcount, bool drop);
int lrec_construct_operation_range(LogRec *logrec, bool begin);

/* Function to write the given log record to log buffer */
void lrec_write_to_buffer(LogRec *logrec, char *bufptr);
/* Function to redo from the given log record. */
ENGINE_ERROR_CODE lrec_redo_from_record(LogRec *logrec);

/* get collection hashitem having ITLinkLog's key. */
hash_item *lrec_get_item_if_collection_link(ITLinkLog *log);
/* set collection hashitem in snapshot elem log record. */
void lrec_set_item_in_snapshot_elem(SnapshotElemLog *log, hash_item *it);
int lrec_check_snapshot_done(SnapshotDoneLog *log);
#endif

#endif
