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
#ifndef MEMCACHED_CMDLOGREC_H
#define MEMCACHED_CMDLOGREC_H

#define MAX_LOG_RECORD_SIZE (2 * 1024 * 1024)

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
    LOG_SNAPSHOT_ELEM,
    LOG_SNAPSHOT_HEAD,
    LOG_SNAPSHOT_TAIL
};

enum upd_type {
    /* key value command */
    UPD_SET = 0,
    UPD_DELETE,
    UPD_SETATTR_EXPTIME,
    UPD_SETATTR_EXPTIME_INFO,
    UPD_SETATTR_EXPTIME_INFO_BKEY,
    UPD_FLUSH,
    /* list command */
    UPD_LIST_CREATE,
    UPD_LIST_ELEM_INSERT,
    UPD_LIST_ELEM_DELETE,
    /* set command */
    UPD_SET_CREATE,
    UPD_SET_ELEM_INSERT,
    UPD_SET_ELEM_DELETE,
    /* map command */
    UPD_MAP_CREATE,
    UPD_MAP_ELEM_INSERT,
    UPD_MAP_ELEM_DELETE,
    UPD_MAP_ELEM_UPDATE,
    /* btree command */
    UPD_BT_CREATE,
    UPD_BT_ELEM_INSERT,
    UPD_BT_ELEM_UPSERT,
    UPD_BT_ELEM_DELETE,
    UPD_BT_ELEM_UPDATE,
    UPD_BT_ELEM_ARITHMETIC,
    /* not command */
    UPD_NONE
};

/* key hash item common */
struct lrec_item_common {
    uint8_t     ittype;         /* item type */
    uint8_t     reserved_8[1];
    uint16_t    keylen;         /* key length */
    uint32_t    vallen;         /* value length */
    uint32_t    flags;          /* item flags */
    uint32_t    exptime;        /* expire time */
};

struct lrec_coll_meta {
    uint8_t     ovflact;        /* overflow action */
    uint8_t     mflags;         /* sticky, readable, trimmed flags */
    uint8_t     maxbkrlen;      /* maxbkeyrange length */
    uint8_t     reserved_8[1];
    int32_t     mcnt;           /* maximum element count */
};

/* Log Record Structure */

typedef struct _loghdr {
    uint8_t     logtype;        /* Log type */
    uint8_t     updtype;        /* update command type */
    uint8_t     reserved_8[2];
    uint32_t    body_length;    /* LogRec body length */
} LogHdr;

typedef struct _logrec {
    LogHdr      header;
    char        *body;          /* specific log record data */
} LogRec;

/* Specific Log Record Structure */

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
typedef struct _List_elem_insert_data {
    uint16_t keylen;  /* key length */
    uint16_t reserved_16[1];
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
} ListElemInsLog;

/* List Elem Delete Log Record */
typedef struct _List_elem_delete_data {
    uint16_t keylen;  /* key length */
    uint16_t reserved_16[1];
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
    uint8_t  fldlen;  /* field length */
    uint8_t  reserved_8[1];
    uint32_t vallen;  /* value length */
    char     data[1];
} MapElemInsData;

typedef struct _Map_elem_insert_log {
    LogHdr         header;
    MapElemInsData body;
    char           *keyptr;
    char           *datptr;
} MapElemInsLog;

/* Map Elem Delete Log Record */
typedef struct _Map_elem_delete_data {
    uint16_t keylen;  /* key length */
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
    uint16_t reserved_16[1];
    uint32_t vallen;  /* value length */
    char     data[1];
} SetElemInsData;

typedef struct _Set_elem_insert_log {
    LogHdr         header;
    SetElemInsData body;
    char           *keyptr;
    char           *valptr;
} SetElemInsLog;

/* Set Elem Delete Log Record */
typedef struct _Set_elem_delete_data {
    uint16_t keylen;  /* key length */
    uint16_t reserved_16[1];
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
    uint8_t  nbkey;   /* bkey length */
    uint8_t  neflag;  /* eflag length */
    uint32_t vallen;  /* value length */
    char     data[1];
} BtreeElemInsData;

typedef struct _Btree_elem_insert_log {
    LogHdr           header;
    BtreeElemInsData body;
    char             *keyptr;
    char             *datptr;
} BtreeElemInsLog;

/* Btree Elem Delete Log Record */
typedef struct _Btree_elem_delete_data {
    uint16_t keylen;  /* key length */
    uint8_t  nbkey;   /* bkey length */
    char     data[1];
} BtreeElemDelData;

typedef struct _Btree_elem_delete_log {
    LogHdr           header;
    BtreeElemDelData body;
    char             *keyptr;
    char             *datptr;
} BtreeElemDelLog;

#ifdef ENABLE_PERSISTENCE_03_SNAPSHOT_HEAD_LOG
/* Snapshot File Head Record */
typedef struct _Snapshot_head_data {
    char     engine_name[32];     /* engine name containing null character. */
    int64_t  checkpoint_time;     /* -1 : normal file, > 0 : checkpoint file */
    uint32_t persistence_version; /* latest persistence version */
    char     data[1];
} SnapshotHeadData;

typedef struct _snapshot_head_log {
    LogHdr           header;
    SnapshotHeadData body;
} SnapshotHeadLog;
#endif
/* Snapshot File Tail Record */
typedef struct _snapshot_tail_log {
    LogHdr header;
} SnapshotTailLog;

/* Function to initialize log record manager */
void cmdlog_rec_init(struct default_engine *engine);

/* Construct Log Record Functions */
#ifdef ENABLE_PERSISTENCE_03_SNAPSHOT_HEAD_LOG
int lrec_construct_snapshot_head(LogRec *logrec, int64_t chkpt_time);
#else
int lrec_construct_snapshot_head(LogRec *logrec);
#endif
int lrec_construct_snapshot_tail(LogRec *logrec);
int lrec_construct_snapshot_elem(LogRec *logrec, hash_item *it, void *elem);
int lrec_construct_link_item(LogRec *logrec, hash_item *it);
int lrec_construct_unlink_item(LogRec *logrec, hash_item *it);
int lrec_construct_flush_item(LogRec *logrec, const char *prefix, const int nprefix);
int lrec_construct_setattr(LogRec *logrec, hash_item *it, uint8_t updtype);
int lrec_construct_list_elem_insert(LogRec *logrec, hash_item *it, uint32_t totcnt, int eindex, list_elem_item *elem);
int lrec_construct_list_elem_delete(LogRec *logrec, hash_item *it, uint32_t totcnt, int eindex, uint32_t delcnt);
int lrec_construct_map_elem_insert(LogRec *logrec, hash_item *it, map_elem_item *elem);
int lrec_construct_map_elem_delete(LogRec *logrec, hash_item *it, map_elem_item *elem);
int lrec_construct_set_elem_insert(LogRec *logrec, hash_item *it, set_elem_item *elem);
int lrec_construct_set_elem_delete(LogRec *logrec, hash_item *it, set_elem_item *elem);
int lrec_construct_btree_elem_insert(LogRec *logrec, hash_item *it, btree_elem_item *elem);
int lrec_construct_btree_elem_delete(LogRec *logrec, hash_item *it, btree_elem_item *elem);

/* Function to write the given log record to log buffer */
void lrec_write_to_buffer(LogRec *logrec, char *bufptr);
/* Function to redo from the given log record. */
ENGINE_ERROR_CODE lrec_redo_from_record(LogRec *logrec);

/* get collection hashitem having ITLinkLog's key. */
hash_item *lrec_get_item_if_collection_link(ITLinkLog *log);
/* set collection hashitem in snapshot elem log record. */
void lrec_set_item_in_snapshot_elem(SnapshotElemLog *log, hash_item *it);
#endif
