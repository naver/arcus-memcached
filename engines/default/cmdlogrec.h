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
#ifndef MEMCACHED_LOGREC_H
#define MEMCACHED_LOGREC_H

enum log_type {
    LOG_IT_LINK = 0,
    LOG_COLL_LINK,
    LOG_IT_UNLINK,
    LOG_IT_ARITHMETIC,
    LOG_IT_SETATTR,
    LOG_LIST_ELEM,
    LOG_LIST_ELEM_DELETE,
    LOG_SET_ELEM,
    LOG_SET_ELEM_DELETE,
    LOG_MAP_ELEM,
    LOG_MAP_ELEM_DELETE,
    LOG_BT_ELEM,
    LOG_BT_ELEM_DELETE,
    LOG_BT_ELEM_ARITHMETIC,
    LOG_SNAPSHOT_HEAD,
    LOG_SNAPSHOT_TAIL
};

enum cmd_type {
    /* key value command */
    CMD_ADD = 0,
    CMD_SET,
    CMD_REPLACE,
    CMD_APPEND,
    CMD_PREPEND,
    CMD_CAS,
    CMD_INCR,
    CMD_DECR,
    CMD_DELETE,
    CMD_SETATTR,
    /* list command */
    CMD_LIST_CREATE,
    CMD_LIST_ELEM_INSERT,
    CMD_LIST_ELEM_DELETE,
    /* set command */
    CMD_SET_CREATE,
    CMD_SET_ELEM_INSERT,
    CMD_SET_ELEM_DELETE,
    /* map command */
    CMD_MAP_CREATE,
    CMD_MAP_ELEM_INSERT,
    CMD_MAP_ELEM_DELETE,
    CMD_MAP_ELEM_UPDATE,
    /* btree command */
    CMD_BT_CREATE,
    CMD_BT_ELEM_INSERT,
    CMD_BT_ELEM_UPSERT,
    CMD_BT_ELEM_DELETE,
    CMD_BT_ELEM_UPDATE,
    CMD_BT_ELEM_ARITHMETIC,
    /* not command */
    CMD_NONE
};

/* key hash item common */
struct lrec_common {
    uint8_t     ittype;     /* item type */
    uint8_t     reserved[1];
    uint16_t    keylen;     /* key length */
    uint32_t    flags;      /* item flags */
    uint32_t    exptime;    /* expire time */
};

struct lrec_val {
    uint32_t    vallen;     /* value length */
    uint32_t    reserved[1];
    uint64_t    cas;
};

/* Log Record Structure */

typedef struct loghdr {
    uint8_t     logtype;     /* Log type */
    uint8_t     cmdtype;     /* command type */
    uint8_t     reserved[2];
    uint32_t    body_length; /* LogRec body length */
} LogHdr;

typedef struct logrec {
    LogHdr      header;
    char        *body;       /* specific log record data */
} LogRec;

/* Specific Log Record Structure */

/* Item Link Log Record */
typedef struct _IT_link_data {
    struct lrec_common  cm;
    struct lrec_val     val;
    char data[1];
} ITLinkData;

typedef struct _IT_link_log {
    LogHdr      header;
    ITLinkData  body;
    char        *keyptr;
} ITLinkLog;

/* Snapshot File Tail Record */
typedef struct _snapshot_tail_log {
    LogHdr header;
} SnapshotTailLog;

/* Construct Log Record Function For Snapshot*/
void lrec_it_link_record_for_snapshot(LogRec *logRec, hash_item *it);
void lrec_coll_link_record_for_snapshot(LogRec *logRec, hash_item *it);
/* FIXME : pass arguments according to logtype */
/* FIXME : below record functions would be deleted if directly construct log record in items.c when command logging */
void lrec_it_unlink_record(LogRec *logRec);
void lrec_it_arithmetic_record(LogRec *logRec);
void lrec_it_setattr_record(LogRec *logRec);

void lrec_list_elem_record(LogRec *logRec);
void lrec_list_elem_delete_record(LogRec *logRec);

void lrec_set_elem_record(LogRec *logRec);
void lrec_set_elem_delete_record(LogRec *logRec);

void lrec_map_elem_record(LogRec *logRec);
void lrec_map_elem_delete_record(LogRec *logRec);

void lrec_bt_elem_record(LogRec *logRec);
void lrec_bt_elem_delete_record(LogRec *logRec);
void lrec_bt_elem_arithmetic_record(LogRec *logRec);

void lrec_snapshot_head_record(LogRec *logRec);
void lrec_snapshot_tail_record(LogRec *logRec);

void lrec_write(LogRec *logRec, char *bufptr);
void lrec_print(LogRec *logRec);
#endif
