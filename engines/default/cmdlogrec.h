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

struct lrec_key {
    uint8_t     type;
    uint16_t    len;
};

struct lrec_meta {
    uint32_t    flags;
    uint32_t    exptime;
};

struct lrec_val {
    uint64_t    cas;
    uint32_t    len;
};

/* Log Record Structure */

typedef struct loghdr {
    uint32_t    body_length; /* LogRec body length */
    uint8_t     logtype;     /* Log type */
    uint8_t     cmdtype;     /* command type */
    uint8_t     reserved[2];
} LogHdr;

typedef struct logrec {
    LogHdr      header;
    char*       body;   /* specific log record data */
} LogRec;

/* Specific Log Record Structure */

/* Item Link Log Record */
typedef struct _IT_link_data{
    struct lrec_key key;
    struct lrec_meta meta;
    struct lrec_val val;
    char   data[1];
} ITLinkData;

typedef struct _IT_link_log {
    LogHdr      header;
    ITLinkData  body;
    char        *keyptr;
    char        *valptr;
} ITLinkLog;

/* Snapshot File Tail Record */
typedef struct _snapshot_tail_log {
    LogHdr header;
} SnapshotTailLog;

/* Construct Log Record Function */
void lrec_it_link_record(LogRec *logRec, hash_item *it, uint8_t cmdtype);
/* FIXME : pass arguments according to logtype */
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
