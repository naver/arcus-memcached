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

#include <string.h>
#include <ctype.h>

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE_04_KV_DATA_SNAPSHOT
#include "cmdlogrec.h"

#define DEBUG_PERSISTENCE_DISK_FORMAT_PRINT

#ifdef offsetof
#error "offsetof is already defined"
#endif
#define offsetof(type, member) __builtin_offsetof(type, member)

/* Get aligned size */
#define GET_8_ALIGN_SIZE(size) \
    (((size) % 8) == 0 ? (size) : ((size) + (8 - ((size) % 8))))

static char *get_logtype_text(uint8_t type)
{
    switch (type) {
        case LOG_IT_LINK:
            return "IT_LINK";
        case LOG_SNAPSHOT_TAIL:
            return "SNAPSHOT_TAIL";
    }
    return "unknown";
}

static char *get_updtype_text(uint8_t type)
{
    switch (type) {
        case UPD_SET:
            return "SET";
        case UPD_NONE:
            return "NONE";
    }
    return "unknown";
}

static char *get_itemtype_text(uint8_t type)
{
    switch (type) {
        case ITEM_TYPE_KV:
            return "KV";
        case ITEM_TYPE_LIST:
            return "LIST";
        case ITEM_TYPE_SET:
            return "SET";
        case ITEM_TYPE_MAP:
            return "MAP";
        case ITEM_TYPE_BTREE:
            return "B+TREE";
    }
    return "unknown";
}

static void log_record_header_print(LogHdr *hdr)
{
    fprintf(stderr, "[HEADER] body_length=%u | logtype=%s | updtype=%s\n",
            hdr->body_length, get_logtype_text(hdr->logtype), get_updtype_text(hdr->updtype));
}

/* Item Link Log Record */
static void lrec_it_link_write(LogRec *logrec, char *bufptr)
{
    ITLinkLog   *log = (ITLinkLog*)logrec;
    ITLinkData  *body = &log->body;
    int offset = offsetof(ITLinkData, data);

    memcpy(bufptr, logrec, offset);
    /* key value copy */
    memcpy(bufptr+offset, log->keyptr, body->cm.keylen+body->cm.vallen);
}

static void lrec_it_link_print(LogRec *logrec)
{
    ITLinkLog  *log = (ITLinkLog*)logrec;
    ITLinkData *body = &log->body;
    struct lrec_item_common *cm = (struct lrec_item_common*)&body->cm;

    log_record_header_print(&log->header);
    fprintf(stderr, "[BODY]   ittype=%s | flags=%u | exptime=%u | cas=%"PRIu64" | "
            "keylen=%u | keystr=%.*s | vallen=%u | valstr=%.*s\n",
            get_itemtype_text(cm->ittype), cm->flags, cm->exptime, body->ptr.cas,
            cm->keylen, (cm->keylen <= 250 ? cm->keylen : 250), log->keyptr,
            cm->vallen, (cm->vallen <= 250 ? cm->vallen : 250), log->keyptr+cm->keylen);
}

/* Item Unlink Log Record */
static void lrec_it_unlink_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_it_unlink_print(LogRec *logrec)
{
}

/* Item Arithmetic Log Record */
static void lrec_it_arithmetic_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_it_arithmetic_print(LogRec *logrec)
{
}

/* Item Setattr Log Record */
static void lrec_it_setattr_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_it_setattr_print(LogRec *logrec)
{
}

/* List Element Log Record */
static void lrec_list_elem_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_list_elem_print(LogRec *logrec)
{
}

/* List Element Insert Log Record */
static void lrec_list_elem_insert_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_list_elem_insert_print(LogRec *logrec)
{
}

/* List Element Delete Log Record */
static void lrec_list_elem_delete_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_list_elem_delete_print(LogRec *logrec)
{
}

/* Set Element Log Record */
static void lrec_set_elem_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_set_elem_print(LogRec *logrec)
{
}

/* Set Element Insert Log Record */
static void lrec_set_elem_insert_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_set_elem_insert_print(LogRec *logrec)
{
}

/* Set Element Delete Log Record */
static void lrec_set_elem_delete_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_set_elem_delete_print(LogRec *logrec)
{
}

/* Map Element Log Record */
static void lrec_map_elem_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_map_elem_print(LogRec *logrec)
{
}

/* Map Element Insert Log Record */
static void lrec_map_elem_insert_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_map_elem_insert_print(LogRec *logrec)
{
}

/* Map Element Delete Log Record */
static void lrec_map_elem_delete_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_map_elem_delete_print(LogRec *logrec)
{
}

/* BTree Element Log Record */
static void lrec_bt_elem_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_bt_elem_print(LogRec *logrec)
{
}

/* BTree Element Insert Log Record */
static void lrec_bt_elem_insert_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_bt_elem_insert_print(LogRec *logrec)
{
}

/* BTree Element Delete Log Record */
static void lrec_bt_elem_delete_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_bt_elem_delete_print(LogRec *logrec)
{
}

/* BTree Element Arithmetic Log Record */
static void lrec_bt_elem_arithmetic_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_bt_elem_arithmetic_print(LogRec *logrec)
{
}

/* Snapshot Head Log Record */
static void lrec_snapshot_head_write(LogRec *logrec, char *bufptr)
{
}

static void lrec_snapshot_head_print(LogRec *logrec)
{
}

/* Snapshot Tail Log Record */
static void lrec_snapshot_tail_write(LogRec *logrec, char *bufptr)
{
    SnapshotTailLog *log = (SnapshotTailLog*)logrec;
    memcpy(bufptr, log, sizeof(SnapshotTailLog));
}

static void lrec_snapshot_tail_print(LogRec *logrec)
{
    LogHdr *hdr = &logrec->header;
    log_record_header_print(hdr);
}

/* Log Record Function */
typedef struct _logrec_func {
    void (*write)(LogRec *logrec, char *bufptr);
    void (*print)(LogRec *logrec);
} LOGREC_FUNC;

LOGREC_FUNC logrec_func[] = {
    { lrec_it_link_write,            lrec_it_link_print },
    { lrec_it_unlink_write,          lrec_it_unlink_print },
    { lrec_it_arithmetic_write,      lrec_it_arithmetic_print },
    { lrec_it_setattr_write,         lrec_it_setattr_print },
    { lrec_list_elem_write,          lrec_list_elem_print },
    { lrec_list_elem_insert_write,   lrec_list_elem_insert_print },
    { lrec_list_elem_delete_write,   lrec_list_elem_delete_print },
    { lrec_set_elem_write,           lrec_set_elem_print },
    { lrec_set_elem_insert_write,    lrec_set_elem_insert_print },
    { lrec_set_elem_delete_write,    lrec_set_elem_delete_print },
    { lrec_map_elem_write,           lrec_map_elem_print },
    { lrec_map_elem_insert_write,    lrec_map_elem_insert_print },
    { lrec_map_elem_delete_write,    lrec_map_elem_delete_print },
    { lrec_bt_elem_write,            lrec_bt_elem_print },
    { lrec_bt_elem_insert_write,     lrec_bt_elem_insert_print },
    { lrec_bt_elem_delete_write,     lrec_bt_elem_delete_print },
    { lrec_bt_elem_arithmetic_write, lrec_bt_elem_arithmetic_print },
    { lrec_snapshot_head_write,      lrec_snapshot_head_print },
    { lrec_snapshot_tail_write,      lrec_snapshot_tail_print }
};

/* external function */

void lrec_write(LogRec *logrec, char *bufptr)
{
    logrec_func[logrec->header.logtype].write(logrec, bufptr);
#ifdef DEBUG_PERSISTENCE_DISK_FORMAT_PRINT
    logrec_func[logrec->header.logtype].print(logrec);
#endif
}

/* Construct Log Record Function For Snapshot */
int lrec_construct_snapshot_head(LogRec *logrec)
{
    return 0;
}

int lrec_construct_snapshot_tail(LogRec *logrec)
{
    SnapshotTailLog *log = (SnapshotTailLog*)logrec;
    log->header.logtype = LOG_SNAPSHOT_TAIL;
    log->header.updtype = UPD_NONE;
    log->header.body_length = 0;
    return sizeof(SnapshotTailLog);
}

int lrec_construct_snapshot_item(LogRec *logrec, hash_item *it)
{
    ITLinkLog *log = (ITLinkLog*)logrec;
    ITLinkData *body = &log->body;
    log->keyptr = (char*)item_get_key(it);

    struct lrec_item_common *cm = (struct lrec_item_common*)&body->cm;
    cm->ittype = GET_ITEM_TYPE(it);
    cm->keylen = it->nkey;
    cm->vallen = it->nbytes;
    cm->flags = it->flags;
    cm->exptime = it->exptime;
    body->ptr.cas = item_get_cas(it);

    log->header.logtype = LOG_IT_LINK;
    log->header.updtype = UPD_SET;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(ITLinkData, data) + cm->keylen + cm->vallen);
    return log->header.body_length;
}
#endif
