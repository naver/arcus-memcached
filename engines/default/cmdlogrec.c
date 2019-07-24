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
#ifdef ENABLE_PERSISTENCE_04_DATA_SNAPSHOT
#include "cmdlogrec.h"

#ifdef offsetof
#error "offsetof is already defined"
#endif
#define offsetof(type, member) __builtin_offsetof(type, member)

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

static char *get_cmdtype_text(uint8_t type)
{
    switch (type) {
        case CMD_SET:
            return "SET";
        case CMD_NONE:
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
    fprintf(stderr, "[HEADER] body_length: %u | logtype: %s | cmdtype: %s\n",
            hdr->body_length, get_logtype_text(hdr->logtype), get_cmdtype_text(hdr->cmdtype));
}

/* Item Link Log Record */
static void lrec_it_link_write(LogRec *logRec, char *bufptr)
{
    ITLinkLog   *log = (ITLinkLog*)logRec;
    ITLinkData  *body = &log->body;
    struct lrec_common *cm = (struct lrec_common*)&body->cm;
    struct lrec_val    *val = (struct lrec_val*)&body->val;
    int kvlength = cm->keylen + val->vallen;
    int offset = log->header.body_length - kvlength;

    memcpy(bufptr, logRec, offset);
    /* key value copy */
    memcpy(bufptr+offset, log->keyptr, kvlength);
}

static void lrec_it_link_print(LogRec *logRec)
{
    ITLinkLog  *log = (ITLinkLog*)logRec;
    ITLinkData *body = &log->body;
    struct lrec_common *cm = (struct lrec_common*)&body->cm;
    struct lrec_val    *val = (struct lrec_val*)&body->val;

    log_record_header_print(&log->header);
    fprintf(stderr, "[BODY]   ittype: %s | keylen: %u | flags: %u | exptime: %u |"
            " cas: %"PRIu64" | vallen: %u | keystr: %.*s | valstr: %.*s",
            get_itemtype_text(cm->ittype), cm->keylen, cm->flags, cm->exptime,
            val->cas, val->vallen, cm->keylen, log->keyptr, val->vallen, log->keyptr+cm->keylen);
}

void lrec_it_link_record_for_snapshot(LogRec *logRec, hash_item *it)
{
    ITLinkLog *log = (ITLinkLog*)logRec;
    ITLinkData *body = &log->body;
    log->keyptr = (char*)item_get_key(it);

    struct lrec_common *cm = (struct lrec_common*)&body->cm;
    cm->ittype = GET_ITEM_TYPE(it);
    cm->keylen = it->nkey;
    cm->flags = it->flags;
    cm->exptime = it->exptime;

    struct lrec_val *val = (struct lrec_val*)&body->val;
    val->vallen = it->nbytes;
    val->cas = item_get_cas(it);

    log->header.logtype = LOG_IT_LINK;
    log->header.cmdtype = CMD_SET;
    log->header.body_length = offsetof(ITLinkData, data) + cm->keylen + val->vallen;
}

/* Collection Item Link Log Record */
static void lrec_coll_link_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_coll_link_print(LogRec *logRec)
{
}

void lrec_coll_link_record_for_snapshot(LogRec *logRec, hash_item *it)
{
}

/* Item Unlink Log Record */
static void lrec_it_unlink_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_it_unlink_print(LogRec *logRec)
{
}

void lrec_it_unlink_record(LogRec *logRec)
{
}

/* Item Arithmetic Log Record */
static void lrec_it_arithmetic_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_it_arithmetic_print(LogRec *logRec)
{
}

void lrec_it_arithmetic_record(LogRec *logRec)
{
}

/* Item Setattr Log Record */
static void lrec_it_setattr_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_it_setattr_print(LogRec *logRec)
{
}

void lrec_it_setattr_record(LogRec *logRec)
{
}

/* List Element Log Record */
static void lrec_list_elem_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_list_elem_print(LogRec *logRec)
{
}

void lrec_list_elem_record(LogRec *logRec)
{
}

/* List Element Delete Log Record */
static void lrec_list_elem_delete_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_list_elem_delete_print(LogRec *logRec)
{
}

void lrec_list_elem_delete_record(LogRec *logRec)
{
}

/* Set Element Log Record */
static void lrec_set_elem_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_set_elem_print(LogRec *logRec)
{
}

void lrec_set_elem_record(LogRec *logRec)
{
}

/* Set Element Delete Log Record */
static void lrec_set_elem_delete_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_set_elem_delete_print(LogRec *logRec)
{
}

void lrec_set_elem_delete_record(LogRec *logRec)
{
}

/* Map Element Log Record */
static void lrec_map_elem_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_map_elem_print(LogRec *logRec)
{
}

void lrec_map_elem_record(LogRec *logRec)
{
}

/* Map Element Delete Log Record */
static void lrec_map_elem_delete_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_map_elem_delete_print(LogRec *logRec)
{
}

void lrec_map_elem_delete_record(LogRec *logRec)
{
}

/* BTree Element Log Record */
static void lrec_bt_elem_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_bt_elem_print(LogRec *logRec)
{
}

void lrec_bt_elem_record(LogRec *logRec)
{
}

/* BTree Element Delete Log Record */
static void lrec_bt_elem_delete_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_bt_elem_delete_print(LogRec *logRec)
{
}

void lrec_bt_elem_delete_record(LogRec *logRec)
{
}

/* BTree Element Arithmetic Log Record */
static void lrec_bt_elem_arithmetic_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_bt_elem_arithmetic_print(LogRec *logRec)
{
}

void lrec_bt_elem_arithmetic_record(LogRec *logRec)
{
}

/* Snapshot Head Log Record */
static void lrec_snapshot_head_write(LogRec *logRec, char *bufptr)
{
}

static void lrec_snapshot_head_print(LogRec *logRec)
{
}

void lrec_snapshot_head_record(LogRec *logRec)
{
}

/* Snapshot Tail Log Record */
static void lrec_snapshot_tail_write(LogRec *logRec, char *bufptr)
{
    SnapshotTailLog *log = (void*)logRec;
    memcpy(bufptr, log, sizeof(SnapshotTailLog));
}

static void lrec_snapshot_tail_print(LogRec *logRec)
{
    LogHdr *hdr = &logRec->header;
    log_record_header_print(hdr);
}

void lrec_snapshot_tail_record(LogRec *logRec)
{
    SnapshotTailLog *log = (void*)logRec;
    log->header.logtype = LOG_SNAPSHOT_TAIL;
    log->header.cmdtype = CMD_NONE;
    log->header.body_length = 0;
}

/* Log Record Function */
typedef struct _logrec_func {
    void (*write)(LogRec *logRec, char *bufptr);
    void (*print)(LogRec *logRec);
} LOGREC_FUNC;

LOGREC_FUNC logrec_func[] = {
    { lrec_it_link_write,            lrec_it_link_print },
    { lrec_coll_link_write,          lrec_coll_link_print },
    { lrec_it_unlink_write,          lrec_it_unlink_print },
    { lrec_it_arithmetic_write,      lrec_it_arithmetic_print },
    { lrec_it_setattr_write,         lrec_it_setattr_print },
    { lrec_list_elem_write,          lrec_list_elem_print },
    { lrec_list_elem_delete_write,   lrec_list_elem_delete_print },
    { lrec_set_elem_write,           lrec_set_elem_print },
    { lrec_set_elem_delete_write,    lrec_set_elem_delete_print },
    { lrec_map_elem_write,           lrec_map_elem_print },
    { lrec_map_elem_delete_write,    lrec_map_elem_delete_print },
    { lrec_bt_elem_write,            lrec_bt_elem_print },
    { lrec_bt_elem_delete_write,     lrec_bt_elem_delete_print },
    { lrec_bt_elem_arithmetic_write, lrec_bt_elem_arithmetic_print },
    { lrec_snapshot_head_write,      lrec_snapshot_head_print },
    { lrec_snapshot_tail_write,      lrec_snapshot_tail_print }
};

void lrec_write(LogRec *logRec, char *bufptr)
{
    logrec_func[logRec->header.logtype].write(logRec, bufptr);
}

void lrec_print(LogRec *logRec)
{
    logrec_func[logRec->header.logtype].print(logRec);
}
#endif
