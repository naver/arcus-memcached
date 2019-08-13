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
#ifdef ENABLE_PERSISTENCE
#include "cmdlogrec.h"

#define DEBUG_PERSISTENCE_DISK_FORMAT_PRINT

#ifdef offsetof
#error "offsetof is already defined"
#endif
#define offsetof(type, member) __builtin_offsetof(type, member)

/* Get aligned size */
#define GET_8_ALIGN_SIZE(size) \
    (((size) % 8) == 0 ? (size) : ((size) + (8 - ((size) % 8))))

/* get bkey real size */
#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))

static char *get_logtype_text(uint8_t type)
{
    switch (type) {
        case LOG_IT_LINK:
            return "IT_LINK";
        case LOG_IT_UNLINK:
            return "IT_UNLINK";
        case LOG_IT_SETATTR:
            return "IT_SETATTR";
        case LOG_IT_FLUSH:
            return "IT_FLUSH";
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
        case UPD_DELETE:
            return "DELETE";
        case UPD_SETATTR_EXPTIME:
            return "SETATTR_EXPTIME";
        case UPD_SETATTR_EXPTIME_INFO:
            return "SETATTR_EXPTIME_INFO";
        case UPD_SETATTR_EXPTIME_INFO_BKEY:
            return "SETATTR_EXPTIME_BKEY";
        case UPD_FLUSH:
            return "UPD_FLUSH";
        case UPD_LIST_CREATE:
            return "LIST_CREATE";
        case UPD_SET_CREATE:
            return "SET_CREATE";
        case UPD_MAP_CREATE:
            return "MAP_CREATE";
        case UPD_BT_CREATE:
            return "BT_CREATE";
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

static uint8_t get_it_link_updtype(uint8_t type)
{
    switch (type) {
        case ITEM_TYPE_LIST:
            return UPD_LIST_CREATE;
        case ITEM_TYPE_SET:
            return UPD_SET_CREATE;
        case ITEM_TYPE_MAP:
            return UPD_MAP_CREATE;
        case ITEM_TYPE_BTREE:
            return UPD_BT_CREATE;
    }
    return UPD_SET;
}

static char *get_coll_ovflact_text(uint8_t ovflact)
{
    char *ovfarr[8] = { "null", "error", "head_trim", "tail_trim",
                        "smallest_trim", "largest_trim",
                        "smallest_silent_trim", "largest_silent_trim" };

    return ovfarr[ovflact];
}

static void lrec_header_print(LogHdr *hdr)
{
    fprintf(stderr, "\n[HEADER] body_length=%u | logtype=%s | updtype=%s\n",
            hdr->body_length, get_logtype_text(hdr->logtype), get_updtype_text(hdr->updtype));
}

static void lrec_bkey_print(uint8_t nbkey, unsigned char *bkey, char *str)
{
    if (nbkey != BKEY_NULL) {
        if (nbkey == 0) {
            sprintf(str, "len=%u val=%"PRIu64, nbkey, *(uint64_t*)bkey);
        } else {
            char bkey_temp[MAX_BKEY_LENG*2 + 2];
            safe_hexatostr(bkey, nbkey, bkey_temp);
            sprintf(str, "len=%u val=0x%s", nbkey, bkey_temp);
        }
    } else {
        sprintf(str, "len=BKEY_NULL val=0");
    }
}

/* Item Link Log Record */
/* KV :         header | lrec_item_common | cas            | key + value
 * Collection : header | lrec_item_common | lrec_coll_meta | key + value(\r\n)
 * BTree :      header | lrec_item_common | lrec_coll_meta | maxbkeyrange | key + value(\r\n)
 */
static void lrec_it_link_write(LogRec *logrec, char *bufptr)
{
    ITLinkLog  *log = (ITLinkLog*)logrec;
    ITLinkData *body = &log->body;
    struct lrec_item_common *cm = (struct lrec_item_common*)&body->cm;
    int offset = sizeof(LogHdr) + offsetof(ITLinkData, data);

    memcpy(bufptr, (void*)logrec, offset);

    if (cm->ittype == ITEM_TYPE_BTREE) {
        struct lrec_coll_meta *meta = (struct lrec_coll_meta*)&body->ptr.meta;
        if (meta->maxbkrlen != BKEY_NULL) {
            /* maxbkeyrange value copy */
            memcpy(bufptr + offset, log->maxbkrptr, BTREE_REAL_NBKEY(meta->maxbkrlen));
            offset += BTREE_REAL_NBKEY(meta->maxbkrlen);
        }
    }

    /* key value copy */
    memcpy(bufptr + offset, log->keyptr, cm->keylen + cm->vallen);
}

static void lrec_it_link_print(LogRec *logrec)
{
    ITLinkLog  *log  = (ITLinkLog*)logrec;
    ITLinkData *body = &log->body;
    struct lrec_item_common *cm = (struct lrec_item_common*)&body->cm;

    lrec_header_print(&log->header);

    char metastr[180];
    if (cm->ittype == ITEM_TYPE_KV) {
        sprintf(metastr, "cas=%"PRIu64, body->ptr.cas);
    } else {
        struct lrec_coll_meta *meta = (struct lrec_coll_meta*)&body->ptr.meta;
        int leng = sprintf(metastr, "ovflact=%s | mflags=%u | mcnt=%u",
                           get_coll_ovflact_text(meta->ovflact), meta->mflags, meta->mcnt);

        if (cm->ittype == ITEM_TYPE_BTREE) {
            leng += sprintf(metastr + leng, " | maxbkeyrange ");
            lrec_bkey_print(meta->maxbkrlen, log->maxbkrptr, metastr + leng);
        }
    }

    /* vallen >= 2, valstr = ...\r\n */
    fprintf(stderr, "[BODY]   ittype=%s | flags=%u | exptime=%u | %s | "
            "keylen=%u | keystr=%.*s | vallen=%u | valstr=%.*s",
            get_itemtype_text(cm->ittype), cm->flags, cm->exptime, metastr,
            cm->keylen, (cm->keylen <= 250 ? cm->keylen : 250), log->keyptr,
            cm->vallen, (cm->vallen <= 250 ? cm->vallen : 250), log->keyptr+cm->keylen);
}

/* Item Unlink Log Record */
static void lrec_it_unlink_write(LogRec *logrec, char *bufptr)
{
    ITUnlinkLog *log = (ITUnlinkLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(ITUnlinkData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
}

static void lrec_it_unlink_print(LogRec *logrec)
{
    ITUnlinkLog *log  = (ITUnlinkLog*)logrec;
    lrec_header_print(&log->header);

    fprintf(stderr, "[BODY]   keylen=%u | keystr=%.*s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr);
}

/* Item SetAttr Log Record */
/* UPD_SETATTR_EXPTIME           : header | body | key
 * UPD_SETATTR_EXPTIME_INFO      : header | body | key
 * UPD_SETATTR_EXPTIME_INFO_BKEY : header | body | maxbkeyrange | key
 */
static void lrec_it_setattr_write(LogRec *logrec, char *bufptr)
{
    ITSetAttrLog *log = (ITSetAttrLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(ITSetAttrData, data);

    memcpy(bufptr, (void*)logrec, offset);

    if (log->body.maxbkrlen != BKEY_NULL) {
        /* maxbkeyrange value copy */
        memcpy(bufptr + offset, log->maxbkrptr, BTREE_REAL_NBKEY(log->body.maxbkrlen));
        offset += BTREE_REAL_NBKEY(log->body.maxbkrlen);
    }

    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
}

static void lrec_it_setattr_print(LogRec *logrec)
{
    ITSetAttrLog *log  = (ITSetAttrLog*)logrec;
    lrec_header_print(&log->header);

    switch (log->header.updtype) {
      case UPD_SETATTR_EXPTIME:
      {
        fprintf(stderr, "[BODY]   exptime=%u | keylen=%u | keystr=%.*s\r\n",
                log->body.exptime, log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr);
      }
      break;
      case UPD_SETATTR_EXPTIME_INFO:
      case UPD_SETATTR_EXPTIME_INFO_BKEY:
      {
        char metastr[180];
        if (log->body.maxbkrlen != BKEY_NULL) {
            int leng = sprintf(metastr, "maxbkeyrange ");
            lrec_bkey_print(log->body.maxbkrlen, log->maxbkrptr, metastr + leng);
        } else {
            sprintf(metastr, "maxbkeyrange NULL");
        }

        fprintf(stderr, "[BODY]   ovflact=%s | mflags=%u | mcnt=%u | "
                "exptime=%u | %s | keylen=%u | keystr=%.*s\r\n",
                get_coll_ovflact_text(log->body.ovflact), log->body.mflags, log->body.mcnt,
                log->body.exptime, metastr, log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr);
      }
      break;
    }
}

/* Item Flush Log Record */
static void lrec_it_flush_write(LogRec *logrec, char *bufptr)
{
    ITFlushLog *log = (ITFlushLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(ITFlushData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* prefix copy
     * nprefix == 0 : null prefix, nprefix == 255 : all prefixes */
    if (log->body.nprefix > 0 && log->body.nprefix < 255) {
        memcpy(bufptr + offset, log->prefixptr, log->body.nprefix);
    }
}

static void lrec_it_flush_print(LogRec *logrec)
{
    ITFlushLog *log = (ITFlushLog*)logrec;
    lrec_header_print(&log->header);

    bool print_prefix = (log->body.nprefix > 0 && log->body.nprefix < 255 ? true : false);
    fprintf(stderr, "[BODY]   nprefix=%u | keystr=%.*s\r\n",
            log->body.nprefix, (print_prefix ? log->body.nprefix : 4),
            (print_prefix ? log->prefixptr : "NULL"));
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
    memcpy(bufptr, (void*)log, sizeof(SnapshotTailLog));
}

static void lrec_snapshot_tail_print(LogRec *logrec)
{
    LogHdr *hdr = &logrec->header;
    lrec_header_print(hdr);
}

/* Log Record Function */
typedef struct _logrec_func {
    void (*write)(LogRec *logrec, char *bufptr);
    void (*print)(LogRec *logrec);
} LOGREC_FUNC;

LOGREC_FUNC logrec_func[] = {
    { lrec_it_link_write,            lrec_it_link_print },
    { lrec_it_unlink_write,          lrec_it_unlink_print },
    { lrec_it_setattr_write,         lrec_it_setattr_print },
    { lrec_it_flush_write,           lrec_it_flush_print },
    { lrec_list_elem_insert_write,   lrec_list_elem_insert_print },
    { lrec_list_elem_delete_write,   lrec_list_elem_delete_print },
    { lrec_set_elem_insert_write,    lrec_set_elem_insert_print },
    { lrec_set_elem_delete_write,    lrec_set_elem_delete_print },
    { lrec_map_elem_insert_write,    lrec_map_elem_insert_print },
    { lrec_map_elem_delete_write,    lrec_map_elem_delete_print },
    { lrec_bt_elem_insert_write,     lrec_bt_elem_insert_print },
    { lrec_bt_elem_delete_write,     lrec_bt_elem_delete_print },
    { lrec_bt_elem_arithmetic_write, lrec_bt_elem_arithmetic_print },
    { lrec_snapshot_head_write,      lrec_snapshot_head_print },
    { lrec_snapshot_tail_write,      lrec_snapshot_tail_print }
};

/* external function */

void lrec_write_to_buffer(LogRec *logrec, char *bufptr)
{
    logrec_func[logrec->header.logtype].write(logrec, bufptr);
#ifdef DEBUG_PERSISTENCE_DISK_FORMAT_PRINT
    logrec_func[logrec->header.logtype].print(logrec);
#endif
}

/* Construct Log Record Functions */
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

int lrec_construct_link_item(LogRec *logrec, hash_item *it)
{
    ITLinkLog *log = (ITLinkLog*)logrec;
    ITLinkData *body = &log->body;
    log->keyptr = (char*)item_get_key(it);
    int naddition = 0;

    struct lrec_item_common *cm = (struct lrec_item_common*)&body->cm;
    cm->ittype  = GET_ITEM_TYPE(it);
    cm->keylen  = it->nkey;
    cm->vallen  = it->nbytes;
    cm->flags   = it->flags;
    cm->exptime = it->exptime;
    if (IS_COLL_ITEM(it)) {
        coll_meta_info *info = (coll_meta_info*)item_get_meta(it);
        struct lrec_coll_meta *meta = (struct lrec_coll_meta*)&body->ptr.meta;
        meta->ovflact = info->ovflact;
        meta->mflags  = info->mflags;
        meta->mcnt    = info->mcnt;
        if (IS_BTREE_ITEM(it)) {
            btree_meta_info *info = (btree_meta_info*)item_get_meta(it);
            meta->maxbkrlen       = info->maxbkeyrange.len;
            log->maxbkrptr        = info->maxbkeyrange.val;
        } else {
            meta->maxbkrlen = BKEY_NULL;
            log->maxbkrptr  = NULL;
        }
        if (meta->maxbkrlen != BKEY_NULL) {
            naddition = BTREE_REAL_NBKEY(meta->maxbkrlen);
        }
    } else {
        body->ptr.cas = item_get_cas(it);
    }

    log->header.logtype = LOG_IT_LINK;
    log->header.updtype = get_it_link_updtype(cm->ittype);
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(ITLinkData, data) +
                                               naddition + cm->keylen + cm->vallen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_unlink_item(LogRec *logrec, hash_item *it)
{
    ITUnlinkLog *log = (ITUnlinkLog*)logrec;

    log->body.keylen = it->nkey;
    log->keyptr = (char*)item_get_key(it);

    log->header.logtype = LOG_IT_UNLINK;
    log->header.updtype = UPD_DELETE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(ITUnlinkData, data) + log->body.keylen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_flush_item(LogRec *logrec, const char *prefix, int nprefix)
{
    ITFlushLog *log = (ITFlushLog*)logrec;

    /* nprefix == 0 : null prefix, nprefix < 0 : all prefix, nprefix > 0 : prefix */
    if (nprefix < 0) {
        nprefix = 0; /* real prefix length */
        log->body.nprefix = 255; /* flush_all semantic */
    } else {
        log->body.nprefix = (uint8_t)nprefix;
    }
    log->prefixptr = (char*)prefix;

    log->header.logtype = LOG_IT_FLUSH;
    log->header.updtype = UPD_FLUSH;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(ITFlushData, data) + nprefix);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_setattr(LogRec *logrec, hash_item *it, uint8_t updtype)
{
    ITSetAttrLog *log = (ITSetAttrLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->body.keylen = it->nkey;

    int naddition = 0;
    switch (updtype) {
      case UPD_SETATTR_EXPTIME:
      {
          log->body.exptime = it->exptime;
      }
      break;
      case UPD_SETATTR_EXPTIME_INFO:
      case UPD_SETATTR_EXPTIME_INFO_BKEY:
      {
          coll_meta_info *info = (coll_meta_info*)item_get_meta(it);
          log->body.exptime = it->exptime;
          log->body.ovflact = info->ovflact;
          log->body.mflags = info->mflags;
          log->body.mcnt = info->mcnt;
          if (updtype == UPD_SETATTR_EXPTIME_INFO_BKEY) {
              log->body.maxbkrlen = ((btree_meta_info*)info)->maxbkeyrange.len;
              log->maxbkrptr      = ((btree_meta_info*)info)->maxbkeyrange.val;
          } else {
              log->body.maxbkrlen = BKEY_NULL;
              log->maxbkrptr      = NULL;
          }
          if (log->body.maxbkrlen != BKEY_NULL) {
              naddition = BTREE_REAL_NBKEY(log->body.maxbkrlen);
          }
      }
      break;
    }

    log->header.logtype = LOG_IT_SETATTR;
    log->header.updtype = updtype;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(ITSetAttrData, data) +
                                               naddition + log->body.keylen);
    return log->header.body_length+sizeof(LogHdr);
}
#endif
