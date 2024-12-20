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
#include <assert.h>
#include <time.h>

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE
#include "cmdlogrec.h"

/* persistence meta data */
#define PERSISTENCE_ENGINE_NAME   "ARCUS-DEFAULT_ENGINE"
#define PERSISTENCE_MAJOR_VERSION 1
#define PERSISTENCE_MINOR_VERSION 0 /* backward compatibility */
//#define DEBUG_PERSISTENCE_DISK_FORMAT_PRINT

#ifdef offsetof
#error "offsetof is already defined"
#endif
#define offsetof(type, member) __builtin_offsetof(type, member)

#define EXPIRED_REL_EXPTIME(exptime) ((exptime) == 1)

/* Get aligned size */
#define GET_8_ALIGN_SIZE(size) \
    (((size) % 8) == 0 ? (size) : ((size) + (8 - ((size) % 8))))

/* get bkey real size */
#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))

/* global data */
static struct default_engine *engine = NULL;
static SERVER_CORE_API *svcore = NULL; /* server core api */
static EXTENSION_LOGGER_DESCRIPTOR *logger;

/* Convert server-start-relative time to absolute unix time */
static rel_time_t CONVERT_ABS_EXPTIME(rel_time_t exptime)
{
    if (exptime == 0 || exptime == (rel_time_t)(-1)) {
        return exptime; /* 0 (never), -1 (sticky) */
    }

    rel_time_t curtime = svcore->get_current_time();
    if (exptime > curtime) {
        return time(NULL) + (exptime - curtime);
    } else {
        /* expired. */
        return time(NULL);
    }
}

/* Convert absolute unix time to server-start-relative time */
static rel_time_t CONVERT_REL_EXPTIME(rel_time_t exptime)
{
    if (exptime == 0 || exptime == (rel_time_t)(-1)) {
        return exptime; /* 0 (never), -1 (sticky) */
    }

    rel_time_t abstime = time(NULL);
    if (exptime > abstime) {
        return svcore->get_current_time() + (exptime - abstime);
    } else {
        /* expired. */
        return 1;
    }
}

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
        case LOG_SNAPSHOT_ELEM:
            return "SNAPSHOT_ELEM";
        case LOG_LIST_ELEM_INSERT:
            return "LIST_ELEM_INSERT";
        case LOG_LIST_ELEM_DELETE:
            return "LIST_ELEM_DELETE";
        case LOG_SET_ELEM_INSERT:
            return "SET_ELEM_INSERT";
        case LOG_SET_ELEM_DELETE:
            return "SET_ELEM_DELETE";
        case LOG_MAP_ELEM_INSERT:
            return "MAP_ELEM_INSERT";
        case LOG_MAP_ELEM_DELETE:
            return "MAP_ELEM_DELETE";
        case LOG_BT_ELEM_INSERT:
            return "BT_ELEM_INSERT";
        case LOG_BT_ELEM_DELETE:
            return "BT_ELEM_DELETE";
        case LOG_BT_ELEM_DELETE_LOGICAL:
            return "BT_ELEM_DELETE_LOGICAL";
        case LOG_OPERATION_BEGIN:
            return "OPERATION_BEGIN";
        case LOG_OPERATION_END:
            return "OPERATION_END";
        case LOG_SNAPSHOT_DONE:
            return "SNAPSHOT_DONE";
    }
    return "unknown";
}

static char *get_updtype_text(uint8_t type)
{
    switch (type) {
        case UPD_STORE:
            return "STORE";
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
        case UPD_LIST_ELEM_INSERT:
            return "LIST_ELEM_INSERT";
        case UPD_LIST_ELEM_DELETE:
            return "LIST_ELEM_DELETE";
        case UPD_SET_CREATE:
            return "SET_CREATE";
        case UPD_SET_ELEM_INSERT:
            return "SET_ELEM_INSERT";
        case UPD_SET_ELEM_DELETE:
            return "SET_ELEM_DELETE";
        case UPD_MAP_CREATE:
            return "MAP_CREATE";
        case UPD_MAP_ELEM_INSERT:
            return "MAP_ELEM_INSERT";
        case UPD_MAP_ELEM_DELETE:
            return "MAP_ELEM_DELETE";
        case UPD_BT_CREATE:
            return "BT_CREATE";
        case UPD_BT_ELEM_INSERT:
            return "BT_ELEM_INSERT";
        case UPD_BT_ELEM_DELETE:
            return "BT_ELEM_DELETE";
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
    return UPD_STORE;
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
            sprintf(str, "(len=%u, val=%"PRIu64")", nbkey, *(uint64_t*)bkey);
        } else {
            char bkey_temp[MAX_BKEY_LENG*2 + 2];
            safe_hexatostr(bkey, nbkey, bkey_temp);
            sprintf(str, "(len=%u, val=0x%s)", nbkey, bkey_temp);
        }
    } else {
        sprintf(str, "NULL");
    }
}

static void lrec_eflag_print(uint8_t neflag, unsigned char *eflag, char *str)
{
    char eflag_temp[MAX_EFLAG_LENG*2 + 2];
    safe_hexatostr(eflag, neflag, eflag_temp);
    sprintf(str, "(len=%u, val=0x%s)", neflag, eflag_temp);
}

static void lrec_attr_print(char *data, char *str)
{
    lrec_attr_info info;
    /* because lrec_attr_info is not aligned, set info with memcpy */
    memcpy(&info, data, sizeof(lrec_attr_info));
    sprintf(str, "(flags=%u, exptime=%u, maxcount=%d, ovflaction=%u, readable=%d)",
            info.flags, CONVERT_REL_EXPTIME(info.exptime), info.maxcount, info.ovflaction,
            (info.mflags & COLL_META_FLAG_READABLE ? 1 : 0));
}

static inline void do_construct_item_attr(char *ptr, item_attr *attr)
{
    lrec_attr_info info;
    /* because lrec_attr_info is not aligned, set info with memcpy */
    memcpy(&info, ptr, sizeof(lrec_attr_info));
    attr->flags      = info.flags;
    attr->exptime    = CONVERT_REL_EXPTIME(info.exptime);
    attr->maxcount   = info.maxcount;
    attr->ovflaction = info.ovflaction;
    attr->readable   = (info.mflags & COLL_META_FLAG_READABLE ? 1 : 0);
    attr->trimmed    = 0;
}

static inline void do_construct_lrec_attr(hash_item *it, lrec_attr_info *attr)
{
    coll_meta_info *info = (coll_meta_info*)item_get_meta(it);
    attr->flags      = it->flags;
    attr->exptime    = CONVERT_ABS_EXPTIME(it->exptime);
    attr->maxcount   = info->mcnt;
    attr->ovflaction = info->ovflact;
    attr->mflags     = info->mflags;
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

static ENGINE_ERROR_CODE lrec_it_link_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret = ENGINE_FAILED;
    ITLinkLog  *log  = (ITLinkLog*)logrec;
    ITLinkData *body = &log->body;
    struct lrec_item_common cm = body->cm;
    char *keyptr = body->data;

    cm.exptime = CONVERT_REL_EXPTIME(cm.exptime);
    if (EXPIRED_REL_EXPTIME(cm.exptime)) {
        return ENGINE_SUCCESS;
    }

    if (cm.ittype == ITEM_TYPE_KV) {
        ret = item_apply_kv_link(engine, keyptr, cm.keylen, cm.flags, cm.exptime,
                                 cm.vallen, (keyptr + cm.keylen), body->ptr.cas);
    } else {
        struct lrec_coll_meta meta = body->ptr.meta;
        item_attr attr;
        attr.flags      = cm.flags;
        attr.exptime    = cm.exptime;
        attr.maxcount   = meta.mcnt;
        attr.ovflaction = meta.ovflact;
        attr.readable   = (meta.mflags & COLL_META_FLAG_READABLE ? 1 : 0);
        attr.trimmed    = (meta.mflags & COLL_META_FLAG_TRIMMED ? 1 : 0);
        if (cm.ittype == ITEM_TYPE_LIST) {
            ret = list_apply_item_link(engine, keyptr, cm.keylen, &attr);
        } else if (cm.ittype == ITEM_TYPE_SET) {
            ret = set_apply_item_link(engine, keyptr, cm.keylen, &attr);
        } else if (cm.ittype == ITEM_TYPE_MAP) {
            ret = map_apply_item_link(engine, keyptr, cm.keylen, &attr);
        } else if (cm.ittype == ITEM_TYPE_BTREE) {
            struct lrec_coll_meta meta = body->ptr.meta;
            attr.maxbkeyrange.len = meta.maxbkrlen;
            if (attr.maxbkeyrange.len != BKEY_NULL) {
                unsigned char *maxbkrptr = (unsigned char*)body->data;
                memcpy(attr.maxbkeyrange.val, maxbkrptr, BTREE_REAL_NBKEY(attr.maxbkeyrange.len));
                keyptr += BTREE_REAL_NBKEY(attr.maxbkeyrange.len);
            }
            ret = btree_apply_item_link(engine, keyptr, cm.keylen, &attr);
        }
    }

    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_link_redo failed.\n");
    }
    return ret;
}

static void lrec_it_link_print(LogRec *logrec)
{
    ITLinkLog  *log  = (ITLinkLog*)logrec;
    ITLinkData *body = &log->body;
    struct lrec_item_common *cm = (struct lrec_item_common*)&body->cm;
    char *keyptr = body->data;

    char metastr[180];
    if (cm->ittype == ITEM_TYPE_KV) {
        sprintf(metastr, "cas=%"PRIu64, body->ptr.cas);
    } else {
        struct lrec_coll_meta *meta = (struct lrec_coll_meta*)&body->ptr.meta;
        int leng = sprintf(metastr, "ovflact=%s | mflags=%u | mcnt=%d",
                           get_coll_ovflact_text(meta->ovflact), meta->mflags, meta->mcnt);

        if (cm->ittype == ITEM_TYPE_BTREE && meta->maxbkrlen != BKEY_NULL) {
            unsigned char *maxbkrptr = (unsigned char*)body->data;
            leng += sprintf(metastr + leng, " | maxbkeyrange=");
            lrec_bkey_print(meta->maxbkrlen, maxbkrptr, metastr + leng);
            keyptr += BTREE_REAL_NBKEY(meta->maxbkrlen);
        }
    }

    lrec_header_print(&log->header);
    /* vallen >= 2, valstr = ...\r\n */
    fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | ittype=%s | "
            "flags=%u | exptime=%u | %s | vallen=%u | valstr=%.*s\r\n",
            cm->keylen, (cm->keylen <= 250 ? cm->keylen : 250), keyptr,
            get_itemtype_text(cm->ittype), htonl(cm->flags), CONVERT_REL_EXPTIME(cm->exptime), metastr,
            cm->vallen, (cm->vallen-2 <= 250 ? cm->vallen-2 : 250), keyptr + cm->keylen);
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

static ENGINE_ERROR_CODE lrec_it_unlink_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    ITUnlinkLog  *log  = (ITUnlinkLog*)logrec;
    ITUnlinkData *body = &log->body;
    char *keyptr = body->data;

    ret = item_apply_unlink(engine, keyptr, body->keylen);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_unlink_redo failed.\n");
    }
    return ret;
}

static void lrec_it_unlink_print(LogRec *logrec)
{
    ITUnlinkLog *log = (ITUnlinkLog*)logrec;
    char *keyptr = log->body.data;

    lrec_header_print(&log->header);
    fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr);
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

static ENGINE_ERROR_CODE lrec_it_setattr_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    ITSetAttrLog  *log  = (ITSetAttrLog*)logrec;
    ITSetAttrData *body = &log->body;
    char *keyptr = body->data;

    body->exptime = CONVERT_REL_EXPTIME(body->exptime);
    if (log->header.updtype == UPD_SETATTR_EXPTIME) {
        ret = item_apply_setattr_exptime(engine, keyptr, body->keylen, body->exptime);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_setattr_redo failed.\n");
        }
    } else {
        bkey_t maxbkeyrange;
        bkey_t *maxbkrptr = NULL;
        if (body->maxbkrlen != BKEY_NULL) {
            maxbkeyrange.len = body->maxbkrlen;
            memcpy(maxbkeyrange.val, body->data, BTREE_REAL_NBKEY(body->maxbkrlen));
            maxbkrptr = &maxbkeyrange;
            keyptr += BTREE_REAL_NBKEY(body->maxbkrlen);
        }
        hash_item *it = item_get(keyptr, body->keylen);
        if (it) {
            ret = item_apply_setattr_collinfo(engine, it, body->exptime, body->mcnt,
                                              body->ovflact, body->mflags, maxbkrptr);
            if (ret != ENGINE_SUCCESS) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_setattr_redo failed.\n");
            }
            item_release(it);
        } else {
            ret = ENGINE_KEY_ENOENT;
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_setattr_redo failed. "
                        "not found. key=%.*s\n", body->keylen, keyptr);
        }
    }

    return ret;
}

static void lrec_it_setattr_print(LogRec *logrec)
{
    ITSetAttrLog *log = (ITSetAttrLog*)logrec;
    char *keyptr = log->body.data;

    lrec_header_print(&log->header);

    switch (log->header.updtype) {
      case UPD_SETATTR_EXPTIME:
      {
        fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | exptime=%u\r\n",
                log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr,
                CONVERT_REL_EXPTIME(log->body.exptime));
      }
      break;
      case UPD_SETATTR_EXPTIME_INFO:
      case UPD_SETATTR_EXPTIME_INFO_BKEY:
      {
        char metastr[180];
        if (log->body.maxbkrlen != BKEY_NULL) {
            unsigned char *maxbkrptr = (unsigned char*)log->body.data;
            lrec_bkey_print(log->body.maxbkrlen, maxbkrptr, metastr);
            keyptr += BTREE_REAL_NBKEY(log->body.maxbkrlen);
        } else {
            sprintf(metastr, "NULL");
        }

        fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | ovflact=%s | "
                "mflags=%u | mcnt=%d | exptime=%u | maxbkeyrange=%s\r\n",
                log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr,
                get_coll_ovflact_text(log->body.ovflact), log->body.mflags, log->body.mcnt,
                CONVERT_REL_EXPTIME(log->body.exptime), metastr);
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

static ENGINE_ERROR_CODE lrec_it_flush_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    ITFlushLog  *log  = (ITFlushLog*)logrec;
    ITFlushData *body = &log->body;
    char *prefixptr = body->data;

    if (body->nprefix == 255) {
        ret = item_apply_flush(engine, NULL, -1); /* flush_all */
    } else if (body->nprefix == 0) {
        ret = item_apply_flush(engine, NULL, 0); /* null prefix */
    } else {
        ret = item_apply_flush(engine, prefixptr, body->nprefix); /* flush specific prefix */
    }

    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_flush_redo failed.\n");
    }
    return ret;
}

static void lrec_it_flush_print(LogRec *logrec)
{
    ITFlushLog *log = (ITFlushLog*)logrec;
    char *prefixptr = log->body.data;
    bool print_prefix = (log->body.nprefix > 0 && log->body.nprefix < 255 ? true : false);

    lrec_header_print(&log->header);
    fprintf(stderr, "[BODY  ] nprefix=%u | prefixstr=%.*s\r\n",
            log->body.nprefix, (print_prefix ? log->body.nprefix : 4),
            (print_prefix ? prefixptr : "NULL"));
}

/* List Element Insert Log Record */
static void lrec_list_elem_insert_write(LogRec *logrec, char *bufptr)
{
    ListElemInsLog *log = (ListElemInsLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(ListElemInsData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    offset += log->body.keylen;
    /* value copy */
    memcpy(bufptr + offset, log->valptr, log->body.vallen);
    offset += log->body.vallen;
    /* attribute copy */
    if (log->body.create) {
        memcpy(bufptr + offset, log->attrp, sizeof(lrec_attr_info));
    }
}

static ENGINE_ERROR_CODE lrec_list_elem_insert_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret = ENGINE_FAILED;
    ListElemInsLog  *log  = (ListElemInsLog*)logrec;
    ListElemInsData *body = &log->body;
    char *keyptr = body->data;

    hash_item *it = item_get(keyptr, body->keylen);
    if (body->create) {
        if (it) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_list_elem_insert_redo failed. "
                                                     "already exist.\n");
            item_release(it);
            return ENGINE_KEY_EEXISTS;
        }

        /* create collection item */
        item_attr attr;
        do_construct_item_attr(keyptr + body->keylen + body->vallen, &attr);
        if (EXPIRED_REL_EXPTIME(attr.exptime)) {
            return ENGINE_SUCCESS;
        }
        ret = list_apply_item_link(engine, keyptr, body->keylen, &attr);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_list_elem_insert_redo failed. "
                                                     "item allocate failed.\n");
            return ret;
        }
        it = item_get(keyptr, body->keylen);
    }

    if (it) {
        ret = list_apply_elem_insert(engine, it, body->totcnt, body->eindex,
                                     (keyptr + body->keylen), body->vallen);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_list_elem_insert_redo failed.\n");
        }
        item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_list_elem_insert_redo failed. "
                    "not found. nkey=%d, key=%.*s\n", body->keylen, body->keylen, keyptr);
    }
    return ret;
}

static void lrec_list_elem_insert_print(LogRec *logrec)
{
    ListElemInsLog *log = (ListElemInsLog*)logrec;
    char *keyptr = log->body.data;
    char *valptr = keyptr + log->body.keylen;
    char *attrptr = valptr + log->body.vallen;

    char attrstr[180];
    if (log->body.create) {
        lrec_attr_print(attrptr, attrstr);
    } else {
        sprintf(attrstr, "NULL");
    }
    lrec_header_print(&log->header);
    /* vallen >= 2, valstr = ...\r\n */
    /* <key> <totcnt> <eindex> [create <attributes>] <data> */
    fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | "
            "totcnt=%u | eindex=%d | create=%s | vallen=%u | valstr=%.*s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr,
            log->body.totcnt, log->body.eindex, attrstr,
            log->body.vallen, (log->body.vallen-2 <= 250 ? log->body.vallen-2 : 250), valptr);
}

/* List Element Delete Log Record */
static void lrec_list_elem_delete_write(LogRec *logrec, char *bufptr)
{
    ListElemDelLog *log = (ListElemDelLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(ListElemDelData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
}

static ENGINE_ERROR_CODE lrec_list_elem_delete_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    ListElemDelLog  *log  = (ListElemDelLog*)logrec;
    ListElemDelData *body = &log->body;
    char *keyptr = body->data;

    hash_item *it = item_get(keyptr, body->keylen);
    if (it) {
        ret = list_apply_elem_delete(engine, it, body->totcnt, body->eindex, body->delcnt,
                                     body->drop);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_list_elem_delete_redo failed.\n");
        }
        item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_list_elem_delete_redo failed. "
                    "not found. key=%.*s\n", body->keylen, keyptr);
    }
    return ret;
}

static void lrec_list_elem_delete_print(LogRec *logrec)
{
    ListElemDelLog *log = (ListElemDelLog*)logrec;
    char *keyptr = log->body.data;

    lrec_header_print(&log->header);
    fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | totcnt=%u | eindex=%d | delcnt=%u | drop=%s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr,
            log->body.totcnt, log->body.eindex, log->body.delcnt, (log->body.drop ? "true" : "false"));
}

/* Set Element Insert Log Record */
static void lrec_set_elem_insert_write(LogRec *logrec, char *bufptr)
{
    SetElemInsLog *log = (SetElemInsLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(SetElemInsData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    offset += log->body.keylen;
    /* value copy */
    memcpy(bufptr + offset, log->valptr, log->body.vallen);
    offset += log->body.vallen;
    /* attribute copy */
    if (log->body.create) {
        memcpy(bufptr + offset, log->attrp, sizeof(lrec_attr_info));
    }
}

static ENGINE_ERROR_CODE lrec_set_elem_insert_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    SetElemInsLog  *log  = (SetElemInsLog*)logrec;
    SetElemInsData *body = &log->body;
    char *keyptr = body->data;
    char *valptr = keyptr + body->keylen;

    hash_item *it = item_get(keyptr, body->keylen);
    if (body->create) {
        if (it) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_set_elem_insert_redo failed. "
                                                     "already exist.\n");
            item_release(it);
            return ENGINE_KEY_EEXISTS;
        }

        /* create collection item */
        item_attr attr;
        do_construct_item_attr(valptr + body->vallen, &attr);
        if (EXPIRED_REL_EXPTIME(attr.exptime)) {
            return ENGINE_SUCCESS;
        }
        ret = set_apply_item_link(engine, keyptr, body->keylen, &attr);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_set_elem_insert_redo failed. "
                                                     "item allocate failed.\n");
            return ret;
        }
        it = item_get(keyptr, body->keylen);
    }

    if (it) {
        ret = set_apply_elem_insert(engine, it, valptr, body->vallen);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_set_elem_insert_redo failed.\n");
        }
        item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_set_elem_insert_redo failed. "
                    "not found. key=%.*s\n", body->keylen, keyptr);
    }
    return ret;
}

static void lrec_set_elem_insert_print(LogRec *logrec)
{
    SetElemInsLog *log = (SetElemInsLog*)logrec;
    char *keyptr = log->body.data;
    char *valptr = keyptr + log->body.keylen;
    char *attrptr = valptr + log->body.vallen;

    char attrstr[180];
    if (log->body.create) {
        lrec_attr_print(attrptr, attrstr);
    } else {
        sprintf(attrstr, "NULL");
    }
    lrec_header_print(&log->header);
    /* vallen >= 2, valstr = ...\r\n */
    /* <key> [create <attributes>] <data> */
    fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | create=%s | vallen=%u | valstr=%.*s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr, attrstr,
            log->body.vallen, (log->body.vallen-2 <= 250 ? log->body.vallen-2 : 250), valptr);
}

/* Set Element Delete Log Record */
static void lrec_set_elem_delete_write(LogRec *logrec, char *bufptr)
{
    SetElemDelLog *log = (SetElemDelLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(SetElemDelData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    /* value copy */
    memcpy(bufptr + offset + log->body.keylen, log->valptr, log->body.vallen);
}

static ENGINE_ERROR_CODE lrec_set_elem_delete_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    SetElemDelLog  *log  = (SetElemDelLog*)logrec;
    SetElemDelData *body = &log->body;
    char *keyptr = body->data;
    char *valptr = keyptr + body->keylen;

    hash_item *it = item_get(keyptr, body->keylen);
    if (it) {
        ret = set_apply_elem_delete(engine, it, valptr, body->vallen, body->drop);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_set_elem_delete_redo failed.\n");
        }
        item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_set_elem_delete_redo failed. "
                    "not found. key=%.*s\n", body->keylen, keyptr);
    }
    return ret;
}

static void lrec_set_elem_delete_print(LogRec *logrec)
{
    SetElemDelLog *log = (SetElemDelLog*)logrec;
    char *keyptr = log->body.data;
    char *valptr = keyptr + log->body.keylen;

    lrec_header_print(&log->header);
    /* vallen >= 2, valstr = ...\r\n */
    fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | drop=%s | vallen=%u | valstr=%.*s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr,
            (log->body.drop ? "true" : "false"),
            log->body.vallen, (log->body.vallen-2 <= 250 ? log->body.vallen-2 : 250), valptr);
}

/* Map Element Insert Log Record */
static void lrec_map_elem_insert_write(LogRec *logrec, char *bufptr)
{
    MapElemInsLog *log = (MapElemInsLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(MapElemInsData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    offset += log->body.keylen;
    /* field + value copy */
    memcpy(bufptr + offset, log->datptr, log->body.fldlen + log->body.vallen);
    offset += log->body.fldlen + log->body.vallen;
    /* attribute copy */
    if (log->body.create) {
        memcpy(bufptr + offset, log->attrp, sizeof(lrec_attr_info));
    }
}

static ENGINE_ERROR_CODE lrec_map_elem_insert_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    MapElemInsLog  *log  = (MapElemInsLog*)logrec;
    MapElemInsData *body = &log->body;
    char *keyptr = body->data;
    char *datptr = keyptr + body->keylen;

    hash_item *it = item_get(keyptr, body->keylen);
    if (body->create) {
        if (it) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_map_elem_insert_redo failed. "
                                                     "already exist.\n");
            item_release(it);
            return ENGINE_KEY_EEXISTS;
        }

        /* create collection item */
        item_attr attr;
        do_construct_item_attr(datptr + body->fldlen + body->vallen, &attr);
        if (EXPIRED_REL_EXPTIME(attr.exptime)) {
            return ENGINE_SUCCESS;
        }
        ret = map_apply_item_link(engine, keyptr, body->keylen, &attr);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_map_elem_insert_redo failed. "
                                                     "item allocate failed.\n");
            return ret;
        }
        it = item_get(keyptr, body->keylen);
    }

    if (it) {
        ret = map_apply_elem_insert(engine, it, datptr, body->fldlen, body->vallen);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_map_elem_insert_redo failed.\n");
        }
        item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_map_elem_insert_redo failed. "
                    "not found. key=%.*s\n", body->keylen, keyptr);
    }
    return ret;
}

static void lrec_map_elem_insert_print(LogRec *logrec)
{
    MapElemInsLog *log = (MapElemInsLog*)logrec;
    char *keyptr = log->body.data;
    char *fldptr = keyptr + log->body.keylen;
    char *valptr = fldptr + log->body.fldlen;
    char *attrptr = valptr + log->body.vallen;

    char attrstr[180];
    if (log->body.create) {
        lrec_attr_print(attrptr, attrstr);
    } else {
        sprintf(attrstr, "NULL");
    }
    lrec_header_print(&log->header);
    /* vallen >= 2, valstr = ...\r\n */
    /* <key> <field> [create <attributes>] <data> */
    fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | fldlen=%u | fldstr=%.*s | "
            "create=%s | vallen=%u | valstr=%.*s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr,
            log->body.fldlen, log->body.fldlen, fldptr, attrstr,
            log->body.vallen, (log->body.vallen-2 <= 250 ? log->body.vallen-2 : 250), valptr);
}

/* Map Element Delete Log Record */
static void lrec_map_elem_delete_write(LogRec *logrec, char *bufptr)
{
    MapElemDelLog *log = (MapElemDelLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(MapElemDelData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    /* field copy */
    memcpy(bufptr + offset + log->body.keylen, log->datptr, log->body.fldlen);
}

static ENGINE_ERROR_CODE lrec_map_elem_delete_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    MapElemDelLog  *log  = (MapElemDelLog*)logrec;
    MapElemDelData *body = &log->body;
    char *keyptr = body->data;
    char *fldptr = keyptr + body->keylen;

    hash_item *it = item_get(keyptr, body->keylen);
    if (it) {
        ret = map_apply_elem_delete(engine, it, fldptr, body->fldlen, body->drop);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_map_elem_delete_redo failed.\n");
        }
        item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_map_elem_delete_redo failed. "
                    "not found. key=%.*s\n", body->keylen, keyptr);
    }
    return ret;
}

static void lrec_map_elem_delete_print(LogRec *logrec)
{
    MapElemDelLog *log = (MapElemDelLog*)logrec;
    char *keyptr = log->body.data;
    char *fldptr = keyptr + log->body.keylen;

    lrec_header_print(&log->header);
    fprintf(stderr, "[BODY]   keylen=%u | keystr=%.*s | fldlen=%u | fldstr=%.*s | drop=%s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr,
            log->body.fldlen, log->body.fldlen, fldptr, (log->body.drop ? "true" : "false"));
}

/* BTree Element Insert Log Record */
static void lrec_bt_elem_insert_write(LogRec *logrec, char *bufptr)
{
    BtreeElemInsLog *log = (BtreeElemInsLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(BtreeElemInsData, data);
    int datlen = BTREE_REAL_NBKEY(log->body.nbkey) + log->body.neflag + log->body.vallen;

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    offset += log->body.keylen;
    /* bkey | eflag | value copy */
    memcpy(bufptr + offset, log->datptr, datlen);
    offset += datlen;
    /* attribute copy */
    if (log->body.create) {
        memcpy(bufptr + offset, log->attrp, sizeof(lrec_attr_info));
    }
}

static ENGINE_ERROR_CODE lrec_bt_elem_insert_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    BtreeElemInsLog  *log  = (BtreeElemInsLog*)logrec;
    BtreeElemInsData *body = &log->body;
    char *keyptr = body->data;
    char *datptr = keyptr + body->keylen;

    hash_item *it = item_get(keyptr, body->keylen);
    if (body->create) {
        if (it) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_insert_redo failed. "
                                                     "already exist.\n");
            item_release(it);
            return ENGINE_KEY_EEXISTS;
        }

        /* create collection item */
        item_attr attr;
        int datlen = BTREE_REAL_NBKEY(log->body.nbkey) + log->body.neflag + log->body.vallen;
        do_construct_item_attr(datptr + datlen, &attr);
        if (EXPIRED_REL_EXPTIME(attr.exptime)) {
            return ENGINE_SUCCESS;
        }
        attr.maxbkeyrange.len = BKEY_NULL;
        ret = btree_apply_item_link(engine, keyptr, body->keylen, &attr);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_insert_redo failed. "
                                                     "item allocate failed.\n");
            return ret;
        }
        it = item_get(keyptr, body->keylen);
    }

    if (it) {
        ret = btree_apply_elem_insert(engine, it, datptr, body->nbkey, body->neflag, body->vallen);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_insert_redo failed.\n");
        }
        item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_insert_redo failed. "
                    "not found. key=%.*s\n", body->keylen, keyptr);
    }
    return ret;
}

static void lrec_bt_elem_insert_print(LogRec *logrec)
{
    BtreeElemInsLog *log = (BtreeElemInsLog*)logrec;
    char *keyptr = log->body.data;
    uint64_t real_nbkey = BTREE_REAL_NBKEY(log->body.nbkey);
    unsigned char *bkeyptr = (unsigned char*)(keyptr + log->body.keylen);
    unsigned char *eflagptr = bkeyptr + real_nbkey;
    char *attrptr = (char*)(eflagptr + log->body.neflag + log->body.vallen);

    char bkeystr[90];
    char eflagstr[90];
    char attrstr[180];
    lrec_bkey_print(log->body.nbkey, bkeyptr, bkeystr);
    if (log->body.neflag > 0) {
        lrec_eflag_print(log->body.neflag, eflagptr, eflagstr);
    } else {
        sprintf(eflagstr, "NULL");
    }
    if (log->body.create) {
        lrec_attr_print(attrptr, attrstr);
    } else {
        sprintf(attrstr, "NULL");
    }
    lrec_header_print(&log->header);
    /* vallen >= 2, valstr = ...\r\n */
    /* <key> <bkey> [<eflag>] [create <attributes>] <data> */
    fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | "
            "bkey=%s | eflag=%s | create=%s | vallen=%u | valstr=%.*s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr,
            bkeystr, eflagstr, attrstr,
            log->body.vallen, (log->body.vallen-2 <= 250 ? log->body.vallen-2 : 250),
            bkeyptr + real_nbkey + log->body.neflag);
}

/* BTree Element Delete Log Record */
static void lrec_bt_elem_delete_write(LogRec *logrec, char *bufptr)
{
    BtreeElemDelLog *log = (BtreeElemDelLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(BtreeElemDelData, data);
    int datlen = BTREE_REAL_NBKEY(log->body.nbkey);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    /* bkey copy */
    memcpy(bufptr + offset + log->body.keylen, log->datptr, datlen);
}

static ENGINE_ERROR_CODE lrec_bt_elem_delete_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    BtreeElemDelLog  *log  = (BtreeElemDelLog*)logrec;
    BtreeElemDelData *body = &log->body;
    char *keyptr  = body->data;
    char *bkeyptr = keyptr + body->keylen;

    hash_item *it = item_get(keyptr, body->keylen);
    if (it) {
        ret = btree_apply_elem_delete(engine, it, bkeyptr, body->nbkey, body->drop);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_delete_redo failed.\n");
        }
        item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_delete_redo failed. "
                    "not found. key=%.*s\n", body->keylen, keyptr);
    }
    return ret;
}

static void lrec_bt_elem_delete_print(LogRec *logrec)
{
    BtreeElemDelLog *log = (BtreeElemDelLog*)logrec;
    char *keyptr = log->body.data;
    unsigned char *bkeyptr = (unsigned char*)(keyptr + log->body.keylen);

    char bkeystr[90];
    lrec_bkey_print(log->body.nbkey, bkeyptr, bkeystr);
    lrec_header_print(&log->header);
    fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | bkey=%s | drop=%s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr,
            bkeystr, (log->body.drop ? "true" : "false"));
}

/* BTree Element Delete Logical Log Record */
static void lrec_bt_elem_delete_logical_write(LogRec *logrec, char *bufptr)
{
    BtreeElemDelLgcLog *log = (BtreeElemDelLgcLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(BtreeElemDelLgcData, data);
    int real_from_nbkey = BTREE_REAL_NBKEY(log->body.from_nbkey);
    int real_to_nbkey = BTREE_REAL_NBKEY(log->body.to_nbkey);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    offset += log->body.keylen;
    /* from bkey copy */
    memcpy(bufptr + offset, log->bkrangep->from_bkey, real_from_nbkey);
    offset += real_from_nbkey;
    /* to bkey copy */
    if (real_to_nbkey != BKEY_NULL) {
        memcpy(bufptr + offset, log->bkrangep->to_bkey, real_to_nbkey);
        offset += real_to_nbkey;
    }
    /* eflag filter copy */
    if (log->body.filtering) {
        if (log->body.nbitwval > 0) {
            /* bitwval */
            memcpy(bufptr + offset, log->efilterp->bitwval, log->body.nbitwval);
            offset += log->body.nbitwval;
        }
        /* compval */
        memcpy(bufptr + offset, log->efilterp->compval, log->body.compvcnt*log->body.ncompval);
    }
}

static ENGINE_ERROR_CODE lrec_bt_elem_delete_logical_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    BtreeElemDelLgcLog  *log  = (BtreeElemDelLgcLog*)logrec;
    BtreeElemDelLgcData *body = &log->body;
    char *keyptr  = body->data;
    int  real_from_nbkey = BTREE_REAL_NBKEY(body->from_nbkey);
    int  real_to_nbkey = BTREE_REAL_NBKEY(body->to_nbkey);
    char *from_bkeyptr = keyptr + body->keylen;
    char *to_bkeyptr = from_bkeyptr + real_from_nbkey;

    /* element range */
    bkey_range bkrange;
    memcpy(bkrange.from_bkey, from_bkeyptr, real_from_nbkey);
    bkrange.from_nbkey = body->from_nbkey;
    if (real_to_nbkey != BKEY_NULL) {
        memcpy(bkrange.to_bkey, to_bkeyptr, real_to_nbkey);
    }
    bkrange.to_nbkey = body->to_nbkey;

    /* efilter */
    eflag_filter efilter;
    if (body->filtering) {
        char *bitwvalptr = to_bkeyptr + (real_to_nbkey == BKEY_NULL ? 0 : real_to_nbkey);
        char *compvalptr = bitwvalptr + body->nbitwval;

        memcpy(efilter.bitwval, bitwvalptr, body->nbitwval);
        efilter.nbitwval = body->nbitwval;
        memcpy(efilter.compval, compvalptr, body->compvcnt*body->ncompval);
        efilter.ncompval = body->ncompval;
        efilter.compvcnt = body->compvcnt;
        efilter.offset   = body->f_offset;
        efilter.bitwop   = body->bitwop;
        efilter.compop   = body->compop;
    }

    hash_item *it = item_get(keyptr, body->keylen);
    if (it) {
        ret = btree_apply_elem_delete_logical(engine, it, &bkrange,
                                              (body->filtering ? &efilter : NULL),
                                              body->offset, body->reqcount, body->drop);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_delete_logical_redo failed.\n");
        }
        item_release(it);
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_delete_logical_redo failed. "
                    "not found. key=%.*s\n", body->keylen, keyptr);
    }
    return ret;
}

static void lrec_bt_elem_delete_logical_print(LogRec *logrec)
{
    BtreeElemDelLgcLog *log = (BtreeElemDelLgcLog*)logrec;
    char *keyptr = log->body.data;
    char *fbkeyptr = keyptr + log->body.keylen;
    char *tbkeyptr = fbkeyptr + BTREE_REAL_NBKEY(log->body.from_nbkey);

    char fbkeystr[90];
    char tbkeystr[90];
    lrec_bkey_print(log->body.from_nbkey, (unsigned char *)fbkeyptr, fbkeystr);
    lrec_bkey_print(log->body.to_nbkey, (unsigned char*)tbkeyptr, tbkeystr);

    int efilterlen = MAX_EFLAG_LENG*2 + (MAX_EFLAG_LENG*2+3)*MAX_EFLAG_COMPARE_COUNT + 100;
    char efilterstr[efilterlen];
    if (log->body.filtering) {
        bool single_bkey = (BTREE_REAL_NBKEY(log->body.to_nbkey) == BKEY_NULL ? true : false);
        char *bitwvalptr = tbkeyptr + (single_bkey ? 0 : BTREE_REAL_NBKEY(log->body.to_nbkey));
        char *compvalptr = bitwvalptr + log->body.nbitwval;
        char bitwvalstr[MAX_EFLAG_LENG*2];
        char compvalstr[(MAX_EFLAG_LENG*2+3)*MAX_EFLAG_COMPARE_COUNT];

        /* bitwval string prepare */
        if (log->body.nbitwval > 0) {
            safe_hexatostr((const unsigned char*)bitwvalptr, log->body.nbitwval, bitwvalstr);
        }

        /* compval string prepare */
        char *tmpptr = &compvalstr[0];
        char *tmpstr;
        for (int i = 0; i < log->body.compvcnt; i++) {
            tmpstr = &compvalptr[i*log->body.ncompval];
            safe_hexatostr((const unsigned char*)tmpstr, log->body.ncompval, tmpptr);
            tmpptr += strlen(tmpptr);
            if (i + 1 < log->body.compvcnt) {
                memcpy(tmpptr, ",0x", 3); tmpptr += 3;
            }
        }
        sprintf(efilterstr, "(f_offset=%u, bitwop=%u, bitwval=0x%s, compop=%u, compval(count=%u)=0x%s)",
                log->body.f_offset, log->body.bitwop, (log->body.nbitwval > 0 ? bitwvalstr : "NULL"),
                log->body.compop, log->body.compvcnt, compvalstr);
    } else {
        sprintf(efilterstr, "NULL");
    }
    lrec_header_print(&log->header);
    /* <key> <drop> <bkrange> [<efilter>] */
    fprintf(stderr, "[BODY  ] keylen=%u | keystr=%.*s | drop=%s | offset=%u | reqcount=%u |\r\n"
                    "         bkrange=(from_bkey=%s, to_bkey=%s)\r\n"
                    "         efilter=%s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), keyptr, (log->body.drop ? "true" : "false"),
            log->body.offset, log->body.reqcount, fbkeystr, tbkeystr, efilterstr);
}

/* Operation Begin Log Record */
static void lrec_operation_begin_write(LogRec *logrec, char *bufptr)
{
    OperationRangeLog *log = (OperationRangeLog*)logrec;
    memcpy(bufptr, (void*)log, sizeof(LogHdr));
}

static void lrec_operation_begin_print(LogRec *logrec)
{
    LogHdr *hdr = &logrec->header;
    lrec_header_print(hdr);
}

/* Opertaion End Log Record */
static void lrec_operation_end_write(LogRec *logrec, char *bufptr)
{
    OperationRangeLog *log = (OperationRangeLog*)logrec;
    memcpy(bufptr, (void*)log, sizeof(LogHdr));
}

static void lrec_operation_end_print(LogRec *logrec)
{
    LogHdr *hdr = &logrec->header;
    lrec_header_print(hdr);
}

/* Snapshot Element Log Record */
static void lrec_snapshot_elem_link_write(LogRec *logrec, char *bufptr)
{
    SnapshotElemLog  *log  = (SnapshotElemLog*)logrec;
    SnapshotElemData *body = &log->body;
    int offset = sizeof(LogHdr) + offsetof(SnapshotElemData, data);

    memcpy(bufptr, logrec, offset);
    if (log->header.updtype == UPD_MAP_ELEM_INSERT) {
        /* field, value copy */
        memcpy(bufptr + offset, log->valptr, body->nekey + body->nbytes);
    } else if (log->header.updtype == UPD_BT_ELEM_INSERT) {
        /* bkey, <eflag>, value copy */
        memcpy(bufptr + offset, log->valptr, BTREE_REAL_NBKEY(body->nekey) + body->neflag + body->nbytes);
    } else {
        /* value copy */
        memcpy(bufptr + offset, log->valptr, body->nbytes);
    }
}

static ENGINE_ERROR_CODE lrec_snapshot_elem_link_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret = ENGINE_FAILED;
    SnapshotElemLog  *log  = (SnapshotElemLog*)logrec;
    SnapshotElemData *body = &log->body;
    char *valptr = body->data;

    if (IS_LIST_ITEM(log->it)) {
        ret = list_apply_elem_insert(engine, log->it, -1, -1, valptr, body->nbytes);
    } else if (IS_SET_ITEM(log->it)) {
        ret = set_apply_elem_insert(engine, log->it, valptr, body->nbytes);
    } else if (IS_MAP_ITEM(log->it)) {
        ret = map_apply_elem_insert(engine, log->it, valptr, body->nekey, body->nbytes);
    } else if (IS_BTREE_ITEM(log->it)) {
        ret = btree_apply_elem_insert(engine, log->it, valptr, body->nekey, body->neflag, body->nbytes);
    }

    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_snapshot_elem_link_redo failed.\n");
    }
    return ret;
}

static void lrec_snapshot_elem_link_print(LogRec *logrec)
{
    SnapshotElemLog  *log  = (SnapshotElemLog*)logrec;
    SnapshotElemData *body = &log->body;
    char *valptr = body->data;

    lrec_header_print(&log->header);
    /* vallen >= 2, valstr = ...\r\n */
    if (log->header.updtype == UPD_MAP_ELEM_INSERT) {
        fprintf(stderr, "[BODY  ] nfield=%u | field=%.*s | vallen=%u | value=%.*s\r\n",
                body->nekey, body->nekey, valptr,
                body->nbytes, (body->nbytes-2 <= 250 ? body->nbytes-2 : 250), (valptr + body->nekey));
    } else if (log->header.updtype == UPD_BT_ELEM_INSERT) {
        char bkeystr[90];
        char eflagstr[90];
        unsigned char *bkeyptr = (unsigned char*)valptr;
        unsigned char *eflagptr = (unsigned char*)(valptr + BTREE_REAL_NBKEY(body->nekey));
        lrec_bkey_print(body->nekey, bkeyptr, bkeystr);
        if (log->body.neflag > 0) {
            lrec_eflag_print(log->body.neflag, eflagptr, eflagstr);
        } else {
            sprintf(eflagstr, "NULL");
        }

        /* <bkey> [<eflag>] <bytes> <data> */
        fprintf(stderr, "[BODY  ] bkey=%s | eflag=%s | vallen=%u | value=%.*s\r\n",
                bkeystr, eflagstr, body->nbytes,
                (body->nbytes-2 <= 250 ? body->nbytes-2 : 250),
                (valptr + BTREE_REAL_NBKEY(body->nekey) + body->neflag));
    } else {
        fprintf(stderr, "[BODY  ] vallen=%u | value=%.*s\r\n",
                body->nbytes, (body->nbytes-2 <= 250 ? body->nbytes-2 : 250), valptr);
    }
}

/* Snapshot Done Log Record */
static void lrec_snapshot_done_write(LogRec *logrec, char *bufptr)
{
    SnapshotDoneLog *log = (SnapshotDoneLog*)logrec;
    memcpy(bufptr, (void*)log, sizeof(LogHdr) + log->header.body_length);
}

static void lrec_snapshot_done_print(LogRec *logrec)
{
    SnapshotDoneLog  *log  = (SnapshotDoneLog*)logrec;
    SnapshotDoneData *body = &log->body;

    lrec_header_print(&log->header);
    fprintf(stderr, "[BODY  ] engine_name=%s | persistence_major_version=%u | "
            "persistence_minor_version=%u\r\n",
            body->engine_name, body->persistence_major_version, body->persistence_minor_version);
}

/* Log Record Function */
typedef struct _logrec_func {
    void (*write)(LogRec *logrec, char *bufptr);
    ENGINE_ERROR_CODE (*redo)(LogRec *logrec);
    void (*print)(LogRec *logrec);
} LOGREC_FUNC;

LOGREC_FUNC logrec_func[] = {
    { lrec_it_link_write,                lrec_it_link_redo,                lrec_it_link_print },
    { lrec_it_unlink_write,              lrec_it_unlink_redo,              lrec_it_unlink_print },
    { lrec_it_setattr_write,             lrec_it_setattr_redo,             lrec_it_setattr_print },
    { lrec_it_flush_write,               lrec_it_flush_redo,               lrec_it_flush_print },
    { lrec_list_elem_insert_write,       lrec_list_elem_insert_redo,       lrec_list_elem_insert_print },
    { lrec_list_elem_delete_write,       lrec_list_elem_delete_redo,       lrec_list_elem_delete_print },
    { lrec_set_elem_insert_write,        lrec_set_elem_insert_redo,        lrec_set_elem_insert_print },
    { lrec_set_elem_delete_write,        lrec_set_elem_delete_redo,        lrec_set_elem_delete_print },
    { lrec_map_elem_insert_write,        lrec_map_elem_insert_redo,        lrec_map_elem_insert_print },
    { lrec_map_elem_delete_write,        lrec_map_elem_delete_redo,        lrec_map_elem_delete_print },
    { lrec_bt_elem_insert_write,         lrec_bt_elem_insert_redo,         lrec_bt_elem_insert_print },
    { lrec_bt_elem_delete_write,         lrec_bt_elem_delete_redo,         lrec_bt_elem_delete_print },
    { lrec_bt_elem_delete_logical_write, lrec_bt_elem_delete_logical_redo, lrec_bt_elem_delete_logical_print },
    { lrec_operation_begin_write,        NULL,                             lrec_operation_begin_print },
    { lrec_operation_end_write,          NULL,                             lrec_operation_end_print },
    { lrec_snapshot_elem_link_write,     lrec_snapshot_elem_link_redo,     lrec_snapshot_elem_link_print },
    { lrec_snapshot_done_write,          NULL,                             lrec_snapshot_done_print }
};

/* external function */

void cmdlog_rec_init(struct default_engine *engine_ptr)
{
    engine = engine_ptr;
    logger = engine->server.log->get_logger();
    svcore = engine->server.core;
    logger->log(EXTENSION_LOG_INFO, NULL, "COMMNAD LOG RECORD module initialized.\n");
}

void lrec_write_to_buffer(LogRec *logrec, char *bufptr)
{
    logrec_func[logrec->header.logtype].write(logrec, bufptr);
#ifdef DEBUG_PERSISTENCE_DISK_FORMAT_PRINT
    logrec_func[logrec->header.logtype].print((LogRec*)bufptr);
#endif
}

ENGINE_ERROR_CODE lrec_redo_from_record(LogRec *logrec)
{
#ifdef DEBUG_PERSISTENCE_DISK_FORMAT_PRINT
    logrec_func[logrec->header.logtype].print(logrec);
#endif
    if (logrec_func[logrec->header.logtype].redo != NULL) {
        return logrec_func[logrec->header.logtype].redo(logrec);
    }
    /* exist some log records don't need to redo (ex. SnapshotDoneLog). */
    return ENGINE_SUCCESS;
}

/* Construct Log Record Functions */
int lrec_construct_snapshot_done(LogRec *logrec)
{
    SnapshotDoneLog  *log  = (SnapshotDoneLog*)logrec;
    SnapshotDoneData *body = &log->body;

    assert(sizeof(body->engine_name) > strlen(PERSISTENCE_ENGINE_NAME));
    memset(body->engine_name, 0, sizeof(body->engine_name));
    memcpy(body->engine_name, PERSISTENCE_ENGINE_NAME, strlen(PERSISTENCE_ENGINE_NAME));
    body->persistence_major_version = PERSISTENCE_MAJOR_VERSION;
    body->persistence_minor_version = PERSISTENCE_MINOR_VERSION;

    log->header.logtype = LOG_SNAPSHOT_DONE;
    log->header.updtype = UPD_NONE;
    log->header.body_length = GET_8_ALIGN_SIZE(sizeof(SnapshotDoneData));
    return log->header.body_length+sizeof(LogHdr);
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
    cm->exptime = CONVERT_ABS_EXPTIME(it->exptime);
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
    log->body.maxbkrlen = BKEY_NULL;

    int naddition = 0;
    switch (updtype) {
      case UPD_SETATTR_EXPTIME:
      {
          log->body.exptime = CONVERT_ABS_EXPTIME(it->exptime);
      }
      break;
      case UPD_SETATTR_EXPTIME_INFO:
      case UPD_SETATTR_EXPTIME_INFO_BKEY:
      {
          coll_meta_info *info = (coll_meta_info*)item_get_meta(it);
          log->body.exptime = CONVERT_ABS_EXPTIME(it->exptime);
          log->body.ovflact = info->ovflact;
          log->body.mflags = info->mflags;
          log->body.mcnt = info->mcnt;
          if (updtype == UPD_SETATTR_EXPTIME_INFO_BKEY) {
              log->body.maxbkrlen = ((btree_meta_info*)info)->maxbkeyrange.len;
              log->maxbkrptr      = ((btree_meta_info*)info)->maxbkeyrange.val;
              if (log->body.maxbkrlen != BKEY_NULL) {
                  naddition = BTREE_REAL_NBKEY(log->body.maxbkrlen);
              }
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

int lrec_construct_snapshot_elem(LogRec *logrec, hash_item *it, void *elem)
{
    SnapshotElemLog  *log   = (SnapshotElemLog*)logrec;
    SnapshotElemData *body  = &log->body;
    uint8_t updtype = UPD_NONE;
    int bodylen = offsetof(SnapshotElemData, data);

    if (IS_LIST_ITEM(it)) {
        list_elem_item *e = (list_elem_item*)elem;
        body->nbytes      = e->nbytes;

        bodylen += body->nbytes;
        log->valptr = e->value;
        updtype = UPD_LIST_ELEM_INSERT;
    } else if (IS_SET_ITEM(it)) {
        set_elem_item *e = (set_elem_item*)elem;
        body->nbytes     = e->nbytes;

        bodylen += body->nbytes;
        log->valptr = e->value;
        updtype = UPD_SET_ELEM_INSERT;
    } else if (IS_MAP_ITEM(it)) {
        map_elem_item *e = (map_elem_item*)elem;
        body->nbytes     = e->nbytes;
        body->nekey      = e->nfield;

        bodylen += body->nbytes + body->nekey;
        log->valptr = (char*)e->data;
        updtype = UPD_MAP_ELEM_INSERT;
    } else if (IS_BTREE_ITEM(it)) {
        btree_elem_item *e = (btree_elem_item*)elem;
        body->nbytes       = e->nbytes;
        body->nekey        = e->nbkey;
        body->neflag       = e->neflag;

        bodylen += body->nbytes + BTREE_REAL_NBKEY(body->nekey) + body->neflag;
        log->valptr = (char*)e->data;
        updtype = UPD_BT_ELEM_INSERT;
    }

    log->header.logtype = LOG_SNAPSHOT_ELEM;
    log->header.updtype = updtype;
    log->header.body_length = GET_8_ALIGN_SIZE(bodylen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_list_elem_insert(LogRec *logrec, hash_item *it,
                                    uint32_t totcnt, int eindex, list_elem_item *elem,
                                    bool create, lrec_attr_info *attr)
{
    ListElemInsLog *log = (ListElemInsLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->valptr = (char*)elem->value;
    log->body.keylen = it->nkey;
    log->body.vallen = elem->nbytes;
    log->body.totcnt = totcnt;
    log->body.eindex = eindex;
    log->body.create = create;
    if (log->body.create) {
        do_construct_lrec_attr(it, attr);
        log->attrp = attr;
    }

    log->header.logtype = LOG_LIST_ELEM_INSERT;
    log->header.updtype = UPD_LIST_ELEM_INSERT;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(ListElemInsData, data)
                                               + log->body.keylen + log->body.vallen)
                                               + (log->body.create ? sizeof(lrec_attr_info) : 0);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_list_elem_delete(LogRec *logrec, hash_item *it,
                                    uint32_t totcnt, int eindex, uint32_t delcnt,
                                    bool drop)
{
    ListElemDelLog *log = (ListElemDelLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->body.keylen = it->nkey;
    log->body.drop   = drop;
    log->body.totcnt = totcnt;
    log->body.eindex = eindex;
    log->body.delcnt = delcnt;

    log->header.logtype = LOG_LIST_ELEM_DELETE;
    log->header.updtype = UPD_LIST_ELEM_DELETE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(ListElemDelData, data) + log->body.keylen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_map_elem_insert(LogRec *logrec, hash_item *it, map_elem_item *elem,
                                   bool create, lrec_attr_info *attr)
{
    MapElemInsLog *log = (MapElemInsLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->datptr = (char*)elem->data;
    log->body.keylen = it->nkey;
    log->body.fldlen = elem->nfield;
    log->body.vallen = elem->nbytes;
    log->body.create = create;
    if (log->body.create) {
        do_construct_lrec_attr(it, attr);
        log->attrp = attr;
    }

    log->header.logtype = LOG_MAP_ELEM_INSERT;
    log->header.updtype = UPD_MAP_ELEM_INSERT;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(MapElemInsData, data)
                                               + log->body.keylen + log->body.fldlen + log->body.vallen)
                                               + (log->body.create ? sizeof(lrec_attr_info) : 0);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_map_elem_delete(LogRec *logrec, hash_item *it, map_elem_item *elem,
                                   bool drop)
{
    MapElemDelLog *log = (MapElemDelLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->datptr = (char*)elem->data;
    log->body.keylen = it->nkey;
    log->body.drop   = drop;
    log->body.fldlen = elem->nfield;

    log->header.logtype = LOG_MAP_ELEM_DELETE;
    log->header.updtype = UPD_MAP_ELEM_DELETE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(MapElemDelData, data) +
                                               log->body.keylen + log->body.fldlen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_set_elem_insert(LogRec *logrec, hash_item *it, set_elem_item *elem,
                                   bool create, lrec_attr_info *attr)
{
    SetElemInsLog *log = (SetElemInsLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->valptr = (char*)elem->value;
    log->body.keylen = it->nkey;
    log->body.vallen = elem->nbytes;
    log->body.create = create;
    if (log->body.create) {
        do_construct_lrec_attr(it, attr);
        log->attrp = attr;
    }

    log->header.logtype = LOG_SET_ELEM_INSERT;
    log->header.updtype = UPD_SET_ELEM_INSERT;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(SetElemInsData, data)
                                               + log->body.keylen + log->body.vallen)
                                               + (log->body.create ? sizeof(lrec_attr_info) : 0);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_set_elem_delete(LogRec *logrec, hash_item *it, set_elem_item *elem,
                                   bool drop)
{
    SetElemDelLog *log = (SetElemDelLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->valptr = (char*)elem->value;
    log->body.keylen = it->nkey;
    log->body.drop   = drop;
    log->body.vallen = elem->nbytes;

    log->header.logtype = LOG_SET_ELEM_DELETE;
    log->header.updtype = UPD_SET_ELEM_DELETE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(SetElemDelData, data) +
                                               log->body.keylen + log->body.vallen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_btree_elem_insert(LogRec *logrec, hash_item *it, btree_elem_item *elem,
                                     bool create, lrec_attr_info *attr)
{
    BtreeElemInsLog *log = (BtreeElemInsLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->datptr = (char*)elem->data;
    log->body.keylen = it->nkey;
    log->body.nbkey  = elem->nbkey;
    log->body.neflag = elem->neflag;
    log->body.vallen = elem->nbytes;
    log->body.create = create;
    if (log->body.create) {
        do_construct_lrec_attr(it, attr);
        log->attrp = attr;
    }

    log->header.logtype = LOG_BT_ELEM_INSERT;
    log->header.updtype = UPD_BT_ELEM_INSERT;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(BtreeElemInsData, data) + log->body.keylen
                              + BTREE_REAL_NBKEY(log->body.nbkey) + log->body.neflag + log->body.vallen)
                              + (log->body.create ? sizeof(lrec_attr_info) : 0);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_btree_elem_delete(LogRec *logrec, hash_item *it, btree_elem_item *elem,
                                     bool drop)
{
    BtreeElemDelLog *log = (BtreeElemDelLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->datptr = (char*)elem->data;
    log->body.keylen = it->nkey;
    log->body.drop  = drop;
    log->body.nbkey = elem->nbkey;

    log->header.logtype = LOG_BT_ELEM_DELETE;
    log->header.updtype = UPD_BT_ELEM_DELETE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(BtreeElemDelData, data) +
                              BTREE_REAL_NBKEY(log->body.nbkey) + log->body.keylen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_btree_elem_delete_logical(LogRec *logrec, hash_item *it,
                                             const bkey_range *bkrange,
                                             const eflag_filter *efilter,
                                             uint32_t offset, uint32_t reqcount, bool drop)
{
    BtreeElemDelLgcLog *log = (BtreeElemDelLgcLog*)logrec;
    log->keyptr = (char*)item_get_key(it);

    memset(&log->body, 0, sizeof(log->body));
    log->body.keylen = it->nkey;
    log->body.drop   = drop;
    log->body.offset = offset;
    log->body.reqcount = reqcount;
    log->body.from_nbkey = bkrange->from_nbkey;
    log->body.to_nbkey = bkrange->to_nbkey;
    log->body.filtering = (efilter == NULL ? 0 : 1);
    if (log->body.filtering) {
        log->body.nbitwval = efilter->nbitwval;
        log->body.ncompval = efilter->ncompval;
        log->body.compvcnt = efilter->compvcnt;
        log->body.f_offset = efilter->offset;
        log->body.bitwop   = efilter->bitwop;
        log->body.compop   = efilter->compop;
    }
    log->bkrangep = (bkey_range*)bkrange;
    log->efilterp = (eflag_filter*)efilter;

    uint8_t real_from_nbkey = BTREE_REAL_NBKEY(log->body.from_nbkey);
    uint8_t real_to_nbkey = BTREE_REAL_NBKEY(log->body.to_nbkey);
    log->header.logtype = LOG_BT_ELEM_DELETE_LOGICAL;
    log->header.updtype = UPD_BT_ELEM_DELETE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(BtreeElemDelLgcData, data)
                            + log->body.keylen
                            + real_from_nbkey + (real_to_nbkey == BKEY_NULL ? 0 : real_to_nbkey)
                            + log->body.nbitwval + (log->body.compvcnt*log->body.ncompval));
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_operation_range(LogRec *logrec, bool begin)
{
    OperationRangeLog *log = (OperationRangeLog*)logrec;
    log->header.logtype = (begin ? LOG_OPERATION_BEGIN : LOG_OPERATION_END);
    log->header.updtype = UPD_NONE;
    log->header.body_length = 0;
    return sizeof(LogHdr);
}

hash_item *lrec_get_item_if_collection_link(ITLinkLog *log)
{
    if (log->header.logtype != LOG_IT_LINK ||
        log->header.updtype == UPD_STORE) {
        return NULL;
    }
    hash_item *it = item_get(&log->body.data, log->body.cm.keylen);
    return it;
}

void lrec_set_item_in_snapshot_elem(SnapshotElemLog *log, hash_item *it)
{
    assert(it != NULL);
    if (log->header.logtype == LOG_SNAPSHOT_ELEM) {
        log->it = it;
    }
}

int lrec_check_snapshot_done(SnapshotDoneLog *log)
{
    /* check header */
    if (log->header.logtype != LOG_SNAPSHOT_DONE ||
        log->header.updtype != UPD_NONE) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "no snapshot done log record. incompleted file.\n");
        return -1;
    }

    /* check body */
    /* engine_name */
    if (strncmp(log->body.engine_name, PERSISTENCE_ENGINE_NAME, strlen(PERSISTENCE_ENGINE_NAME)) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "%s is invalid engine name."
                    " expected=%s\n", log->body.engine_name, PERSISTENCE_ENGINE_NAME);
        return -1;
    }
    /* persistence_version */
    if (log->body.persistence_major_version != PERSISTENCE_MAJOR_VERSION) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "persistence major version-%u is not supported."
                    " current latest version(%u.%u)\n",
                    log->body.persistence_major_version, PERSISTENCE_MAJOR_VERSION, PERSISTENCE_MINOR_VERSION);
        return -1;
    }
    if (log->body.persistence_minor_version > PERSISTENCE_MINOR_VERSION) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "persistence minor version-%u is not supported."
                    " current latest version(%u.%u)\n",
                    log->body.persistence_minor_version, PERSISTENCE_MAJOR_VERSION, PERSISTENCE_MINOR_VERSION);
        return -1;
    }
    return 0;
}
#endif
