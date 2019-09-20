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

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE
#include "cmdlogrec.h"

#ifdef ENABLE_PERSISTENCE_03_SNAPSHOT_HEAD_LOG
/* persistence meta data */
#define PERSISTENCE_ENGINE_NAME "ARCUS-DEFAULT_ENGINE"
#define PERSISTENCE_VERSION     1 /* latest version */
#endif

//#define DEBUG_PERSISTENCE_DISK_FORMAT_PRINT

#ifdef offsetof
#error "offsetof is already defined"
#endif
#define offsetof(type, member) __builtin_offsetof(type, member)

/* Get aligned size */
#define GET_8_ALIGN_SIZE(size) \
    (((size) % 8) == 0 ? (size) : ((size) + (8 - ((size) % 8))))

/* get bkey real size */
#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))

static EXTENSION_LOGGER_DESCRIPTOR *logger;


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

static void lrec_eflag_print(uint8_t neflag, unsigned char *eflag, char *str)
{
    assert(neflag > 0);

    char eflag_temp[MAX_EFLAG_LENG*2 + 2];
    safe_hexatostr(eflag, neflag, eflag_temp);
    sprintf(str, "len=%u val=0x%s", neflag, eflag_temp);
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
    if (cm.ittype == ITEM_TYPE_KV) {
        ret = item_apply_kv_link(keyptr, cm.keylen, cm.flags, cm.exptime,
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
            ret = item_apply_list_link(keyptr, cm.keylen, &attr);
        } else if (cm.ittype == ITEM_TYPE_SET) {
            ret = item_apply_set_link(keyptr, cm.keylen, &attr);
        } else if (cm.ittype == ITEM_TYPE_MAP) {
            ret = item_apply_map_link(keyptr, cm.keylen, &attr);
        } else if (cm.ittype == ITEM_TYPE_BTREE) {
            struct lrec_coll_meta meta = body->ptr.meta;
            if (meta.maxbkrlen != BKEY_NULL) {
                attr.maxbkeyrange.len = meta.maxbkrlen;
                memcpy(attr.maxbkeyrange.val, keyptr, BTREE_REAL_NBKEY(attr.maxbkeyrange.len));
                keyptr += BTREE_REAL_NBKEY(attr.maxbkeyrange.len);
            }
            ret = item_apply_btree_link(keyptr, cm.keylen, &attr);
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

static ENGINE_ERROR_CODE lrec_it_unlink_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    ITUnlinkLog  *log  = (ITUnlinkLog*)logrec;
    ITUnlinkData *body = &log->body;

    ret = item_apply_unlink(body->data, body->keylen);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_unlink_redo failed.\n");
    }
    return ret;
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

static ENGINE_ERROR_CODE lrec_it_setattr_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    ITSetAttrLog  *log  = (ITSetAttrLog*)logrec;
    ITSetAttrData *body = &log->body;

    if (log->header.updtype == UPD_SETATTR_EXPTIME) {
        ret = item_apply_setattr_exptime(body->data, body->keylen, body->exptime);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_setattr_redo failed.\n");
        }
    } else {
        hash_item *it = item_get(body->data, body->keylen);
        if (it) {
            if (body->maxbkrlen != BKEY_NULL) {
                bkey_t maxbkeyrange;
                maxbkeyrange.len = body->maxbkrlen;
                memcpy(maxbkeyrange.val, (body->data + body->keylen), body->maxbkrlen);
                ret = item_apply_setattr_meta_info(it, body->ovflact, body->mflags,
                                                   body->exptime, body->mcnt, &maxbkeyrange);
            } else {
                ret = item_apply_setattr_meta_info(it, body->ovflact, body->mflags,
                                                   body->exptime, body->mcnt, NULL);
            }
            if (ret == ENGINE_SUCCESS) {
                item_release(it);
            } else {
                logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_setattr_redo failed.\n");
            }
        } else {
            ret = ENGINE_KEY_ENOENT;
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_setattr_redo failed."
                        "not found. key=%.*s\n", body->keylen, body->data);
        }
    }

    return ret;
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

static ENGINE_ERROR_CODE lrec_it_flush_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    ITFlushLog  *log  = (ITFlushLog*)logrec;
    ITFlushData *body = &log->body;

    if (body->nprefix == 255) {
        ret = item_apply_flush(NULL, -1); /* flush_all */
    } else if (body->nprefix == 0) {
        ret = item_apply_flush(NULL, 0); /* null prefix */
    } else {
        ret = item_apply_flush(body->data, body->nprefix); /* flush specific prefix */
    }

    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_it_flush_redo failed.\n");
    }
    return ret;
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
    ListElemInsLog *log = (ListElemInsLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(ListElemInsData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    /* value copy */
    memcpy(bufptr + offset + log->body.keylen, log->valptr, log->body.vallen);
}

static ENGINE_ERROR_CODE lrec_list_elem_insert_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret = ENGINE_FAILED;
    ListElemInsLog  *log  = (ListElemInsLog*)logrec;
    ListElemInsData *body = &log->body;

    hash_item *it = item_get(body->data, body->keylen);
    if (it) {
        ret = item_apply_list_elem_insert(it, body->totcnt, body->eindex, (body->data + body->keylen), body->vallen);
        if (ret == ENGINE_SUCCESS) {
            item_release(it);
        } else {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_list_elem_insert_redo failed.\n");
        }
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_list_elem_insert_redo failed. "
                    "not found. nkey=%d, key=%.*s\n", body->keylen, body->keylen, body->data);
    }
    return ret;
}

static void lrec_list_elem_insert_print(LogRec *logrec)
{
    ListElemInsLog *log = (ListElemInsLog*)logrec;
    lrec_header_print(&log->header);

    fprintf(stderr, "[BODY]   totcnt=%u | eindex=%d | "
            "keylen=%u | keystr=%.*s | vallen=%u | valstr=%.*s",
            log->body.totcnt, log->body.eindex,
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr,
            log->body.vallen, (log->body.vallen <= 250 ? log->body.vallen : 250), log->valptr);
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

    hash_item *it = item_get(body->data, body->keylen);
    if (it) {
        ret = item_apply_list_elem_delete(it, body->totcnt, body->eindex, body->delcnt);
        if (ret == ENGINE_SUCCESS) {
            item_release(it);
        } else {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_list_elem_delete_redo failed.\n");
        }
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_list_elem_delete_redo failed. "
                    "not found. key=%.*s\n", body->keylen, body->data);
    }
    return ret;
}

static void lrec_list_elem_delete_print(LogRec *logrec)
{
    ListElemDelLog *log = (ListElemDelLog*)logrec;
    lrec_header_print(&log->header);

    fprintf(stderr, "[BODY]   totcnt=%u | eindex=%d | delcnt=%u | keylen=%u | keystr=%.*s\r\n",
            log->body.totcnt, log->body.eindex, log->body.delcnt,
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr);
}

/* Set Element Insert Log Record */
static void lrec_set_elem_insert_write(LogRec *logrec, char *bufptr)
{
    SetElemInsLog *log = (SetElemInsLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(SetElemInsData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    /* value copy */
    memcpy(bufptr + offset + log->body.keylen, log->valptr, log->body.vallen);
}

static ENGINE_ERROR_CODE lrec_set_elem_insert_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    SetElemInsLog  *log  = (SetElemInsLog*)logrec;
    SetElemInsData *body = &log->body;

    hash_item *it = item_get(body->data, body->keylen);
    if (it) {
        ret = item_apply_set_elem_insert(it, (body->data + body->keylen), body->vallen);
        if (ret == ENGINE_SUCCESS) {
            item_release(it);
        } else {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_set_elem_insert_redo failed.\n");
        }
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_set_elem_insert_redo failed. "
                    "not found. key=%.*s\n", body->keylen, body->data);
    }
    return ret;
}

static void lrec_set_elem_insert_print(LogRec *logrec)
{
    SetElemInsLog *log = (SetElemInsLog*)logrec;
    lrec_header_print(&log->header);

    fprintf(stderr, "[BODY]   keylen=%u | keystr=%.*s | vallen=%u | valstr=%.*s",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr,
            log->body.vallen, (log->body.vallen <= 250 ? log->body.vallen : 250), log->valptr);
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

    hash_item *it = item_get(body->data, body->keylen);
    if (it) {
        ret = item_apply_set_elem_delete(it, (body->data + body->keylen), body->vallen);
        if (ret == ENGINE_SUCCESS) {
            item_release(it);
        } else {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_set_elem_delete_redo failed.\n");
        }
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_set_elem_delete_redo failed. "
                    "not found. key=%.*s\n", body->keylen, body->data);
    }
    return ret;
}

static void lrec_set_elem_delete_print(LogRec *logrec)
{
    SetElemDelLog *log = (SetElemDelLog*)logrec;
    lrec_header_print(&log->header);

    fprintf(stderr, "[BODY]   keylen=%u | keystr=%.*s | vallen=%u | valstr=%.*s",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr,
            log->body.vallen, (log->body.vallen <= 250 ? log->body.vallen : 250), log->valptr);
}

/* Map Element Insert Log Record */
static void lrec_map_elem_insert_write(LogRec *logrec, char *bufptr)
{
    MapElemInsLog *log = (MapElemInsLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(MapElemInsData, data);

    memcpy(bufptr, (void*)logrec, offset);
    /* key copy */
    memcpy(bufptr + offset, log->keyptr, log->body.keylen);
    /* field + value copy */
    memcpy(bufptr + offset + log->body.keylen, log->datptr, log->body.fldlen + log->body.vallen);
}

static ENGINE_ERROR_CODE lrec_map_elem_insert_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    MapElemInsLog  *log  = (MapElemInsLog*)logrec;
    MapElemInsData *body = &log->body;

    hash_item *it = item_get(body->data, body->keylen);
    if (it) {
        ret = item_apply_map_elem_insert(it, (body->data + body->keylen), body->fldlen, body->vallen);
        if (ret == ENGINE_SUCCESS) {
            item_release(it);
        } else {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_map_elem_insert_redo failed.\n");
        }
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_map_elem_insert_redo failed. "
                    "not found. key=%.*s\n", body->keylen, body->data);
    }
    return ret;
}

static void lrec_map_elem_insert_print(LogRec *logrec)
{
    MapElemInsLog *log = (MapElemInsLog*)logrec;
    lrec_header_print(&log->header);

    fprintf(stderr, "[BODY]   keylen=%u | keystr=%.*s | fldlen=%u | fldstr=%.*s | vallen=%u | valstr=%.*s",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr,
            log->body.fldlen, log->body.fldlen, log->datptr,
            log->body.vallen, (log->body.vallen <= 250 ? log->body.vallen : 250), log->datptr+log->body.fldlen);
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

    hash_item *it = item_get(body->data, body->keylen);
    if (it) {
        ret = item_apply_map_elem_delete(it, (body->data + body->keylen), body->fldlen);
        if (ret == ENGINE_SUCCESS) {
            item_release(it);
        } else {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_map_elem_delete_redo failed.\n");
        }
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_map_elem_delete_redo failed. "
                    "not found. key=%.*s\n", body->keylen, body->data);
    }
    return ret;
}

static void lrec_map_elem_delete_print(LogRec *logrec)
{
    MapElemDelLog *log = (MapElemDelLog*)logrec;
    lrec_header_print(&log->header);

    fprintf(stderr, "[BODY]   keylen=%u | keystr=%.*s | fldlen=%u | fldstr=%.*s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr,
            log->body.fldlen, log->body.fldlen, log->datptr);
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
    /* bkey | eflag | value copy */
    memcpy(bufptr + offset + log->body.keylen, log->datptr, datlen);
}

static ENGINE_ERROR_CODE lrec_bt_elem_insert_redo(LogRec *logrec)
{
    ENGINE_ERROR_CODE ret;
    BtreeElemInsLog  *log  = (BtreeElemInsLog*)logrec;
    BtreeElemInsData *body = &log->body;

    hash_item *it = item_get(body->data, body->keylen);
    if (it) {
        ret = item_apply_btree_elem_insert(it, (body->data + body->keylen), body->nbkey, body->neflag, body->vallen);
        if (ret == ENGINE_SUCCESS) {
            item_release(it);
        } else {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_insert_redo failed.\n");
        }
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_insert_redo failed. "
                    "not found. key=%.*s\n", body->keylen, body->data);
    }
    return ret;
}

static void lrec_bt_elem_insert_print(LogRec *logrec)
{
    BtreeElemInsLog *log = (BtreeElemInsLog*)logrec;
    lrec_header_print(&log->header);

    char bkeystr[60];
    char eflagstr[60];
    uint64_t real_nbkey = BTREE_REAL_NBKEY(log->body.nbkey);
    int leng = sprintf(bkeystr, "bkey");
    lrec_bkey_print(log->body.nbkey, (unsigned char*)log->datptr, bkeystr + leng);
    if (log->body.neflag != 0) {
        leng = sprintf(eflagstr, "eflag");
        lrec_eflag_print(log->body.neflag, (unsigned char*)log->datptr + real_nbkey, eflagstr + leng);
    }

    /* <key> <bkey> [<eflag>] <data> */
    fprintf(stderr, "[BODY]   keylen=%u | keystr=%.*s | "
            "%s | %s | vallen = %u | valstr=%.*s",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr,
            bkeystr, (log->body.neflag != 0 ? eflagstr : ""),
            log->body.vallen, (log->body.vallen <= 250 ? log->body.vallen : 250),
            log->datptr + real_nbkey + log->body.neflag);
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

    hash_item *it = item_get(body->data, body->keylen);
    if (it) {
        ret = item_apply_btree_elem_delete(it, (body->data + body->keylen), body->nbkey);
        if (ret == ENGINE_SUCCESS) {
            item_release(it);
        } else {
            logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_delete_redo failed.\n");
        }
    } else {
        ret = ENGINE_KEY_ENOENT;
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_bt_elem_delete_redo failed. "
                    "not found. key=%.*s\n", body->keylen, body->data);
    }
    return ret;
}

static void lrec_bt_elem_delete_print(LogRec *logrec)
{
    BtreeElemDelLog *log = (BtreeElemDelLog*)logrec;
    lrec_header_print(&log->header);

    char metastr[180];
    int leng = sprintf(metastr, "bkey");
    lrec_bkey_print(log->body.nbkey, (unsigned char*)log->datptr, metastr + leng);

    fprintf(stderr, "[BODY]   keylen=%u | keystr=%.*s | %s\r\n",
            log->body.keylen, (log->body.keylen <= 250 ? log->body.keylen : 250), log->keyptr, metastr);
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

    if (IS_LIST_ITEM(log->it)) {
        ret = item_apply_list_elem_insert(log->it, -1, -1, body->data, body->nbytes);
    } else if (IS_SET_ITEM(log->it)) {
        ret = item_apply_set_elem_insert(log->it, body->data, body->nbytes);
    } else if (IS_MAP_ITEM(log->it)) {
        ret = item_apply_map_elem_insert(log->it, body->data, body->nekey, body->nbytes);
    } else if (IS_BTREE_ITEM(log->it)) {
        ret = item_apply_btree_elem_insert(log->it, body->data, body->nekey, body->neflag, body->nbytes);
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

    lrec_header_print(&log->header);
    /* vallen >= 2, valstr = ...\r\n */
    if (log->header.updtype == UPD_MAP_ELEM_INSERT) {
        fprintf(stderr, "[BODY]   nfield=%u | field=%.*s | vallen=%u | value=%.*s",
                body->nekey, body->nekey, log->valptr,
                body->nbytes, (body->nbytes <= 250 ? body->nbytes : 250), (log->valptr + body->nekey));
    } else if (log->header.updtype == UPD_BT_ELEM_INSERT) {
        char bkeystr[60];
        char eflagstr[60];

        int leng = sprintf(bkeystr, "bkey ");
        lrec_bkey_print(body->nekey, (unsigned char*)log->valptr, bkeystr + leng);
        if (body->neflag != 0) {
            leng = sprintf(eflagstr, " | eflag ");
            lrec_eflag_print(body->neflag, (unsigned char*)(log->valptr + BTREE_REAL_NBKEY(body->nekey)), eflagstr + leng);
        }

        /* <key> <bkey> [<eflag>] <bytes> <data> */
        fprintf(stderr, "[BODY]   %s%s | vallen=%u | value=%.*s",
                bkeystr, (body->neflag != 0 ? eflagstr : ""), body->nbytes,
                (body->nbytes <= 250 ? body->nbytes : 250),
                (log->valptr + BTREE_REAL_NBKEY(body->nekey) + body->neflag));
    } else {
        fprintf(stderr, "[BODY]   nbytes=%u | value=%.*s",
                body->nbytes, body->nbytes, log->valptr);
    }
}

/* Snapshot Head Log Record */
static void lrec_snapshot_head_write(LogRec *logrec, char *bufptr)
{
#ifdef ENABLE_PERSISTENCE_03_SNAPSHOT_HEAD_LOG
    SnapshotHeadLog *log = (SnapshotHeadLog*)logrec;
    int offset = sizeof(LogHdr) + offsetof(SnapshotHeadData, data);

    memcpy(bufptr, (void*)log, offset);
#endif
}

static void lrec_snapshot_head_print(LogRec *logrec)
{
#ifdef ENABLE_PERSISTENCE_03_SNAPSHOT_HEAD_LOG
    SnapshotHeadLog  *log  = (SnapshotHeadLog*)logrec;
    SnapshotHeadData *body = &log->body;

    lrec_header_print(&log->header);

    fprintf(stderr, "[BODY]   engine_name=%s | checkpoint_time=%"PRId64" | "
            "persistence_version=%u\n",
            body->engine_name, body->checkpoint_time, body->persistence_version);
#endif
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
    ENGINE_ERROR_CODE (*redo)(LogRec *logrec);
    void (*print)(LogRec *logrec);
} LOGREC_FUNC;

LOGREC_FUNC logrec_func[] = {
    { lrec_it_link_write,            lrec_it_link_redo,            lrec_it_link_print },
    { lrec_it_unlink_write,          lrec_it_unlink_redo,          lrec_it_unlink_print },
    { lrec_it_setattr_write,         lrec_it_setattr_redo,         lrec_it_setattr_print },
    { lrec_it_flush_write,           lrec_it_flush_redo,           lrec_it_flush_print },
    { lrec_list_elem_insert_write,   lrec_list_elem_insert_redo,   lrec_list_elem_insert_print },
    { lrec_list_elem_delete_write,   lrec_list_elem_delete_redo,   lrec_list_elem_delete_print },
    { lrec_set_elem_insert_write,    lrec_set_elem_insert_redo,    lrec_set_elem_insert_print },
    { lrec_set_elem_delete_write,    lrec_set_elem_delete_redo,    lrec_set_elem_delete_print },
    { lrec_map_elem_insert_write,    lrec_map_elem_insert_redo,    lrec_map_elem_insert_print },
    { lrec_map_elem_delete_write,    lrec_map_elem_delete_redo,    lrec_map_elem_delete_print },
    { lrec_bt_elem_insert_write,     lrec_bt_elem_insert_redo,     lrec_bt_elem_insert_print },
    { lrec_bt_elem_delete_write,     lrec_bt_elem_delete_redo,     lrec_bt_elem_delete_print },
    { lrec_snapshot_elem_link_write, lrec_snapshot_elem_link_redo, lrec_snapshot_elem_link_print },
    { lrec_snapshot_head_write,      NULL,                         lrec_snapshot_head_print },
    { lrec_snapshot_tail_write,      NULL,                         lrec_snapshot_tail_print }
};

/* external function */

void cmdlog_rec_init(struct default_engine *engine)
{
    logger = engine->server.log->get_logger();
    logger->log(EXTENSION_LOG_INFO, NULL, "COMMNAD LOG RECORD module initialized.\n");
}

void lrec_write_to_buffer(LogRec *logrec, char *bufptr)
{
    logrec_func[logrec->header.logtype].write(logrec, bufptr);
#ifdef DEBUG_PERSISTENCE_DISK_FORMAT_PRINT
    logrec_func[logrec->header.logtype].print(logrec);
#endif
}

ENGINE_ERROR_CODE lrec_redo_from_record(LogRec *logrec)
{
#ifdef DEBUG_PERSISTENCE_DISK_FORMAT_PRINT
    logrec_func[logrec->header.logtype].print(logrec);
#endif
    if (logrec_func[logrec->header.logtype].redo != NULL) {
        return logrec_func[logrec->header.logtype].redo(logrec);
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "lrec_redo_from_record(logtype=%s) "
                    "is not supported.\n", get_logtype_text(logrec->header.logtype));
        return ENGINE_ENOTSUP;
    }
}

/* Construct Log Record Functions */

#ifdef ENABLE_PERSISTENCE_03_SNAPSHOT_HEAD_LOG
int lrec_construct_snapshot_head(LogRec *logrec, int64_t chkpt_time)
{
    SnapshotHeadLog  *log  = (SnapshotHeadLog*)logrec;
    SnapshotHeadData *body = &log->body;

    assert(sizeof(body->engine_name) > strlen(PERSISTENCE_ENGINE_NAME));
    memset(body->engine_name, 0, sizeof(body->engine_name));
    memcpy(body->engine_name, PERSISTENCE_ENGINE_NAME, strlen(PERSISTENCE_ENGINE_NAME));
    body->checkpoint_time = chkpt_time;
    body->persistence_version = PERSISTENCE_VERSION;

    log->header.logtype = LOG_SNAPSHOT_HEAD;
    log->header.updtype = UPD_NONE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(SnapshotHeadData, data));
    return log->header.body_length+sizeof(LogHdr);
}
#else
int lrec_construct_snapshot_head(LogRec *logrec)
{
    return 0;
}

#endif
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
    log->body.maxbkrlen = BKEY_NULL;

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

int lrec_construct_list_elem_insert(LogRec *logrec, hash_item *it, uint32_t totcnt, int eindex, list_elem_item *elem)
{
    ListElemInsLog *log = (ListElemInsLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->valptr = (char*)elem->value;
    log->body.keylen = it->nkey;
    log->body.vallen = elem->nbytes;
    log->body.totcnt = totcnt;
    log->body.eindex = eindex;

    log->header.logtype = LOG_LIST_ELEM_INSERT;
    log->header.updtype = UPD_LIST_ELEM_INSERT;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(ListElemInsData, data) +
                                               log->body.keylen + log->body.vallen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_list_elem_delete(LogRec *logrec, hash_item *it, uint32_t totcnt, int eindex, uint32_t delcnt)
{
    ListElemDelLog *log = (ListElemDelLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->body.keylen = it->nkey;
    log->body.totcnt = totcnt;
    log->body.eindex = eindex;
    log->body.delcnt = delcnt;

    log->header.logtype = LOG_LIST_ELEM_DELETE;
    log->header.updtype = UPD_LIST_ELEM_DELETE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(ListElemDelData, data) + log->body.keylen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_map_elem_insert(LogRec *logrec, hash_item *it, map_elem_item *elem)
{
    MapElemInsLog *log = (MapElemInsLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->datptr = (char*)elem->data;
    log->body.keylen = it->nkey;
    log->body.fldlen = elem->nfield;
    log->body.vallen = elem->nbytes;

    log->header.logtype = LOG_MAP_ELEM_INSERT;
    log->header.updtype = UPD_MAP_ELEM_INSERT;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(MapElemDelData, data) +
                                               log->body.keylen + log->body.fldlen + log->body.vallen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_map_elem_delete(LogRec *logrec, hash_item *it, map_elem_item *elem)
{
    MapElemDelLog *log = (MapElemDelLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->datptr = (char*)elem->data;
    log->body.keylen = it->nkey;
    log->body.fldlen = elem->nfield;

    log->header.logtype = LOG_MAP_ELEM_DELETE;
    log->header.updtype = UPD_MAP_ELEM_DELETE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(MapElemDelData, data) +
                                               log->body.keylen + log->body.fldlen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_set_elem_insert(LogRec *logrec, hash_item *it, set_elem_item *elem)
{
    SetElemInsLog *log = (SetElemInsLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->valptr = (char*)elem->value;
    log->body.keylen = it->nkey;
    log->body.vallen = elem->nbytes;

    log->header.logtype = LOG_SET_ELEM_INSERT;
    log->header.updtype = UPD_SET_ELEM_INSERT;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(SetElemInsData, data) +
                                               log->body.keylen + log->body.vallen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_set_elem_delete(LogRec *logrec, hash_item *it, set_elem_item *elem)
{
    SetElemDelLog *log = (SetElemDelLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->valptr = (char*)elem->value;
    log->body.keylen = it->nkey;
    log->body.vallen = elem->nbytes;

    log->header.logtype = LOG_SET_ELEM_DELETE;
    log->header.updtype = UPD_SET_ELEM_DELETE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(SetElemDelData, data) +
                                               log->body.keylen + log->body.vallen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_btree_elem_insert(LogRec *logrec, hash_item *it, btree_elem_item *elem)
{
    BtreeElemInsLog *log = (BtreeElemInsLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->datptr = (char*)elem->data;
    log->body.keylen = it->nkey;
    log->body.nbkey  = elem->nbkey;
    log->body.neflag = elem->neflag;
    log->body.vallen = elem->nbytes;

    log->header.logtype = LOG_BT_ELEM_INSERT;
    log->header.updtype = UPD_BT_ELEM_INSERT;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(BtreeElemInsData, data) + log->body.keylen +
                              BTREE_REAL_NBKEY(log->body.nbkey) + log->body.neflag + log->body.vallen);
    return log->header.body_length+sizeof(LogHdr);
}

int lrec_construct_btree_elem_delete(LogRec *logrec, hash_item *it, btree_elem_item *elem)
{
    BtreeElemDelLog *log = (BtreeElemDelLog*)logrec;
    log->keyptr = (char*)item_get_key(it);
    log->datptr = (char*)elem->data;
    log->body.keylen = it->nkey;
    log->body.nbkey = elem->nbkey;

    log->header.logtype = LOG_BT_ELEM_DELETE;
    log->header.updtype = UPD_BT_ELEM_DELETE;
    log->header.body_length = GET_8_ALIGN_SIZE(offsetof(BtreeElemDelData, data) +
                              BTREE_REAL_NBKEY(log->body.nbkey) + log->body.keylen);
    return log->header.body_length+sizeof(LogHdr);
}

hash_item *lrec_get_item_if_collection_link(ITLinkLog *log)
{
    if (log->header.logtype != LOG_IT_LINK ||
        log->header.updtype == UPD_SET) {
        return NULL;
    }
    hash_item *it = item_get(&log->body.data, log->body.cm.keylen);
    assert(it != NULL && IS_COLL_ITEM(it));
    return it;
}

void lrec_set_item_in_snapshot_elem(SnapshotElemLog *log, hash_item *it)
{
    assert(it != NULL);
    if (log->header.logtype == LOG_SNAPSHOT_ELEM) {
        log->it = it;
    }
}
#endif
