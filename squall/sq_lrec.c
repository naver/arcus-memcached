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
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "squall_config.h"
#include "sq_buffer.h"
#include "sq_assoc.h"
#include "sq_prefix.h"
#include "sq_log.h"

#if 0 // OLD CODE
typedef void (*LOGREC_WRITE_FUNC)(LogRec *logrec, char *bufptr);
typedef int  (*LOGREC_REDO_FUNC)(LogRec *logrec);
typedef int  (*LOGREC_UNDO_FUNC)(LogRec *logrec);

LOGREC_WRITE_FUNC lrec_write_func[ACT_MAX];
LOGREC_REDO_FUNC  lrec_redo_func[ACT_MAX];
LOGREC_UNDO_FUNC  lrec_undo_func[ACT_MAX];
#endif

LOGREC_FUNC logrec_func[ACT_MAX] = {
        { lrec_pf_create_write,  lrec_pf_create_redo,  NULL,                lrec_pf_create_print },
        { lrec_pf_drop_write,    lrec_pf_drop_redo,    NULL,                lrec_pf_drop_print },
        { lrec_fl_grow_write,    lrec_fl_grow_redo,    NULL,                lrec_fl_grow_print },
        { lrec_pg_compact_write, lrec_pg_compact_redo, NULL,                lrec_pg_compact_print },
        { lrec_pg_alloc_write,   lrec_pg_alloc_redo,   lrec_pg_alloc_undo,  lrec_pg_alloc_print },
        { lrec_pg_free_write,    lrec_pg_free_redo,    NULL,                lrec_pg_free_print },
        { lrec_it_alloc_write,   lrec_it_alloc_redo,   lrec_it_alloc_undo,  lrec_it_alloc_print },
        { lrec_it_ovfadd_write,  lrec_it_ovfadd_redo,  NULL,                lrec_it_ovfadd_print },
        { lrec_it_ovfdel_write,  lrec_it_ovfdel_redo,  NULL,                lrec_it_ovfdel_print },
        { lrec_it_free_write,    lrec_it_free_redo,    NULL,                lrec_it_free_print },
        { lrec_it_link_write,    lrec_it_link_redo,    NULL,                lrec_it_link_print },
        { lrec_it_unlink_write,  lrec_it_unlink_redo,  NULL,                lrec_it_unlink_print },
        { lrec_it_replace_write, lrec_it_replace_redo, NULL,                lrec_it_replace_print },
        { lrec_it_delmark_write, lrec_it_delmark_redo, NULL,                lrec_it_delmark_print },
        { lrec_it_update_write,  lrec_it_update_redo,  NULL,                lrec_it_update_print },
        { lrec_it_attr_write,    lrec_it_attr_redo,    NULL,                lrec_it_attr_print },
        { lrec_bt_rtbuild_write, lrec_bt_rtbuild_redo, NULL,                lrec_bt_rtbuild_print },
        { lrec_bt_rtsplit_write, lrec_bt_rtsplit_redo, NULL,                lrec_bt_rtsplit_print },
        { lrec_bt_pgsplit_write, lrec_bt_pgsplit_redo, NULL,                lrec_bt_pgsplit_print },
        { lrec_bt_rtmerge_write, lrec_bt_rtmerge_redo, NULL,                lrec_bt_rtmerge_print },
        { lrec_bt_pgmerge_write, lrec_bt_pgmerge_redo, NULL,                lrec_bt_pgmerge_print },
        { lrec_bt_insert_write,  lrec_bt_insert_redo,  lrec_bt_insert_undo, lrec_bt_insert_print },
        { lrec_bt_delete_write,  lrec_bt_delete_redo,  lrec_bt_delete_undo, lrec_bt_delete_print },
        { lrec_bt_toggle_write,  lrec_bt_toggle_redo,  lrec_bt_toggle_undo, lrec_bt_toggle_print },
        { lrec_chkpt_bgn_write,  lrec_chkpt_bgn_redo,  NULL,                lrec_chkpt_bgn_print },
        { lrec_chkpt_end_write,  lrec_chkpt_end_redo,  NULL,                lrec_chkpt_end_print }
    };

static SERVER_HANDLE_V1 *server = NULL;
static struct config    *config = NULL;
static EXTENSION_LOGGER_DESCRIPTOR *logger = NULL;

static char *get_log_logtype_text(int type)
{
    char *text;
    switch (type) {
      case LOG_REDO_UNDO:
           text = "REDO_UNDO"; break;
      case LOG_REDO_ONLY:
           text = "REDO_ONLY"; break;
      case LOG_UNDO_ONLY:
           text = "UNDO_ONLY"; break;
      default:
           text = "NOLOGTYPE";
    }
    return text;
}

static char *get_log_acttype_text(int type)
{
    char *text;
    switch (type) {
      case ACT_PF_CREATE:
           text = "PF_CREATE "; break;
      case ACT_PF_DROP:
           text = "PF_DROP   "; break;
      case ACT_FL_GROW:
           text = "FL_GROW   "; break;
      case ACT_PG_COMPACT:
           text = "PG_COMPACT"; break;
      case ACT_PG_ALLOC:
           text = "PG_ALLOC  "; break;
      case ACT_PG_FREE:
           text = "PG_FREE   "; break;
      case ACT_IT_ALLOC:
           text = "IT_ALLOC  "; break;
      case ACT_IT_OVFADD:
           text = "IT_OVFADD "; break;
      case ACT_IT_OVFDEL:
           text = "IT_OVFDEL "; break;
      case ACT_IT_FREE:
           text = "IT_FREE   "; break;
      case ACT_IT_LINK:
           text = "IT_LINK   "; break;
      case ACT_IT_UNLINK:
           text = "IT_UNLINK "; break;
      case ACT_IT_REPLACE:
           text = "IT_REPLACE"; break;
      case ACT_IT_DELMARK:
           text = "IT_DELMARK"; break;
      case ACT_IT_UPDATE:
           text = "IT_UPDATE "; break;
      case ACT_IT_ATTR:
           text = "IT_ATTR   "; break;
      case ACT_BT_RTBUILD:
           text = "BT_RTBUILD"; break;
      case ACT_BT_RTSPLIT:
           text = "BT_RTSPLIT"; break;
      case ACT_BT_PGSPLIT:
           text = "BT_PGSPLIT"; break;
      case ACT_BT_RTMERGE:
           text = "BT_RTMERGE"; break;
      case ACT_BT_PGMERGE:
           text = "BT_PGMERGE"; break;
      case ACT_BT_INSERT:
           text = "BT_INSERT "; break;
      case ACT_BT_DELETE:
           text = "BT_DELETE "; break;
      case ACT_BT_TOGGLE:
           text = "BT_TOGGLE "; break;
      case ACT_CHKPT_BGN:
           text = "CHKPT_BGN "; break;
      case ACT_CHKPT_END:
           text = "CHKPT_END "; break;
      default:
           text = "NOACTTYPE ";
    }
    return text;
}

static char *get_log_cmttype_text(int type)
{
    char *text;
    switch (type) {
      case LOG_NTA_COMMIT:
           text = "NTA_COMMIT"; break;
      case LOG_ACT_BEGIN:
           text = "ACT_BEGIN "; break;
      case LOG_ACT_NORMAL:
           text = "ACT_NORMAL"; break;
      case LOG_ACT_COMMIT:
           text = "ACT_COMMIT"; break;
      case LOG_ACT_SINGLE:
           text = "ACT_SINGLE"; break;
      default:
           text = "NOCMTTYPE ";
    }
    return text;
}

static char *get_file_type_text(int type)
{
    char *text;
    switch (type) {
      case SQUALL_FILE_TYPE_ANCH:
           text = "ANCHOR  "; break;
      case SQUALL_FILE_TYPE_DATA:
           text = "DATA    "; break;
      case SQUALL_FILE_TYPE_OVFL:
           text = "OVERFLOW"; break;
      case SQUALL_FILE_TYPE_INDX:
           text = "INDEX   "; break;
      default:
           text = "NOFLTYPE";
    }
    return text;
}

static char *get_page_type_text(int type)
{
    char *text;
    switch (type) {
      case SQUALL_PAGE_TYPE_PMAP:
           text = "PAGEMAP "; break;
      case SQUALL_PAGE_TYPE_DATA:
           text = "DATA    "; break;
      case SQUALL_PAGE_TYPE_OVFL:
           text = "OVERFLOW"; break;
      case SQUALL_PAGE_TYPE_INDX:
           text = "INDEX   "; break;
      default:
           text = "NOPGTYPE";
    }
    return text;
}

/* Prefix Create Log Record */
void lrec_pf_create_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_PF_CREATE;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_pf_create_write(LogRec *logrec, char *bufptr)
{
    PFCreateLog *logptr = (PFCreateLog*)logrec;
    uint32_t     loglen = offsetof(PFCreateData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->prefix, logptr->body.nprefix);
}

int lrec_pf_create_redo(LogRec *logrec)
{
    PFCreateLog *logptr = (PFCreateLog*)logrec;
    prefix_desc *pfdesc;
    char         pfxname[256];

    /* prefix name */
    memcpy(pfxname, logptr->body.data, logptr->body.nprefix);
    pfxname[logptr->body.nprefix] = '\0';

    pfdesc = pfx_find(pfxname, logptr->body.nprefix);
    if (pfdesc != NULL) {
        if (LOGSN_IS_GE(&pfdesc->d.crtlsn, &logptr->header.self_lsn)) {
            return 0; /* nothing to redo */
        } else {
            /* some old prefix was not deleted */
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[REDO] prefix create - old prefix(%s) exists\n", pfxname);
            return -1; /* redo fail */
        }
    }
    /* check use_cas flag */
    if (logptr->body.use_cas != config->use_cas) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "[REDO] prefix create - prefix(%s) use_cas mismatch\n", pfxname);
        return -1; /* redo fail */
    }
    /* create the prefix */
    if (pfx_redo_create(pfxname, logptr->body.nprefix, logptr->body.pfxid, logptr->body.spcid,
                        logptr->body.exp_crt, logptr->header.self_lsn) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "[REDO] prefix create - prefix(%s) create fail\n", pfxname);
        return -1; /* redo fail */
    }
    return 0;
}

void lrec_pf_create_print(LogRec *logrec)
{
    LOGHDR       *hdr  = &logrec->header;
    PFCreateData *body = &((PFCreateLog*)logrec)->body;
    char          name[256]; /* KEY_MAX_LENGTH + 1 */

    assert(body->nprefix < 256);
    if (body->nprefix > 0) {
        memcpy(name, body->data, body->nprefix); name[body->nprefix] = '\0';
    } else {
        memcpy(name, "<NULL>", 6); name[6] = '\0';
    }
    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %s p(%u) s(%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            name, body->pfxid, body->spcid);
}

/* Prefix Drop Log Record */
void lrec_pf_drop_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_PF_DROP;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_pf_drop_write(LogRec *logrec, char *bufptr)
{
    PFDropLog *logptr = (PFDropLog*)logrec;
    uint32_t   loglen = offsetof(PFDropData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->prefix, logptr->body.nprefix);
}

int lrec_pf_drop_redo(LogRec *logrec)
{
    PFDropLog *logptr = (PFDropLog*)logrec;
    prefix_desc *pfdesc;
    char         pfxname[256];

    /* prefix name */
    memcpy(pfxname, logptr->body.data, logptr->body.nprefix);
    pfxname[logptr->body.nprefix] = '\0';

    pfdesc = pfx_find(pfxname, logptr->body.nprefix);
    if (pfdesc == NULL) {
        return 0; /* nothing to redo */
    }
    if (LOGSN_IS_GE(&pfdesc->d.updlsn, &logptr->header.self_lsn)) {
        return 0; /* nothing to redo */
    }
    /* delete the prefix */
    if (pfx_redo_delete(pfxname, logptr->body.nprefix, logptr->header.self_lsn) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "[REDO] prefix drop - prefix(%s) drop fail\n", pfxname);
        return -1; /* redo fail */
    }
    return 0;
}

void lrec_pf_drop_print(LogRec *logrec)
{
    LOGHDR     *hdr  = &logrec->header;
    PFDropData *body = &((PFDropLog*)logrec)->body;
    char        name[256]; /* KEY_MAX_LENGTH + 1 */

    assert(body->nprefix < 256);
    if (body->nprefix > 0) {
        memcpy(name, body->data, body->nprefix); name[body->nprefix] = '\0';
    } else {
        memcpy(name, "<NULL>", 6); name[6] = '\0';
    }
    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %s p(%u) s(%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            name, body->pfxid, body->spcid);
}

/* File Grow Log Record */
void lrec_fl_grow_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_FL_GROW;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_fl_grow_write(LogRec *logrec, char *bufptr)
{
    FLGrowLog *logptr = (FLGrowLog*)logrec;
    memcpy(bufptr, &logptr->body, logptr->header.length);
}

int lrec_fl_grow_redo(LogRec *logrec)
{
    FLGrowLog *logptr = (FLGrowLog*)logrec;

    if (pfx_redo_flgrow(logptr->body.spcid, logptr->body.ftype, logptr->body.flnum, logptr->body.pgnum,
                        logptr->body.pgcnt, logptr->header.self_lsn) < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] file grow - fail\n");
        return -1; /* redo fail */
    }
    /* reflect the redo action into tran table */
    return 0;
}

void lrec_fl_grow_print(LogRec *logrec)
{
    LOGHDR     *hdr  = &logrec->header;
    FLGrowData *body = &((FLGrowLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %u %s f(%u) p(%u-%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            body->spcid, get_file_type_text(body->ftype), body->flnum, body->pgnum, body->pgcnt);
}

/* Page Compaction Log Record */
void lrec_pg_compact_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_PG_COMPACT;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_pg_compact_write(LogRec *logrec, char *bufptr)
{
    PGCompactLog *logptr = (PGCompactLog*)logrec;
    uint32_t      loglen = offsetof(PGCompactData, data);
    memcpy(bufptr, (char*)&logptr->body, loglen);
    memcpy(bufptr + loglen, (char*)logptr->pgptr + logptr->body.doffset, logptr->body.dlength);
    loglen += logptr->body.dlength;
    memcpy(bufptr + loglen, (char*)logptr->pgptr + logptr->body.soffset, logptr->body.slength);
}

int lrec_pg_compact_redo(LogRec *logrec)
{
    PGCompactLog *logptr = (PGCompactLog*)logrec;
    Page         *pgptr;

    pgptr = buf_fix(logptr->body.pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] page compact - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        char *logdata = logptr->body.data;
        /* redo the action */
        memcpy((char*)pgptr + logptr->body.doffset, logdata, logptr->body.dlength);
        logdata += logptr->body.dlength;
        memcpy((char*)pgptr + logptr->body.soffset, logdata, logptr->body.slength);
        pgptr->header.scat_frslot.offset = 0;
        pgptr->header.scat_frslot.length = 0;
        pgptr->header.cont_offset = (logptr->body.doffset + logptr->body.dlength);

        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);
    return 0;
}

void lrec_pg_compact_print(LogRec *logrec)
{
    LOGHDR        *hdr  = &logrec->header;
    PGCompactData *body = &((PGCompactLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            body->pgid.pfxid, body->pgid.flnum, body->pgid.pgnum);
}

/* Page Alloc Log Record */
void lrec_pg_alloc_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_PG_ALLOC;
    logrec->header.logtype = LOG_REDO_UNDO;
    logrec->header.cmttype = LOG_ACT_NORMAL;
}

void lrec_pg_alloc_write(LogRec *logrec, char *bufptr)
{
    PGAllocLog *logptr = (PGAllocLog *)logrec;
    memcpy(bufptr, &logptr->body, logrec->header.length);
}

int lrec_pg_alloc_redo(LogRec *logrec)
{
    PGAllocLog *logptr = (PGAllocLog *)logrec;

    if (pfx_redo_pgalloc_v2(&logptr->body.pgid[0], logptr->body.pgcnt, logptr->body.partial,
                            logptr->header.self_lsn) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] page alloc fail\n");
        return -1;
    }
    return 0;
}

int lrec_pg_alloc_undo(LogRec *logrec, void *tran)
{
    //PGAllocLog *logptr = (PGAllocLog *)logrec;
    return 0;
}

void lrec_pg_alloc_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    PGAllocData *body = &((PGAllocLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %s %u (%u|%u|%u) %s\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            get_page_type_text(body->pgtype), body->pgcnt,
            body->pgid[0].pfxid, body->pgid[0].flnum, body->pgid[0].pgnum,
            (body->partial ? "partial" : ""));
}

/* Page Free Log Record */
void lrec_pg_free_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_PG_FREE;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_ACT_NORMAL;
}

void lrec_pg_free_write(LogRec *logrec, char *bufptr)
{
    PGFreeLog *logptr = (PGFreeLog *)logrec;
    memcpy(bufptr, &logptr->body, logrec->header.length);
}

int lrec_pg_free_redo(LogRec *logrec)
{
    PGFreeLog *logptr = (PGFreeLog *)logrec;

    if (pfx_redo_pgfree_v2(&logptr->body.pgid[0], logptr->body.pgcnt, logptr->body.partial,
                           logptr->header.self_lsn) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] page free fail\n");
        return -1;
    }
    return 0;
}

void lrec_pg_free_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    PGFreeData  *body = &((PGFreeLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %s %u (%u|%u|%u) %s\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            get_page_type_text(body->pgtype), body->pgcnt,
            body->pgid[0].pfxid, body->pgid[0].flnum, body->pgid[0].pgnum,
            (body->partial ? "partial" : ""));
}

/* Item Alloc Log Record */
void lrec_it_alloc_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_IT_ALLOC;
    logrec->header.logtype = LOG_REDO_UNDO;
    logrec->header.cmttype = LOG_ACT_NORMAL;
}

void lrec_it_alloc_write(LogRec *logrec, char *bufptr)
{
    ITAllocLog *logptr = (ITAllocLog*)logrec;
    memcpy(bufptr, &logptr->body, logptr->header.length);
}

int lrec_it_alloc_redo(LogRec *logrec)
{
    ITAllocLog *logptr = (ITAllocLog*)logrec;
    Page       *pgptr;
    PageID      pgid;

    if (logptr->body.method < 1 || logptr->body.method > 3) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item alloc - method(%u) fail\n",
                    logptr->body.method);
        return -1; /* redo fail */
    }

    /* get pageid from itemid */
    GET_PAGEID_FROM_ITEMID(&logptr->body.itmid, &pgid);

    /* logptr->body.method *****
     *  1: allocated an item from warm free page
     *  2: allocated an item from partial free page
     *  3: allocated an item from total free page
     */
    if (logptr->body.method > 1) { /* 2 or 3 */
        /* check page bitmap status */
        if (pfx_redo_pgalloc(pgid, logptr->body.pgspace, logptr->header.self_lsn) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item alloc - page alloc fail\n");
            return -1;
        }
    }

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item alloc - fix page fail\n");
        return -1;
    }
    if (PAGEID_IS_NULL(&pgptr->header.self_pgid)) {
        /* The page is absent in disk.
         * so. initialize the page as new data page.
         */
        dpage_init(pgptr, pgid);
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        ItemID itmid;
        void  *itptr = dpage_alloc_slot(pgptr, logptr->body.size, &itmid);
        assert(itptr != NULL && ITEMID_IS_EQ(&itmid, &logptr->body.itmid));

        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* reflect the redo action into tran table */
    return 0;
}

int lrec_it_alloc_undo(LogRec *logrec, void *tran)
{
    ITAllocLog *logptr = (ITAllocLog*)logrec;
    ItemDesc    itdbuf;
    Page       *pgptr;
    PageID      pgid;
    uint32_t    slotid;

    /* get pageid and slotid from itemid */
    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.itmid, &pgid, &slotid);

    pgptr = buf_pin_only(pgid, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[UNDO] item alloc - fix page fail\n");
        return -1;
    }

    itdbuf.ptr = dpage_find_slot(pgptr, slotid);
    if (itdbuf.ptr == NULL) {
        buf_unpin_only(pgptr);
        logger->log(EXTENSION_LOG_WARNING, NULL, "[UNDO] item alloc - find slot fail\n");
        return -1;
    }

    itdbuf.iid = logptr->body.itmid;
    itdbuf.ofl = NULL;
    itdbuf.trx = tran;

    /* free the item */
    int pfxid = PREFIXID_FROM_ITEMID(&logptr->body.itmid);
    pfx_data_slot_free(pfxid, &itdbuf);
    //item_undo_free(&itdbuf);
    return 0;
}

void lrec_it_alloc_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    ITAllocData *body = &((ITAllocLog*)logrec)->body;
    PageID       pageid;
    uint32_t     slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&body->itmid, &pageid, &slotid);
    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u %u m(%u) s(%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            pageid.pfxid, pageid.flnum, pageid.pgnum, slotid,
            body->size, body->method, body->pgspace);
}

/* Item Overflow Add Log Record */
void lrec_it_ovfadd_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_IT_OVFADD;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_ACT_NORMAL;
}

void lrec_it_ovfadd_write(LogRec *logrec, char *bufptr)
{
    ITOvfAddLog *logptr = (ITOvfAddLog*)logrec;
    uint32_t     loglen = offsetof(ITOvfAddData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->itptr, logptr->header.length - loglen);
}

int lrec_it_ovfadd_redo(LogRec *logrec)
{
    ITOvfAddLog *logptr = (ITOvfAddLog*)logrec;
    Page        *pgptr;

    /* check page bitmap status */
    if (pfx_redo_pgalloc(logptr->body.pgid, PAGE_SPACE_FULL, logptr->header.self_lsn) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item overflow - page alloc fail\n");
        return -1;
    }

    pgptr = buf_fix(logptr->body.pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item overflow - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        void      *oitp;
        ItemID     osid;
        uint32_t   page_body_size = config->sq.data_page_size - sizeof(PGHDR);
        uint32_t   ovfl_item_leng = logptr->header.length - offsetof(ITOvfAddData,data);

        opage_init(pgptr, logptr->body.pgid, page_body_size);
        oitp = opage_alloc_slot(pgptr, page_body_size, &osid);
        memcpy((void*)oitp, logptr->body.data, ovfl_item_leng);

        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* reflect the redo action into tran table */
    return 0;
}

void lrec_it_ovfadd_print(LogRec *logrec)
{
    LOGHDR       *hdr  = &logrec->header;
    ITOvfAddData *body = &((ITOvfAddLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            body->pgid.pfxid, body->pgid.flnum, body->pgid.pgnum);
}

/* Item Overflow Delete Log Record */
void lrec_it_ovfdel_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_IT_OVFDEL;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_ACT_NORMAL;
}

void lrec_it_ovfdel_write(LogRec *logrec, char *bufptr)
{
    ITOvfDelLog *logptr = (ITOvfDelLog*)logrec;
    memcpy(bufptr, logptr->osids, logptr->header.length);
}

int lrec_it_ovfdel_redo(LogRec *logrec)
{
    ITOvfDelLog *logptr = (ITOvfDelLog*)logrec;
    ItemID  *ovfl_iids = (ItemID*)&logptr->body.data;
    PageID   pgid;
    uint32_t slotid;
    int      novfl = logptr->header.length / sizeof(ItemID);

    /* check page bitmap status */
    for (int i = 0; i < novfl; i++) {
        GET_PAGEID_SLOTID_FROM_ITEMID(ovfl_iids+i, &pgid, &slotid);
        if (pfx_redo_pgfree(pgid, logptr->header.self_lsn) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item overflow delete - page free fail\n");
            return -1;
        }
    }

    /* reflect the redo action into tran table */
    return 0;
}

void lrec_it_ovfdel_print(LogRec *logrec)
{
    LOGHDR       *hdr  = &logrec->header;
    ITOvfDelData *body = &((ITOvfDelLog*)logrec)->body;
    ItemID        itemid;
    PageID        pageid;
    uint32_t      slotid;
    uint32_t      ovfcnt= (hdr->length / sizeof(ItemID));

    /* header + < count, osid > : only 1 osid is printed */
    itemid = *(ItemID*)&body->data[0];
    GET_PAGEID_SLOTID_FROM_ITEMID(&itemid, &pageid, &slotid);

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %u (%u|%u|%u)-%u\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            ovfcnt, pageid.pfxid, pageid.flnum, pageid.pgnum, slotid);
}


/* Item Free Log Record */
void lrec_it_free_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_IT_FREE;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_ACT_COMMIT; /* commit log */
}

void lrec_it_free_write(LogRec *logrec, char *bufptr)
{
    ITFreeLog *logptr = (ITFreeLog*)logrec;
    memcpy(bufptr, &logptr->body, logptr->header.length);
}

int lrec_it_free_redo(LogRec *logrec)
{
    ITFreeLog *logptr = (ITFreeLog*)logrec;
    Page      *pgptr;
    PageID     pgid;
    uint32_t   slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.itmid, &pgid, &slotid);

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item free - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        (void)dpage_free_slot(pgptr, slotid);
        if (logptr->body.empty) {
            assert(dpage_is_empty(pgptr));
            dpage_clear_space(pgptr);
        } else {
            if (logptr->body.compact) {
                int ret = dpage_compact_space(pgptr);
                assert(ret == 1); /* compaction is performed */
            }
        }
        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    assert(logptr->body.free == false || logptr->body.partial == false);
    if (logptr->body.free) {
        if (pfx_redo_pgfree(pgid, logptr->header.self_lsn) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item free - page free fail\n");
            return -1;
        }
    }
    if (logptr->body.partial) {
        if (pfx_redo_pgpartial(pgid, logptr->header.self_lsn) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item free - page partial fail\n");
            return -1;
        }
    }

    /* reflect the redo action into tran table */
    return 0;
}

void lrec_it_free_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    ITFreeData  *body = &((ITFreeLog*)logrec)->body;
    PageID       pageid;
    uint32_t     slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&body->itmid, &pageid, &slotid);
    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u %s %s %s %s\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            pageid.pfxid, pageid.flnum, pageid.pgnum, slotid,
            (body->empty ? "empty" : "none"),
            (body->free ? "free" : "none"),
            (body->compact ? "compact" : "none"),
            (body->partial ? "partial" : "none"));
}

/* Item Link Log Record */
void lrec_it_link_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_IT_LINK;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_ACT_COMMIT; /* commit log */
}

void lrec_it_link_write(LogRec *logrec, char *bufptr)
{
    ITLinkLog *logptr = (ITLinkLog*)logrec;
    uint32_t   loglen = offsetof(ITLinkData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->itptr, logptr->header.length - loglen);
}

int lrec_it_link_redo(LogRec *logrec)
{
    ITLinkLog *logptr = (ITLinkLog*)logrec;
    Page      *pgptr;
    PageID     pgid;
    uint32_t   slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.itmid, &pgid, &slotid);

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item link - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        data_item *ditp = dpage_find_slot(pgptr, slotid);
        memcpy(ditp, logptr->body.data, logptr->header.length - offsetof(ITLinkData,data));
        /* clear the refcount of data_item.
         * The logged data item might have refcount larger than 0.
         * So, it must be cleared to 0.
         */
        ditp->refcount = 0; /* clear refcount */
        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* update prefix item stats */
    int pfxid = PREFIXID_FROM_PAGEID(&pgid);
    pfx_redo_item_stats(pfxid, item_get_type((data_item*)logptr->body.data),
                        logptr->body.items_cnt, logptr->body.items_len);

    /* reflect the redo action into tran table */
    return 0;
}

void lrec_it_link_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    ITLinkData  *body = &((ITLinkLog*)logrec)->body;
    PageID       pageid;
    uint32_t     slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&body->itmid, &pageid, &slotid);
    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            pageid.pfxid, pageid.flnum, pageid.pgnum, slotid);
}

/* Item Unlink Log Record */
void lrec_it_unlink_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_IT_UNLINK;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_ACT_NORMAL;
}

void lrec_it_unlink_write(LogRec *logrec, char *bufptr)
{
    ITUnlinkLog *logptr = (ITUnlinkLog*)logrec;
    memcpy(bufptr, &logptr->body, logptr->header.length);
}

int lrec_it_unlink_redo(LogRec *logrec)
{
    ITUnlinkLog *logptr = (ITUnlinkLog*)logrec;
    Page        *pgptr;
    PageID       pgid;
    uint32_t     slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.itmid, &pgid, &slotid);

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item unlink - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        data_item *ditp = dpage_find_slot(pgptr, slotid);
        ditp->iflag &= ~ITEM_LINKED; /* clear ITEM_LINKED flag */
        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* update prefix item stats */
    int pfxid = PREFIXID_FROM_PAGEID(&pgid);
    pfx_redo_item_stats(pfxid, (int)logptr->body.ittype,
                        logptr->body.items_cnt, logptr->body.items_len);

    /* reflect the redo action into tran table */
    return 0;
}

void lrec_it_unlink_print(LogRec *logrec)
{
    LOGHDR       *hdr  = &logrec->header;
    ITUnlinkData *body = &((ITUnlinkLog*)logrec)->body;
    PageID        pageid;
    uint32_t      slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&body->itmid, &pageid, &slotid);
    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            pageid.pfxid, pageid.flnum, pageid.pgnum, slotid);
}

/* Item Replace Log Record */
void lrec_it_replace_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_IT_REPLACE;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_ACT_NORMAL;
}

void lrec_it_replace_write(LogRec *logrec, char *bufptr)
{
    ITReplaceLog *logptr = (ITReplaceLog*)logrec;
    uint32_t      loglen = offsetof(ITReplaceData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->itptr, logptr->body.new_itlen);
}

int  lrec_it_replace_redo(LogRec *logrec)
{
    ITReplaceLog *logptr = (ITReplaceLog*)logrec;
    data_item    *ditptr;
    Page         *old_pgptr;
    Page         *new_pgptr;
    PageID        old_pgid;
    PageID        new_pgid;
    uint32_t      old_slotid;
    uint32_t      new_slotid;
    bool          same_page;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.old_itmid, &old_pgid, &old_slotid);
    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.new_itmid, &new_pgid, &new_slotid);

    if (PAGEID_IS_EQ(&old_pgid, &new_pgid))
        same_page = true;
    else
        same_page = false;

    old_pgptr = buf_fix(old_pgid, BUF_MODE_WRITE, false, false);
    if (old_pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item replace - fix old page fail\n");
        return -1;
    }

    if (same_page != true) {
        new_pgptr = buf_fix(new_pgid, BUF_MODE_WRITE, false, false);
        if (new_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item replace - fix new page fail\n");
            buf_unfix(old_pgptr);
            return -1;
        }

        /* new item */
        if (LOGSN_IS_LT(&new_pgptr->header.pglsn, &logptr->header.self_lsn)) {
            /* redo the action */
            ditptr = dpage_find_slot(new_pgptr, new_slotid);
            memcpy((void*)ditptr, logptr->body.data, logptr->body.new_itlen);
            /* clear the refcount of data_item.
             * The logged data item might have refcount larger than 0.
             * So, it must be cleared to 0.
             */
            ditptr->refcount = 0; /* clear refcount */
            buf_set_lsn(new_pgptr, logptr->header.self_lsn); /* set PageLSN */
            buf_set_dirty(new_pgptr); /* set buffer page dirty */
        }
        buf_unfix(new_pgptr);
    }

    /* old item */
    if (LOGSN_IS_LT(&old_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        if (same_page == true) {
            ditptr = dpage_find_slot(old_pgptr, new_slotid);
            memcpy((void*)ditptr, logptr->body.data, logptr->body.new_itlen);
            /* clear the refcount of data_item.
             * The logged data item might have refcount larger than 0.
             * So, it must be cleared to 0.
             */
            ditptr->refcount = 0; /* clear refcount */
        }
        ditptr = dpage_find_slot(old_pgptr, old_slotid);
        ditptr->iflag &= ~ITEM_LINKED; /* clear ITEM_LINKED flag */
        buf_set_lsn(old_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(old_pgptr); /* set buffer page dirty */
    }
    buf_unfix(old_pgptr);

    /* update prefix item stats */
    if (logptr->body.items_len != 0) {
        int pfxid = PREFIXID_FROM_PAGEID(&new_pgid);
        pfx_redo_item_stats(pfxid, item_get_type((data_item*)logptr->body.data),
                            logptr->body.items_cnt, logptr->body.items_len);
    }

    /* reflect the redo action into tran table */
    return 0;
}

void lrec_it_replace_print(LogRec *logrec)
{
    LOGHDR        *hdr  = &logrec->header;
    ITReplaceData *body = &((ITReplaceLog*)logrec)->body;
    PageID         old_pageid;
    PageID         new_pageid;
    uint32_t       old_slotid;
    uint32_t       new_slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&body->old_itmid, &old_pageid, &old_slotid);
    GET_PAGEID_SLOTID_FROM_ITEMID(&body->new_itmid, &new_pageid, &new_slotid);
    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u (%u|%u|%u)-%u\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            old_pageid.pfxid, old_pageid.flnum, old_pageid.pgnum, old_slotid,
            new_pageid.pfxid, new_pageid.flnum, new_pageid.pgnum, new_slotid);
}

/* Item DelMark Log Record */
void lrec_it_delmark_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_IT_DELMARK;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_ACT_COMMIT; /* commit log */
}

void lrec_it_delmark_write(LogRec *logrec, char *bufptr)
{
    ITDelMarkLog *logptr = (ITDelMarkLog*)logrec;
    memcpy(bufptr, &logptr->body, logptr->header.length);
}

int lrec_it_delmark_redo(LogRec *logrec)
{
    ITDelMarkLog *logptr = (ITDelMarkLog*)logrec;
    Page         *pgptr;
    PageID        pgid;
    uint32_t      slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.itmid, &pgid, &slotid);
    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item delmark - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logrec->header.self_lsn)) {
        /* redo the action */
        data_item *dit = dpage_find_slot(pgptr, slotid);
        dit->iflag |= ITEM_DELMARK;
        buf_set_lsn(pgptr, logrec->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* reflect the redo action into tran table */
    return 0;
}

void lrec_it_delmark_print(LogRec *logrec)
{
    LOGHDR        *hdr  = &logrec->header;
    ITDelMarkData *body = &((ITDelMarkLog*)logrec)->body;
    PageID         pageid;
    uint32_t       slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&body->itmid, &pageid, &slotid);
    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            pageid.pfxid, pageid.flnum, pageid.pgnum, slotid);
}


/* Item Update Log Record (by add_delta operation) */
void lrec_it_update_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_IT_UPDATE;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_ACT_COMMIT; /* commit log */
}

void lrec_it_update_write(LogRec *logrec, char *bufptr)
{
    ITUpdateLog *logptr = (ITUpdateLog*)logrec;
    uint32_t     loglen = offsetof(ITUpdateData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->value, logptr->body.nbytes);
}

int lrec_it_update_redo(LogRec *logrec)
{
    ITUpdateLog *logptr = (ITUpdateLog*)logrec;
    Page        *pgptr;
    PageID       pgid;
    uint32_t     slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.itmid, &pgid, &slotid);
    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item update - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logrec->header.self_lsn)) {
        /* redo the action */
        data_item *dit = dpage_find_slot(pgptr, slotid);
        memcpy(item_get_data(dit), logptr->body.data, logptr->body.nbytes);
        if (dit->nbytes != logptr->body.nbytes)
            dit->nbytes = logptr->body.nbytes;
        item_set_cas(dit, logptr->body.cas);

        buf_set_lsn(pgptr, logrec->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* reflect the redo action into tran table */
    return 0;
}

void lrec_it_update_print(LogRec *logrec)
{
    LOGHDR        *hdr  = &logrec->header;
    ITUpdateData  *body = &((ITUpdateLog*)logrec)->body;
    PageID         pageid;
    uint32_t       slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&body->itmid, &pageid, &slotid);
    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            pageid.pfxid, pageid.flnum, pageid.pgnum, slotid);
}

/* Item Attribute Log Record */
void lrec_it_attr_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_IT_ATTR;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_ACT_COMMIT; /* commit log */
}

void lrec_it_attr_write(LogRec *logrec, char *bufptr)
{
    ITAttrLog *logptr = (ITAttrLog*)logrec;
    uint32_t   loglen = offsetof(ITAttrData, data);
    char      *dptr;

    memcpy(bufptr, &logptr->body, loglen);
    if (logptr->body.itemat_length > 0) {
        dptr = (char*)logptr->itptr + logptr->body.itemat_offset;
        memcpy(bufptr + loglen, dptr, logptr->body.itemat_length);
        loglen += logptr->body.itemat_length;
    }
    if (logptr->body.collat_length > 0) {
        dptr = (char*)logptr->itptr + logptr->body.collat_offset;
        memcpy(bufptr + loglen, dptr, logptr->body.collat_length);
    }
}

int lrec_it_attr_redo(LogRec *logrec)
{
    ITAttrLog *logptr = (ITAttrLog*)logrec;
    Page      *pgptr;
    PageID     pgid;
    uint32_t   slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.itmid, &pgid, &slotid);

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] item attr - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        char *ditp = dpage_find_slot(pgptr, slotid);
        if (logptr->body.itemat_length > 0) {
            memcpy(ditp + logptr->body.itemat_offset, logptr->body.data, logptr->body.itemat_length);
        }
        if (logptr->body.collat_length > 0) {
            memcpy(ditp + logptr->body.collat_offset, logptr->body.data + logptr->body.itemat_length,
                   logptr->body.itemat_length);
        }
        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* reflect the redo action into tran table */
    return 0;
}

void lrec_it_attr_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    ITAttrData  *body = &((ITAttrLog*)logrec)->body;
    PageID       pageid;
    uint32_t     slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&body->itmid, &pageid, &slotid);
    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            pageid.pfxid, pageid.flnum, pageid.pgnum, slotid);
}

/* BTree Root Build Log Record */
void lrec_bt_rtbuild_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_BT_RTBUILD;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_bt_rtbuild_write(LogRec *logrec, char *bufptr)
{
    BTRtBldLog *logptr = (BTRtBldLog*)logrec;
    memcpy(bufptr, &logptr->body, logptr->header.length);
}

int lrec_bt_rtbuild_redo(LogRec *logrec)
{
    BTRtBldLog *logptr = (BTRtBldLog*)logrec;
    Page       *pgptr;

    /* index page allocation redo */
    if (pfx_redo_pgalloc(logptr->body.pgid, PAGE_SPACE_FULL, logptr->header.self_lsn) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree root build - page alloc fail\n");
        return -1;
    }

    /* index root page redo */
    pgptr = buf_fix(logptr->body.pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree root build - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        xpage_init(pgptr, logptr->body.pgid, 0);
        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* update prefix meta info */
    int pfxid = PREFIXID_FROM_PAGEID(&logptr->body.pgid);
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    pfdesc->d.bndx_root_pgid = logptr->body.pgid;
    pfdesc->d.bndx_global_depth = 0;

    return 0;
}

void lrec_bt_rtbuild_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    BTRtBldData *body = &((BTRtBldLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            body->pgid.pfxid, body->pgid.flnum, body->pgid.pgnum);
}


/* BTree Root Split Log Record */
void lrec_bt_rtsplit_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_BT_RTSPLIT;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_bt_rtsplit_write(LogRec *logrec, char *bufptr)
{
    BTRtSplLog *logptr = (BTRtSplLog*)logrec;
    uint32_t    loglen = offsetof(BTRtSplData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, (char*)logptr->spl_pgptr + sizeof(PGHDR), logptr->body.spl_dleng);
    loglen += logptr->body.spl_dleng;
    memcpy(bufptr + loglen, (char*)logptr->spl_pgptr + config->sq.data_page_size - logptr->body.spl_sleng,
           logptr->body.spl_sleng);
    loglen += GET_8_ALIGN_SIZE(logptr->body.spl_sleng);
    memcpy(bufptr + loglen, (char*)logptr->lft_pgptr + sizeof(PGHDR), logptr->body.lft_dleng);
    loglen += logptr->body.lft_dleng;
    memcpy(bufptr + loglen, (char*)logptr->lft_pgptr + config->sq.data_page_size - logptr->body.lft_sleng,
           logptr->body.lft_sleng);
    loglen += GET_8_ALIGN_SIZE(logptr->body.lft_sleng);
    memcpy(bufptr + loglen, (char*)logptr->rht_pgptr + sizeof(PGHDR), logptr->body.rht_dleng);
    loglen += logptr->body.rht_dleng;
    memcpy(bufptr + loglen, (char*)logptr->rht_pgptr + config->sq.data_page_size - logptr->body.rht_sleng,
           logptr->body.rht_sleng);
}

int lrec_bt_rtsplit_redo(LogRec *logrec)
{
    BTRtSplLog *logptr = (BTRtSplLog*)logrec;
    Page       *spl_pgptr = NULL;
    Page       *lft_pgptr = NULL;
    Page       *rht_pgptr = NULL;
    int         ret = 0;

    /* index page allocation redo */
    if (pfx_redo_pgalloc(logptr->body.lft_pgid, PAGE_SPACE_FULL, logptr->header.self_lsn) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree root split - left page alloc fail\n");
        return -1;
    }
    if (pfx_redo_pgalloc(logptr->body.rht_pgid, PAGE_SPACE_FULL, logptr->header.self_lsn) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree root split - right page alloc fail\n");
        return -1;
    }

    do {
        spl_pgptr = buf_fix(logptr->body.spl_pgid, BUF_MODE_WRITE, false, false);
        if (spl_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree root split - fix root page fail\n");
            ret = -1; break;
        }
        lft_pgptr = buf_fix(logptr->body.lft_pgid, BUF_MODE_WRITE, false, false);
        if (lft_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree root split - fix left child page fail\n");
            ret = -1; break;
        }
        rht_pgptr = buf_fix(logptr->body.rht_pgid, BUF_MODE_WRITE, false, false);
        if (rht_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree root split - fix right child page fail\n");
            ret = -1; break;
        }
    } while(0);

    if (ret != 0) {
        if (lft_pgptr != NULL) buf_unfix(lft_pgptr);
        if (spl_pgptr != NULL) buf_unfix(spl_pgptr);
        return ret;
    }

    /* split(root) page */
    if (LOGSN_IS_LT(&spl_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        char *logdata = logptr->body.data;
        xpage_redo_build(spl_pgptr, logdata, logptr->body.spl_dleng, logptr->body.spl_sleng);
        spl_pgptr->header.pgdepth = logptr->body.spl_depth;
        buf_set_lsn(spl_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(spl_pgptr); /* set buffer page dirty */
    }

    /* left child page */
    if (LOGSN_IS_LT(&lft_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        char *logdata = logptr->body.data
                      + logptr->body.spl_dleng + GET_8_ALIGN_SIZE(logptr->body.spl_sleng);
        xpage_init(lft_pgptr, logptr->body.lft_pgid, logptr->body.spl_depth-1);
        xpage_redo_build(lft_pgptr, logdata, logptr->body.lft_dleng, logptr->body.lft_sleng);
        if (lft_pgptr->header.pgdepth == 0) {
            lft_pgptr->header.next_pgid = logptr->body.rht_pgid;
        }
        buf_set_lsn(lft_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(lft_pgptr); /* set buffer page dirty */
    }

    /* right child page */
    if (LOGSN_IS_LT(&rht_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        char *logdata = logptr->body.data
                      + logptr->body.spl_dleng + GET_8_ALIGN_SIZE(logptr->body.spl_sleng)
                      + logptr->body.lft_dleng + GET_8_ALIGN_SIZE(logptr->body.lft_sleng);
        xpage_init(rht_pgptr, logptr->body.rht_pgid, logptr->body.spl_depth-1);
        xpage_redo_build(rht_pgptr, logdata, logptr->body.rht_dleng, logptr->body.rht_sleng);
        if (rht_pgptr->header.pgdepth == 0)
            rht_pgptr->header.prev_pgid = logptr->body.lft_pgid;
        buf_set_lsn(rht_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(rht_pgptr); /* set buffer page dirty */
    }

    buf_unfix(spl_pgptr);
    buf_unfix(lft_pgptr);
    buf_unfix(rht_pgptr);

    /* update prefix meta info */
    int pfxid = PREFIXID_FROM_PAGEID(&logptr->body.spl_pgid);
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    pfdesc->d.bndx_global_depth = logptr->body.spl_depth;

    return 0;
}

void lrec_bt_rtsplit_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    BTRtSplData *body = &((BTRtSplLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %u r(%u|%u|%u) l(%u|%u|%u) r(%u|%u|%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            body->spl_depth,
            body->spl_pgid.pfxid, body->spl_pgid.flnum, body->spl_pgid.pgnum,
            body->lft_pgid.pfxid, body->lft_pgid.flnum, body->lft_pgid.pgnum,
            body->rht_pgid.pfxid, body->rht_pgid.flnum, body->rht_pgid.pgnum);
}

/* BTree Page Split Log Record */
void lrec_bt_pgsplit_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_BT_PGSPLIT;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_bt_pgsplit_write(LogRec *logrec, char *bufptr)
{
    BTPgSplLog *logptr = (BTPgSplLog*)logrec;
    uint32_t    loglen = offsetof(BTPgSplData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->upp_itptr, logptr->body.upp_itemsz);
    loglen += logptr->body.upp_itemsz;
    memcpy(bufptr + loglen, (char*)logptr->rht_pgptr + sizeof(PGHDR), logptr->body.rht_dleng);
    loglen += logptr->body.rht_dleng;
    memcpy(bufptr + loglen, (char*)logptr->rht_pgptr + config->sq.data_page_size - logptr->body.rht_sleng,
           logptr->body.rht_sleng);
}

int lrec_bt_pgsplit_redo(LogRec *logrec)
{
    BTPgSplLog *logptr = (BTPgSplLog*)logrec;
    Page       *upp_pgptr = NULL;
    Page       *spl_pgptr = NULL;
    Page       *rht_pgptr = NULL;
    Page       *nxt_pgptr = NULL;
    int         ret = 0;

    /* index page allocation redo */
    if (pfx_redo_pgalloc(logptr->body.rht_pgid, PAGE_SPACE_FULL, logptr->header.self_lsn) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree page split - page alloc fail\n");
        return -1;
    }

    do {
        upp_pgptr = buf_fix(logptr->body.upp_pgid, BUF_MODE_WRITE, false, false);
        if (upp_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree page split - fix upper page fail\n");
            ret = -1; break;
        }
        spl_pgptr = buf_fix(logptr->body.spl_pgid, BUF_MODE_WRITE, false, false);
        if (spl_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree page split - fix split page fail\n");
            ret = -1; break;
        }
        rht_pgptr = buf_fix(logptr->body.rht_pgid, BUF_MODE_WRITE, false, false);
        if (rht_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree page split - fix right page fail\n");
            ret = -1; break;
        }
        if (PAGEID_IS_NULL(&logptr->body.nxt_pgid)) break;
        nxt_pgptr = buf_fix(logptr->body.nxt_pgid, BUF_MODE_WRITE, false, false);
        if (nxt_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree page split - fix next page fail\n");
            ret = -1; break;
        }
    } while(0);

    if (ret != 0) {
        if (rht_pgptr != NULL) buf_unfix(rht_pgptr);
        if (spl_pgptr != NULL) buf_unfix(spl_pgptr);
        if (upp_pgptr != NULL) buf_unfix(upp_pgptr);
        return ret;
    }

    /* split page */
    if (LOGSN_IS_LT(&spl_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        uint32_t slot_count = spl_pgptr->header.used_nslots;
        uint32_t half_count = slot_count / 2;
        if ((slot_count % 2) != 0) half_count += 1;
        assert(half_count < slot_count);
        /* truncate teh moved slots */
        xpage_truncate_slots(spl_pgptr, half_count, (slot_count-half_count));
        if (spl_pgptr->header.pgdepth == 0) {
            spl_pgptr->header.next_pgid = logptr->body.rht_pgid;
        }
        buf_set_lsn(spl_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(spl_pgptr); /* set buffer page dirty */
    }

    /* right page */
    if (LOGSN_IS_LT(&rht_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        char *logdata = logptr->body.data + logptr->body.upp_itemsz;
        xpage_init(rht_pgptr, logptr->body.rht_pgid, logptr->body.spl_depth);
        xpage_redo_build(rht_pgptr, logdata, logptr->body.rht_dleng, logptr->body.rht_sleng);
        if (rht_pgptr->header.pgdepth == 0) {
            rht_pgptr->header.prev_pgid = logptr->body.spl_pgid;
            rht_pgptr->header.next_pgid = logptr->body.nxt_pgid;
        }
        buf_set_lsn(rht_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(rht_pgptr); /* set buffer page dirty */
    }

    /* next page */
    if (nxt_pgptr != NULL && LOGSN_IS_LT(&nxt_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        nxt_pgptr->header.prev_pgid = logptr->body.rht_pgid;
        buf_set_lsn(nxt_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(nxt_pgptr); /* set buffer page dirty */
    }

    /* upper page */
    if (LOGSN_IS_LT(&upp_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        void *xit = xpage_alloc_slot(upp_pgptr, logptr->body.upp_slotid, logptr->body.upp_itemsz);
        memcpy(xit, logptr->body.data, logptr->body.upp_itemsz);
        buf_set_lsn(upp_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(upp_pgptr); /* set buffer page dirty */
    }

    /* unfix all index pages */
    if (nxt_pgptr != NULL) buf_unfix(nxt_pgptr);
    buf_unfix(spl_pgptr);
    buf_unfix(rht_pgptr);
    buf_unfix(upp_pgptr);

    return 0;
}

void lrec_bt_pgsplit_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    BTPgSplData *body = &((BTPgSplLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %u u(%u|%u|%u) s(%u|%u|%u) r(%u|%u|%u) n(%u|%u|%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            body->spl_depth,
            body->upp_pgid.pfxid, body->upp_pgid.flnum, body->upp_pgid.pgnum,
            body->spl_pgid.pfxid, body->spl_pgid.flnum, body->spl_pgid.pgnum,
            body->rht_pgid.pfxid, body->rht_pgid.flnum, body->rht_pgid.pgnum,
            body->nxt_pgid.pfxid, body->nxt_pgid.flnum, body->nxt_pgid.pgnum);
}

/* BTree Root Merge Log Record */
void lrec_bt_rtmerge_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_BT_RTMERGE;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_bt_rtmerge_write(LogRec *logrec, char *bufptr)
{
    BTRtMrgLog *logptr = (BTRtMrgLog*)logrec;
    uint32_t    loglen = offsetof(BTRtMrgData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, (char*)logptr->mrg_pgptr + sizeof(PGHDR), logptr->body.mrg_dleng);
    loglen += logptr->body.mrg_dleng;
    memcpy(bufptr + loglen, (char*)logptr->mrg_pgptr + config->sq.data_page_size - logptr->body.mrg_sleng,
           logptr->body.mrg_sleng);
}

int lrec_bt_rtmerge_redo(LogRec *logrec)
{
    BTRtMrgLog *logptr = (BTRtMrgLog*)logrec;
    Page       *mrg_pgptr = NULL;
    Page       *chd_pgptr = NULL;

    mrg_pgptr = buf_fix(logptr->body.mrg_pgid, BUF_MODE_WRITE, false, false);
    if (mrg_pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree root merge - fix root page fail\n");
        return -1;
    }
    chd_pgptr = buf_fix(logptr->body.chd_pgid, BUF_MODE_WRITE, false, false);
    if (chd_pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree root merge - fix child page fail\n");
        buf_unfix(mrg_pgptr);
        return -1;
    }

    /* child page */
    if (LOGSN_IS_LT(&chd_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        xpage_clear_space(chd_pgptr);
        PAGEID_SET_NULL(&chd_pgptr->header.self_pgid); /* mark free page */
        buf_set_lsn(chd_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(chd_pgptr); /* set buffer page dirty */
    }

    /* root page */
    if (LOGSN_IS_LT(&mrg_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        xpage_redo_build(mrg_pgptr, logptr->body.data, logptr->body.mrg_dleng, logptr->body.mrg_sleng);
        buf_set_lsn(mrg_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(mrg_pgptr); /* set buffer page dirty */
    }

    buf_unfix(chd_pgptr);
    buf_unfix(mrg_pgptr);

    /* index page free redo */
    if (pfx_redo_pgfree(logptr->body.chd_pgid, logptr->header.self_lsn) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree root merge - page free fail\n");
        return -1;
    }

    /* update prefix meta info */
    int pfxid = PREFIXID_FROM_PAGEID(&logptr->body.mrg_pgid);
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    pfdesc->d.bndx_global_depth = logptr->body.mrg_depth;

    return 0;
}

void lrec_bt_rtmerge_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    BTRtMrgData *body = &((BTRtMrgLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %u r(%u|%u|%u) c(%u|%u|%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            body->mrg_depth,
            body->mrg_pgid.pfxid, body->mrg_pgid.flnum, body->mrg_pgid.pgnum,
            body->chd_pgid.pfxid, body->chd_pgid.flnum, body->chd_pgid.pgnum);
}

/* BTree Page Merge Log Record */
void lrec_bt_pgmerge_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_BT_PGMERGE;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_bt_pgmerge_write(LogRec *logrec, char *bufptr)
{
    BTPgMrgLog *logptr = (BTPgMrgLog*)logrec;
    uint32_t    loglen = offsetof(BTPgMrgData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->idx_itptr, logptr->body.idx_itemsz);
    if (logptr->mrg_pgptr != NULL) {
        loglen += logptr->body.idx_itemsz;
        memcpy(bufptr + loglen, (char*)logptr->mrg_pgptr + sizeof(PGHDR), logptr->body.mrg_dleng);
        loglen += logptr->body.mrg_dleng;
        memcpy(bufptr + loglen, (char*)logptr->mrg_pgptr + config->sq.data_page_size - logptr->body.mrg_sleng,
               logptr->body.mrg_sleng);
    }
}

int lrec_bt_pgmerge_redo(LogRec *logrec)
{
    BTPgMrgLog *logptr = (BTPgMrgLog*)logrec;
    Page       *upp_pgptr = NULL;
    Page       *mrg_pgptr = NULL;
    Page       *oth_pgptr = NULL;
    Page       *opp_pgptr = NULL;
    int         ret = 0;

    do {
        upp_pgptr = buf_fix(logptr->body.upp_pgid, BUF_MODE_WRITE, false, false);
        if (upp_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree page merge - fix upper page fail\n");
            ret = -1; break;
        }
        mrg_pgptr = buf_fix(logptr->body.mrg_pgid, BUF_MODE_WRITE, false, false);
        if (mrg_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree page merge - fix merge page fail\n");
            ret = -1; break;
        }
        oth_pgptr = buf_fix(logptr->body.oth_pgid, BUF_MODE_WRITE, false, false);
        if (oth_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree page merge - fix other page fail\n");
            ret = -1; break;
        }
        if (PAGEID_IS_NULL(&logptr->body.opp_pgid)) break;
        opp_pgptr = buf_fix(logptr->body.opp_pgid, BUF_MODE_WRITE, false, false);
        if (opp_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree page merge - fix opposite page fail\n");
            ret = -1; break;
        }
    } while(0);

    if (ret != 0) {
        if (oth_pgptr != NULL) buf_unfix(oth_pgptr);
        if (mrg_pgptr != NULL) buf_unfix(mrg_pgptr);
        if (upp_pgptr != NULL) buf_unfix(upp_pgptr);
        return ret;
    }

    /* merge page */
    if (LOGSN_IS_LT(&mrg_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        if (logptr->body.mrg_type == BT_PGMRG_RHT_REBAL ||
            logptr->body.mrg_type == BT_PGMRG_LFT_REBAL) {
            /* rebalanced */
            char *logdata = logptr->body.data + logptr->body.idx_itemsz;
            assert(logptr->body.mrg_dleng > 0 && logptr->body.mrg_sleng > 0);
            xpage_redo_build(mrg_pgptr, logdata, logptr->body.mrg_dleng, logptr->body.mrg_sleng);
        } else { /* BT_PGMRG_RHT_MERGE | BT_PGMRG_LFT_MERGE */
            /* merged */
            xpage_clear_space(mrg_pgptr);
            PAGEID_SET_NULL(&mrg_pgptr->header.self_pgid); /* mark free page */
        }
        buf_set_lsn(mrg_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(mrg_pgptr); /* set buffer page dirty */
    }

    /* other page */
    if (LOGSN_IS_LT(&oth_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        if (logptr->body.mrg_type == BT_PGMRG_RHT_REBAL ||
            logptr->body.mrg_type == BT_PGMRG_LFT_REBAL) {
            /* rebalanced */
            uint32_t slot_count;
            uint32_t half_count;
            if (logptr->body.mrg_type == BT_PGMRG_RHT_REBAL) {
                slot_count = oth_pgptr->header.used_nslots;
                half_count = slot_count / 2;
                assert(half_count > 0);
                /* truncate the moved slots */
                xpage_truncate_slots(oth_pgptr, 0, half_count);
            } else {
                slot_count = oth_pgptr->header.used_nslots;
                half_count = slot_count / 2;
                if ((slot_count % 2) != 0) half_count += 1;
                assert(half_count < slot_count);
                /* truncate the moved slots */
                xpage_truncate_slots(oth_pgptr, half_count, (slot_count-half_count));
            }
        } else { /* BT_PGMRG_RHT_MERGE | BT_PGMRG_LFT_MERGE */
            /* merged */
            void *xit = xpage_alloc_slot(oth_pgptr, logptr->body.oth_slotid, logptr->body.idx_itemsz);
            memcpy(xit, logptr->body.data, logptr->body.idx_itemsz);
            if (logptr->body.mrg_depth == 0) {
                if (logptr->body.mrg_type == BT_PGMRG_RHT_MERGE) {
                    oth_pgptr->header.prev_pgid = logptr->body.opp_pgid;
                } else {
                    oth_pgptr->header.next_pgid = logptr->body.opp_pgid;
                }
            }
        }
        buf_set_lsn(oth_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(oth_pgptr); /* set buffer page dirty */
    }

    /* opposite page */
    if (opp_pgptr != NULL && LOGSN_IS_LT(&opp_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        assert(logptr->body.mrg_type == BT_PGMRG_RHT_MERGE ||
               logptr->body.mrg_type == BT_PGMRG_LFT_MERGE);
        if (logptr->body.mrg_type == BT_PGMRG_RHT_MERGE) {
            opp_pgptr->header.next_pgid = logptr->body.oth_pgid;
        } else {
            opp_pgptr->header.prev_pgid = logptr->body.oth_pgid;
        }
        buf_set_lsn(opp_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(opp_pgptr); /* set buffer page dirty */
    }

    /* upper page */
    if (LOGSN_IS_LT(&upp_pgptr->header.pglsn, &logptr->header.self_lsn)) {
        if (logptr->body.mrg_type == BT_PGMRG_RHT_REBAL ||
            logptr->body.mrg_type == BT_PGMRG_LFT_REBAL) {
            /* rebalanced */
            void *xit = xpage_realloc_slot(upp_pgptr, logptr->body.upp_slotid,
                                                      logptr->body.idx_itemsz);
            memcpy(xit, logptr->body.data, logptr->body.idx_itemsz);
        } else { /* BT_PGMRG_RHT_MERGE | BT_PGMRG_LFT_MERGE */
            /* merged */
            if (logptr->body.mrg_type == BT_PGMRG_RHT_MERGE) {
                void *xit = xpage_find_slot(upp_pgptr, logptr->body.upp_slotid);
                assert(memcmp(xit, (void*)&logptr->body.mrg_pgid, sizeof(PageID)) == 0);
                memcpy(xit, (void*)&logptr->body.oth_pgid, sizeof(PageID));
                xpage_free_slot(upp_pgptr, logptr->body.upp_slotid+1);
            } else {
                xpage_free_slot(upp_pgptr, logptr->body.upp_slotid);
            }
        }
        buf_set_lsn(upp_pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(upp_pgptr); /* set buffer page dirty */
    }

    /* unfix all index pages */
    if (opp_pgptr != NULL) buf_unfix(opp_pgptr);
    buf_unfix(oth_pgptr);
    buf_unfix(mrg_pgptr);
    buf_unfix(upp_pgptr);

    /* index page allocation redo */
    if (logptr->body.mrg_dleng == 0) { /* empty page */
        if (pfx_redo_pgfree(logptr->body.mrg_pgid, logptr->header.self_lsn) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree page merge - page free fail\n");
            return -1;
        }
    }

    return 0;
}

void lrec_bt_pgmerge_print(LogRec *logrec)
{
    LOGHDR      *hdr  = &logrec->header;
    BTPgMrgData *body = &((BTPgMrgLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %u %u u(%u|%u|%u) m(%u|%u|%u) n(%u|%u|%u) o(%u|%u|%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            body->mrg_depth, body->mrg_type,
            body->upp_pgid.pfxid, body->upp_pgid.flnum, body->upp_pgid.pgnum,
            body->mrg_pgid.pfxid, body->mrg_pgid.flnum, body->mrg_pgid.pgnum,
            body->oth_pgid.pfxid, body->oth_pgid.flnum, body->oth_pgid.pgnum,
            body->opp_pgid.pfxid, body->opp_pgid.flnum, body->opp_pgid.pgnum);
}

/* BTree Insert Log Record */
void lrec_bt_insert_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_BT_INSERT;
    logrec->header.logtype = LOG_REDO_UNDO;
    logrec->header.cmttype = LOG_ACT_NORMAL;
}

void lrec_bt_insert_write(LogRec *logrec, char *bufptr)
{
    BTInsertLog *logptr = (BTInsertLog*)logrec;
    uint32_t     loglen = offsetof(BTInsertData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->itptr, logptr->body.length);
}

int lrec_bt_insert_redo(LogRec *logrec)
{
    BTInsertLog *logptr = (BTInsertLog*)logrec;
    Page        *pgptr;
    PageID       pgid;
    uint32_t     slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.xitmid, &pgid, &slotid);

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree insert - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        void *xit = xpage_alloc_slot(pgptr, slotid, logptr->body.length);
        memcpy(xit, logptr->body.data, logptr->body.length);
        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* reflect the redo action into tran table */
    return 0;
}

int lrec_bt_insert_undo(LogRec *logrec, void *tran)
{
    BTInsertLog *logptr = (BTInsertLog*)logrec;
    Page        *pgptr;
    PageID       pgid;
    uint32_t     slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.xitmid, &pgid, &slotid);

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[UNDO] btree insert - fix page fail\n");
        return -1;
    }
    if (1) {
        /* undo the action */
        void *xit = xpage_alloc_slot(pgptr, slotid, logptr->body.length);
        memcpy(xit, logptr->body.data, logptr->body.length);
        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* reflect the redo action into tran table */
    return 0;
}

void lrec_bt_insert_print(LogRec *logrec)
{
    LOGHDR       *hdr  = &logrec->header;
    BTInsertData *body = &((BTInsertLog*)logrec)->body;
    ItemID       ditemid;
    PageID       dpageid, xpageid;
    uint32_t     dslotid, xslotid;

    ditemid = *(ItemID*)&body->data[0];
    GET_PAGEID_SLOTID_FROM_ITEMID(&ditemid, &dpageid, &dslotid);
    GET_PAGEID_SLOTID_FROM_ITEMID(&body->xitmid, &xpageid, &xslotid);

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u (%u|%u|%u)-%u\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            xpageid.pfxid, xpageid.flnum, xpageid.pgnum, xslotid,
            dpageid.pfxid, dpageid.flnum, dpageid.pgnum, dslotid);
}


/* BTree Delete Log Record */
void lrec_bt_delete_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_BT_DELETE;
    logrec->header.logtype = LOG_REDO_UNDO;
    logrec->header.cmttype = LOG_ACT_NORMAL;
}

void lrec_bt_delete_write(LogRec *logrec, char *bufptr)
{
    BTDeleteLog *logptr = (BTDeleteLog*)logrec;
    uint32_t     loglen = offsetof(BTDeleteData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->key, logptr->body.length);
}

int lrec_bt_delete_redo(LogRec *logrec)
{
    BTDeleteLog *logptr = (BTDeleteLog*)logrec;
    Page        *pgptr;
    PageID       pgid;
    uint32_t     slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.xitmid, &pgid, &slotid);

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree delete - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        xpage_free_slot(pgptr, slotid);
        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);

    /* reflect the redo action into tran table */
    return 0;
}

int lrec_bt_delete_undo(LogRec *logrec, void *tran)
{
    return 0;
}

void lrec_bt_delete_print(LogRec *logrec)
{
    LOGHDR       *hdr  = &logrec->header;
    BTDeleteData *body = &((BTDeleteLog*)logrec)->body;
    PageID       dpageid, xpageid;
    uint32_t     dslotid, xslotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&body->ditmid, &dpageid, &dslotid);
    GET_PAGEID_SLOTID_FROM_ITEMID(&body->xitmid, &xpageid, &xslotid);

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u (%u|%u|%u)-%u\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            xpageid.pfxid, xpageid.flnum, xpageid.pgnum, xslotid,
            dpageid.pfxid, dpageid.flnum, dpageid.pgnum, dslotid);
}

/* BTree Toggle Log Record */
void lrec_bt_toggle_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_BT_TOGGLE;
    logrec->header.logtype = LOG_REDO_UNDO;
    logrec->header.cmttype = LOG_ACT_NORMAL;
}

void lrec_bt_toggle_write(LogRec *logrec, char *bufptr)
{
    BTToggleLog *logptr = (BTToggleLog*)logrec;
    uint32_t     loglen = offsetof(BTToggleData, data);
    memcpy(bufptr, &logptr->body, loglen);
    memcpy(bufptr + loglen, logptr->key, logptr->body.length);
}

int lrec_bt_toggle_redo(LogRec *logrec)
{
    BTToggleLog *logptr = (BTToggleLog*)logrec;
    Page        *pgptr;
    PageID      pgid;
    uint32_t    slotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&logptr->body.xitmid, &pgid, &slotid);

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[REDO] btree toggle - fix page fail\n");
        return -1;
    }
    if (LOGSN_IS_LT(&pgptr->header.pglsn, &logptr->header.self_lsn)) {
        /* redo the action */
        void *xit = xpage_find_slot(pgptr, slotid);
        memcpy(xit, &logptr->body.new_ditmid, sizeof(ItemID));
        buf_set_lsn(pgptr, logptr->header.self_lsn); /* set PageLSN */
        buf_set_dirty(pgptr); /* set buffer page dirty */
    }
    buf_unfix(pgptr);
    return 0;
}

int lrec_bt_toggle_undo(LogRec *logrec, void *tran)
{
    return 0;
}

void lrec_bt_toggle_print(LogRec *logrec)
{
    LOGHDR       *hdr  = &logrec->header;
    BTToggleData *body = &((BTToggleLog*)logrec)->body;
    PageID       xpageid, odpageid, ndpageid;
    uint32_t     xslotid, odslotid, ndslotid;

    GET_PAGEID_SLOTID_FROM_ITEMID(&body->xitmid, &xpageid, &xslotid);
    GET_PAGEID_SLOTID_FROM_ITEMID(&body->old_ditmid, &odpageid, &odslotid);
    GET_PAGEID_SLOTID_FROM_ITEMID(&body->new_ditmid, &ndpageid, &ndslotid);

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - (%u|%u|%u)-%u (%u|%u|%u)-%u (%u|%u|%u)-%u\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            xpageid.pfxid, xpageid.flnum, xpageid.pgnum, xslotid,
            odpageid.pfxid, odpageid.flnum, odpageid.pgnum, odslotid,
            ndpageid.pfxid, ndpageid.flnum, ndpageid.pgnum, ndslotid);
}

/* Checkpoint Begin Log Record */

void lrec_chkpt_bgn_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_CHKPT_BGN;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_chkpt_bgn_write(LogRec *logrec, char *bufptr)
{
    return;
}

int lrec_chkpt_bgn_redo(LogRec *logrec)
{
    return 0;
}

void lrec_chkpt_bgn_print(LogRec *logrec)
{
    LOGHDR *hdr = &logrec->header;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u]\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset);
}


/* Checkpoint End Log Record */

void lrec_chkpt_end_init(LogRec *logrec)
{
    logrec->header.acttype = ACT_CHKPT_END;
    logrec->header.logtype = LOG_REDO_ONLY;
    logrec->header.cmttype = LOG_NTA_COMMIT;
}

void lrec_chkpt_end_write(LogRec *logrec, char *bufptr)
{
    ChkptEndLog *logptr = (ChkptEndLog*)logrec;
    memcpy(bufptr, &logptr->body, logptr->header.length);
}

int lrec_chkpt_end_redo(LogRec *logrec)
{
    return 0;
}

void lrec_chkpt_end_print(LogRec *logrec)
{
    LOGHDR       *hdr  = &logrec->header;
    ChkptEndData *body = &((ChkptEndLog*)logrec)->body;

    fprintf(stderr, "LSN[%u|%10u] - %"PRIu64" %s %s %s %6u [%u|%10u] - %"PRIu64" t(%u|%u) d(%u|%u)\n",
            hdr->self_lsn.filenum, hdr->self_lsn.roffset, hdr->tranid,
            get_log_logtype_text(hdr->logtype),
            get_log_acttype_text(hdr->acttype),
            get_log_cmttype_text(hdr->cmttype),
            hdr->length, hdr->unxt_lsn.filenum, hdr->unxt_lsn.roffset,
            body->nxt_tranid,
            body->min_trans_lsn.filenum, body->min_trans_lsn.roffset,
            body->min_dirty_lsn.filenum, body->min_dirty_lsn.roffset);
}

/* Log Record Manager */

void lrec_mgr_init(void *conf, void *srvr)
{
    server = (SERVER_HANDLE_V1 *)srvr;
    config = (struct config *)conf;
    logger = server->log->get_logger();

}

void lrec_mgr_final(void)
{
    return;
}
