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
#ifndef SQ_LOG_H
#define SQ_LOG_H

#include "sq_lrec.h"
#include "sq_tran.h"

#define ENABLE_RECOVERY

typedef enum _log_type {
    LOG_REDO_UNDO = 1,
    LOG_REDO_ONLY,
    LOG_UNDO_ONLY
} LogType;

typedef enum _action_type {
    /* Prefix Actions */
    ACT_PF_CREATE = 0,
    ACT_PF_DROP,
    /* File & Page Actions */
    ACT_FL_GROW,   /* file grow can contain file create semantic */
    ACT_PG_COMPACT,
    ACT_PG_ALLOC,
    ACT_PG_FREE,
    /* Item Actions */
    ACT_IT_ALLOC,
    ACT_IT_OVFADD,
    ACT_IT_OVFDEL,
    ACT_IT_FREE,
    ACT_IT_LINK,
    ACT_IT_UNLINK,
    ACT_IT_REPLACE,
    ACT_IT_DELMARK,
    ACT_IT_UPDATE,
    ACT_IT_ATTR,
    /* BTree Actions */
    ACT_BT_RTBUILD, /* btree root build */
    ACT_BT_RTSPLIT, /* btree root split */
    ACT_BT_PGSPLIT, /* btree page split */
    ACT_BT_RTMERGE, /* btree root merge */
    ACT_BT_PGMERGE, /* btree page merge */
    ACT_BT_INSERT,  /* btree insert */
    ACT_BT_DELETE,  /* btree delete */
    ACT_BT_TOGGLE,  /* btree toggle */
    /* Chkpt Actions */
    ACT_CHKPT_BGN,
    ACT_CHKPT_END,
    /* Others */
    ACT_MAX
} ActionType;

typedef enum _commit_type {
    LOG_NTA_COMMIT = 0, /* nested top action commit */
    LOG_ACT_BEGIN,      /* normal action begin */
    LOG_ACT_NORMAL,     /* normal action */
    LOG_ACT_COMMIT,     /* normal action commit */
    LOG_ACT_SINGLE      /* normal action single commit */
} CommitType;

struct log_conf {
    char       *log_path;
    uint32_t    log_buff_size;
    uint32_t    log_file_size;
    //uint32_t        log_page_size;
};

struct log_stat {
    LogSN       log_write_lsn;
    LogSN       log_flush_lsn;
    LogSN       log_fsync_lsn;
};

void log_record_init(LogRec *logrec, uint64_t tranid, uint8_t logtype);
void log_record_write(LogRec *logrec, Trans *tran);
int  log_record_read(LogSN lsn, LogRec *logrec);
void log_buffer_flush(LogSN *upto_lsn, bool sync);
void log_file_sync(LogSN *upto_lsn);

void log_get_write_lsn(LogSN *lsn);
void log_get_flush_lsn(LogSN *lsn);
void log_get_fsync_lsn(LogSN *lsn);
void log_stat_get(struct log_stat *stats);

int  log_mgr_init(void *conf, void *srvr);
void log_mgr_restart(void);
void log_mgr_checkpoint(void);
void log_mgr_final(void);

int  log_mgr_flush_thread_start(void);
void log_mgr_flush_thread_stop(void);
int  log_mgr_chkpt_thread_start(void);
void log_mgr_chkpt_thread_stop(void);
#endif
