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
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

#include "squall_config.h"
#include "sq_log.h"

/* transcation group commit structure */
typedef struct _tran_group_commit {
    uint32_t        waitcnt;    /* group commit wait count */
    bool            on_sync;    /* is syncing now ? true or false */
    pthread_mutex_t lock;       /* group commit mutex */
    pthread_cond_t  cond;       /* group commit conditional variable */
} tran_GROUP_COMMIT;

/* transaction global structure */
struct tran_global {
    pthread_mutex_t     tran_lock;
    uint64_t            nxt_tranid; /* the next tranid */
    Trans              *tran_table;
    int16_t             free_list;  /* free transaction list : -1(none), othewise(exist) */
    int16_t             used_head;
    int16_t             used_tail;
    uint16_t            imp_trans;  /* # of implicit transactions among cur_trans */
    uint16_t            cur_trans;  /* # of active transcations */
    uint16_t            max_trans;  /* tran table size */
    tran_GROUP_COMMIT   tran_gcommit;
    volatile bool       initialized;
};

static SERVER_HANDLE_V1 *server = NULL;
static struct config    *config = NULL;
static EXTENSION_LOGGER_DESCRIPTOR *logger = NULL;
static struct tran_global tran_gl;

static void do_tran_group_commit(Trans *tran)
{
    tran_GROUP_COMMIT *gcommit = &tran_gl.tran_gcommit;
    LogSN log_fsync_lsn;

    /* group commit is used
     * to make only one transaction to call log_file_sync and
     * to make other transcations to wait the file sync.
     */
    while (1) {
        log_get_fsync_lsn(&log_fsync_lsn);
        if (LOGSN_IS_GT(&log_fsync_lsn, &tran->last_lsn)) {
            break;
        }
        pthread_mutex_lock(&gcommit->lock);
        if (gcommit->on_sync == false) {
            gcommit->on_sync = true;
            pthread_mutex_unlock(&gcommit->lock);

            /* sync log file */
            log_file_sync(&tran->last_lsn);

            pthread_mutex_lock(&gcommit->lock);
            gcommit->on_sync = false;
            if (gcommit->waitcnt > 0) {
                pthread_cond_broadcast(&gcommit->cond);
            }
            pthread_mutex_unlock(&gcommit->lock);
            break;
        }
        while (gcommit->on_sync == true) {
            gcommit->waitcnt += 1;
            pthread_cond_wait(&gcommit->cond, &gcommit->lock);
            gcommit->waitcnt -= 1;
        }
        pthread_mutex_unlock(&gcommit->lock);
    }
#if 0 // COMMIT_VERSION_3
    while (1) {
        log_get_fsync_lsn(&log_fsync_lsn);
        if (LOGSN_IS_GT(&log_fsync_lsn, &tran->last_lsn)) {
            break;
        }
        if (pthread_mutex_trylock(&tran_gl.gcommit_lock) == 0) {
            log_get_fsync_lsn(&log_fsync_lsn);
            if (LOGSN_IS_LE(&log_fsync_lsn, &tran->last_lsn)) {
                log_file_sync(&tran->last_lsn);
            }
            pthread_mutex_unlock(&tran_gl.gcommit_lock);
            break;
        }
        pthread_mutex_lock(&tran_gl.gcommit_lock);
        pthread_mutex_unlock(&tran_gl.gcommit_lock);
    }
#endif
#if 0 // COMMIT_VERSION_2
    log_get_fsync_lsn(&log_fsync_lsn);
    if (LOGSN_IS_LE(&log_fsync_lsn, &tran->last_lsn)) {
        pthread_mutex_lock(&tran_gl.gcommit_lock);
        log_get_fsync_lsn(&log_fsync_lsn);
        if (LOGSN_IS_LE(&log_fsync_lsn, &tran->last_lsn)) {
            log_file_sync(&tran->last_lsn);
        }
        pthread_mutex_unlock(&tran_gl.gcommit_lock);
    }
#endif
#if 0 // COMMIT_VERSION_1
    pthread_mutex_lock(&tran_gl.gcommit_lock);
    log_get_fsync_lsn(&log_fsync_lsn);
    if (LOGSN_IS_LE(&log_fsync_lsn, &tran->last_lsn)) {
        log_file_sync(&tran->last_lsn);
    }
    pthread_mutex_unlock(&tran_gl.gcommit_lock);
#endif
#if 0 // OLD_SLEEP_BASED_GROUP_COMMIT
    int num_act_trans = tran_gl.cur_trans - 1; /* exclude current transaction */
    if (num_act_trans > 0) { /* other active transactions exist */
        /* max 5ms sleep for group commit */
        struct timespec sleep_time;
        sleep_time.tv_sec = 0;
        sleep_time.tv_nsec = ((5000/config->num_threads) * num_act_trans);
        /*****
        float intvl = 5000.0;
        intvl /= ((float)config->num_threads * sqrt((float)config->num_threads));
        intvl *= ((float)act_ntrans * sqrt((float)act_ntrans));
        sleep_time.tv_nsec = (int)intvl;
         *****/
        nanosleep(&sleep_time, NULL);
    }
#endif
}

static void do_tran_commit(Trans *tran)
{
#if 0
    TranCommitLog commit_log;

    lrec_trancommit_init(&commit_log);
    commit_log.header.length = 0;
    log_record_write(&commit_log);
#endif

    tran->state = TRAN_STATE_COMMIT;

    if (tran->implicit) {
        return; /* do nothing */
    }

    if (config->sq.tran_commit_mode != MEMORY_COMMIT)
    {
        LogSN log_flush_lsn;

        /* flush all transaction log records */
        log_get_flush_lsn(&log_flush_lsn);
        if (LOGSN_IS_LE(&log_flush_lsn, &tran->last_lsn)) {
            log_buffer_flush(&tran->last_lsn, false); /* no SYNC */
        }

        /* SYSTEM_COMMIT => nothing to do
         * DISK_COMMIT_EVERY_SECOND
         *   Refer to the actions of log flush thread.
         *   The thread calls fsync() on log files every 1 second.
         * DISK_COMMIT_EVERY_COMMAND
         *   Refer to below execution.
         */
        if (config->sq.tran_commit_mode == DISK_COMMIT_EVERY_COMMAND) {
            /* sync all transaction log record to disk */
            do_tran_group_commit(tran);
        }
    }
}

static void do_tran_abort(Trans *tran)
{
#if 0
    TranCommitLog abort_log;
    char    buffer[64*1024]; /* max page size */
    LogRec *logrec = (LogRec*)&buffer[0];
    LogSN   cur_undonxt_lsn;
#endif

    tran->state = TRAN_STATE_ABORT;

#if 0
    cur_undonxt_lsn = tran->undonxt_lsn;
    while (!LOGSN_IS_NULL(&cur_undonxt_lsn))
    {
        if (log_record_read(cur_undonxt_lsn, logrec) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "log record read - fail\n");
            return -1;
        }

        //log_undo_record(logrec->header.acttype, logrec);
        cur_undonxt_lsn = logrec->header.unxt_lsn;
    }

    lrec_tranabort_init(&abort_log);
    abort_log.header.length = 0;
    log_record_write(&abort_log);
#endif
}

/* External Functions */

Trans *tran_find(int entryid)
{
    Trans *tran = &tran_gl.tran_table[entryid];
    return (tran->state == TRAN_STATE_FREE ? NULL : tran);
}

Trans *tran_search(uint64_t tranid)
{
    Trans *tran = NULL;
    int    curr_eid;

    pthread_mutex_lock(&tran_gl.tran_lock);
    curr_eid = tran_gl.used_head;
    while (curr_eid != -1) {
        if (tran_gl.tran_table[curr_eid].tranid == tranid) {
            tran = &tran_gl.tran_table[curr_eid];
            break;
        }
        curr_eid = tran_gl.tran_table[curr_eid].next_eid;
    }
    pthread_mutex_unlock(&tran_gl.tran_lock);
    return tran;
}

Trans *tran_first(void)
{
    Trans *found = NULL;
    pthread_mutex_lock(&tran_gl.tran_lock);
    if (tran_gl.used_head != -1) {
        found = &tran_gl.tran_table[tran_gl.used_head];
    }
    pthread_mutex_unlock(&tran_gl.tran_lock);
    return found;
}

Trans *tran_next(Trans *tran)
{
    Trans *found = NULL;
    pthread_mutex_lock(&tran_gl.tran_lock);
    if (tran->next_eid != -1) {
        found = &tran_gl.tran_table[tran->next_eid];
    }
    pthread_mutex_unlock(&tran_gl.tran_lock);
    return found;
}

Trans *tran_alloc(bool implicit_tran)
{
    Trans *tran = NULL;
    pthread_mutex_lock(&tran_gl.tran_lock);
    if (tran_gl.free_list != -1) {
        tran = &tran_gl.tran_table[tran_gl.free_list];
        tran_gl.free_list = tran->next_eid;

        tran->next_eid = -1;
        if (tran_gl.used_tail == -1) {
            tran->prev_eid = -1;
            tran_gl.used_head = tran->curr_eid;
            tran_gl.used_tail = tran->curr_eid;
        } else {
            tran->prev_eid = tran_gl.used_tail;
            tran_gl.tran_table[tran_gl.used_tail].next_eid = tran->curr_eid;
            tran_gl.used_tail = tran->curr_eid;
        }

        tran->tranid = tran_gl.nxt_tranid;
        tran_gl.nxt_tranid += 1;
        if (implicit_tran) tran_gl.imp_trans += 1; /* # of implicit transcations */
        tran_gl.cur_trans += 1; /* # of current active transcations */
    }
    pthread_mutex_unlock(&tran_gl.tran_lock);
    if (tran != NULL) {
        tran->implicit = implicit_tran;
        tran->state = TRAN_STATE_ALLOC;
    }
    return tran;
}

void tran_free(Trans *tran, TranCommand com)
{
    assert(tran->state == TRAN_STATE_ALLOC ||
           tran->state == TRAN_STATE_COMMIT);

    if (tran->state != TRAN_STATE_ALLOC) {
        if (com == TRAN_COM_COMMIT) do_tran_commit(tran);
        else                        do_tran_abort(tran);
        /* clear transaction entry */
        LOGSN_SET_NULL(&tran->first_lsn);
        LOGSN_SET_NULL(&tran->last_lsn);
        LOGSN_SET_NULL(&tran->undonxt_lsn);
    }
    tran->state = TRAN_STATE_FREE;

    pthread_mutex_lock(&tran_gl.tran_lock);
    if (tran->prev_eid == -1) tran_gl.used_head = tran->next_eid;
    else tran_gl.tran_table[tran->prev_eid].next_eid = tran->next_eid;
    if (tran->next_eid == -1) tran_gl.used_tail = tran->prev_eid;
    else tran_gl.tran_table[tran->next_eid].prev_eid = tran->prev_eid;
    tran->prev_eid = -1;
    tran->next_eid = tran_gl.free_list;
    tran_gl.free_list = tran->curr_eid;
    if (tran->implicit) tran_gl.imp_trans -= 1; /* # of implicit transcations */
    tran_gl.cur_trans -= 1; /* # of current active transcations */
    pthread_mutex_unlock(&tran_gl.tran_lock);
}

Trans *tran_reco_alloc(uint64_t tranid)
{
    Trans *tran = NULL;
    pthread_mutex_lock(&tran_gl.tran_lock);
    if (tran_gl.free_list != -1) {
        tran = &tran_gl.tran_table[tran_gl.free_list];
        tran_gl.free_list = tran->next_eid;

        tran->next_eid = -1;
        if (tran_gl.used_tail == -1) {
            tran->prev_eid = -1;
            tran_gl.used_head = tran->curr_eid;
            tran_gl.used_tail = tran->curr_eid;
        } else {
            tran->prev_eid = tran_gl.used_tail;
            tran_gl.tran_table[tran_gl.used_tail].next_eid = tran->curr_eid;
            tran_gl.used_tail = tran->curr_eid;
        }

        tran->tranid = tranid;
        tran_gl.nxt_tranid = tranid + 1;
        tran_gl.cur_trans += 1; /* # of current active transcations */
    }
    pthread_mutex_unlock(&tran_gl.tran_lock);
    if (tran != NULL) {
        tran->state = TRAN_STATE_ALLOC;
    }
    return tran;
}

void tran_reco_free(Trans *tran)
{
    assert(tran->state == TRAN_STATE_COMMIT);

    /* clear transaction entry */
    LOGSN_SET_NULL(&tran->first_lsn);
    LOGSN_SET_NULL(&tran->last_lsn);
    LOGSN_SET_NULL(&tran->undonxt_lsn);
    tran->state = TRAN_STATE_FREE;

    pthread_mutex_lock(&tran_gl.tran_lock);
    if (tran->prev_eid == -1) tran_gl.used_head = tran->next_eid;
    else tran_gl.tran_table[tran->prev_eid].next_eid = tran->next_eid;
    if (tran->next_eid == -1) tran_gl.used_tail = tran->prev_eid;
    else tran_gl.tran_table[tran->next_eid].prev_eid = tran->prev_eid;
    tran->prev_eid = -1;
    tran->next_eid = tran_gl.free_list;
    tran_gl.free_list = tran->curr_eid;
    tran_gl.cur_trans -= 1; /* # of current active transcations */
    pthread_mutex_unlock(&tran_gl.tran_lock);
}

void tran_start(Trans *tran)
{
    LOGSN_SET_NULL(&tran->first_lsn);
    LOGSN_SET_NULL(&tran->last_lsn);
    LOGSN_SET_NULL(&tran->undonxt_lsn);
    tran->implicit = true; /* implicit transaction */
    tran->state = TRAN_STATE_ALLOC;

    pthread_mutex_lock(&tran_gl.tran_lock);
    tran->tranid = tran_gl.nxt_tranid;
    tran_gl.nxt_tranid += 1;
    tran_gl.imp_trans += 1;
    tran_gl.cur_trans += 1;
    pthread_mutex_unlock(&tran_gl.tran_lock);
}

void tran_stop(Trans *tran, TranCommand com)
{
    assert(tran->state == TRAN_STATE_ALLOC ||
           tran->state == TRAN_STATE_COMMIT);

    if (tran->state != TRAN_STATE_ALLOC) {
        if (com == TRAN_COM_COMMIT) do_tran_commit(tran);
        else                        do_tran_abort(tran);
    }
    tran->state = TRAN_STATE_FREE;

    pthread_mutex_lock(&tran_gl.tran_lock);
    tran_gl.imp_trans -= 1;
    tran_gl.cur_trans -= 1;
    pthread_mutex_unlock(&tran_gl.tran_lock);
}

/* Transaction Statistics */

void tran_stat_get(struct tran_stat *stats)
{
    int num_trans = 0;
    int curr_eid = -1;

    pthread_mutex_lock(&tran_gl.tran_lock);
    curr_eid = tran_gl.used_head;
    while (curr_eid != -1) {
        if (tran_gl.tran_table[curr_eid].state != TRAN_STATE_ALLOC) {
            stats->tran_info[num_trans] = tran_gl.tran_table[curr_eid];
            if ((++num_trans) >= TRAN_STAT_MAX_TRANS) break;
        }
        curr_eid = tran_gl.tran_table[curr_eid].next_eid;
    }
    stats->nxt_tranid = tran_gl.nxt_tranid;
    stats->cur_trans = tran_gl.cur_trans;
    stats->imp_trans = tran_gl.imp_trans;
    pthread_mutex_unlock(&tran_gl.tran_lock);
#if 0 /* get transaction stats without lock */
    stats->nxt_tranid = tran_gl.nxt_tranid;
    stats->cur_trans = tran_gl.cur_trans;
    stats->imp_trans = tran_gl.imp_trans;
    for (int i = 0; i < tran_gl.max_trans; i++) {
        if (tran_gl.tran_table[i].state == TRAN_STATE_FREE) continue;
        stats->tran_info[num_trans] = tran_gl.tran_table[i];
        if (stats->tran_info[num_trans].state != TRAN_STATE_FREE) {
            if ((++num_trans) >= TRAN_STAT_MAX_TRANS) break;
        }
    }
#endif
    stats->num_trans = num_trans;
    stats->max_trans = tran_gl.max_trans;
}

uint64_t tran_mgr_get_min_tranid(void)
{
    uint64_t min_tranid = 0;
    pthread_mutex_lock(&tran_gl.tran_lock);
    if (tran_gl.used_head != -1) {
        min_tranid = tran_gl.tran_table[tran_gl.used_head].tranid;
    }
    pthread_mutex_unlock(&tran_gl.tran_lock);
    return min_tranid;
}

uint64_t tran_mgr_get_nxt_tranid(void)
{
    uint64_t nxt_tranid;
    pthread_mutex_lock(&tran_gl.tran_lock);
    nxt_tranid = tran_gl.nxt_tranid;
    pthread_mutex_unlock(&tran_gl.tran_lock);
    return nxt_tranid;
}

void tran_mgr_set_nxt_tranid(uint64_t nxt_tranid)
{
    pthread_mutex_lock(&tran_gl.tran_lock);
    tran_gl.nxt_tranid = nxt_tranid;
    pthread_mutex_unlock(&tran_gl.tran_lock);
}

int tran_mgr_init(void *conf, void *srvr)
{
    server = (SERVER_HANDLE_V1 *)srvr;
    config = (struct config *)conf;
    logger = server->log->get_logger();

    tran_gl.initialized = false;

    tran_gl.imp_trans = 0;
    tran_gl.cur_trans = 0;
    tran_gl.max_trans = 4096; /* must be adjusted by configuration, later */

    void  *tran_table_space;
    size_t malloc_size = (tran_gl.max_trans * sizeof(Trans));
    if (posix_memalign(&tran_table_space, (size_t)config->sq.data_page_size, malloc_size) != 0) {
        fprintf(stderr, "tran manager - tran table alloc error\n");
        return -1; /* no more memory */
    }
    tran_gl.tran_table = (Trans*)tran_table_space;

    for (int i = 0; i < tran_gl.max_trans; i++) {
        tran_gl.tran_table[i].curr_eid = i;
        if (i < (tran_gl.max_trans-1)) tran_gl.tran_table[i].next_eid = (i+1);
        else                           tran_gl.tran_table[i].next_eid = -1;
        LOGSN_SET_NULL(&tran_gl.tran_table[i].first_lsn);
        LOGSN_SET_NULL(&tran_gl.tran_table[i].last_lsn);
        LOGSN_SET_NULL(&tran_gl.tran_table[i].undonxt_lsn);
        tran_gl.tran_table[i].state = TRAN_STATE_FREE;
    }
    tran_gl.free_list = 0; /* the first tran entry */
    tran_gl.used_head = -1;
    tran_gl.used_tail = -1;

    pthread_mutex_init(&tran_gl.tran_lock, NULL);
    tran_gl.nxt_tranid = 1; /* min tranid: 1 */

    /* initialize group commit structure */
    tran_gl.tran_gcommit.waitcnt = 0;
    tran_gl.tran_gcommit.on_sync = false;
    pthread_mutex_init(&tran_gl.tran_gcommit.lock, NULL);
    pthread_cond_init(&tran_gl.tran_gcommit.cond, NULL);

    tran_gl.initialized = true;
    return 0;
}

void tran_mgr_final(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.

    tran_gl.initialized = false;
    while (tran_gl.cur_trans > 0) {
        nanosleep(&sleep_time, NULL);
    }
    free(tran_gl.tran_table);
}
