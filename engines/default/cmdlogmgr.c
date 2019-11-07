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

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE
#include "cmdlogmgr.h"
#include "cmdlogbuf.h"
#include "checkpoint.h"
#include "item_clog.h"

typedef struct _group_commit {
    log_waiter_t *    wait_head;
    log_waiter_t *    wait_tail;
    uint32_t          wait_cnt;   /* group commit wait count */
    bool              sleep;      /* used for group commit thread */
    bool              start;      /* used for group commit thread */
    bool              init;       /* used for group commit thread */
    pthread_mutex_t   lock;       /* group commit mutex */
    pthread_cond_t    cond;       /* group commit conditional variable */
} group_commit_t;

typedef struct _wait_entry_info {
    int16_t           free_list;
    int16_t           used_head;
    int16_t           used_tail;
    uint16_t          cur_waiters;
    uint16_t          max_waiters;
} log_wait_entry_info;

/* commandlog global structure */
struct cmdlog_global {
    log_waiter_t        *wait_entry_table;
    log_wait_entry_info  wait_entry_info;
    pthread_mutex_t      wait_entry_lock;
    group_commit_t       group_commit;
    bool                 async_mode;
    volatile bool        initialized;
};

/* global data */
static struct default_engine *engine = NULL;
static EXTENSION_LOGGER_DESCRIPTOR* logger = NULL;
static struct cmdlog_global logmgr_gl;
static __thread log_waiter_t *tls_waiter = NULL;

/* Recovery Function */
static ENGINE_ERROR_CODE cmdlog_mgr_recovery()
{
    /* find and set the last completed snapshot info. */
    int ret = chkpt_recovery_analysis();
    if (ret < 0) {
        return ENGINE_FAILED;
    }
    /* do recovery. */
    ret = chkpt_recovery_redo();
    if (ret < 0) {
        return ENGINE_FAILED;
    }
    return ENGINE_SUCCESS;
}

/*
 * External Functions
 */
log_waiter_t *cmdlog_waiter_alloc(const void *cookie)
{
    log_waiter_t *waiter = NULL;
    log_wait_entry_info *info = &logmgr_gl.wait_entry_info;
    pthread_mutex_lock(&logmgr_gl.wait_entry_lock);
    if (info->free_list != -1) {
        waiter = &logmgr_gl.wait_entry_table[info->free_list];
        info->free_list = waiter->next_eid;

        waiter->next_eid = -1;
        if (info->used_tail == -1) {
            waiter->prev_eid = -1;
            info->used_head = waiter->curr_eid;
            info->used_tail = waiter->curr_eid;
        } else {
            waiter->prev_eid = info->used_tail;
            logmgr_gl.wait_entry_table[info->used_tail].next_eid = waiter->curr_eid;
            info->used_tail = waiter->curr_eid;
        }
        info->cur_waiters += 1;

        waiter->cookie = cookie;
    }
    pthread_mutex_unlock(&logmgr_gl.wait_entry_lock);
    if (waiter) {
      tls_waiter = waiter;
    }
    return waiter;
}

static log_waiter_t* do_cmdlog_get_commit_waiter(group_commit_t *gcommit, LogSN *now_fsync_lsn)
{
    log_waiter_t *waiters;
    log_waiter_t *prv, *now;
    int cnt;

    if (gcommit->wait_head == NULL) {
        return NULL;
    }
    waiters = gcommit->wait_head;

    cnt = 0;
    prv = NULL;
    now = waiters;

    while (now != NULL) {
        if (LOGSN_IS_LE(now_fsync_lsn, &now->lsn)) {
            /* NOT yet fsynced */
            if (prv != NULL) prv->wait_next = NULL;
            else             waiters = NULL;
            break;
        }
        prv = now;
        now = now->wait_next;
        cnt += 1;
    }

    if (waiters != NULL) {
        if (now != NULL) {
            gcommit->wait_head = now;
            gcommit->wait_cnt -= cnt;
        } else {
            gcommit->wait_head = NULL;
            gcommit->wait_tail = NULL;
            gcommit->wait_cnt = 0;
        }
    }
    return waiters;
}

static void do_cmdlog_waiter_free(log_waiter_t *waiter, bool lock_hold)
{
    LOGSN_SET_NULL(&waiter->lsn);
    waiter->wait_next = NULL;

    log_wait_entry_info *info = &logmgr_gl.wait_entry_info;
    pthread_mutex_lock(&logmgr_gl.wait_entry_lock);
    if (waiter->prev_eid == -1) info->used_head = waiter->next_eid;
    else logmgr_gl.wait_entry_table[waiter->prev_eid].next_eid = waiter->next_eid;
    if (waiter->next_eid == -1) info->used_tail = waiter->prev_eid;
    else logmgr_gl.wait_entry_table[waiter->next_eid].prev_eid = waiter->prev_eid;
    waiter->prev_eid = -1;
    waiter->next_eid = info->free_list;
    info->free_list = waiter->curr_eid;
    info->cur_waiters -= 1;
    pthread_mutex_unlock(&logmgr_gl.wait_entry_lock);
}

static void do_cmdlog_gcommit_thread_wakeup(bool lock_hold)
{
    if (lock_hold)
        pthread_mutex_lock(&logmgr_gl.group_commit.lock);
    if (logmgr_gl.group_commit.sleep == true) {
        /* wake up group commit thead */
        pthread_cond_signal(&logmgr_gl.group_commit.cond);
    }
    if (lock_hold)
        pthread_mutex_unlock(&logmgr_gl.group_commit.lock);
}

static void do_cmdlog_callback_and_free_waiters(log_waiter_t *waiters)
{
    log_waiter_t *waiter = waiters;

    /* Do callbacks on all waiters */
    while (waiter != NULL) {
        engine->server.core->notify_io_complete(waiter->cookie, ENGINE_SUCCESS);
        waiter = waiter->wait_next;
    }

    /* free all waiter entries */
    while (waiters != NULL) {
        waiter = waiters;
        waiters = waiter->wait_next;
        do_cmdlog_waiter_free(waiter, true);
    }
}

static void *do_cmdlog_gcommit_thread_main(void *arg)
{
    group_commit_t *gcommit = &logmgr_gl.group_commit;
    log_waiter_t *waiters = NULL;
    LogSN now_fsync_lsn;
    struct timeval  tv;
    struct timespec to;

    assert(gcommit->init == true);
    gcommit->start = true;
    logger->log(EXTENSION_LOG_INFO, NULL, "group commit thread started\n");
    while (gcommit->init)
    {
        pthread_mutex_lock(&gcommit->lock);
        if (gcommit->wait_cnt == 0) {
            /* 1 second sleep */
            gettimeofday(&tv, NULL);
            to.tv_sec = tv.tv_sec + 1;
            to.tv_nsec = tv.tv_usec * 1000;
            gcommit->sleep = true;
            pthread_cond_timedwait(&gcommit->cond, &gcommit->lock, &to);
            gcommit->sleep = false;
        } else {
            pthread_mutex_unlock(&gcommit->lock);

            /* synchronize log file after 2ms*/
            usleep(2000);
            log_file_sync();
            log_get_fsync_lsn(&now_fsync_lsn);

            pthread_mutex_lock(&gcommit->lock);
            waiters = do_cmdlog_get_commit_waiter(gcommit, &now_fsync_lsn);
        }
        pthread_mutex_unlock(&gcommit->lock);

        if (waiters) {
            do_cmdlog_callback_and_free_waiters(waiters);
            waiters = NULL;
        }
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "group commit thread terminated\n");
    gcommit->start = false;
    return NULL;
}

static int do_cmdlog_gcommit_thread_start(void)
{
    group_commit_t *gcommit = &logmgr_gl.group_commit;
    struct timespec sleep_time = {0, 10000000}; // 10 msec.
    pthread_t tid;
    int       ret;

    /* start request */
    gcommit->init = true;
    ret = pthread_create(&tid, NULL, do_cmdlog_gcommit_thread_main, NULL);
    if (ret != 0) {
        gcommit->init = false;
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Can't create item gc thread: %s\n", strerror(ret));
        return -1;
    }
    /* wait until item gc thread starts */
    while (gcommit->start == false) {
        nanosleep(&sleep_time, NULL);
    }
    return 0;
}

static void do_cmdlog_gcommit_thread_stop(void)
{
    group_commit_t *gcommit = &logmgr_gl.group_commit;
    struct timespec sleep_time = {0, 10000000}; // 10 msec.

    if (gcommit->init) {
        /* stop request */
        pthread_mutex_lock(&gcommit->lock);
        gcommit->init = false;
        pthread_mutex_unlock(&gcommit->lock);

        /* wait until item gc thread finishes its task */
        while (gcommit->start == true) {
            if (gcommit->sleep == true) {
                do_cmdlog_gcommit_thread_wakeup(true);
            }
            nanosleep(&sleep_time, NULL);
        }
    }
    assert(logmgr_gl.group_commit.wait_cnt == 0);
}

static inline void
do_cmdlog_add_commit_waiter(log_waiter_t *waiter)
{
    waiter->wait_next = NULL;
    if (logmgr_gl.group_commit.wait_tail == NULL) {
        logmgr_gl.group_commit.wait_head = waiter;
        logmgr_gl.group_commit.wait_tail = waiter;
    } else {
        logmgr_gl.group_commit.wait_tail->wait_next = waiter;
        logmgr_gl.group_commit.wait_tail = waiter;
    }
    logmgr_gl.group_commit.wait_cnt += 1;
}

void cmdlog_waiter_free(log_waiter_t *waiter, ENGINE_ERROR_CODE *result)
{
    if (logmgr_gl.async_mode == false && *result == ENGINE_SUCCESS) {
        group_commit_t *gcommit = &logmgr_gl.group_commit;
        LogSN now_flush_lsn, now_fsync_lsn;

        /* flush log records from now_flush_lsn to waiter->lsn */
        log_get_flush_lsn(&now_flush_lsn);
        if (LOGSN_IS_LE(&now_flush_lsn, &waiter->lsn)) {
            log_buffer_flush(&waiter->lsn);
        }

        log_get_fsync_lsn(&now_fsync_lsn);
        if (LOGSN_IS_LE(&now_fsync_lsn, &waiter->lsn)) {
            /* add waiter to group commit list */
            pthread_mutex_lock(&gcommit->lock);
            do_cmdlog_add_commit_waiter(waiter);
            if (gcommit->wait_cnt == 1) {
                do_cmdlog_gcommit_thread_wakeup(false);
            }
            pthread_mutex_unlock(&gcommit->lock);
            *result = ENGINE_EWOULDBLOCK;
            return;
        }
    }
    do_cmdlog_waiter_free(waiter, false);
}

log_waiter_t *cmdlog_get_cur_waiter(void)
{
    return tls_waiter;
}

ENGINE_ERROR_CODE cmdlog_waiter_init(struct default_engine *engine)
{
    int i;

    log_wait_entry_info *info = &logmgr_gl.wait_entry_info;
    info->cur_waiters = 0;
    info->max_waiters = 4096; /* FIXME: recomputation max logmgr and change configurable */

    logmgr_gl.wait_entry_table = (log_waiter_t *)malloc(info->max_waiters * sizeof(log_waiter_t));
    if (logmgr_gl.wait_entry_table == NULL) {
        return ENGINE_ENOMEM;
    }

    for (i = 0; i < info->max_waiters; i++) {
        logmgr_gl.wait_entry_table[i].curr_eid = i;
        if (i < (info->max_waiters-1)) logmgr_gl.wait_entry_table[i].next_eid = i+1;
        else                          logmgr_gl.wait_entry_table[i].next_eid = -1;
        LOGSN_SET_NULL(&logmgr_gl.wait_entry_table[i].lsn);
    }
    info->free_list = 0; /* the first entry */
    info->used_head = -1;
    info->used_tail = -1;
    pthread_mutex_init(&logmgr_gl.wait_entry_lock, NULL);

    /* initialize group commit */
    logmgr_gl.group_commit.wait_head = NULL;
    logmgr_gl.group_commit.wait_cnt = 0;
    logmgr_gl.group_commit.sleep = false;
    logmgr_gl.group_commit.start = false;
    logmgr_gl.group_commit.init = false;
    pthread_mutex_init(&logmgr_gl.group_commit.lock, NULL);
    pthread_cond_init(&logmgr_gl.group_commit.cond, NULL);

    return ENGINE_SUCCESS;
}

void cmdlog_waiter_final(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.

    log_wait_entry_info *info = &logmgr_gl.wait_entry_info;
    while (info->cur_waiters > 0) {
        nanosleep(&sleep_time, NULL);
    }

    free((void*)logmgr_gl.wait_entry_table);
    pthread_mutex_destroy(&logmgr_gl.wait_entry_lock);
}

ENGINE_ERROR_CODE cmdlog_mgr_init(struct default_engine* engine_ptr)
{
    ENGINE_ERROR_CODE ret;

    engine = engine_ptr;
    logger = engine->server.log->get_logger();

    memset(&logmgr_gl, 0, sizeof(logmgr_gl));
    logmgr_gl.async_mode = engine->config.async_logging;

    ret = cmdlog_waiter_init(engine);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    ret = cmdlog_buf_init(engine);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    (void)cmdlog_rec_init(engine);
    ret = chkpt_init(engine);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    ret = cmdlog_mgr_recovery();
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    /* set enable change log */
    (void)item_clog_set_enable(true);

    ret = do_cmdlog_gcommit_thread_start();
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    ret = cmdlog_buf_flush_thread_start();
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }
    ret = chkpt_thread_start();
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    logmgr_gl.initialized = true;
    logger->log(EXTENSION_LOG_INFO, NULL, "COMMAND LOG MANAGER module initialized.\n");
    return ENGINE_SUCCESS;
}

void cmdlog_mgr_final(void)
{
    chkpt_thread_stop();
    cmdlog_buf_flush_thread_stop();
    do_cmdlog_gcommit_thread_stop();

    /* CONSIDER: do last checkpoint before shutdown engine. */
    chkpt_final();
    cmdlog_buf_final();
    cmdlog_waiter_final();

    if (logmgr_gl.initialized == true) {
        logmgr_gl.initialized = false;
        logger->log(EXTENSION_LOG_INFO, NULL, "COMMAND LOG MANAGER module destroyed.\n");
    }
}
#endif
