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

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE
#include "cmdlogmgr.h"
#include "cmdlogbuf.h"
#include "checkpoint.h"
#include "item_clog.h"

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
    log_waiter_t        *waiters;
    log_wait_entry_info  wait_entry_info;
    pthread_mutex_t      wait_entry_lock;
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
    }
    pthread_mutex_unlock(&logmgr_gl.wait_entry_lock);
    if (waiter) {
      tls_waiter = waiter;
    }
    return waiter;
}

void cmdlog_waiter_free(log_waiter_t *waiter, ENGINE_ERROR_CODE *result)
{
    assert(waiter && result);

    LOGSN_SET_NULL(&waiter->lsn);

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

    /* TODO::initialize group commit with waiters */

    return ENGINE_SUCCESS;
}

void cmdlog_waiter_final(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.

    log_wait_entry_info *info = &logmgr_gl.wait_entry_info;
    while (info->cur_waiters > 0) {
        nanosleep(&sleep_time, NULL);
    }

    /* TODO::check wait count is 0 */

    free((void*)logmgr_gl.wait_entry_table);
    pthread_mutex_destroy(&logmgr_gl.wait_entry_lock);
}

ENGINE_ERROR_CODE cmdlog_mgr_init(struct default_engine* engine_ptr)
{
    ENGINE_ERROR_CODE ret;

    engine = engine_ptr;
    logger = engine->server.log->get_logger();

    memset(&logmgr_gl, 0, sizeof(logmgr_gl));

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
