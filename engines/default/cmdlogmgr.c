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
#include <errno.h>

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE
#include "cmdlogmgr.h"
#include "cmdlogbuf.h"
#include "checkpoint.h"
#include "item_clog.h"

static struct assoc_scan *chkpt_scanp=NULL; // checkpoint scan pointer

static bool gen_logical_btree_delete_log=false; // btree generate logical delete log

#define NEED_DUAL_WRITE(it) (chkpt_scanp != NULL && (it == NULL || assoc_scan_in_visited_area(chkpt_scanp, it)))

/* The size of memory chunk for log waiters */
#define LOG_WAITER_CHUNK_SIZE (4 * 1024)

#define IS_UPD_ELEM_INSERT(updtype)                                           \
    ((updtype) == UPD_LIST_ELEM_INSERT || (updtype) == UPD_SET_ELEM_INSERT || \
     (updtype) == UPD_MAP_ELEM_INSERT  || (updtype) == UPD_BT_ELEM_INSERT)
#define IS_UPD_ELEM_DELETE(updtype)                                           \
    ((updtype) == UPD_LIST_ELEM_DELETE || (updtype) == UPD_SET_ELEM_DELETE || \
     (updtype) == UPD_MAP_ELEM_DELETE  || (updtype) == UPD_BT_ELEM_DELETE)

typedef struct _group_commit {
    pthread_mutex_t   lock;       /* group commit mutex */
    pthread_cond_t    cond;       /* group commit conditional variable */
    log_waiter_t *    wait_head;
    log_waiter_t *    wait_tail;
    uint32_t          wait_cnt;   /* group commit wait count */
    bool              sleep;      /* group commit thread sleep */
    volatile uint8_t  running;    /* Is it running, now ? */
    volatile bool     reqstop;    /* request to stop group commit thread */
} group_commit_t;

typedef struct _waiter_chunk {
    struct _waiter_chunk *next;
    log_waiter_t          waiters[1];
} log_waiter_chunk;

typedef struct _waiter_info {
    pthread_mutex_t   waiter_lock;
    log_waiter_chunk *chunk_list;
    log_waiter_t     *free_list;
    uint16_t          chunk_count; /* chunk count */
    uint16_t          waiter_pchk; /* wait entry per chunk */
    uint32_t          cur_waiters; /* wait entry count in use */
} log_waiter_info;

/* commandlog global structure */
struct cmdlog_global {
    log_waiter_info waiter_info;
    group_commit_t  group_commit;
    bool            async_mode;
    volatile bool   initialized;
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

inline static void do_cmdlog_waiter_entry_reset(log_waiter_t *waiter)
{
    LOGSN_SET_NULL(&waiter->lsn);
    waiter->updtype = UPD_NONE;
    waiter->elem_insert_with_create = false;
    waiter->elem_delete_with_drop = false;
    waiter->generated_range_clog = false;
    waiter->wait_next = NULL;
}

static int do_cmdlog_waiter_grow(log_waiter_info *info)
{
    log_waiter_chunk *chunk = (log_waiter_chunk *)malloc(LOG_WAITER_CHUNK_SIZE);
    if (chunk == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to allocate cmdlog waiter chunk. chunk size : %d\n", LOG_WAITER_CHUNK_SIZE);
        return -1;
    }

    int i;
    for (i = 0; i < info->waiter_pchk; i++) {
        log_waiter_t *waiter = &chunk->waiters[i];
        if (i < (info->waiter_pchk-1)) waiter->free_next = &chunk->waiters[i+1];
        else                           waiter->free_next = NULL;
        do_cmdlog_waiter_entry_reset(waiter);
    }
    chunk->next = info->chunk_list;
    info->free_list = &chunk->waiters[0];
    info->chunk_list = chunk;
    info->chunk_count += 1;

    return 0;
}

static log_waiter_t *do_cmdlog_waiter_alloc(void)
{
    log_waiter_t *waiter = NULL;
    log_waiter_info *info = &logmgr_gl.waiter_info;
    pthread_mutex_lock(&info->waiter_lock);
    if (info->free_list == NULL) {
        if (do_cmdlog_waiter_grow(info) < 0) {
            pthread_mutex_unlock(&info->waiter_lock);
            return NULL;
        }
    }
    waiter = info->free_list;
    info->free_list = waiter->free_next;
    waiter->free_next = NULL;
    info->cur_waiters += 1;
    pthread_mutex_unlock(&info->waiter_lock);
    return waiter;
}

/*
 * External Functions
 */
log_waiter_t *cmdlog_waiter_alloc(const void *cookie, uint8_t updtype)
{
    log_waiter_t *waiter = do_cmdlog_waiter_alloc();
    if (waiter) {
      waiter->cookie = cookie;
      waiter->updtype = updtype;
      tls_waiter = waiter; /* set tls_waiter */
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

static void do_cmdlog_waiter_free(log_waiter_t *waiter)
{
    log_waiter_info *info = &logmgr_gl.waiter_info;

    do_cmdlog_waiter_entry_reset(waiter);

    pthread_mutex_lock(&info->waiter_lock);
    waiter->free_next = info->free_list;
    info->free_list = waiter;
    info->cur_waiters -= 1;
    pthread_mutex_unlock(&info->waiter_lock);
}

static void do_cmdlog_gcommit_thread_wakeup(group_commit_t *gcommit, bool lock_hold)
{
    if (lock_hold)
        pthread_mutex_lock(&gcommit->lock);
    if (gcommit->sleep == true) {
        /* wake up group commit thead */
        pthread_cond_signal(&gcommit->cond);
    }
    if (lock_hold)
        pthread_mutex_unlock(&gcommit->lock);
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
        do_cmdlog_waiter_free(waiter);
    }
}

static void *do_cmdlog_gcommit_thread_main(void *arg)
{
    group_commit_t *gcommit = &logmgr_gl.group_commit;
    log_waiter_t *waiters = NULL;
    LogSN now_fsync_lsn;
    struct timeval  tv;
    struct timespec to;

    gcommit->running = RUNNING_STARTED;
    while (1)
    {
        if (gcommit->reqstop) {
            logger->log(EXTENSION_LOG_INFO, NULL, "Group commit thread recognized stop request.\n");
            break;
        }

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

            /* synchronize log file after 2ms */
            usleep(2000);
            cmdlog_file_sync();
            cmdlog_get_fsync_lsn(&now_fsync_lsn);

            pthread_mutex_lock(&gcommit->lock);
            waiters = do_cmdlog_get_commit_waiter(gcommit, &now_fsync_lsn);
        }
        pthread_mutex_unlock(&gcommit->lock);

        if (waiters) {
            do_cmdlog_callback_and_free_waiters(waiters);
            waiters = NULL;
        }
    }
    gcommit->running = RUNNING_STOPPED;
    return NULL;
}

static int do_cmdlog_gcommit_thread_start(void)
{
    pthread_t tid;
    group_commit_t *gcommit = &logmgr_gl.group_commit;
    gcommit->running = RUNNING_UNSTARTED;
    /* create group commit thread */
    if (pthread_create(&tid, NULL, do_cmdlog_gcommit_thread_main, NULL) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create group commit thread. error=%s\n", strerror(errno));
        return -1;
    }

    /* wait until group commit thread starts */
    while (gcommit->running == RUNNING_UNSTARTED) {
        usleep(5000); /* sleep 5ms */
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "Group commit thread started.\n");

    return 0;
}

static void do_cmdlog_gcommit_thread_stop(void)
{
    group_commit_t *gcommit = &logmgr_gl.group_commit;
    if (gcommit->running == RUNNING_UNSTARTED) {
        return;
    }

    while (gcommit->running == RUNNING_STARTED) {
        gcommit->reqstop = true;
        do_cmdlog_gcommit_thread_wakeup(gcommit, true);
        usleep(5000); /* sleep 5ms */
    }
    assert(logmgr_gl.group_commit.wait_cnt == 0);
    logger->log(EXTENSION_LOG_INFO, NULL, "Group commit thread stopped.\n");
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
    tls_waiter = NULL; /* clear tls_waiter */

    if (logmgr_gl.async_mode == false && *result == ENGINE_SUCCESS) {
        group_commit_t *gcommit = &logmgr_gl.group_commit;
        LogSN now_flush_lsn, now_fsync_lsn;

        /* flush log records from now_flush_lsn to waiter->lsn */
        cmdlog_get_flush_lsn(&now_flush_lsn);
        if (LOGSN_IS_LE(&now_flush_lsn, &waiter->lsn)) {
            cmdlog_buff_flush(&waiter->lsn);
        }

        cmdlog_get_fsync_lsn(&now_fsync_lsn);
        if (LOGSN_IS_LE(&now_fsync_lsn, &waiter->lsn)) {
            /* add waiter to group commit list */
            pthread_mutex_lock(&gcommit->lock);
            do_cmdlog_add_commit_waiter(waiter);
            if (gcommit->wait_cnt == 1) {
                do_cmdlog_gcommit_thread_wakeup(gcommit, false);
            }
            pthread_mutex_unlock(&gcommit->lock);
            *result = ENGINE_EWOULDBLOCK;
            return;
        }
    }
    do_cmdlog_waiter_free(waiter);
}

log_waiter_t *cmdlog_get_my_waiter(void)
{
    return tls_waiter;
}

ENGINE_ERROR_CODE cmdlog_waiter_init(struct default_engine *engine)
{
    log_waiter_info *info = &logmgr_gl.waiter_info;
    info->chunk_list  = NULL;
    info->free_list   = NULL;
    info->chunk_count = 0;
    info->waiter_pchk = (LOG_WAITER_CHUNK_SIZE - sizeof(void*)) / sizeof(log_waiter_t);
    info->cur_waiters = 0;

    if (do_cmdlog_waiter_grow(info) < 0) {
        return ENGINE_ENOMEM;
    }
    pthread_mutex_init(&info->waiter_lock, NULL);

    /* initialize group commit */
    pthread_mutex_init(&logmgr_gl.group_commit.lock, NULL);
    pthread_cond_init(&logmgr_gl.group_commit.cond, NULL);
    logmgr_gl.group_commit.wait_head = NULL;
    logmgr_gl.group_commit.wait_cnt = 0;
    logmgr_gl.group_commit.sleep = false;
    logmgr_gl.group_commit.running = RUNNING_UNSTARTED;
    logmgr_gl.group_commit.reqstop = false;

    return ENGINE_SUCCESS;
}

void cmdlog_waiter_final(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.

    log_waiter_info *info = &logmgr_gl.waiter_info;
    while (info->cur_waiters > 0) {
        nanosleep(&sleep_time, NULL);
    }

    log_waiter_chunk *chunk;
    while (info->chunk_list != NULL) {
        chunk = info->chunk_list;
        info->chunk_list = chunk->next;
        free((void*)chunk);
    }
    pthread_mutex_destroy(&info->waiter_lock);
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

/* Generate Log Record Functions */
void cmdlog_generate_link_item(hash_item *it)
{
    log_waiter_t *waiter = cmdlog_get_my_waiter();
    if (!IS_UPD_ELEM_INSERT(waiter->updtype)) {
        ITLinkLog log;
        (void)lrec_construct_link_item((LogRec*)&log, it);
        cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
    } else {
        /* To create link_item lrec when UPD_ELEM_INSERT,
         * collection insert with create case only.
         */
        waiter->elem_insert_with_create = true;
    }
}

void cmdlog_generate_unlink_item(hash_item *it)
{
    log_waiter_t *waiter = cmdlog_get_my_waiter();
    /* If generated by background eviction or scrub stale, waiter is NULL. */
    if (waiter == NULL || waiter->elem_delete_with_drop == false) {
        ITUnlinkLog log;
        (void)lrec_construct_unlink_item((LogRec*)&log, it);
        cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
    }
}

void cmdlog_generate_flush_item(const char *prefix, const int nprefix, const time_t when)
{
    if (when <= 0) {
        ITFlushLog log;
        (void)lrec_construct_flush_item((LogRec*)&log, prefix, nprefix);
        cmdlog_buff_write((LogRec*)&log, cmdlog_get_my_waiter(), NEED_DUAL_WRITE(NULL));
    }
}

void cmdlog_generate_setattr(hash_item *it, const ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt)
{
    uint8_t attr_type = 0;
    for (int i = 0; i < attr_cnt; i++) {
        if (attr_ids[i] == ATTR_EXPIRETIME) {
            if (attr_type < UPD_SETATTR_EXPTIME)
                attr_type = UPD_SETATTR_EXPTIME;
        }
        else if (attr_ids[i] == ATTR_MAXCOUNT ||
                 attr_ids[i] == ATTR_OVFLACTION ||
                 attr_ids[i] == ATTR_READABLE) {
            if (attr_type < UPD_SETATTR_EXPTIME_INFO)
                attr_type = UPD_SETATTR_EXPTIME_INFO;
        }
        else if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
            if (attr_type < UPD_SETATTR_EXPTIME_INFO_BKEY)
                attr_type = UPD_SETATTR_EXPTIME_INFO_BKEY;
        }
    }
    if (attr_type > 0) {
        ITSetAttrLog log;
        (void)lrec_construct_setattr((LogRec*)&log, it, attr_type);
        cmdlog_buff_write((LogRec*)&log, cmdlog_get_my_waiter(), NEED_DUAL_WRITE(it));
    }
}

void cmdlog_generate_list_elem_insert(hash_item *it, const uint32_t total,
                                      const int index, list_elem_item *elem)
{
    ListElemInsLog log;
    lrec_attr_info attr;
    log_waiter_t *waiter = cmdlog_get_my_waiter();
    bool create = waiter->elem_insert_with_create;
    waiter->elem_insert_with_create = false;
    (void)lrec_construct_list_elem_insert((LogRec*)&log, it, total,
                                          index, elem, create, &attr);
    cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
}

void cmdlog_generate_list_elem_delete(hash_item *it, const uint32_t total,
                                      const int index, const uint32_t count)
{
    ListElemDelLog log;
    log_waiter_t *waiter = cmdlog_get_my_waiter();
    bool drop = waiter->elem_delete_with_drop;
    (void)lrec_construct_list_elem_delete((LogRec*)&log, it, total, index, count, drop);
    cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
}

void cmdlog_generate_map_elem_insert(hash_item *it, map_elem_item *elem)
{
    MapElemInsLog log;
    lrec_attr_info attr;
    log_waiter_t *waiter = cmdlog_get_my_waiter();
    bool create = waiter->elem_insert_with_create;
    waiter->elem_insert_with_create = false;
    (void)lrec_construct_map_elem_insert((LogRec*)&log, it, elem, create, &attr);
    cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
}

void cmdlog_generate_map_elem_delete(hash_item *it, map_elem_item *elem)
{
    MapElemDelLog log;
    log_waiter_t *waiter = cmdlog_get_my_waiter();
    bool drop = waiter->elem_delete_with_drop;
    (void)lrec_construct_map_elem_delete((LogRec*)&log, it, elem, drop);
    cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
}

void cmdlog_generate_set_elem_insert(hash_item *it, set_elem_item *elem)
{
    SetElemInsLog log;
    lrec_attr_info attr;
    log_waiter_t *waiter = cmdlog_get_my_waiter();
    bool create = waiter->elem_insert_with_create;
    waiter->elem_insert_with_create = false;
    (void)lrec_construct_set_elem_insert((LogRec*)&log, it, elem, create, &attr);
    cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
}

void cmdlog_generate_set_elem_delete(hash_item *it, set_elem_item *elem)
{
    SetElemDelLog log;
    log_waiter_t *waiter = cmdlog_get_my_waiter();
    bool drop = waiter->elem_delete_with_drop;
    (void)lrec_construct_set_elem_delete((LogRec*)&log, it, elem, drop);
    cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
}

void cmdlog_generate_btree_elem_insert(hash_item *it, btree_elem_item *elem)
{
    BtreeElemInsLog log;
    lrec_attr_info attr;
    log_waiter_t *waiter = cmdlog_get_my_waiter();
    bool create = waiter->elem_insert_with_create;
    waiter->elem_insert_with_create = false;
    (void)lrec_construct_btree_elem_insert((LogRec*)&log, it, elem, create, &attr);
    cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
}

void cmdlog_generate_btree_elem_delete(hash_item *it, btree_elem_item *elem)
{
    if (!gen_logical_btree_delete_log) {
        BtreeElemDelLog log;
        log_waiter_t *waiter = cmdlog_get_my_waiter();
        bool drop = waiter->elem_delete_with_drop;
        (void)lrec_construct_btree_elem_delete((LogRec*)&log, it, elem, drop);
        cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
    }
}

void cmdlog_generate_btree_elem_delete_logical(hash_item *it,
                                               const bkey_range *bkrange,
                                               const eflag_filter *efilter,
                                               uint32_t offset, uint32_t reqcount)
{
    if (gen_logical_btree_delete_log) {
        BtreeElemDelLgcLog log;
        log_waiter_t *waiter = cmdlog_get_my_waiter();
        bool drop = waiter->elem_delete_with_drop;
        (void)lrec_construct_btree_elem_delete_logical((LogRec*)&log, it, bkrange, efilter,
                                                       offset, reqcount, drop);
        cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
    }
}

void cmdlog_generate_operation_range(bool begin)
{
    log_waiter_t *waiter = cmdlog_get_my_waiter();

    if (waiter->updtype == UPD_BT_ELEM_DELETE && gen_logical_btree_delete_log) {
        /* If updtype is btree element delete and
         * gen_logical_btree_delete_log mode,
         * do not generate range log records.
         */
        return;
    }

    if (begin || waiter->generated_range_clog) {
        OperationRangeLog log;
        (void)lrec_construct_operation_range((LogRec*)&log, begin);
        cmdlog_buff_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(NULL));
        waiter->generated_range_clog = (begin ? true : false);
    }
}

void cmdlog_set_chkpt_scan(struct assoc_scan *cs)
{
    /* Cache locked */
    assert(chkpt_scanp == NULL);
    chkpt_scanp = cs;
}

void cmdlog_reset_chkpt_scan(bool chkpt_success)
{
    /* Cache locked */
    if (chkpt_scanp != NULL) {
        chkpt_scanp = NULL;
        cmdlog_complete_dual_write(chkpt_success);
    }
}
#endif
