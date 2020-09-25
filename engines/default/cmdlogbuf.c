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
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE
#include "cmdlogbuf.h"
#include "cmdlogfile.h"

/* FIXME: config log buffer size */
#define CMDLOG_BUFFER_SIZE (100 * 1024 * 1024) /* 100 MB */
#define CMDLOG_FLUSH_AUTO_SIZE (32 * 1024) /* 32 KB : see the nflush data type of log_FREQ */
#define CMDLOG_RECORD_MIN_SIZE 16          /* 8 bytes header + 8 bytes body */

#define ENABLE_DEBUG 0

/* flush request structure */
typedef struct _log_freq {
    uint16_t  nflush;     /* amount of log buffer to flush */
    uint8_t   dual_write; /* flag of dual write */
    uint8_t   unused;
} log_FREQ;

/* log buffer structure */
typedef struct _log_buffer {
    /* log buffer */
    char       *data;   /* log buffer pointer */
    uint32_t    size;   /* log buffer size */
    uint32_t    head;   /* the head position in log buffer */
    uint32_t    tail;   /* the tail position in log buffer */
    int32_t     last;   /* the last position in log buffer */

    /* flush request queue */
    log_FREQ   *fque;   /* flush request queue pointer */
    uint32_t    fqsz;   /* flush request queue size */
    uint32_t    fbgn;   /* the queue index to begin flush */
    uint32_t    fend;   /* the queue index to end flush */
    int32_t     dw_end; /* the queue index to end dual write */
} log_BUFFER;

/* log flusher structure */
typedef struct _log_flusher {
    pthread_mutex_t  lock;
    pthread_cond_t   cond;
    bool             sleep;
    volatile uint8_t running;
    volatile bool    reqstop;
} log_FLUSHER;

/* log buffer global structure */
struct log_buff_global {
    log_BUFFER      log_buffer;
    log_FLUSHER     log_flusher;
    LogSN           nxt_write_lsn;
    LogSN           nxt_flush_lsn;
    pthread_mutex_t log_write_lock;
    pthread_mutex_t log_flush_lock;
    pthread_mutex_t flush_lsn_lock;
    volatile bool   initialized;
};

/* global data */
static EXTENSION_LOGGER_DESCRIPTOR* logger = NULL;
static struct log_buff_global log_buff_gl;

static void do_log_flusher_wakeup(log_FLUSHER *flusher)
{
    pthread_mutex_lock(&flusher->lock);
    if (flusher->sleep) {
        pthread_cond_signal(&flusher->cond);
    }
    pthread_mutex_unlock(&flusher->lock);
}

static uint32_t do_log_buff_flush(bool flush_all)
{
    log_BUFFER *logbuff = &log_buff_gl.log_buffer;
    uint32_t    nflush = 0;
    bool        dual_write_flag = false;
    bool        dual_write_complete_flag = false;

    /* computate flush size */
    pthread_mutex_lock(&log_buff_gl.log_write_lock);
    if (logbuff->fbgn == logbuff->dw_end) {
        logbuff->dw_end = -1;
        dual_write_complete_flag = true;
    }
    if (logbuff->fbgn != logbuff->fend) {
        nflush = logbuff->fque[logbuff->fbgn].nflush;
        dual_write_flag = logbuff->fque[logbuff->fbgn].dual_write;
        assert(nflush > 0);
    } else {
        if (flush_all && logbuff->fque[logbuff->fend].nflush > 0) {
            nflush = logbuff->fque[logbuff->fend].nflush;
            dual_write_flag = logbuff->fque[logbuff->fend].dual_write;
            if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
        }
    }
    if (nflush > 0) {
        if (logbuff->head == logbuff->last) {
            logbuff->last = -1;
            logbuff->head = 0;
        }
    }
    pthread_mutex_unlock(&log_buff_gl.log_write_lock);

    if (dual_write_complete_flag) {
        cmdlog_file_complete_dual_write();

        pthread_mutex_lock(&log_buff_gl.flush_lsn_lock);
        log_buff_gl.nxt_flush_lsn.filenum += 1;
        log_buff_gl.nxt_flush_lsn.roffset = 0;
        pthread_mutex_unlock(&log_buff_gl.flush_lsn_lock);
    }

    if (nflush > 0) {
        cmdlog_file_write(&logbuff->data[logbuff->head], nflush, dual_write_flag);

        /* update nxt_flush_lsn */
        pthread_mutex_lock(&log_buff_gl.flush_lsn_lock);
        log_buff_gl.nxt_flush_lsn.roffset += nflush;
        pthread_mutex_unlock(&log_buff_gl.flush_lsn_lock);

        /* update next flush position */
        pthread_mutex_lock(&log_buff_gl.log_write_lock);
        logbuff->head += nflush;
        if (logbuff->head == logbuff->last) {
            logbuff->last = -1;
            logbuff->head = 0;
        }
        /* clear the flush request itself */
        logbuff->fque[logbuff->fbgn].nflush = 0;
        logbuff->fque[logbuff->fbgn].dual_write = false;
        if ((++logbuff->fbgn) == logbuff->fqsz) logbuff->fbgn = 0;
        pthread_mutex_unlock(&log_buff_gl.log_write_lock);
    }
    return nflush;
}

static LogSN do_log_buff_write(LogRec *logrec, bool dual_write)
{
    log_BUFFER *logbuff = &log_buff_gl.log_buffer;
    LogSN current_lsn;
    uint32_t total_length = sizeof(LogHdr) + logrec->header.body_length;
    uint32_t spare_length;
    assert(total_length < logbuff->size);

    pthread_mutex_lock(&log_buff_gl.log_write_lock);

    /* find the position to write in log buffer */
    while (1) {
        if (logbuff->head <= logbuff->tail) {
            assert(logbuff->last == -1);
            /* logbuff->head == logbuff->tail: empty state (NO full state) */
            if (total_length < (logbuff->size - logbuff->tail)) {
                break; /* enough buffer space */
            }
            if (logbuff->head > 0) {
                logbuff->last = logbuff->tail;
                logbuff->tail = 0;
                /* increase log flush end pointer
                 * to make to-be-flushed log data contiguous in memory.
                 */
                if (logbuff->fque[logbuff->fend].nflush > 0) {
                    if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
                }
                if (total_length < logbuff->head) {
                    break; /* enough buffer space */
                }
            }
        } else { /* logbuff->head > logbuff->tail */
            assert(logbuff->last != -1);
            if (total_length < (logbuff->head - logbuff->tail)) {
                break; /* enough buffer space */
            }
        }
        /* Lack of log buffer space: force flushing data on log buffer */
        pthread_mutex_unlock(&log_buff_gl.log_write_lock);
        pthread_mutex_lock(&log_buff_gl.log_flush_lock);
        (void)do_log_buff_flush(false);
        pthread_mutex_unlock(&log_buff_gl.log_flush_lock);
        pthread_mutex_lock(&log_buff_gl.log_write_lock);
    }

    /* write log record at the found location of log buffer */
    lrec_write_to_buffer(logrec, &logbuff->data[logbuff->tail]);
    logbuff->tail += total_length;

    /* update nxt_write_lsn */
    current_lsn = log_buff_gl.nxt_write_lsn;
    log_buff_gl.nxt_write_lsn.roffset += total_length;

    /* update log flush request */
    if (logbuff->fque[logbuff->fend].nflush > 0 &&
        logbuff->fque[logbuff->fend].dual_write != dual_write) {
        if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
    }
    while (total_length > 0) {
        /* check remain length */
        spare_length = CMDLOG_FLUSH_AUTO_SIZE - logbuff->fque[logbuff->fend].nflush;
        if (spare_length >= total_length) spare_length = total_length;

        logbuff->fque[logbuff->fend].nflush += spare_length;
        logbuff->fque[logbuff->fend].dual_write = dual_write;
        if (logbuff->fque[logbuff->fend].nflush == CMDLOG_FLUSH_AUTO_SIZE) {
            if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
        }
        total_length -= spare_length;
    }

    pthread_mutex_unlock(&log_buff_gl.log_write_lock);

    /* wake up log flush thread if flush requests exist */
    if (logbuff->fbgn != logbuff->fend) {
        if (log_buff_gl.log_flusher.sleep == true) {
            do_log_flusher_wakeup(&log_buff_gl.log_flusher);
        }
    }
    return current_lsn;
}

static void do_log_buff_complete_dual_write(bool success)
{
    log_BUFFER *logbuff = &log_buff_gl.log_buffer;

    pthread_mutex_lock(&log_buff_gl.log_write_lock);
    if (success) {
        if (logbuff->fque[logbuff->fend].nflush > 0) {
            if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
        }
        /* Set the position where a dual write end. */
        assert(logbuff->dw_end == -1);
        logbuff->dw_end = logbuff->fend;

        /* update nxt_write_lsn */
        log_buff_gl.nxt_write_lsn.filenum += 1;
        log_buff_gl.nxt_write_lsn.roffset = 0;
    } else {
        /* reset dual_write flag in flush reqeust queue */
        int index = logbuff->fbgn;
        while (logbuff->fque[index].nflush > 0) {
            if (logbuff->fque[index].dual_write) {
                logbuff->fque[index].dual_write = false;
            }
            if ((++index) == logbuff->fqsz) index = 0;
            if (index == logbuff->fbgn) break;
        }
    }
    pthread_mutex_unlock(&log_buff_gl.log_write_lock);
}

/* Log Flush Thread */
static void *log_flush_thread_main(void *arg)
{
    log_FLUSHER *flusher = &log_buff_gl.log_flusher;
    struct timeval  tv;
    struct timespec to;
    uint32_t nflush;

    flusher->running = RUNNING_STARTED;
    while (1)
    {
        if (flusher->reqstop) {
            logger->log(EXTENSION_LOG_INFO, NULL, "Command log flush thread recognized stop request.\n");
            break;
        }

        pthread_mutex_lock(&log_buff_gl.log_flush_lock);
        nflush = do_log_buff_flush(false);
        pthread_mutex_unlock(&log_buff_gl.log_flush_lock);

        if (nflush == 0) {
            /* nothing to flush: do 10 ms sleep */
            gettimeofday(&tv, NULL);
            if ((tv.tv_usec + 10000) < 1000000) {
                tv.tv_usec += 10000;
                to.tv_sec  = tv.tv_sec;
                to.tv_nsec = tv.tv_usec * 1000;
            } else {
                to.tv_sec  = tv.tv_sec + 1;
                to.tv_nsec = 0;
            }
            pthread_mutex_lock(&flusher->lock);
            flusher->sleep = true;
            pthread_cond_timedwait(&flusher->cond, &flusher->lock, &to);
            flusher->sleep = false;
            pthread_mutex_unlock(&flusher->lock);
        }
    }
    flusher->running = RUNNING_STOPPED;
    return NULL;
}

/*
 * External Functions
 */
void cmdlog_buff_write(LogRec *logrec, log_waiter_t *waiter, bool dual_write)
{
    /* write the log record on the log buffer */
    LogSN current_lsn = do_log_buff_write(logrec, dual_write);
    if (waiter) {
        waiter->lsn = current_lsn;
    }
}

void cmdlog_buff_flush(LogSN *upto_lsn)
{
    assert(upto_lsn);
    uint32_t nflush;

    do {
        pthread_mutex_lock(&log_buff_gl.log_flush_lock);
        if (LOGSN_IS_LE(&log_buff_gl.nxt_flush_lsn, upto_lsn)) {
            nflush = do_log_buff_flush(true);
            assert(nflush > 0);
            if (LOGSN_IS_GT(&log_buff_gl.nxt_flush_lsn, upto_lsn)) {
                nflush = 0;
            }
        } else {
            nflush = 0;
        }
        pthread_mutex_unlock(&log_buff_gl.log_flush_lock);
    } while (nflush > 0);
}

/* FIXME: remove later, if not used */
/*
void log_get_write_lsn(LogSN *lsn)
{
    pthread_mutex_lock(&log_buff_gl.log_write_lock);
    *lsn = log_buff_gl.nxt_write_lsn;
    pthread_mutex_unlock(&log_buff_gl.log_write_lock);
}
*/

void cmdlog_get_flush_lsn(LogSN *lsn)
{
    pthread_mutex_lock(&log_buff_gl.flush_lsn_lock);
    *lsn = log_buff_gl.nxt_flush_lsn;
    pthread_mutex_unlock(&log_buff_gl.flush_lsn_lock);
}

size_t cmdlog_get_file_size(void)
{
    size_t file_size = 0;

    pthread_mutex_lock(&log_buff_gl.log_write_lock);
    if (log_buff_gl.log_buffer.dw_end == -1) {
        file_size = cmdlog_get_current_file_size();
    }
    pthread_mutex_unlock(&log_buff_gl.log_write_lock);

    return file_size;
}

void cmdlog_complete_dual_write(bool success)
{
    if (cmdlog_get_next_fd() != -1) {
        do_log_buff_complete_dual_write(success);
    }
}

ENGINE_ERROR_CODE cmdlog_buf_init(struct default_engine* engine)
{
    logger = engine->server.log->get_logger();

    memset(&log_buff_gl, 0, sizeof(log_buff_gl));

    /* log buff global init */
    log_buff_gl.nxt_flush_lsn.filenum = 1;
    log_buff_gl.nxt_flush_lsn.roffset = 0;
    log_buff_gl.nxt_write_lsn = log_buff_gl.nxt_flush_lsn;

    pthread_mutex_init(&log_buff_gl.log_write_lock, NULL);
    pthread_mutex_init(&log_buff_gl.log_flush_lock, NULL);
    pthread_mutex_init(&log_buff_gl.flush_lsn_lock, NULL);

    /* log file init */
    cmdlog_file_init(engine);

    /* log buffer init */
    log_BUFFER *logbuff = &log_buff_gl.log_buffer;

    logbuff->size = CMDLOG_BUFFER_SIZE;
    logbuff->data = malloc(logbuff->size);
    if (logbuff->data == NULL) {
        return ENGINE_ENOMEM;
    }
    logbuff->head = 0;
    logbuff->tail = 0;
    logbuff->last = -1;

    /* log flush request queue init - ring shaped queue */
    logbuff->fqsz = (logbuff->size / CMDLOG_RECORD_MIN_SIZE);
    logbuff->fque = (log_FREQ*)malloc(logbuff->fqsz * sizeof(log_FREQ));
    if (logbuff->fque == NULL) {
        free(logbuff->data);
        return ENGINE_ENOMEM;
    }
    memset(logbuff->fque, 0, logbuff->fqsz * sizeof(log_FREQ));
    logbuff->fbgn = 0;
    logbuff->fend = 0;
    logbuff->dw_end = -1;

    /* log flush thread init */
    log_FLUSHER *flusher = &log_buff_gl.log_flusher;
    pthread_mutex_init(&flusher->lock, NULL);
    pthread_cond_init(&flusher->cond, NULL);
    flusher->sleep = false;
    flusher->running = RUNNING_UNSTARTED;
    flusher->reqstop = false;

    log_buff_gl.initialized = true;
    logger->log(EXTENSION_LOG_INFO, NULL, "CMDLOG BUFFER module initialized.\n");

    return ENGINE_SUCCESS;
}

void cmdlog_buf_final(void)
{
    if (log_buff_gl.initialized == false) {
        return;
    }

    /* log buffer final */
    log_BUFFER *logbuff = &log_buff_gl.log_buffer;

    if (logbuff->data != NULL) {
        free((void*)logbuff->data);
        logbuff->data = NULL;
    }

    if (logbuff->fque != NULL) {
        free((void*)logbuff->fque);
        logbuff->fque = NULL;
    }

    /* log file final */
    cmdlog_file_final();

    /* log buff global final */
    pthread_mutex_destroy(&log_buff_gl.log_write_lock);
    pthread_mutex_destroy(&log_buff_gl.log_flush_lock);
    pthread_mutex_destroy(&log_buff_gl.flush_lsn_lock);

    log_buff_gl.initialized = false;
    logger->log(EXTENSION_LOG_INFO, NULL, "CMDLOG BUFFER module destroyed.\n");
}

ENGINE_ERROR_CODE cmdlog_buf_flush_thread_start(void)
{
    assert(log_buff_gl.initialized == true);

    pthread_t tid;
    log_buff_gl.log_flusher.running = RUNNING_UNSTARTED;
    /* create log flush thread */
    if (pthread_create(&tid, NULL, log_flush_thread_main, NULL) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create command log flush thread. error=%s\n", strerror(errno));
        return ENGINE_FAILED;
    }

    /* wait until log flush thread starts */
    while (log_buff_gl.log_flusher.running == RUNNING_UNSTARTED) {
        usleep(5000); /* sleep 5ms */
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "Command log flush thread started.\n");

    return ENGINE_SUCCESS;
}

void cmdlog_buf_flush_thread_stop(void)
{
    log_FLUSHER *flusher = &log_buff_gl.log_flusher;
    if (flusher->running == RUNNING_UNSTARTED) {
        return;
    }

    while (flusher->running == RUNNING_STARTED) {
        flusher->reqstop = true;
        do_log_flusher_wakeup(flusher);
        usleep(5000); /* sleep 5ms */
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "Command log flush thread stopped.\n");
}
#endif
