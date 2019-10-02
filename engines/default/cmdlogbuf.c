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

/* FIXME: config log buffer size */
#define CMDLOG_BUFFER_SIZE (100 * 1024 * 1024) /* 100 MB */
#define CMDLOG_FLUSH_AUTO_SIZE (64 * 1024) /* 64 KB */

#define CMDLOG_MAX_FILEPATH_LENGTH 255

#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
#define FRQUEUE_CHUNK_SIZE 4096
#endif

#define ENABLE_DEBUG 0

/* log file structure */
typedef struct _log_file {
    char      path[CMDLOG_MAX_FILEPATH_LENGTH+1];
    int       prev_fd;
    int       fd;
    int       next_fd;
    size_t    size;
    size_t    next_size;
} log_FILE;

#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
/* flush request info */
typedef struct _log_freq_info {
    int32_t   max_requests; /* flush request queue size */
    int32_t   cur_requests; /* the number of current flush requests */

    int32_t   free_list;    /* the next queue index of free list */
    int32_t   fbgn;         /* the queue index to begin flush */
    int32_t   fend;         /* the queue index to end flush */
    int32_t   dw_end;       /* the queue index to end dual write */
} log_FREQ_info;
#endif

/* flush request structure */
typedef struct _log_freq {
    uint32_t  nflush;     /* amount of log buffer to flush */
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    int32_t   curr_eid;   /* curr request index id */
    int32_t   next_eid;   /* next request index id */
    int32_t   prev_eid;   /* prev request index id */
#endif
    bool      dual_write; /* flag of dual write */
} log_FREQ;

/* log buffer structure */
typedef struct _log_buffer {
    /* log buffer */
    char          *data;   /* log buffer pointer */
    uint32_t       size;   /* log buffer size */
    uint32_t       head;   /* the head position in log buffer */
    uint32_t       tail;   /* the tail position in log buffer */
    int32_t        last;   /* the last position in log buffer */

    /* flush request queue */
    log_FREQ      *fque;   /* flush request queue pointer */
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    log_FREQ_info  fqinfo; /* flush request queue info */
#else
    uint32_t    fqsz;   /* flush request queue size */
    uint32_t    fbgn;   /* the queue index to begin flush */
    uint32_t    fend;   /* the queue index to end flush */
    int32_t     dw_end; /* the queue index to end dual write */
#endif
} log_BUFFER;

/* log flusher structure */
typedef struct _log_flusher {
    pthread_mutex_t  lock;
    pthread_cond_t   cond;
    bool             sleep;
    bool             start;
    bool             init;   /* start or stop request */
} log_FLUSHER;

/* log global structure */
struct log_global {
    log_FILE        log_file;
    log_BUFFER      log_buffer;
    log_FLUSHER     log_flusher;
    LogSN           nxt_write_lsn;
    LogSN           nxt_flush_lsn;
    LogSN           nxt_fsync_lsn;
    pthread_mutex_t log_write_lock;
    pthread_mutex_t log_flush_lock;
//    pthread_mutex_t log_fsync_lock;
    pthread_mutex_t flush_lsn_lock;
//    pthread_mutex_t fsync_lsn_lock;
    volatile bool   initialized;
};

/* global data */
static EXTENSION_LOGGER_DESCRIPTOR* logger = NULL;
static struct log_global log_gl;

/*
 * Static Functions
 */
/* FIXME: disk_byte_write is temporary function move to disk.c later */
/***************/
static ssize_t disk_byte_write(int fd, void *buf, size_t count)
{
    char   *bfptr = (char*)buf;
    ssize_t nleft = count;
    ssize_t nwrite;

    while (nleft > 0) {
        nwrite = write(fd, bfptr, nleft);
        if (nwrite == 0) break;
        if (nwrite <  0) {
            if (errno == EINTR) continue;
            return nwrite;
        }
        nleft -= nwrite;
        bfptr += nwrite;
    }
    return (count - nleft);
}

static int disk_open(const char *fname, int flags, int mode)
{
    int fd;
    while (1) {
        if ((fd = open(fname, flags, mode)) == -1) {
            if (errno == EINTR) continue;
        }
        break;
    }
    return fd;
}

static int disk_fsync(int fd)
{
    if (fsync(fd) != 0) {
        return -1;
    }
    return 0;
}

static int disk_close(int fd)
{
    while (1) {
        if (close(fd) != 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        break;
    }
    return 0;
}
/***************/

#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
static void grow_flush_request_queue(void)
{
    int i;
    log_FREQ_info *fqinfo = &log_gl.log_buffer.fqinfo;
    int32_t cur_requests = fqinfo->max_requests;
    int32_t new_requests = fqinfo->max_requests + FRQUEUE_CHUNK_SIZE;

    log_FREQ *new_fque = realloc(log_gl.log_buffer.fque, new_requests * sizeof(log_FREQ));
    if (new_fque == NULL) {
        return;
    }
    log_gl.log_buffer.fque = new_fque;
    fqinfo->max_requests = new_requests;

    for (i = cur_requests; i < fqinfo->max_requests; i++) {
        log_gl.log_buffer.fque[i].nflush = 0;
        log_gl.log_buffer.fque[i].curr_eid = i;
        if (i < (fqinfo->max_requests-1)) log_gl.log_buffer.fque[i].next_eid = i+1;
        else                              log_gl.log_buffer.fque[i].next_eid = -1;
        log_gl.log_buffer.fque[i].prev_eid = -1;
        log_gl.log_buffer.fque[i].dual_write = false;
    }
    fqinfo->free_list = cur_requests;
}

static void do_flush_request_alloc(void)
{
    /* log_write locked */
    log_FREQ      *tail = NULL;
    log_FREQ_info *fqinfo = &log_gl.log_buffer.fqinfo;

    if (fqinfo->free_list == -1) {
        grow_flush_request_queue();

        /* TODO: More complete handling is needed */
        assert(fqinfo->free_list != -1);
    }

    tail = &log_gl.log_buffer.fque[fqinfo->free_list];
    fqinfo->free_list = tail->next_eid;

    tail->next_eid = -1;
    if (fqinfo->fend == -1) {
        tail->prev_eid = -1;
        fqinfo->fbgn = tail->curr_eid;
        fqinfo->fend = tail->curr_eid;
    } else {
        tail->prev_eid = fqinfo->fend;
        log_gl.log_buffer.fque[fqinfo->fend].next_eid = tail->curr_eid;
        fqinfo->fend = tail->curr_eid;
    }
    fqinfo->cur_requests += 1;
}

static void do_flush_request_free(void)
{
    /* log_wirte locked */
    log_FREQ      *head = NULL;
    log_FREQ_info *fqinfo = &log_gl.log_buffer.fqinfo;

    head = &log_gl.log_buffer.fque[fqinfo->fbgn];
    if (head->prev_eid == -1) fqinfo->fbgn = head->next_eid;
    else log_gl.log_buffer.fque[head->prev_eid].next_eid = head->next_eid;
    if (head->next_eid == -1) fqinfo->fend = head->prev_eid;
    else log_gl.log_buffer.fque[head->next_eid].prev_eid = head->prev_eid;
    head->prev_eid = -1;
    head->next_eid = fqinfo->free_list;
    fqinfo->free_list = head->curr_eid;
    fqinfo->cur_requests -= 1;
}
#endif

static void do_log_flusher_wakeup(void)
{
    log_FLUSHER *flusher = &log_gl.log_flusher;

    pthread_mutex_lock(&flusher->lock);
    if (flusher->sleep) {
        pthread_cond_signal(&flusher->cond);
    }
    pthread_mutex_unlock(&flusher->lock);
}

static void do_log_file_write(char *log_ptr, uint32_t log_size, bool dual_write)
{
    log_FILE *logfile = &log_gl.log_file;
    assert(logfile->fd != -1);

    /* The log data is appended */
    ssize_t nwrite = disk_byte_write(logfile->fd, log_ptr, log_size);
    if (nwrite != log_size) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "log file(%d) write - write(%ld!=%ld) error=(%d:%s)\n",
                    logfile->fd, nwrite, (ssize_t)log_size,
                    errno, strerror(errno));
    }
    /* FIXME::need error handling */
    assert(nwrite == log_size);
    logfile->size += log_size;

    if (dual_write && logfile->next_fd != -1) {
        /* next_fd is guaranteed concurrency by log_flush_lock */

        /* The log data is appended */
        nwrite = disk_byte_write(logfile->next_fd, log_ptr, log_size);
        if (nwrite != log_size) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "log file(%d) write - write(%ld!=%ld) error=(%d:%s)\n",
                        logfile->next_fd, nwrite, (ssize_t)log_size,
                        errno, strerror(errno));
        }
        /* FIXME::need error handling */
        assert(nwrite == log_size);
        logfile->next_size += log_size;
    }
}

static uint32_t do_log_buff_flush(bool flush_all)
{
    log_BUFFER *logbuff = &log_gl.log_buffer;
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    log_FREQ_info *fqinfo = &log_gl.log_buffer.fqinfo;
#endif
    uint32_t    nflush = 0;
    bool        dual_write_flag = false;
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
#else
    bool        last_flush_flag = false;
#endif
    bool        next_fhlsn_flag = false;
    bool        cleanup_process = false;

    /* computate flush size */
    pthread_mutex_lock(&log_gl.log_write_lock);
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    if (fqinfo->fend != -1) {
        if (fqinfo->dw_end != -1) {
            cleanup_process = true;
        }
        if (fqinfo->fbgn == fqinfo->dw_end) {
            fqinfo->dw_end = -1;
            next_fhlsn_flag = true;
        }
        if (fqinfo->fbgn != fqinfo->fend) {
            nflush = logbuff->fque[fqinfo->fbgn].nflush;
            dual_write_flag = logbuff->fque[fqinfo->fbgn].dual_write;
            assert(nflush > 0);
        } else {
            if (flush_all) {
                nflush = logbuff->fque[fqinfo->fend].nflush;
                dual_write_flag = logbuff->fque[fqinfo->fend].dual_write;
            }
        }
    }
#else
    if (logbuff->dw_end != -1) {
        cleanup_process = true;
    }
    if (logbuff->fbgn == logbuff->dw_end) {
        logbuff->dw_end = -1;
        next_fhlsn_flag = true;
    }
    if (logbuff->fbgn != logbuff->fend) {
        nflush = logbuff->fque[logbuff->fbgn].nflush;
        dual_write_flag = logbuff->fque[logbuff->fbgn].dual_write;
        assert(nflush > 0);
    } else {
        if (flush_all && logbuff->fque[logbuff->fend].nflush > 0) {
            nflush = logbuff->fque[logbuff->fend].nflush;
            dual_write_flag = logbuff->fque[logbuff->fend].dual_write;
            last_flush_flag = true;
        }
    }
#endif
    if (nflush > 0) {
        if (logbuff->head == logbuff->last) {
            logbuff->last = -1;
            logbuff->head = 0;
        }
    }
    pthread_mutex_unlock(&log_gl.log_write_lock);

    if (next_fhlsn_flag) {
        pthread_mutex_lock(&log_gl.flush_lsn_lock);
        log_gl.nxt_flush_lsn.filenum += 1;
        log_gl.nxt_flush_lsn.roffset = 0;
        pthread_mutex_unlock(&log_gl.flush_lsn_lock);
    }

    if (nflush > 0) {
        if (cleanup_process) {
            /* Cleanup process. (fd was set to next_fd in the previous step)
             * Skip if requested by old cmdlog file only.
             */
            if (dual_write_flag) {
                do_log_file_write(&logbuff->data[logbuff->head], nflush, false);
            }
        } else {
            do_log_file_write(&logbuff->data[logbuff->head], nflush, dual_write_flag);
        }

        /* update nxt_flush_lsn */
        pthread_mutex_lock(&log_gl.flush_lsn_lock);
        log_gl.nxt_flush_lsn.roffset += nflush;
        pthread_mutex_unlock(&log_gl.flush_lsn_lock);

        /* update next flush position */
        pthread_mutex_lock(&log_gl.log_write_lock);
        logbuff->head += nflush;
        if (logbuff->head == logbuff->last) {
            logbuff->last = -1;
            logbuff->head = 0;
        }
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
        logbuff->fque[fqinfo->fbgn].nflush -= nflush;
        if (logbuff->fque[fqinfo->fbgn].nflush == 0) {
            do_flush_request_free();
        }
#else
        if (last_flush_flag) {
            /* decrement the flush request size */
            logbuff->fque[logbuff->fbgn].nflush -= nflush;
            if (logbuff->fque[logbuff->fbgn].nflush == 0 && logbuff->fbgn != logbuff->fend) {
                if ((++logbuff->fbgn) == logbuff->fqsz) {
                    logbuff->fbgn = 0;
                }
            }
        } else {
            /* clear the flush request itself */
            logbuff->fque[logbuff->fbgn].nflush = 0;
            if ((++logbuff->fbgn) == logbuff->fqsz) {
                logbuff->fbgn = 0;
            }
        }
#endif
        pthread_mutex_unlock(&log_gl.log_write_lock);
    }
    return nflush;
}

static void do_log_buff_write(LogRec *logrec, log_waiter_t *waiter, bool dual_write)
{
    log_BUFFER *logbuff = &log_gl.log_buffer;
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    log_FREQ_info *fqinfo = &log_gl.log_buffer.fqinfo;
#endif
    uint32_t total_length = sizeof(LogHdr) + logrec->header.body_length;
    assert(total_length < logbuff->size);

    pthread_mutex_lock(&log_gl.log_write_lock);

    /* find the positon to write in log buffer */
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
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
                if (fqinfo->fend != -1 && logbuff->fque[fqinfo->fend].nflush > 0) {
                    do_flush_request_alloc();
                }
#else
                /* TODO: ensure flush request not full */
                if (logbuff->fque[logbuff->fend].nflush > 0) {
                    if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
                }
#endif
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
        pthread_mutex_unlock(&log_gl.log_write_lock);
        pthread_mutex_lock(&log_gl.log_flush_lock);
        (void)do_log_buff_flush(false);
        pthread_mutex_unlock(&log_gl.log_flush_lock);
        pthread_mutex_lock(&log_gl.log_write_lock);
    }

    /* write log record at the found location of log buffer */
    lrec_write_to_buffer(logrec, &logbuff->data[logbuff->tail]);
    logbuff->tail += total_length;

    /* update nxt_write_lsn */
    log_gl.nxt_write_lsn.roffset += total_length;

    /* update log flush reqeust */
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    if (fqinfo->fend == -1) {
        do_flush_request_alloc();
    }
    assert(fqinfo->fend != -1);

    if (logbuff->fque[fqinfo->fend].nflush == 0) {
        logbuff->fque[fqinfo->fend].nflush = total_length;
        logbuff->fque[fqinfo->fend].dual_write = dual_write;
    } else {
        if (logbuff->fque[fqinfo->fend].dual_write != dual_write) {
            do_flush_request_alloc();
        }
        logbuff->fque[fqinfo->fend].nflush += total_length;
        logbuff->fque[fqinfo->fend].dual_write = dual_write;
    }
    if (logbuff->fque[fqinfo->fend].nflush > CMDLOG_FLUSH_AUTO_SIZE) {
        do_flush_request_alloc();
    }
#else
    if (logbuff->fque[logbuff->fend].nflush == 0) {
        logbuff->fque[logbuff->fend].nflush = total_length;
        logbuff->fque[logbuff->fend].dual_write = dual_write;
    } else {
        /* TODO: ensure flush request not full */
        if (logbuff->fque[logbuff->fend].dual_write != dual_write) {
            if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
        }
        logbuff->fque[logbuff->fend].nflush += total_length;
        logbuff->fque[logbuff->fend].dual_write = dual_write;
    }
    if (logbuff->fque[logbuff->fend].nflush > CMDLOG_FLUSH_AUTO_SIZE) {
        if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
    }
#endif

    if (waiter != NULL) {
        waiter->lsn = log_gl.nxt_write_lsn;
    }

    pthread_mutex_unlock(&log_gl.log_write_lock);

    /* wake up log flush thread if flush requests exist */
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    if (fqinfo->fbgn != fqinfo->fend) {
#else
    if (logbuff->fbgn != logbuff->fend) {
#endif
        if (log_gl.log_flusher.sleep == true) {
            do_log_flusher_wakeup();
        }
    }
}

/* Log Flush Thread */
static void *log_flush_thread_main(void *arg)
{
    log_FLUSHER *flusher = &log_gl.log_flusher;
    struct timeval  tv;
    struct timespec to;
    uint32_t nflush;

    assert(flusher->init == true);

    flusher->start = true;
    while (flusher->init)
    {
        pthread_mutex_lock(&log_gl.log_flush_lock);
        nflush = do_log_buff_flush(false);
        pthread_mutex_unlock(&log_gl.log_flush_lock);

        /* TODO: file sync thread process or log_file_sync */

        if (nflush == 0 && flusher->init) {
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
    flusher->start = false;
    return NULL;
}

/*
 * External Functions
 */
void log_file_sync(LogSN *prev_flush_lsn)
{
    /* TODO: file sync and update nxt_fsync_lsn with log_fsync_lock and fsync_lsn_lock */
}

void log_record_write(LogRec *logrec, log_waiter_t *waiter, bool dual_write)
{
    /* write the log record on the log buffer */
    do_log_buff_write(logrec, waiter, dual_write);
}

/* FIXME: remove later, if not used */
/*
void log_get_write_lsn(LogSN *lsn)
{
    pthread_mutex_lock(&log_gl.log_write_lock);
    *lsn = log_gl.nxt_write_lsn;
    pthread_mutex_unlock(&log_gl.log_write_lock);
}
*/

void log_get_flush_lsn(LogSN *lsn)
{
    pthread_mutex_lock(&log_gl.flush_lsn_lock);
    *lsn = log_gl.nxt_flush_lsn;
    pthread_mutex_unlock(&log_gl.flush_lsn_lock);
}

void log_get_fsync_lsn(LogSN *lsn)
{
//    pthread_mutex_lock(&log_gl.fsync_lsn_lock);
    pthread_mutex_lock(&log_gl.log_flush_lock);
    *lsn = log_gl.nxt_fsync_lsn;
    pthread_mutex_unlock(&log_gl.log_flush_lock);
//    pthread_mutex_unlock(&log_gl.fsync_lsn_lock);
}

void cmdlog_complete_dual_write(bool success)
{
    log_BUFFER *logbuff = &log_gl.log_buffer;
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    log_FREQ_info *fqinfo = &log_gl.log_buffer.fqinfo;
#endif

    pthread_mutex_lock(&log_gl.log_flush_lock);
    do {
        if (log_gl.log_file.next_fd == -1) {
            /* next_fd == -1 means the first state without log file.
             * created first log file by checkpoint.
             * do not cleanup file fds.
             */
            break;
        }
        if (success) {
            pthread_mutex_lock(&log_gl.log_write_lock);
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
            bool next_fhlsn_flag = false;
            if (fqinfo->fend != -1) {
                if (logbuff->fque[fqinfo->fend].nflush > 0) {
                    do_flush_request_alloc();
                }
                /* Set the position where a dual write end. */
                assert(fqinfo->dw_end == -1);
                fqinfo->dw_end = fqinfo->fend;
            } else {
                /* flush request is empty, update flush_lsn */
                next_fhlsn_flag = true;
            }
#else
            if (logbuff->fque[logbuff->fend].nflush > 0) {
                if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
            }
            /* Set the position where a dual write end. */
            assert(logbuff->dw_end == -1);
            logbuff->dw_end = logbuff->fend;
#endif

            /* update nxt_write_lsn */
            log_gl.nxt_write_lsn.filenum += 1;
            log_gl.nxt_write_lsn.roffset = 0;
            pthread_mutex_unlock(&log_gl.log_write_lock);

            assert(log_gl.log_file.prev_fd == -1);
            log_gl.log_file.prev_fd = log_gl.log_file.fd;
            log_gl.log_file.fd      = log_gl.log_file.next_fd;
            log_gl.log_file.next_fd = -1;
            log_gl.log_file.size = log_gl.log_file.next_size;
            log_gl.log_file.next_size = 0;

#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
            if (next_fhlsn_flag) {
                pthread_mutex_lock(&log_gl.flush_lsn_lock);
                log_gl.nxt_flush_lsn.filenum += 1;
                log_gl.nxt_flush_lsn.roffset = 0;
                pthread_mutex_unlock(&log_gl.flush_lsn_lock);
            }
#endif
        } else {
            pthread_mutex_lock(&log_gl.log_write_lock);
            /* reset dual_write flag in flush reqeust queue */
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
            int32_t index = fqinfo->fbgn;
            while (index != -1) {
                logbuff->fque[index].dual_write = false;
                index = logbuff->fque[index].next_eid;
            }
#else
            int index = logbuff->fbgn;
            while (logbuff->fque[index].nflush > 0) {
                if (logbuff->fque[index].dual_write) {
                    logbuff->fque[index].dual_write = false;
                }
                if ((++index) == logbuff->fqsz) index = 0;
            }
#endif
            pthread_mutex_unlock(&log_gl.log_write_lock);

            assert(log_gl.log_file.prev_fd == -1);
            log_gl.log_file.prev_fd = log_gl.log_file.next_fd;
            log_gl.log_file.next_fd = -1;
            log_gl.log_file.next_size = 0;
        }
    } while(0);
    pthread_mutex_unlock(&log_gl.log_flush_lock);
}

int cmdlog_file_open(char *path)
{
    log_FILE *logfile = &log_gl.log_file;
    int ret = 0;

    pthread_mutex_lock(&log_gl.log_flush_lock);
    /* prepare cmdlog file */
    do {
        int fd = disk_open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP);
        if (fd < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to open the cmdlog file. path=%s err=%s\n",
                        logfile->path, strerror(errno));
            ret = -1;
            break;
        }
        snprintf(logfile->path, CMDLOG_MAX_FILEPATH_LENGTH, "%s", path);
        if (logfile->fd == -1) {
            logfile->fd = fd;
        } else {
            /* fd != -1 means that a new cmdlog file is created by checkpoint */
            logfile->next_fd = fd;
        }
    } while(0);
    pthread_mutex_unlock(&log_gl.log_flush_lock);

    return ret;
}

void cmdlog_file_close(bool shutdown)
{
    log_FILE *logfile = &log_gl.log_file;

    pthread_mutex_lock(&log_gl.log_flush_lock);
    if (logfile->next_fd != -1) {
        (void)disk_close(logfile->next_fd);
        logfile->next_fd = -1;
    }
    if (logfile->prev_fd != -1) {
        (void)disk_close(logfile->prev_fd);
        logfile->prev_fd = -1;
    }
    if (shutdown && logfile->fd != -1) {
        (void)disk_fsync(logfile->fd);
        (void)disk_close(logfile->fd);
        logfile->fd = -1;
    }
    pthread_mutex_unlock(&log_gl.log_flush_lock);
}

size_t cmdlog_file_getsize(void)
{
    log_FILE *logfile = &log_gl.log_file;
    size_t file_size = 0;

    pthread_mutex_lock(&log_gl.log_flush_lock);
    pthread_mutex_lock(&log_gl.log_write_lock);
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    if (log_gl.log_buffer.fqinfo.dw_end == -1) {
        file_size = logfile->size;
    }
#else
    if (log_gl.log_buffer.dw_end == -1) {
        file_size = logfile->size;
    }
#endif
    pthread_mutex_unlock(&log_gl.log_write_lock);
    pthread_mutex_unlock(&log_gl.log_flush_lock);

    return file_size;
}

int cmdlog_file_apply(void)
{
    log_FILE *logfile = &log_gl.log_file;
    assert(logfile->fd > 0);

    logger->log(EXTENSION_LOG_INFO, NULL,
                "[RECOVERY - CMDLOG] applying command log file. path=%s\n", logfile->path);

    struct stat file_stat;
    fstat(logfile->fd, &file_stat);
    logfile->size = file_stat.st_size;
    if (logfile->size == 0) {
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "[RECOVERY - CMDLOG] log file is empty.\n");
        return 0;
    }

    int ret = 0;
    int seek_offset = 0;
    char buf[MAX_LOG_RECORD_SIZE];
    while (log_gl.initialized && seek_offset < logfile->size) {
        LogRec *logrec = (LogRec*)buf;
        LogHdr *loghdr = &logrec->header;

        if (logfile->size - seek_offset < sizeof(LogHdr)) {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "[RECOVERY - CMDLOG] header of last command was not completely written. "
                        "header_length=%ld\n", sizeof(LogHdr));
            break;
        }

        ssize_t nread = read(logfile->fd, loghdr, sizeof(LogHdr));
        if (nread != sizeof(LogHdr)) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[RECOVERY - CMDLOG] failed : read header data "
                        "nread(%zd) != header_length(%lu).\n", nread, sizeof(LogHdr));
            ret = -1; break;
        }
        seek_offset += nread;

        if (logfile->size - seek_offset < loghdr->body_length) {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "[RECOVERY - CMDLOG] body of last command was not completely written. "
                        "body_length=%d\n", loghdr->body_length);
            seek_offset = lseek(logfile->fd, -nread, SEEK_CUR);
            if (seek_offset < 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : lseek(SEEK_CUR-%zd). path=%s, error=%s.\n",
                            nread, logfile->path, strerror(errno));
                ret = -1;
            }
            break;
        }

        if (loghdr->body_length > 0) {
            int free = MAX_LOG_RECORD_SIZE - nread;
            if (free < loghdr->body_length) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : insufficient memory "
                            "free(%d) < body_length(%u).\n", free, loghdr->body_length);
                ret = -1; break;
            }
            logrec->body = buf + nread;
            nread = read(logfile->fd, logrec->body, loghdr->body_length);
            if (nread != loghdr->body_length) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : read body data "
                            "nread(%zd) != body_length(%u).\n", nread, loghdr->body_length);
                ret = -1; break;
            }
            seek_offset += nread;
            if (lrec_redo_from_record(logrec) != ENGINE_SUCCESS) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : log record redo.\n");
                ret = -1; break;
            }
        }
    }
    if (ret < 0) {
        close(logfile->fd);
    } else {
        logfile->size = seek_offset;
        logger->log(EXTENSION_LOG_INFO, NULL, "[RECOVERY - CMDLOG] success.\n");
    }
    return ret;
}

ENGINE_ERROR_CODE cmdlog_buf_init(struct default_engine* engine)
{
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    int i;
#endif
    logger = engine->server.log->get_logger();

    memset(&log_gl, 0, sizeof(log_gl));

    /* log global init */
    LOGSN_SET_NULL(&log_gl.nxt_write_lsn);
    LOGSN_SET_NULL(&log_gl.nxt_flush_lsn);
    LOGSN_SET_NULL(&log_gl.nxt_fsync_lsn);

    pthread_mutex_init(&log_gl.log_write_lock, NULL);
    pthread_mutex_init(&log_gl.log_flush_lock, NULL);
//    pthread_mutex_init(&log_gl.log_fsync_lock, NULL);
    pthread_mutex_init(&log_gl.flush_lsn_lock, NULL);
//    pthread_mutex_init(&log_gl.fsync_lsn_lock, NULL);

    /* log file init */
    log_FILE *logfile = &log_gl.log_file;
    logfile->path[0]   = '\0';
    logfile->prev_fd   = -1;
    logfile->fd        = -1;
    logfile->next_fd   = -1;
    logfile->size      = 0;
    logfile->next_size = 0;

    /* log buffer init */
    log_BUFFER *logbuff = &log_gl.log_buffer;

    logbuff->size = CMDLOG_BUFFER_SIZE;
    logbuff->data = malloc(logbuff->size);
    if (logbuff->data == NULL) {
        return ENGINE_ENOMEM;
    }
    logbuff->head = 0;
    logbuff->tail = 0;
    logbuff->last = -1;

    /* log flush request queue init - ring shaped queue */
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    log_FREQ_info *fqinfo = &log_gl.log_buffer.fqinfo;
    fqinfo->cur_requests = 0;
    fqinfo->max_requests = FRQUEUE_CHUNK_SIZE;
    logbuff->fque = (log_FREQ*)malloc(fqinfo->max_requests * sizeof(log_FREQ));
#else
    logbuff->fqsz = (logbuff->size / CMDLOG_FLUSH_AUTO_SIZE) * 2;
    logbuff->fque = (log_FREQ*)malloc(logbuff->fqsz * sizeof(log_FREQ));
#endif
    if (logbuff->fque == NULL) {
        free(logbuff->data);
        return ENGINE_ENOMEM;
    }
#ifdef ENABLE_PERSISTENCE_03_ENHANCE_FQUEUE
    for (i = 0; i < fqinfo->max_requests; i++) {
        logbuff->fque[i].nflush = 0;
        logbuff->fque[i].curr_eid = i;
        if (i < (fqinfo->max_requests-1)) logbuff->fque[i].next_eid = i+1;
        else                              logbuff->fque[i].next_eid = -1;
        logbuff->fque[i].prev_eid = -1;
        logbuff->fque[i].dual_write = false;
    }
    fqinfo->free_list = 0;
    fqinfo->fbgn = -1;
    fqinfo->fend = -1;
    fqinfo->dw_end = -1;
#else
    memset(logbuff->fque, 0, logbuff->fqsz * sizeof(log_FREQ));
    logbuff->fbgn = 0;
    logbuff->fend = 0;
    logbuff->dw_end = -1;
#endif

    /* log flush thread init */
    log_FLUSHER *flusher = &log_gl.log_flusher;
    pthread_mutex_init(&flusher->lock, NULL);
    pthread_cond_init(&flusher->cond, NULL);
    flusher->sleep = false;
    flusher->start = false;
    flusher->init = false;

    /* TODO: check and initialize fsync thread structure */

    log_gl.initialized = true;
    logger->log(EXTENSION_LOG_INFO, NULL, "CMDLOG BUFFER module initialized.\n");

    return ENGINE_SUCCESS;
}

void cmdlog_buf_final(void)
{
    log_gl.initialized = false;

    /* log buffer final */
    log_BUFFER *logbuff = &log_gl.log_buffer;

    if (logbuff->data != NULL) {
        free((void*)logbuff->data);
        logbuff->data = NULL;
    }

    if (logbuff->fque != NULL) {
        free((void*)logbuff->fque);
        logbuff->fque = NULL;
    }

    /* log file final */
    cmdlog_file_close(true);

    logger->log(EXTENSION_LOG_INFO, NULL, "CMDLOG BUFFER module destroyed.\n");
}

ENGINE_ERROR_CODE cmdlog_buf_flush_thread_start(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.
    pthread_t tid;

    /* create log flush thread */
    log_gl.log_flusher.init = true;
    if (pthread_create(&tid, NULL, log_flush_thread_main, NULL) != 0) {
        log_gl.log_flusher.init = false;
        logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to create cmdlog flush thread\n");
        return ENGINE_FAILED;
    }

    /* TODO: create log fsync thread */

    /* wait until log flush thread starts */
    while (log_gl.log_flusher.start == false) {
        nanosleep(&sleep_time, NULL);
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "[INIT] daemon thread - cmdlog flush thread started.\n");

    /* TODO: wait until log fsync thread starts */


    return ENGINE_SUCCESS;
}

void cmdlog_buf_flush_thread_stop(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.

    if (log_gl.log_flusher.init == true) {
        /* stop request */
        pthread_mutex_lock(&log_gl.log_flusher.lock);
        log_gl.log_flusher.init = false;
        pthread_mutex_unlock(&log_gl.log_flusher.lock);

        /* wait until the log flush thread stops */
        while (log_gl.log_flusher.start == true) {
            if (log_gl.log_flusher.sleep == true) {
                do_log_flusher_wakeup();
            }
            nanosleep(&sleep_time, NULL);
        }
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "[FINAL] daemon thread - cmdlog flush thread stopped.\n");

    /* TODO: fsync thread stop and wait until stopped */
}
#endif
