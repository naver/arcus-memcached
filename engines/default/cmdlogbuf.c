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
#define CMDLOG_FLUSH_AUTO_SIZE (32 * 1024) /* 32 KB : see the nflush data type of log_FREQ */
#define CMDLOG_RECORD_MIN_SIZE 16          /* 8 bytes header + 8 bytes body */
#define CMDLOG_MAX_FILEPATH_LENGTH 255

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
    pthread_mutex_t log_fsync_lock;
    pthread_mutex_t flush_lsn_lock;
    pthread_mutex_t fsync_lsn_lock;
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

static void do_log_flusher_wakeup(log_FLUSHER *flusher)
{
    pthread_mutex_lock(&flusher->lock);
    if (flusher->sleep) {
        pthread_cond_signal(&flusher->cond);
    }
    pthread_mutex_unlock(&flusher->lock);
}

static void do_log_file_sync(int fd, bool close)
{
    int ret = disk_fsync(fd);
    if (ret < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "log file fsync error (%d:%s)\n",
                    errno, strerror(errno));
        /* [FATAL] untreatable error => abnormal shutdown by assertion */
    }
    assert(ret == 0);

    if (close) {
        ret = disk_close(fd);
        if (ret < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "log file close error (%d:%s)\n",
                        errno, strerror(errno));
            /* [FATAL] untreatable error => abnormal shutdown by assertion */
        }
        assert(ret == 0);
    }
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
    uint32_t    nflush = 0;
    bool        dual_write_flag = false;
    bool        next_fhlsn_flag = false;
    bool        cleanup_process = false;

    /* computate flush size */
    pthread_mutex_lock(&log_gl.log_write_lock);
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
            if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
        }
    }
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
        /* clear the flush request itself */
        logbuff->fque[logbuff->fbgn].nflush = 0;
        logbuff->fque[logbuff->fbgn].dual_write = false;
        if ((++logbuff->fbgn) == logbuff->fqsz) logbuff->fbgn = 0;
        pthread_mutex_unlock(&log_gl.log_write_lock);
    }
    return nflush;
}

static void do_log_buff_write(LogRec *logrec, log_waiter_t *waiter, bool dual_write)
{
    log_BUFFER *logbuff = &log_gl.log_buffer;
    uint32_t total_length = sizeof(LogHdr) + logrec->header.body_length;
    uint32_t spare_length;
    assert(total_length < logbuff->size);

    pthread_mutex_lock(&log_gl.log_write_lock);

    if (waiter != NULL) {
        waiter->lsn = log_gl.nxt_write_lsn;
    }

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

    pthread_mutex_unlock(&log_gl.log_write_lock);

    /* wake up log flush thread if flush requests exist */
    if (logbuff->fbgn != logbuff->fend) {
        if (log_gl.log_flusher.sleep == true) {
            do_log_flusher_wakeup(&log_gl.log_flusher);
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
void log_file_sync(void)
{
    LogSN now_flush_lsn;

    /* get current nxt_flush_lsn */
    log_get_flush_lsn(&now_flush_lsn);

    /* fsync the log files */
    pthread_mutex_lock(&log_gl.log_fsync_lock);
    if (1) {
        do_log_file_sync(log_gl.log_file.fd, false); /* do not close */
        if (log_gl.log_file.next_fd != -1) {
            /* fsync and close the prev log file */
            do_log_file_sync(log_gl.log_file.next_fd, false); /* do close */
        }

        /* update nxt_fsync_lsn */
        pthread_mutex_lock(&log_gl.fsync_lsn_lock);
        log_gl.nxt_fsync_lsn = now_flush_lsn;
        pthread_mutex_unlock(&log_gl.fsync_lsn_lock);
    }
    pthread_mutex_unlock(&log_gl.log_fsync_lock);
}

void log_buffer_flush(LogSN *upto_lsn)
{
    assert(upto_lsn);
    uint32_t nflush;

    do {
        pthread_mutex_lock(&log_gl.log_flush_lock);
        if (LOGSN_IS_LE(&log_gl.nxt_flush_lsn, upto_lsn)) {
            nflush = do_log_buff_flush(true);
            assert(nflush > 0);
            if (LOGSN_IS_GT(&log_gl.nxt_flush_lsn, upto_lsn)) {
                nflush = 0;
            }
        } else {
            nflush = 0;
        }
        pthread_mutex_unlock(&log_gl.log_flush_lock);
    } while (nflush > 0);
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
    pthread_mutex_lock(&log_gl.fsync_lsn_lock);
    *lsn = log_gl.nxt_fsync_lsn;
    pthread_mutex_unlock(&log_gl.fsync_lsn_lock);
}

void cmdlog_complete_dual_write(bool success)
{
    log_BUFFER *logbuff = &log_gl.log_buffer;

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
            if (logbuff->fque[logbuff->fend].nflush > 0) {
                if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
            }
            /* Set the position where a dual write end. */
            assert(logbuff->dw_end == -1);
            logbuff->dw_end = logbuff->fend;

            /* update nxt_write_lsn */
            log_gl.nxt_write_lsn.filenum += 1;
            log_gl.nxt_write_lsn.roffset = 0;
            pthread_mutex_unlock(&log_gl.log_write_lock);

            assert(log_gl.log_file.prev_fd == -1);
            pthread_mutex_lock(&log_gl.log_fsync_lock);
            log_gl.log_file.prev_fd = log_gl.log_file.fd;
            log_gl.log_file.fd      = log_gl.log_file.next_fd;
            log_gl.log_file.next_fd = -1;
            pthread_mutex_unlock(&log_gl.log_fsync_lock);
            log_gl.log_file.size = log_gl.log_file.next_size;
            log_gl.log_file.next_size = 0;
        } else {
            pthread_mutex_lock(&log_gl.log_write_lock);
            /* reset dual_write flag in flush reqeust queue */
            int index = logbuff->fbgn;
            while (logbuff->fque[index].nflush > 0) {
                if (logbuff->fque[index].dual_write) {
                    logbuff->fque[index].dual_write = false;
                }
                if ((++index) == logbuff->fqsz) index = 0;
            }
            pthread_mutex_unlock(&log_gl.log_write_lock);

            assert(log_gl.log_file.prev_fd == -1);
            pthread_mutex_lock(&log_gl.log_fsync_lock);
            log_gl.log_file.prev_fd = log_gl.log_file.next_fd;
            log_gl.log_file.next_fd = -1;
            pthread_mutex_unlock(&log_gl.log_fsync_lock);
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
        pthread_mutex_lock(&log_gl.log_fsync_lock);
        if (logfile->fd == -1) {
            logfile->fd = fd;
        } else {
            /* fd != -1 means that a new cmdlog file is created by checkpoint */
            logfile->next_fd = fd;
        }
        pthread_mutex_unlock(&log_gl.log_fsync_lock);
    } while(0);
    pthread_mutex_unlock(&log_gl.log_flush_lock);

    return ret;
}

void cmdlog_file_close(bool shutdown)
{
    log_FILE *logfile = &log_gl.log_file;

    pthread_mutex_lock(&log_gl.log_flush_lock);
    if (logfile->next_fd != -1) {
        pthread_mutex_lock(&log_gl.log_fsync_lock);
        (void)disk_close(logfile->next_fd);
        logfile->next_fd = -1;
        pthread_mutex_unlock(&log_gl.log_fsync_lock);
    }
    if (logfile->prev_fd != -1) {
        (void)disk_close(logfile->prev_fd);
        logfile->prev_fd = -1;
    }
    if (shutdown && logfile->fd != -1) {
        pthread_mutex_lock(&log_gl.log_fsync_lock);
        (void)disk_fsync(logfile->fd);
        (void)disk_close(logfile->fd);
        logfile->fd = -1;
        pthread_mutex_unlock(&log_gl.log_fsync_lock);
    }
    pthread_mutex_unlock(&log_gl.log_flush_lock);
}

size_t cmdlog_file_getsize(void)
{
    log_FILE *logfile = &log_gl.log_file;
    size_t file_size = 0;

    pthread_mutex_lock(&log_gl.log_flush_lock);
    pthread_mutex_lock(&log_gl.log_write_lock);
    if (log_gl.log_buffer.dw_end == -1) {
        file_size = logfile->size;
    }
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
    logger = engine->server.log->get_logger();

    memset(&log_gl, 0, sizeof(log_gl));

    /* log global init */
    log_gl.nxt_fsync_lsn.filenum = 1;
    log_gl.nxt_fsync_lsn.roffset = 0;
    log_gl.nxt_flush_lsn = log_gl.nxt_fsync_lsn;
    log_gl.nxt_write_lsn = log_gl.nxt_fsync_lsn;

    pthread_mutex_init(&log_gl.log_write_lock, NULL);
    pthread_mutex_init(&log_gl.log_flush_lock, NULL);
    pthread_mutex_init(&log_gl.log_fsync_lock, NULL);
    pthread_mutex_init(&log_gl.flush_lsn_lock, NULL);
    pthread_mutex_init(&log_gl.fsync_lsn_lock, NULL);

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
    log_FLUSHER *flusher = &log_gl.log_flusher;
    pthread_mutex_init(&flusher->lock, NULL);
    pthread_cond_init(&flusher->cond, NULL);
    flusher->sleep = false;
    flusher->start = false;
    flusher->init = false;

    log_gl.initialized = true;
    logger->log(EXTENSION_LOG_INFO, NULL, "CMDLOG BUFFER module initialized.\n");

    return ENGINE_SUCCESS;
}

void cmdlog_buf_final(void)
{
    if (log_gl.initialized == false) {
        return;
    }

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

    pthread_mutex_destroy(&log_gl.log_write_lock);
    pthread_mutex_destroy(&log_gl.log_flush_lock);
    pthread_mutex_destroy(&log_gl.flush_lsn_lock);
    log_gl.initialized = false;
    logger->log(EXTENSION_LOG_INFO, NULL, "CMDLOG BUFFER module destroyed.\n");
}

ENGINE_ERROR_CODE cmdlog_buf_flush_thread_start(void)
{
    pthread_t tid;
    /* create log flush thread */
    log_gl.log_flusher.init = true;
    if (pthread_create(&tid, NULL, log_flush_thread_main, NULL) != 0) {
        log_gl.log_flusher.init = false;
        logger->log(EXTENSION_LOG_WARNING, NULL, "Failed to create command log flush thread.\n");
        return ENGINE_FAILED;
    }

    /* wait until log flush thread starts */
    while (log_gl.log_flusher.start == false) {
        usleep(5000); /* sleep 5ms */
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "Command log flush thread started.\n");

    return ENGINE_SUCCESS;
}

void cmdlog_buf_flush_thread_stop(void)
{
    if (log_gl.initialized == false) {
        return;
    }

    log_FLUSHER *flusher = &log_gl.log_flusher;
    if (flusher->init == true) {
        /* stop request */
        pthread_mutex_lock(&flusher->lock);
        flusher->init = false;
        pthread_mutex_unlock(&flusher->lock);

        /* wait until the log flush thread stops */
        while (flusher->start == true) {
            do_log_flusher_wakeup(flusher);
            usleep(5000); /* sleep 5ms */
        }
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "Command log flush thread stopped.\n");
}
#endif
