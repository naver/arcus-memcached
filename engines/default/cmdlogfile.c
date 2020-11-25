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
#include "cmdlogfile.h"
#include "cmdlogrec.h"
#include "cmdlogbuf.h"

#define ENABLE_DEBUG 0

/* log file structure */
typedef struct _log_file {
    char      path[MAX_FILEPATH_LENGTH];
    int       prev_fd;
    int       fd;
    int       next_fd;
    size_t    size;
    size_t    next_size;
} log_FILE;

/* log file global structure */
struct log_file_global {
    log_FILE        log_file;
    LogSN           nxt_fsync_lsn;
    pthread_mutex_t log_fsync_lock;
    pthread_mutex_t fsync_lsn_lock;
    pthread_mutex_t file_access_lock;
    volatile bool   initialized;
};

/* global data */
static struct engine_config *config = NULL;
static EXTENSION_LOGGER_DESCRIPTOR *logger = NULL;
static struct log_file_global log_file_gl;

/*
 * Static Functions for DIsk IO
 * FIXME: These can be moved to a separate disk.c file later.
 */
/***************/

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

static off_t disk_lseek(int fd, off_t offset, int whence)
{
    off_t ret = lseek(fd, offset, whence);
    return ret;
}

static ssize_t disk_read(int fd, void *buf, size_t count)
{
    char   *bfptr = (char*)buf;
    ssize_t nleft = count;
    ssize_t nread;

    while (nleft > 0) {
        nread = read(fd, bfptr, nleft);
        if (nread == 0) break;
        if (nread <  0) {
            if (errno == EINTR) continue;
            return nread;
        }
        nleft -= nread;
        bfptr += nread;
    }
    return (count - nleft);
}

static ssize_t disk_write(int fd, void *buf, size_t count)
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

void cmdlog_file_write(char *log_ptr, uint32_t log_size, bool dual_write)
{
    log_FILE *logfile = &log_file_gl.log_file;
    ssize_t nwrite;
    assert(logfile->fd != -1);

    pthread_mutex_lock(&log_file_gl.file_access_lock);
    /* The log data is appended */
    nwrite = disk_write(logfile->fd, log_ptr, log_size);
    if (nwrite != log_size) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "curr log file(%d) write - write(%ld!=%ld) error=(%d:%s)\n",
                    logfile->fd, nwrite, (ssize_t)log_size,
                    errno, strerror(errno));
    }
    /* FIXME::need error handling */
    assert(nwrite == log_size);
    logfile->size += log_size;

    if (dual_write && logfile->next_fd != -1) {
        /* The log data is appended */
        nwrite = disk_write(logfile->next_fd, log_ptr, log_size);
        if (nwrite != log_size) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "next log file(%d) write - write(%ld!=%ld) error=(%d:%s)\n",
                        logfile->next_fd, nwrite, (ssize_t)log_size,
                        errno, strerror(errno));
        }
        /* FIXME::need error handling */
        assert(nwrite == log_size);
        logfile->next_size += log_size;
    }
    pthread_mutex_unlock(&log_file_gl.file_access_lock);
}

void cmdlog_file_complete_dual_write(void)
{
    log_FILE *logfile = &log_file_gl.log_file;

    pthread_mutex_lock(&log_file_gl.file_access_lock);
    if (logfile->next_fd != -1) {
        logfile->prev_fd   = logfile->fd;
        logfile->fd        = logfile->next_fd;
        logfile->size      = logfile->next_size;
        logfile->next_fd   = -1;
        logfile->next_size = 0;

        if (config->async_logging) {
            (void)disk_close(logfile->prev_fd);
            logfile->prev_fd = -1;
        }
    }
    pthread_mutex_unlock(&log_file_gl.file_access_lock);
}

bool cmdlog_file_dual_write_finished(void)
{
    log_FILE *logfile = &log_file_gl.log_file;
    bool finished = false;
    pthread_mutex_lock(&log_file_gl.file_access_lock);
    if (logfile->next_fd == -1 && logfile->prev_fd == -1) {
        finished = true;
    }
    pthread_mutex_unlock(&log_file_gl.file_access_lock);
    return finished;
}

int cmdlog_file_sync(void)
{
    log_FILE *logfile = &log_file_gl.log_file;
    LogSN now_flush_lsn;
    int fd;
    int prev_fd = -1;
    int next_fd = -1;
    int ret = 0;

    pthread_mutex_lock(&log_file_gl.log_fsync_lock);

    /* get current flush lsn */
    cmdlog_get_flush_lsn(&now_flush_lsn);

    /* get current fd info */
    pthread_mutex_lock(&log_file_gl.file_access_lock);
    if (logfile->prev_fd != -1) {
        prev_fd = logfile->prev_fd;
        logfile->prev_fd = -1;
    }
    fd = logfile->fd;
    if (logfile->next_fd != -1) {
        next_fd = logfile->next_fd;
    }
    pthread_mutex_unlock(&log_file_gl.file_access_lock);

    if (prev_fd != -1) {
        (void)disk_fsync(prev_fd);
        (void)disk_close(prev_fd);
    }

    if (LOGSN_IS_GT(&now_flush_lsn, &log_file_gl.nxt_fsync_lsn)) {
        do {
            /* fsync curr fd */
            ret = disk_fsync(fd);
            if (ret < 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "log file fsync error (%d:%s)\n",
                            errno, strerror(errno));
                break;
            }

            if (next_fd != -1) {
                ret = disk_fsync(next_fd);
                if (ret < 0) {
                    logger->log(EXTENSION_LOG_WARNING, NULL,
                                "log file fsync error (%d:%s)\n",
                                errno, strerror(errno));
                    break;
                }
            }

            /* update nxt_fsync_lsn */
            pthread_mutex_lock(&log_file_gl.fsync_lsn_lock);
            log_file_gl.nxt_fsync_lsn = now_flush_lsn;
            pthread_mutex_unlock(&log_file_gl.fsync_lsn_lock);
        } while(0);
    }
    pthread_mutex_unlock(&log_file_gl.log_fsync_lock);

    return ret;
}

int cmdlog_file_open(char *path)
{
    log_FILE *logfile = &log_file_gl.log_file;
    int fd, ret = 0;

    pthread_mutex_lock(&log_file_gl.file_access_lock);
    do {
        fd = disk_open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP);
        if (fd < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to open the cmdlog file. path=%s err=%s\n",
                        logfile->path, strerror(errno));
            ret = -1; break;
        }
        snprintf(logfile->path, MAX_FILEPATH_LENGTH, "%s", path);
        if (logfile->fd == -1) {
            logfile->fd = fd;
        } else {
            /* fd != -1 means that a new cmdlog file is created by checkpoint */
            logfile->next_fd = fd;
        }
    } while(0);
    pthread_mutex_unlock(&log_file_gl.file_access_lock);

    return ret;
}

void cmdlog_file_close(void)
{
    log_FILE *logfile = &log_file_gl.log_file;
    int remove_fd;

    /* We hold log_fsync_lock to prevent fsync() call
     * on the file being closed. See cmdlog_file_sync().
     */
    pthread_mutex_lock(&log_file_gl.log_fsync_lock);
    pthread_mutex_lock(&log_file_gl.file_access_lock);
    if (logfile->next_fd != -1) {
        remove_fd = logfile->next_fd;
        logfile->next_fd = -1;
    } else { /* the first checkpoint */
        assert(logfile->fd != -1);
        remove_fd = logfile->fd;
        logfile->fd = -1;
    }
    pthread_mutex_unlock(&log_file_gl.file_access_lock);
    pthread_mutex_unlock(&log_file_gl.log_fsync_lock);

    assert(remove_fd != -1);
    (void)disk_close(remove_fd);
}

void cmdlog_file_init(struct default_engine* engine)
{
    config = &engine->config;
    logger = engine->server.log->get_logger();

    /* log file global init */
    memset(&log_file_gl, 0, sizeof(log_file_gl));

    log_file_gl.nxt_fsync_lsn.filenum = 1;
    log_file_gl.nxt_fsync_lsn.roffset = 0;

    pthread_mutex_init(&log_file_gl.log_fsync_lock, NULL);
    pthread_mutex_init(&log_file_gl.fsync_lsn_lock, NULL);
    pthread_mutex_init(&log_file_gl.file_access_lock, NULL);

    /* log file init */
    log_FILE *logfile = &log_file_gl.log_file;
    logfile->path[0]   = '\0';
    logfile->prev_fd   = -1;
    logfile->fd        = -1;
    logfile->next_fd   = -1;
    logfile->size      = 0;
    logfile->next_size = 0;

    log_file_gl.initialized = true;
    logger->log(EXTENSION_LOG_INFO, NULL, "CMDLOG FILE module initialized.\n");
}

void cmdlog_file_final(void)
{
    log_FILE *logfile = &log_file_gl.log_file;

    if (log_file_gl.initialized == false) {
        return;
    }

    /* Don't need to hold log_fsync_lock because this function is called
     * after stopping cmdlog thread. See cmdlog_mgr_final().
     */
    if (logfile->fd != -1) {
        (void)disk_fsync(logfile->fd);
        (void)disk_close(logfile->fd);
        logfile->fd = -1;
    }
    if (logfile->next_fd != -1) {
        (void)disk_close(logfile->next_fd);
        logfile->next_fd = -1;
    }

    pthread_mutex_destroy(&log_file_gl.log_fsync_lock);
    pthread_mutex_destroy(&log_file_gl.fsync_lsn_lock);
    pthread_mutex_destroy(&log_file_gl.file_access_lock);

    log_file_gl.initialized = false;
    logger->log(EXTENSION_LOG_INFO, NULL, "CMDLOG FILE module destroyed.\n");
}

static int do_redo_pending_lrec(int fd, int start_offset, int end_offset)
{
    char buf[MAX_LOG_RECORD_SIZE];
    LogRec *logrec = (LogRec*)buf;
    LogHdr *loghdr = &logrec->header;

    int ret = 0;
    ssize_t nread = 0;
    int redo_offset = lseek(fd, (start_offset - end_offset), SEEK_CUR);
    while (redo_offset < end_offset) {
        nread = disk_read(fd, loghdr, sizeof(LogHdr));
        if (nread != sizeof(LogHdr)) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[RECOVERY - CMDLOG] failed : read header data "
                        "nread(%zd) != header_length(%lu).\n", nread, sizeof(LogHdr));
            ret = -1; break;
        }
        redo_offset += nread;
        if (loghdr->body_length > 0) {
            logrec->body = buf + nread;
            nread = disk_read(fd, logrec->body, loghdr->body_length);
            if (nread != loghdr->body_length) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : read body data "
                            "nread(%zd) != body_length(%u).\n", nread, loghdr->body_length);
                ret = -1; break;
            }
            redo_offset += nread;
            ENGINE_ERROR_CODE err = lrec_redo_from_record(logrec);
            if (err != ENGINE_SUCCESS) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] warning : log record redo failed.\n");
                if (err == ENGINE_ENOMEM) {
                    logger->log(EXTENSION_LOG_WARNING, NULL,
                                "[RECOVERY - CMDLOG] failed : out of memory.\n");
                    ret = -1; break;
                }
            }
        }
    }
    return ret;
}

int cmdlog_file_apply(void)
{
    log_FILE *logfile = &log_file_gl.log_file;
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

    int  ret = 0;
    int  seek_offset = 0;
    int  pending_start_offset = 0;
    int  pending_end_offset = 0;
    bool pending = false;
    char buf[MAX_LOG_RECORD_SIZE];
    LogRec *logrec = (LogRec*)buf;
    LogHdr *loghdr = &logrec->header;

    while (log_file_gl.initialized && seek_offset < logfile->size) {

        /* read header */
        if (logfile->size - seek_offset < sizeof(LogHdr)) {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "[RECOVERY - CMDLOG] header of last log record was not completely written. "
                        "header_length=%ld\n", sizeof(LogHdr));
            break;
        }

        ssize_t nread = disk_read(logfile->fd, loghdr, sizeof(LogHdr));
        if (nread != sizeof(LogHdr)) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[RECOVERY - CMDLOG] failed : read header data "
                        "nread(%zd) != header_length(%lu).\n", nread, sizeof(LogHdr));
            ret = -1; break;
        }
        seek_offset += nread;

        if (loghdr->logtype == LOG_OPERATION_BEGIN) {
            if (pending == true) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : LOG_OPERATION_BEGIN recorded "
                            "before previous kept log record is processed.\n");
                ret = -1; break;
            }
            pending_start_offset = seek_offset;
            pending = true;
            continue;
        }

        if (loghdr->logtype == LOG_OPERATION_END) {
            if (pending == false) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : "
                            "LOG_OPERATION_END recorded without LOG_OPERATION_BEGIN.\n");
                ret = -1; break;
            }
            /* redo pending normal log records */
            pending_end_offset = seek_offset;
            if (do_redo_pending_lrec(logfile->fd, pending_start_offset, pending_end_offset) < 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : pending log record redo failed\n");
                ret = -1; break;
            }

            /* Complete redo of pending log records. cleanup pending info */
            pending_start_offset = 0;
            pending_end_offset = 0;
            pending = false;
            continue;
        }

        /* read body */
        if (loghdr->body_length > 0) {
            if (logfile->size - seek_offset < loghdr->body_length) {
                logger->log(EXTENSION_LOG_INFO, NULL,
                            "[RECOVERY - CMDLOG] body of last log record was not completely written. "
                            "body_length=%d\n", loghdr->body_length);
                seek_offset = disk_lseek(logfile->fd, -sizeof(LogHdr), SEEK_CUR);
                if (seek_offset < 0) {
                    logger->log(EXTENSION_LOG_WARNING, NULL,
                                "[RECOVERY - CMDLOG] failed : lseek(SEEK_CUR-%zd). path=%s, error=%s.\n",
                                sizeof(LogHdr), logfile->path, strerror(errno));
                    ret = -1;
                }
                break;
            }

            int max_body_length = MAX_LOG_RECORD_SIZE - sizeof(LogHdr);
            if (max_body_length < loghdr->body_length) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : body length is abnormally too big "
                            "max_body_length(%d) < body_length(%u).\n",
                            max_body_length, loghdr->body_length);
                ret = -1; break;
            }
            logrec->body = buf + sizeof(LogHdr);
            nread = disk_read(logfile->fd, logrec->body, loghdr->body_length);
            if (nread != loghdr->body_length) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : read body data "
                            "nread(%zd) != body_length(%u).\n", nread, loghdr->body_length);
                ret = -1; break;
            }
            seek_offset += loghdr->body_length;
        }

        if (pending) continue;

        /* redo log record */
        ENGINE_ERROR_CODE err = lrec_redo_from_record(logrec);
        if (err != ENGINE_SUCCESS) {
            /* don't care a log record redo failure. read next log record and redo it. */
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[RECOVERY - CMDLOG] warning : log record redo failed.\n");
            if (err == ENGINE_ENOMEM) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "[RECOVERY - CMDLOG] failed : out of memory.\n");
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

size_t cmdlog_file_getsize(void)
{
    log_FILE *logfile = &log_file_gl.log_file;
    size_t size;
    pthread_mutex_lock(&log_file_gl.file_access_lock);
    size = logfile->size;
    pthread_mutex_unlock(&log_file_gl.file_access_lock);

    return size;
}

void cmdlog_get_fsync_lsn(LogSN *lsn)
{
    pthread_mutex_lock(&log_file_gl.fsync_lsn_lock);
    *lsn = log_file_gl.nxt_fsync_lsn;
    pthread_mutex_unlock(&log_file_gl.fsync_lsn_lock);
}
#endif
