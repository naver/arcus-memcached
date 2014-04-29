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
#include <sys/time.h>

#include "squall_config.h"
#include "sq_log.h"
#include "sq_disk.h"
#include "sq_buffer.h"
#include "sq_prefix.h"

/* checkpoint reasons */
#define CHKPT_BY_INTERVAL  1
#define CHKPT_BY_LOGAMOUNT 2
#define CHKPT_BY_REQUEST   3

/* log & recovery status */
#define LOG_STATUS_STOP   0
#define LOG_STATUS_START  1

/* ?? */
#define LOG_BUFFER_MIN_SIZE (1024 * 1024)
#define LOG_FLUSH_AUTO_SIZE (64 * 1024)

//#define SQUALL_RECOVERY_DEBUG 1

/* disk structure */
typedef struct _log_anchor {
    char         name[MAX_FILE_NAME_LENG];
    uint32_t     pingpong_id;
    uint32_t     log_status;      /* LOG_STATUS_START | LOG_STATUS_STOP */
    uint32_t     frst_filenum;
    uint32_t     last_filenum;
    LogSN        prev_CHKPT_BLSN; /* prev checkpoint begin lsn */
    LogSN        prev_CHKPT_ELSN; /* prev checkpoint end lsn */
    LogSN        last_CHKPT_BLSN; /* last checkpoint begin lsn */
    LogSN        last_CHKPT_ELSN; /* last checkpoint end lsn */
    LogSN        last_write_lsn;
    uint32_t     next_tranid;
} log_ANCHOR;

/* memory resident structure */
typedef struct _log_file {
    char         name[MAX_FILE_NAME_LENG];
    uint32_t     filenum;
    int          fd;
} log_FILE;

#if 0
typedef struct _log_flush_req {
    uint32_t     posi;
    uint32_t     leng;
} log_FLUSHREQ;
#endif

/* memory resident structure */
typedef struct _log_buffer {
    /* log buffer */
    char        *data;  /* log buffer pointer */
    uint32_t     size;  /* log buffer size */
    uint32_t     head;  /* the head position in log buffer */
    uint32_t     tail;  /* the tail position in log buffer */
    int32_t      last;  /* the last position in log buffer */
    int32_t      creat; /* the file create position in log buffer */
    /* flush request queue */
    uint32_t    *fque;  /* flush length queue pointer */
    uint32_t     fqsz;  /* flush length queue size */
    uint32_t     fbgn;  /* the queue index to begin flush */
    uint32_t     fend;  /* the queue index to end flush */
} log_BUFFER;

typedef struct _log_flusher {
    pthread_mutex_t     lock;
    pthread_cond_t      cond;
    bool                sleep;
    bool                start;
    bool                init;   /* start or stop request */
} log_FLUSHER;

typedef struct _log_chkpter {
    pthread_mutex_t     lock;
    pthread_cond_t      cond;
    bool                sleep;
    bool                start;
    bool                init;   /* start or stop request */
} log_CHKPTER;

/* log global structure */
struct log_global {
    char           *log_path;
    log_ANCHOR      log_anchor;
    log_FILE        log_file;
    log_BUFFER      log_buffer;
    log_FLUSHER     log_flusher;
    log_CHKPTER     log_chkpter;
    LogRec         *reco_logrec; /* used only in REDO/UNDO phase */
    uint64_t        redo_tranid; /* used only in REDO only phase */
    LogSN           write_lsn;
    LogSN           flush_lsn;
    LogSN           fsync_lsn;
    struct timeval  fsync_time;
    //uint32_t    chkpt_interval; /* unit: seconds */
    pthread_mutex_t write_lock;
    pthread_mutex_t flush_lock;
    pthread_mutex_t write_lsn_lock;
    pthread_mutex_t flush_lsn_lock;
    pthread_mutex_t fsync_lsn_lock;
    pthread_mutex_t chkpt_lock;
    volatile bool   initialized;
};

static SERVER_HANDLE_V1 *server = NULL;
static struct config    *config = NULL;
static EXTENSION_LOGGER_DESCRIPTOR *logger = NULL;
static struct log_global log_gl;

/* Log Related Daemon Thread Wakeup Functions */

static void do_log_flusher_wakeup()
{
    log_FLUSHER *flusher = &log_gl.log_flusher;

    pthread_mutex_lock(&flusher->lock);
    if (flusher->sleep == true)
        pthread_cond_signal(&flusher->cond);
    pthread_mutex_unlock(&flusher->lock);
}

static void do_log_chkpter_wakeup()
{
    log_CHKPTER *chkpter = &log_gl.log_chkpter;

    pthread_mutex_lock(&chkpter->lock);
    if (chkpter->sleep == true)
        pthread_cond_signal(&chkpter->cond);
    pthread_mutex_unlock(&chkpter->lock);
}

/* Log LSN Retreival Functions */

void log_get_write_lsn(LogSN *lsn)
{
    pthread_mutex_lock(&log_gl.write_lock);
    //pthread_mutex_lock(&log_gl.write_lsn_lock);
    *lsn = log_gl.write_lsn;
    pthread_mutex_unlock(&log_gl.write_lock);
    //pthread_mutex_unlock(&log_gl.write_lsn_lock);
}

void log_get_flush_lsn(LogSN *lsn)
{
    pthread_mutex_lock(&log_gl.flush_lsn_lock);
    *lsn = log_gl.flush_lsn;
    pthread_mutex_unlock(&log_gl.flush_lsn_lock);
}

void log_get_fsync_lsn(LogSN *lsn)
{
    pthread_mutex_lock(&log_gl.fsync_lsn_lock);
    *lsn = log_gl.fsync_lsn;
    pthread_mutex_unlock(&log_gl.fsync_lsn_lock);
}

/* Log Record Functions */

void log_record_init(LogRec *logrec, uint64_t tranid, uint8_t logtype)
{
    LOGSN_SET_NULL(&logrec->header.self_lsn);
    LOGSN_SET_NULL(&logrec->header.unxt_lsn);
    logrec->header.tranid = tranid;
    //PAGEID_SET_NULL(&logrec->header.pageid);
    logrec->header.logtype = logtype;
    logrec->header.acttype = 0;
    logrec->header.cmttype = 0;
    logrec->header.length = 0;
}

static void do_log_record_print(LogRec *logrec)
{
    logrec_func[logrec->header.acttype].print(logrec);
}

static int do_log_record_redo(LogRec *logrec)
{
    Trans *tran = NULL;

    if (logrec->header.tranid >= log_gl.redo_tranid) {
        /* update transaction table */
        switch (logrec->header.cmttype) {
          case LOG_ACT_BEGIN:
               assert(logrec->header.tranid > 0);
               tran = tran_reco_alloc(logrec->header.tranid);
               assert(tran != NULL);
               tran->state = TRAN_STATE_BEGIN;
               tran->first_lsn = logrec->header.self_lsn;
               tran->last_lsn = logrec->header.self_lsn;
               if (logrec->header.logtype != LOG_REDO_ONLY) {
                   tran->undonxt_lsn = logrec->header.self_lsn;
               }
               break;
          case LOG_ACT_NORMAL:
               tran = tran_search(logrec->header.tranid);
               assert(tran != NULL && tran->state == TRAN_STATE_BEGIN);
               tran->last_lsn = logrec->header.self_lsn;
               if (logrec->header.logtype != LOG_REDO_ONLY) {
                   tran->undonxt_lsn = logrec->header.self_lsn;
               }
               break;
        }
    }

    int ret = logrec_func[logrec->header.acttype].redo(logrec);
    if (ret != 0) {
        return ret;
    }

    if (logrec->header.tranid >= log_gl.redo_tranid) {
        /* update transaction table */
        if (logrec->header.cmttype == LOG_ACT_COMMIT) {
            tran = tran_search(logrec->header.tranid);
            assert(tran != NULL && tran->state == TRAN_STATE_BEGIN);
            tran->last_lsn = logrec->header.self_lsn;
            if (logrec->header.logtype != LOG_REDO_ONLY) {
                tran->undonxt_lsn = logrec->header.self_lsn;
            }
            tran->state = TRAN_STATE_COMMIT;
            tran_reco_free(tran);
        }
    }
    return 0;
}

static int do_log_record_undo(LogRec *logrec, Trans *tran)
{
    int ret;

    /* print log record information */
    do_log_record_print(logrec);

    switch (logrec->header.acttype)
    {
       case ACT_IT_ALLOC:
          ret = lrec_it_alloc_undo(logrec, tran);
          break;
       default:
          /* cannot handle the log action type */
          ret = -1;
    }
    return ret;
}

/* Log Anchor Functions */

static int do_log_anch_init()
{
    log_ANCHOR *loganch = &log_gl.log_anchor;
    char loganch_name[MAX_FILE_NAME_LENG];
    int fd, ret=0;

    sprintf(loganch_name, "%s/squall_log_anchor", log_gl.log_path);

    fd = disk_open(loganch_name, O_RDWR | O_SYNC, 0660);
    if (fd == -1) {
        if (errno != ENOENT) { /* SEVERE erorr */
            fprintf(stderr, "log anchor(%s) init - open error=(%d:%s)\n",
                    loganch_name, errno, strerror(errno));
            return -1;
        }
        /* errno == ENOENT */
        /* The log anchor file does not exist - create it */
        fprintf(stderr, "create log anchor file.\n");
        fd = disk_open(loganch_name, O_RDWR | O_CREAT | O_SYNC, 0660);
        if (fd == -1) { /* SEVERE error */
            fprintf(stderr, "log anchor(%s) init - create error=(%d:%s)\n",
                    loganch_name, errno, strerror(errno));
            return -1;
        }
        /* initialize log anchor content */
        strncpy(loganch->name, loganch_name, MAX_FILE_NAME_LENG);
        loganch->pingpong_id = 0;
        loganch->log_status = LOG_STATUS_STOP;
        loganch->frst_filenum = 1;
        loganch->last_filenum = 1;
        LOGSN_SET_NULL(&loganch->prev_CHKPT_BLSN);
        LOGSN_SET_NULL(&loganch->prev_CHKPT_ELSN);
        LOGSN_SET_NULL(&loganch->last_CHKPT_BLSN);
        LOGSN_SET_NULL(&loganch->last_CHKPT_ELSN);
        loganch->last_write_lsn.filenum = 1;
        loganch->last_write_lsn.roffset = 0;
        loganch->next_tranid = 1; /* min tranid */

        /* write log anchor content */
        ssize_t nwrite = disk_byte_pwrite(fd, loganch, sizeof(log_ANCHOR), 0);
        if (nwrite != sizeof(log_ANCHOR)) {
            fprintf(stderr, "log anchor(%s) init - write(%ld!=%ld) error=(%d:%s)\n",
                    loganch_name, nwrite, sizeof(log_ANCHOR), errno, strerror(errno));
            ret = -1;
        }
    } else {
        /* read log anchor content */
        ssize_t nread = disk_byte_pread(fd, loganch, sizeof(log_ANCHOR), 0);
        if (nread != sizeof(log_ANCHOR)) {
            fprintf(stderr, "log anchor(%s) init - read(%ld!=%ld) error=(%d:%s)\n",
                    loganch_name, nread, sizeof(log_ANCHOR), errno, strerror(errno));
            ret = -1;
        }
    }
    disk_close(fd);
    return ret;
}

static int do_log_anch_flush()
{
    log_ANCHOR *loganch = &log_gl.log_anchor;
    ssize_t nwrite;
    int     fd, ret=0;

    fd = disk_open(loganch->name, O_RDWR | O_SYNC, 0660);
    if (fd == -1) {
        if (errno != ENOENT) { /* SEVERE error */
            logger->log(EXTENSION_LOG_WARNING, NULL, "log anchor(%s) flush - open error=(%d:%s)\n",
                        loganch->name, errno, strerror(errno));
            return -1;
        }
        /* errno == ENOENT */
        /* The log anchor file disappeared - recreate it */
        logger->log(EXTENSION_LOG_WARNING, NULL, "log anchor flush - absent\n");
        fd = disk_open(loganch->name, O_RDWR | O_CREAT | O_SYNC, 0660);
        if (fd == -1) { /* SEVERE error */
            logger->log(EXTENSION_LOG_WARNING, NULL, "log anchor(%s) flush - create error=(%d:%s)\n",
                        loganch->name, errno, strerror(errno));
            return -1;
        }
    }

    nwrite = disk_byte_pwrite(fd, loganch, sizeof(log_ANCHOR), 0);
    if (nwrite != sizeof(log_ANCHOR)) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "log anchor(%s) flush - write(%ld!=%ld) error=(%d:%s)\n",
                    loganch->name, nwrite, sizeof(log_ANCHOR), errno, strerror(errno));
        ret = -1;
    }
    disk_close(fd);
    return ret;
}

static void do_log_anch_final()
{
    log_ANCHOR *loganch = &log_gl.log_anchor;
    assert(LOGSN_IS_EQ(&log_gl.write_lsn, &log_gl.flush_lsn));
    assert(LOGSN_IS_EQ(&log_gl.write_lsn, &log_gl.fsync_lsn));

    loganch->last_write_lsn = log_gl.write_lsn;
    loganch->log_status = LOG_STATUS_STOP;
    (void)do_log_anch_flush();
}

/* Log File Functions */

static int do_log_file_init(uint32_t filenum)
{
    log_FILE *logfile = &log_gl.log_file;

    sprintf(logfile->name, "%s/squall_log_%010u", log_gl.log_path, filenum);
    logfile->filenum = filenum;
    logfile->fd = disk_open(logfile->name, O_RDWR | O_APPEND | O_CREAT, 0660);
    if (logfile->fd == -1) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) init - open error=(%d:%s)\n",
                    logfile->name, errno, strerror(errno));
        return -1;
    }

    /* set log related LSNs */
    log_gl.flush_lsn.filenum = log_gl.log_anchor.last_filenum;
    log_gl.flush_lsn.roffset = disk_lseek(logfile->fd, 0, SEEK_END);
    log_gl.write_lsn = log_gl.flush_lsn;
    log_gl.fsync_lsn = log_gl.flush_lsn;
    return 0;
}

static void do_log_file_final()
{
    log_FILE *logfile = &log_gl.log_file;

    if (logfile->fd != -1) {
        (void)disk_fsync(logfile->fd);
        (void)disk_close(logfile->fd);
        logfile->fd = -1;
    }
}

static void do_log_file_prepare(void)
{
    log_FILE *logfile = &log_gl.log_file;
    uint32_t new_filenum = log_gl.flush_lsn.filenum + 1;

    /* close current log file */
    (void)disk_fsync(logfile->fd);
    (void)disk_close(logfile->fd);
    logfile->fd = -1;

    /* update flush_lsn */
    pthread_mutex_lock(&log_gl.flush_lsn_lock);
    log_gl.flush_lsn.filenum = new_filenum;
    log_gl.flush_lsn.roffset = 0;
    pthread_mutex_unlock(&log_gl.flush_lsn_lock);

    /* update fsync_lsn */
    pthread_mutex_lock(&log_gl.fsync_lsn_lock);
    log_gl.fsync_lsn = log_gl.flush_lsn;
    pthread_mutex_unlock(&log_gl.fsync_lsn_lock);
    gettimeofday(&log_gl.fsync_time, NULL);

    log_gl.log_anchor.last_filenum = new_filenum;
    (void)do_log_anch_flush();

    /* open the next log file */
    sprintf(logfile->name, "%s/squall_log_%010u", log_gl.log_path, new_filenum);
    logfile->filenum = new_filenum;
    logfile->fd = disk_open(logfile->name, O_RDWR | O_APPEND | O_CREAT, 0660);
    if (logfile->fd == -1) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) create - open error=(%d:%s)\n",
                    logfile->name, errno, strerror(errno));
        /* [FATAL] untreatable error => abnormal shutdown by assertion */
    }
    assert(logfile->fd != -1);
}

static void do_log_file_sync(void)
{
    log_FILE *logfile = &log_gl.log_file;
    assert(logfile->fd != -1);

    if (LOGSN_IS_LT(&log_gl.fsync_lsn, &log_gl.flush_lsn)) {
        int ret = disk_fsync(logfile->fd);
        if (ret < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) sync - sync error=(%d:%s)\n",
                        logfile->name, errno, strerror(errno));
            /* [FATAL] untreatable error => abnormal shutdown by assertion */
        }
        assert(ret == 0);

        /* update fsync_lsn */
        pthread_mutex_lock(&log_gl.fsync_lsn_lock);
        log_gl.fsync_lsn = log_gl.flush_lsn;
        pthread_mutex_unlock(&log_gl.fsync_lsn_lock);
        gettimeofday(&log_gl.fsync_time, NULL);
    }
}

static void do_log_file_write(char *log_ptr, uint32_t log_size)
{
    log_FILE *logfile = &log_gl.log_file;
    assert(logfile->fd != -1);

    /* The log data is appended because log file was opened with O_APPEND */
    ssize_t nwrite = disk_byte_write(logfile->fd, log_ptr, log_size);
    if (nwrite != log_size) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) write - write(%ld!=%ld) error=(%d:%s)\n",
                    logfile->name, nwrite, (ssize_t)log_size, errno, strerror(errno));
        /* [FATAL] untreatable error => abnormal shutdown by assertion */
    }
    assert(nwrite == log_size);

    /* update flush_lsn */
    pthread_mutex_lock(&log_gl.flush_lsn_lock);
    log_gl.flush_lsn.roffset += log_size;
    pthread_mutex_unlock(&log_gl.flush_lsn_lock);
}

static int do_log_file_read(LogSN lsn, LogRec *logrec, bool active_logfile)
{
    int      fd,ret=0;
    uint32_t offset;
    ssize_t  nread;

    /* open the log file if needed */
    if (active_logfile) {
        assert(lsn.filenum == log_gl.log_file.filenum);
        fd = log_gl.log_file.fd;
    } else {
        char logfile_name[MAX_FILE_NAME_LENG];
        sprintf(logfile_name, "%s/squall_log_%010u", log_gl.log_path, lsn.filenum);
        fd = disk_open(logfile_name, O_RDONLY, 0660);
        if (fd == -1) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) read - open error=(%d:%s)\n",
                        logfile_name, errno, strerror(errno));
            return -1;
        }
    }

    do {
        /* read log record header */
        offset = lsn.roffset;
        nread = disk_byte_pread(fd, &logrec->header, sizeof(LOGHDR), offset);
        if (nread != sizeof(LOGHDR)) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "log file(%u) read - header read(%ld!=%ld) error=(%d:%s)\n",
                        lsn.filenum, nread, sizeof(LOGHDR), errno, strerror(errno));
            ret = -1; break;
        }

        /* read log record body */
        if (logrec->header.length > 0) {
            offset += sizeof(LOGHDR);
            nread = disk_byte_pread(fd, logrec->body, logrec->header.length, offset);
            if (nread != logrec->header.length) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "log file(%u) read - body read(%ld!=%ld) error=(%d:%s)\n",
                            lsn.filenum, nread, (ssize_t)logrec->header.length, errno, strerror(errno));
                ret = -1; break;
            }
        }
    } while(0);

    /* close the log file if needed */
    if (active_logfile) {
        /* nothing to do */
    } else {
        disk_close(fd);
    }
    return ret;
}

static int do_log_file_redo(log_FILE *logfile, LogSN redo_lsn, LogSN *last_lsn)
{
    LogRec *logrec = log_gl.reco_logrec;
    LogSN   lsn = redo_lsn;
    off_t   file_length;
    size_t  seek_offset;
    ssize_t nread;
    int     ret = 0;
#ifdef SQUALL_RECOVERY_DEBUG
    bool    redo_progress = false;
#else
    bool    redo_progress = true;
#endif

#ifdef SQUALL_RECOVERY_DEBUG
    LogSN  print_lsn;
    print_lsn = redo_lsn;
    //print_lsn.filenum = 1;
    //print_lsn.roffset = 1125428864;
#endif

    file_length = disk_lseek(logfile->fd, 0, SEEK_END);
    if (file_length == -1) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) redo - lseek(SEEK_END) error=(%d:%s)\n",
                    logfile->name, errno, strerror(errno));
        return -1;
    }
    seek_offset = disk_lseek(logfile->fd, lsn.roffset, SEEK_SET);
    if (seek_offset != lsn.roffset) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) redo - lseek(%u!=%u) error=(%d:%s)\n",
                    logfile->name, lsn.roffset, (uint32_t)seek_offset, errno, strerror(errno));
        return -1;
    }

    if (redo_progress) {
        fprintf(stderr, "\r[REDO] now - current_redo_lsn=(%u|%10u)", lsn.filenum, lsn.roffset);
    }

    while (lsn.roffset < file_length)
    {
        if (log_gl.initialized != true) {
            ret = -2; break; /* stop the redo */
        }

        nread = disk_byte_read(logfile->fd, &logrec->header, sizeof(LOGHDR));
        if (nread != sizeof(LOGHDR)) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "log file(%s) read - log header read(%ld!=%ld) error=(%d:%s)\n",
                        logfile->name, nread, sizeof(LOGHDR), errno, strerror(errno));
            ret = -1; break;
        }

        if (logrec->header.length > 0) {
            nread = disk_byte_read(logfile->fd, logrec->body, logrec->header.length);
            if (nread != logrec->header.length) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "log file(%s) read - log body read(%ld!=%ld) error=(%d:%s)\n",
                            logfile->name, nread, (ssize_t)logrec->header.length, errno, strerror(errno));
                ret = -1; break;
            }
        }

#ifdef SQUALL_RECOVERY_DEBUG
        if (LOGSN_IS_GE(&logrec->header.self_lsn, &print_lsn)) {
            /* print log record information */
            do_log_record_print(logrec);
        }
#endif
        if (redo_progress) {
            if (lsn.roffset >= (seek_offset + (1024*1024))) {
                fprintf(stderr, "\r[REDO] now - current_redo_lsn=(%u|%10u)", lsn.filenum, lsn.roffset);
                seek_offset = lsn.roffset;
            }
        }

        ret = do_log_record_redo(logrec);
        if (ret != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) read - log record[%u|%u] redo error\n",
                        logfile->name, lsn.filenum, lsn.roffset);
            break;
        }

        /* get the next LSN */
        lsn.roffset += (sizeof(LOGHDR) + logrec->header.length);
        /* find the write_lsn */
        //log_gl.write_lsn.roffset = lsn.roffset;
    }
    if (ret != 0) return ret;

    if (lsn.filenum < log_gl.log_anchor.last_filenum) {
        if (lsn.roffset != file_length) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "log file(%s) read - last_log_offset(%u) != file_length(%u)\n",
                        logfile->name, lsn.roffset, (uint32_t)file_length);
            return -1;
        }
    }

    if (redo_progress) {
        fprintf(stderr, "\r[REDO] now - current_redo_lsn=(%u|%10u)", lsn.filenum, lsn.roffset);
    }
    *last_lsn = lsn;
    return 0;
}


/* Log Buffer Functions */

static int do_log_buff_init(uint32_t log_buffer_size)
{
    log_BUFFER *logbuff = &log_gl.log_buffer;

    /* check log_buffer_size argument */
    if (log_buffer_size < LOG_BUFFER_MIN_SIZE) {
        fprintf(stderr, "log buf init - too small (min=%d)\n", LOG_BUFFER_MIN_SIZE);
        return -1; /* too small log buffer */
    }

    /* log buffer init - ring shaped buffer */
    logbuff->size = log_buffer_size;
#ifdef SQUALL_USE_MMAP
    logbuff->data = mmap(NULL, (size_t)logbuff->size, PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_SHARED, -1, 0);
    if (logbuff->data == MAP_FAILED) {
        fprintf(stderr, "log buf init - mmap error=(%d:%s)\n", errno, strerror(errno));
        return -1; /* no more memory */
    }
#else
    void *log_buffer_space;
    if (posix_memalign(&log_buffer_space, sysconf(_SC_PAGESIZE), (size_t)logbuff->size) != 0) {
        fprintf(stderr, "log buf init - posix_memalign error=(%d:%s)\n", errno, strerror(errno));
        return -1; /* no more memory */
    }
    logbuff->data = log_buffer_space;
#endif
    logbuff->head = 0;
    logbuff->tail = 0;
    logbuff->last = -1;
    logbuff->creat = -1;

    /* log flush reqeust queue init - ring shaped queue */
    logbuff->fqsz = (log_buffer_size / LOG_FLUSH_AUTO_SIZE) + 10;
    logbuff->fque = (uint32_t*)malloc(logbuff->fqsz * sizeof(uint32_t));
    if (logbuff->fque == NULL) {
        fprintf(stderr, "log buf init - malloc error=(%d:%s)\n", errno, strerror(errno));
        return -1; /* no more memory */
    }
    memset(logbuff->fque, 0, logbuff->fqsz * sizeof(uint32_t));
    logbuff->fbgn = 0;
    logbuff->fend = 0;

    return 0;
}

static void do_log_buff_final()
{
    log_BUFFER *logbuff = &log_gl.log_buffer;

    /* free log buffer */
#ifdef SQUALL_USE_MMAP
    if (logbuff->data != MAP_FAILED) {
        munmap((void*)logbuff->data, logbuff->size);
        logbuff->data = MAP_FAILED;
    }
#else
    if (logbuff->data != NULL) {
        free((void*)logbuff->data);
        logbuff->data = NULL;
    }
#endif

    /* free log flush request queue */
    if (logbuff->fque != NULL) {
        free((void*)logbuff->fque);
        logbuff->fque = NULL;
    }
}

static uint32_t do_log_buff_flush(bool flush_all)
{
    log_BUFFER *logbuff = &log_gl.log_buffer;
    uint32_t nflush = 0;
    bool file_creat_flag = false;
    bool last_flush_flag = false;

    pthread_mutex_lock(&log_gl.write_lock);
    if (logbuff->fbgn != logbuff->fend) {
        nflush = logbuff->fque[logbuff->fbgn];
        assert(nflush > 0);
    } else {
        if (flush_all && logbuff->fque[logbuff->fend] > 0) {
            nflush = logbuff->fque[logbuff->fbgn];
            last_flush_flag = true;
        }
    }
    if (nflush > 0) {
        if (logbuff->head == logbuff->creat) {
            logbuff->creat = -1;
            file_creat_flag = true;
        }
        if (logbuff->head == logbuff->last) {
            logbuff->last = -1;
            logbuff->head = 0;
        }
    }
    pthread_mutex_unlock(&log_gl.write_lock);

    if (nflush > 0) {
        if (file_creat_flag) {
            do_log_file_prepare();
        }
        do_log_file_write(&logbuff->data[logbuff->head], nflush);

        pthread_mutex_lock(&log_gl.write_lock);
        logbuff->head += nflush;
        if (last_flush_flag) {
            /* decrement the flush request size */
            logbuff->fque[logbuff->fbgn] -= nflush;
            if (logbuff->fque[logbuff->fbgn] == 0 && logbuff->fbgn != logbuff->fend) {
                if ((++logbuff->fbgn) == logbuff->fqsz) {
                    logbuff->fbgn = 0;
                }
            }
        } else {
            /* clear the flush request itself */
            logbuff->fque[logbuff->fbgn] = 0;
            if ((++logbuff->fbgn) == logbuff->fqsz) {
                logbuff->fbgn = 0;
            }
        }
        pthread_mutex_unlock(&log_gl.write_lock);
    }
    return nflush;
}

static void do_log_buff_write(LogRec *logrec)
{
    log_BUFFER *logbuff = &log_gl.log_buffer;
    char       *bufptr;

    uint32_t total_length = sizeof(LOGHDR) + logrec->header.length;
    assert(total_length < logbuff->size);

    pthread_mutex_lock(&log_gl.write_lock);

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
                /* increase log flush end pointer to make to-be-flushed log data contiguous in memory. */
                if (logbuff->fque[logbuff->fend] > 0) {
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
        pthread_mutex_unlock(&log_gl.write_lock);
        pthread_mutex_lock(&log_gl.flush_lock);
        (void)do_log_buff_flush(false);
        pthread_mutex_unlock(&log_gl.flush_lock);
        pthread_mutex_lock(&log_gl.write_lock);
    }

    /* check log file boundary */
    if ((log_gl.write_lsn.roffset + total_length) > (config->sq.log_file_size*1024*1024)) {
        /* request log flush when a new log file is created */
        assert(logbuff->creat == -1);
        logbuff->creat = logbuff->tail;

        /* A new log file should be created.
         * increase log flush end pointer to make to-be-flushed log data contiguous in memory.
         */
        if (logbuff->fque[logbuff->fend] > 0) {
            if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
        }

        /* update write_lsn */
        //pthread_mutex_lock(&log_gl.write_lsn_lock);
        log_gl.write_lsn.filenum += 1;
        log_gl.write_lsn.roffset = 0;
        //pthread_mutex_unlock(&log_gl.write_lsn_lock);
    }

    /* set LSN of the log record */
    logrec->header.self_lsn = log_gl.write_lsn;

    /* write log record at the found location of log buffer */
    bufptr = &logbuff->data[logbuff->tail];
    memcpy(bufptr, (void*)logrec, sizeof(LOGHDR));
    if (logrec->header.length > 0) {
        logrec_func[logrec->header.acttype].write(logrec, bufptr+sizeof(LOGHDR));
    }
    logbuff->tail += total_length;

    /* update(increment) write_lsn */
    //pthread_mutex_lock(&log_gl.write_lsn_lock);
    log_gl.write_lsn.roffset += total_length;
    //pthread_mutex_unlock(&log_gl.write_lsn_lock);

    /* increase log flush request length.
     * increase log flush end pointer, if the log flush requests are accumulated enough.
     */
    logbuff->fque[logbuff->fend] += total_length;
    if (logbuff->fque[logbuff->fend] > LOG_FLUSH_AUTO_SIZE) {
        if ((++logbuff->fend) == logbuff->fqsz) logbuff->fend = 0;
    }

    pthread_mutex_unlock(&log_gl.write_lock);

    /* wake up log flush thread if flush requests exist */
    if (logbuff->fbgn != logbuff->fend) {
        if (log_gl.log_flusher.sleep == true) {
            do_log_flusher_wakeup();
        }
    }
}

static int do_log_buff_read(LogSN lsn, LogRec *logrec)
{
    log_BUFFER *logbuff = &log_gl.log_buffer;
    uint32_t bfhead;
    uint32_t offset;
    char    *recptr = NULL;

    /* the thread is also holding flush_lock, now */

    pthread_mutex_lock(&log_gl.write_lock);
    if (LOGSN_IS_LT(&lsn, &log_gl.write_lsn)) {
        assert(logbuff->head != logbuff->tail);
        if (lsn.filenum == log_gl.flush_lsn.filenum) {
            bfhead = logbuff->head;
            offset = lsn.roffset - log_gl.flush_lsn.roffset;
        } else {
            assert(logbuff->creat != -1);
            bfhead = logbuff->creat;
            offset = lsn.roffset;
        }
        if (bfhead < logbuff->tail) {
            assert(offset < (logbuff->tail - bfhead));
            recptr = &logbuff->data[bfhead + offset];
        } else {
            assert(logbuff->last != -1);
            if (offset < (logbuff->last - bfhead)) {
                recptr = &logbuff->data[bfhead + offset];
            } else {
                offset -= (logbuff->last - bfhead);
                assert(offset < logbuff->tail);
                recptr = &logbuff->data[offset];
            }
        }
    }
    pthread_mutex_unlock(&log_gl.write_lock);

    if (recptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "log buf read - invalid lsn(%u-%u)\n",
                    lsn.filenum, lsn.roffset);
        return -1;
    }

    memcpy(&logrec->header, recptr, sizeof(LOGHDR));
    if (logrec->header.length > 0) {
        memcpy(logrec->body, recptr + sizeof(LOGHDR), logrec->header.length);
    }
    return 0;
}

/* Log Manager Functions */

void log_record_write(LogRec *logrec, Trans *tran)
{
    if (tran != NULL) {
        assert(tran->tranid > 0);
        assert(logrec->header.cmttype != LOG_NTA_COMMIT);
        if (LOGSN_IS_NULL(&tran->first_lsn)) {
            if (logrec->header.cmttype == LOG_ACT_NORMAL) {
                logrec->header.cmttype = LOG_ACT_BEGIN;
            } else {
                assert(logrec->header.cmttype == LOG_ACT_COMMIT);
                logrec->header.cmttype = LOG_ACT_SINGLE; /* single action commit */
            }
        }
        logrec->header.tranid = tran->tranid;
        logrec->header.unxt_lsn = tran->undonxt_lsn;
    } else {
        assert(logrec->header.cmttype == LOG_NTA_COMMIT);
        logrec->header.tranid = 0;
        LOGSN_SET_NULL(&logrec->header.unxt_lsn);
    }

    /* write the log record on the log buffer */
    do_log_buff_write(logrec);

    if (tran != NULL) {
        if (LOGSN_IS_NULL(&tran->first_lsn)) {
            tran->state = TRAN_STATE_BEGIN;
            tran->first_lsn = logrec->header.self_lsn;
        }
        tran->last_lsn = logrec->header.self_lsn;
        if (logrec->header.logtype != LOG_REDO_ONLY) {
            /* LOG_REDO_UNDO or LOG_UNDO_ONLY */
            tran->undonxt_lsn = logrec->header.self_lsn;
        }
        if (logrec->header.cmttype == LOG_ACT_COMMIT ||
            logrec->header.cmttype == LOG_ACT_SINGLE) {
            tran->state = TRAN_STATE_COMMIT;
        }
    }
}

int log_record_read(LogSN lsn, LogRec *logrec)
{
    bool read_from_archive_log_file;
    int  ret;

    if (lsn.filenum > log_gl.write_lsn.filenum) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "log record read - invalid lsn(%u-%u)\n", lsn.filenum, lsn.roffset);
        return -1;
    }

    pthread_mutex_lock(&log_gl.flush_lock);

    if (lsn.filenum >= log_gl.flush_lsn.filenum) {
        if (lsn.filenum == log_gl.flush_lsn.filenum && lsn.roffset < log_gl.flush_lsn.roffset) {
            ret = do_log_file_read(lsn, logrec, true); /* read from active log file */
            if (ret < 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "log record read - active file fail\n");
            }
        } else {
            ret = do_log_buff_read(lsn, logrec);
            if (ret < 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "log record read - buffer fail\n");
            }
        }
        read_from_archive_log_file = false;
    } else {
        read_from_archive_log_file = true;
    }

    pthread_mutex_unlock(&log_gl.flush_lock);

    if (read_from_archive_log_file) {
        ret = do_log_file_read(lsn, logrec, false); /* read from archive log file */
        if (ret < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "log record read - archive file fail\n");
        }
    }
    return ret;
}

void log_buffer_flush(LogSN *upto_lsn, bool sync)
{
    uint32_t nflush;

    if (upto_lsn == NULL) {
        do {
            pthread_mutex_lock(&log_gl.flush_lock);
            nflush = do_log_buff_flush(true);
            if (nflush == 0) { /* nothing to flush */
                if (sync == true) do_log_file_sync();
            }
            pthread_mutex_unlock(&log_gl.flush_lock);
        } while (nflush > 0);
    } else {
        do {
            pthread_mutex_lock(&log_gl.flush_lock);
            if (LOGSN_IS_LE(&log_gl.flush_lsn, upto_lsn)) {
                nflush = do_log_buff_flush(true);
                /* NOTE) PageLSN of page map page ****
                 * Page map pages must have pageLSN smaller than the write_lsn at the time of update
                 * in order to guarantee that log_gl.flush_lsn is larger than upto_lsn.
                 *************************************/
                assert(nflush > 0);
            } else {
                nflush = 0;
                if (sync == true) do_log_file_sync();
            }
            pthread_mutex_unlock(&log_gl.flush_lock);
        } while (nflush > 0);
    }
}

void log_file_sync(LogSN *upto_lsn)
{
    pthread_mutex_lock(&log_gl.flush_lock);
    assert(LOGSN_IS_GT(&log_gl.flush_lsn, upto_lsn));
    if (LOGSN_IS_LE(&log_gl.fsync_lsn, upto_lsn)) {
        do_log_file_sync();
    }
    pthread_mutex_unlock(&log_gl.flush_lock);
}

/* Log Flush Thread */

static void *log_flush_thread(void *arg)
{
    log_FLUSHER *flusher = &log_gl.log_flusher;
    struct timeval  prev, curr;
    struct timeval  tv;
    struct timespec to;
    uint32_t nflush;

    assert(flusher->init == true);

    flusher->start = true;
    //fprintf(stderr, "log flush thread started\n");
    gettimeofday(&prev, NULL);
    while (1)
    {
        if (config->sq.tran_commit_mode == DISK_COMMIT_EVERY_SECOND) {
            gettimeofday(&curr, NULL);
            if ((curr.tv_sec > prev.tv_sec && curr.tv_usec > prev.tv_usec) || curr.tv_sec > (prev.tv_sec+1)) {
                pthread_mutex_lock(&log_gl.flush_lock);
                do_log_file_sync();
                pthread_mutex_unlock(&log_gl.flush_lock);
                prev = curr;
            }
        }
        pthread_mutex_lock(&log_gl.flush_lock);
        nflush = do_log_buff_flush(false);
        pthread_mutex_unlock(&log_gl.flush_lock);
        if (nflush > 0) continue;

        if (flusher->init == false) break;

        /* 10 ms sleep */
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
    //fprintf(stderr, "log flush thread terminated\n");
    flusher->start = false;
    return NULL;
}

/* Checkpoint Thread */
static int do_chkpt_write_begin_logrec(LogSN *bgn_chkpt_lsn)
{
    ChkptBgnLog chkptbgn_log;
    lrec_chkpt_bgn_init((LogRec*)&chkptbgn_log);
    chkptbgn_log.header.length = 0;

    log_record_write((LogRec*)&chkptbgn_log, NULL);
    *bgn_chkpt_lsn = chkptbgn_log.header.self_lsn;
    return 0;
}

static int do_chkpt_write_end_logrec(uint64_t nxt_tranid, LogSN min_trans_lsn, LogSN min_dirty_lsn,
                                     LogSN *end_chkpt_lsn)
{
    ChkptEndLog chkptend_log;
    lrec_chkpt_end_init((LogRec*)&chkptend_log);

    /* transaction table */
    /* dirty page table */

    chkptend_log.body.nxt_tranid = nxt_tranid;
    chkptend_log.body.min_trans_lsn = min_trans_lsn;
    chkptend_log.body.min_dirty_lsn = min_dirty_lsn;
    chkptend_log.header.length = sizeof(ChkptEndData);

    log_record_write((LogRec*)&chkptend_log, NULL);
    log_buffer_flush(&chkptend_log.header.self_lsn, true);
    *end_chkpt_lsn = chkptend_log.header.self_lsn;
    return (sizeof(LOGHDR) + chkptend_log.header.length);
}

static void do_checkpoint(int reason)
{
    log_ANCHOR *loganch = &log_gl.log_anchor;
    LogSN    curr_CHKPT_BLSN;
    LogSN    curr_CHKPT_ELSN;
    LogSN    min_dirty_lsn;
    uint64_t nxt_tranid;
    uint64_t min_tranid;
    struct timespec sleep_time = {0, 100000000}; // 100 msec.
    time_t   bgn_time, end_time;

    logger->log(EXTENSION_LOG_INFO, NULL, "checkpoint started by %s\n",
                (reason == CHKPT_BY_INTERVAL ? "interval" :
                (reason == CHKPT_BY_LOGAMOUNT ? "logamount" : "request")));
    bgn_time = time(NULL);

    pthread_mutex_lock(&log_gl.chkpt_lock);

    /* write begin checkpoint log record */
    do_chkpt_write_begin_logrec(&curr_CHKPT_BLSN);

    /* Wait until all active transactions were started after current begin_chkpt.
     * This ensures that the min lsn of those transactions is larger than begin_chkpt_lsn.
     */
    nxt_tranid = tran_mgr_get_nxt_tranid();
    min_tranid = tran_mgr_get_min_tranid();
    while (min_tranid != 0 && min_tranid < nxt_tranid) {
        nanosleep(&sleep_time, NULL);
        min_tranid = tran_mgr_get_min_tranid();
    }

    /* flush dirty buffer pages including prefix meta and page map pages */
    buf_flush_checkpoint(&loganch->last_CHKPT_BLSN, &min_dirty_lsn);

    /* flush data anchor file */
    pfx_data_anchor_flush();

    /* write end checkpoint log record */
    uint32_t log_leng = do_chkpt_write_end_logrec(nxt_tranid, curr_CHKPT_BLSN, min_dirty_lsn,
                                                  &curr_CHKPT_ELSN);

    /* update chkpt lsn information */
    LogSN prev_CHKPT_BLSN = loganch->last_CHKPT_BLSN;
    loganch->last_CHKPT_BLSN = curr_CHKPT_BLSN;
    loganch->last_CHKPT_ELSN = curr_CHKPT_ELSN;
    loganch->last_write_lsn.filenum = curr_CHKPT_ELSN.filenum;
    loganch->last_write_lsn.roffset = curr_CHKPT_ELSN.roffset + log_leng;
    loganch->next_tranid = nxt_tranid;
    (void)do_log_anch_flush();

    /* delete old useless log files */
    if (prev_CHKPT_BLSN.filenum > 0 && loganch->frst_filenum < prev_CHKPT_BLSN.filenum) {
        char logfile_name[MAX_FILE_NAME_LENG];
        for (uint32_t i = loganch->frst_filenum; i < prev_CHKPT_BLSN.filenum; i++) {
            sprintf(logfile_name, "%s/squall_log_%010u", log_gl.log_path, i);
            (void)disk_unlink(logfile_name);
        }
        loganch->frst_filenum = prev_CHKPT_BLSN.filenum;
        (void)do_log_anch_flush();
    }

    pthread_mutex_unlock(&log_gl.chkpt_lock);

    end_time = time(NULL);
    logger->log(EXTENSION_LOG_INFO, NULL,
                "checkpoint stopped: bgn_lsn(%u-%u), end_lsn(%u-%u), exec_time(%"PRIu64" secs).\n",
                curr_CHKPT_BLSN.filenum, curr_CHKPT_BLSN.roffset,
                curr_CHKPT_ELSN.filenum, curr_CHKPT_ELSN.roffset, (uint64_t)(end_time-bgn_time));
}

static void *log_chkpt_thread(void *arg)
{
    log_CHKPTER *chkpter = &log_gl.log_chkpter;
    struct timeval  tv;
    struct timespec to;
    LogSN  curr_write_lsn;
    LogSN  last_chkpt_lsn;
    size_t elapsed_time = 0;      /* unit: second */
    size_t log_amount_mb;         /* unit: MB */
    size_t chkpt_amount = 5*1024; /* 5 GB */

    assert(chkpter->init == true);

    chkpter->start = true;
    //fprintf(stderr, "checkpoint thread started\n");
    while (chkpter->init)
    {
        /* 1 second sleep */
        gettimeofday(&tv, NULL);
        to.tv_sec  = tv.tv_sec + 1;
        to.tv_nsec = tv.tv_usec * 1000;

        pthread_mutex_lock(&chkpter->lock);
        chkpter->sleep = true;
        pthread_cond_timedwait(&chkpter->cond, &chkpter->lock, &to);
        chkpter->sleep = false;
        pthread_mutex_unlock(&chkpter->lock);

        if (chkpter->init == false) break;

        /* checkpoint condition
         *  1. amount of log data written to disk: 5 GB internally.
         *  2. checkpoint interval: given as a configuration value.
         */
        log_get_write_lsn(&curr_write_lsn);
        last_chkpt_lsn = log_gl.log_anchor.last_CHKPT_ELSN;
        log_amount_mb = (curr_write_lsn.filenum - last_chkpt_lsn.filenum) * config->sq.log_file_size;
        log_amount_mb += (curr_write_lsn.roffset / (1024*1024));
        log_amount_mb -= (last_chkpt_lsn.roffset / (1024*1024));
        if (log_amount_mb > chkpt_amount) {
            do_checkpoint(CHKPT_BY_LOGAMOUNT);
            elapsed_time = 0;
            continue;
        }

        elapsed_time += 1;
        if (elapsed_time > config->sq.chkpt_interval &&
            LOGSN_IS_GT(&curr_write_lsn, &log_gl.log_anchor.last_write_lsn)) {
            do_checkpoint(CHKPT_BY_INTERVAL);
            elapsed_time = 0;
        }
    }
    //fprintf(stderr, "checkpoint thread terminated\n");
    chkpter->start = false;
    return NULL;
}

static void do_log_flush_thread_init(void)
{
    pthread_mutex_init(&log_gl.log_flusher.lock, NULL);
    pthread_cond_init(&log_gl.log_flusher.cond, NULL);
    log_gl.log_flusher.sleep = false;
    log_gl.log_flusher.start = false;
    log_gl.log_flusher.init = false;
}

static int do_log_flush_thread_start(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.
    pthread_t tid;
    int       ret;

    /* start request */
    log_gl.log_flusher.init = true;

    /* create log flush thread */
    ret = pthread_create(&tid, NULL, log_flush_thread, NULL);
    if (ret != 0) {
        log_gl.log_flusher.init = false;
        fprintf(stderr, "Can't create log flush thread: %s\n", strerror(ret));
        return -1;
    }
    /* wait until the log flush thread starts */
    while (log_gl.log_flusher.start == false) {
        nanosleep(&sleep_time, NULL);
    }
    return 0;
}

static void do_log_flush_thread_stop(void)
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
}

static void do_log_chkpt_thread_init(void)
{
    pthread_mutex_init(&log_gl.log_chkpter.lock, NULL);
    pthread_cond_init(&log_gl.log_chkpter.cond, NULL);
    log_gl.log_chkpter.sleep = false;
    log_gl.log_chkpter.start = false;
    log_gl.log_chkpter.init = false;
}

static int do_log_chkpt_thread_start(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.
    pthread_t tid;
    int       ret;

    /* start request */
    log_gl.log_chkpter.init = true;

    /* create log chkpt thread */
    ret = pthread_create(&tid, NULL, log_chkpt_thread, NULL);
    if (ret != 0) {
        log_gl.log_chkpter.init = false;
        fprintf(stderr, "Can't create log chkpt thread: %s\n", strerror(ret));
        return -1;
    }
    /* wait until the log chkpt thread starts */
    while (log_gl.log_chkpter.start == false) {
        nanosleep(&sleep_time, NULL);
    }
    return 0;
}

static void do_log_chkpt_thread_stop(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.

    if (log_gl.log_chkpter.init == true) {
        /* stop request */
        pthread_mutex_lock(&log_gl.log_chkpter.lock);
        log_gl.log_chkpter.init = false;
        pthread_mutex_unlock(&log_gl.log_chkpter.lock);

        /* wait until the log chkpt thread stops */
        while (log_gl.log_chkpter.start == true) {
            if (log_gl.log_chkpter.sleep == true) {
                do_log_chkpter_wakeup();
            }
            nanosleep(&sleep_time, NULL);
        }
    }
}

/* Log Statistics */

void log_stat_get(struct log_stat *stats)
{
    /* get stats without lock */
    stats->log_write_lsn = log_gl.write_lsn;
    stats->log_flush_lsn = log_gl.flush_lsn;
    stats->log_fsync_lsn = log_gl.fsync_lsn;
}

/* Log Manager Restart */

static void do_log_mgr_analysis(LogSN *redo_start_lsn, uint64_t *min_tranid)
{
    log_ANCHOR *loganch = &log_gl.log_anchor;

    if (loganch->log_status == LOG_STATUS_STOP) {
        /* The server was normally shutdowned. so, recovery is not needed. */
        assert(LOGSN_IS_EQ(&log_gl.write_lsn, &loganch->last_write_lsn));
        LOGSN_SET_NULL(redo_start_lsn);
        loganch->log_status = LOG_STATUS_START;
        do_log_anch_flush();
        return;
    }
   /* The server was abnormally shutdowned */
    if (LOGSN_IS_NULL(&loganch->last_CHKPT_ELSN)) {
        /* no checkpoint was performed before */
        redo_start_lsn->filenum = 1;
        redo_start_lsn->roffset = 0;
        *min_tranid = 1;
    } else {
        ChkptEndLog end_chkpt_log;
        int ret = log_record_read(loganch->last_CHKPT_ELSN, (LogRec*)&end_chkpt_log);
        if (ret != 0) { /* how to handle it ?? */
            fprintf(stderr, "Cannot read the last chkpt log record\n");
        }
        assert(ret == 0);
        assert(!LOGSN_IS_NULL(&end_chkpt_log.body.min_trans_lsn));
        if (LOGSN_IS_NULL(&end_chkpt_log.body.min_dirty_lsn)) {
            *redo_start_lsn = end_chkpt_log.body.min_trans_lsn;
        } else {
            if (LOGSN_IS_LT(&end_chkpt_log.body.min_trans_lsn, &end_chkpt_log.body.min_dirty_lsn))
                *redo_start_lsn = end_chkpt_log.body.min_trans_lsn;
            else
                *redo_start_lsn = end_chkpt_log.body.min_dirty_lsn;
        }
        *min_tranid = end_chkpt_log.body.nxt_tranid;
    }
}


static void do_log_mgr_redo(LogSN redo_start_lsn, uint64_t min_tranid)
{
    log_FILE  redo_log_file;
    log_FILE *logfile;
    LogSN     redo_lsn = redo_start_lsn;
    LogSN     last_lsn;
    int       ret = 0;

    /* rebuild active transactions from end_chkpt_log */
    /* tran_id, last_lsn, undo_next_lsn */

#ifdef SQUALL_RECOVERY_DEBUG
    redo_lsn.filenum = 1;
    redo_lsn.roffset = 0;
#endif

    fprintf(stderr, "[REDO] start - start_redo_lsn=(%u|%10u), end_of_log=(%u|%10u) min_tranid=%"PRIu64"\n",
            redo_lsn.filenum, redo_lsn.roffset,
            log_gl.write_lsn.filenum, log_gl.write_lsn.roffset, min_tranid);

    log_gl.redo_tranid = min_tranid;

    while (redo_lsn.filenum < log_gl.log_anchor.last_filenum)
    {
        logfile = &redo_log_file;
        logfile->filenum = redo_lsn.filenum;
        sprintf(logfile->name, "%s/squall_log_%010u", log_gl.log_path, redo_lsn.filenum);
        logfile->fd = disk_open(logfile->name, O_RDONLY, 0660);
        if (logfile->fd == -1) { /* error or stop the redo ?? */
            logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) redo - open error=(%d:%s)\n",
                        logfile->name, errno, strerror(errno));
            ret = -1; break;
        }

        ret = do_log_file_redo(logfile, redo_lsn, &last_lsn);
        if (ret < 0) { /* how do wo do ?? */
            logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) redo - redo %s\n",
                        logfile->name, (ret == -1 ? "error" : "stop"));
            disk_close(logfile->fd);
            break;
        }

        disk_close(logfile->fd);

        if (log_gl.initialized != true) {
            ret = -2; break; /* stop the redo */
        }

        /* get the first LSN of the next log file */
        redo_lsn.filenum += 1;
        redo_lsn.roffset = 0;
    }
    if (ret == 0) {
        assert(redo_lsn.filenum == log_gl.log_anchor.last_filenum);
        logfile = &log_gl.log_file;

        ret = do_log_file_redo(logfile, redo_lsn, &last_lsn);
        if (ret < 0) { /* how do wo do ?? */
            logger->log(EXTENSION_LOG_WARNING, NULL, "log file(%s) redo - redo %s\n",
                        logfile->name, (ret == -1 ? "error" : "stop"));
            /* flush dirty pages has been redone */
            buf_flush_all(-1);
        } else {
            /* reset write_lsn/flush_lsn/fsync_lsn */
            log_gl.write_lsn = last_lsn;
            log_gl.flush_lsn = last_lsn;
            log_gl.fsync_lsn = last_lsn;
        }
    }

    log_gl.redo_tranid = 0;

    fprintf(stderr, "\n[REDO] end - return=%d\n", ret);
}

static int do_log_mgr_undo()
{
    LogSN   lsn;
    LogRec *logrec = log_gl.reco_logrec;
    Trans  *tran;
    int     count = 0;
    int     ret = 0;
    bool    read_from_archive;

    fprintf(stderr, "[UNDO] start\n");
    tran = tran_first();
    while (tran != NULL) {
        if (log_gl.initialized == false) {
            break;
        }

        count += 1;
        fprintf(stderr, "[UNDO] tranid=%"PRIu64" - %s [%u|%u] [%u|%u] %c undo[%u|%u]\n",
                tran->tranid, (tran->implicit ? "imp" : "exp"),
                tran->first_lsn.filenum, tran->first_lsn.roffset,
                tran->last_lsn.filenum, tran->last_lsn.roffset,
                (LOGSN_IS_EQ(&tran->last_lsn, &tran->undonxt_lsn) ? '=' : '>'),
                tran->undonxt_lsn.filenum, tran->undonxt_lsn.roffset);

        lsn = tran->undonxt_lsn;
        while (!LOGSN_IS_NULL(&lsn))
        {
            read_from_archive = (lsn.filenum < log_gl.flush_lsn.filenum ? true : false);
            ret = do_log_file_read(lsn, logrec, read_from_archive);
            if (ret != 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "log manager undo - log read error=(%d:%s)\n",
                            errno, strerror(errno));
            }
            assert(ret == 0);

            ret = do_log_record_undo(logrec, tran);
            if (ret != 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "log manager undo - log undo error\n");
            }
            assert(ret == 0);

            lsn = logrec->header.unxt_lsn;
        }

        if (ret == 0) {
            Trans *saved = tran;
            tran = tran_next(tran);
            tran_reco_free(saved);
        } else {
            tran = tran_next(tran);
        }
    }
    fprintf(stderr, "[UNDO] end - num_trans=%d\n", count);
    return ret;
}

void log_mgr_restart(void)
{
    LogSN    redo_start_lsn;
    uint64_t min_tranid;

    //sleep(30);
    /* analysis phase */
    config->sq.recovery_phase = SQUALL_RECOVERY_ANALYSIS;
    do_log_mgr_analysis(&redo_start_lsn, &min_tranid);

    if (!LOGSN_IS_NULL(&redo_start_lsn))
    {
        log_gl.reco_logrec = (LogRec*)malloc(config->sq.data_page_size * 10);
        if (log_gl.reco_logrec == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "log manager - logrec malloc error=(%d:%s)\n",
                        errno, strerror(errno));
        }
        assert(log_gl.reco_logrec != NULL);

        /* 1) redo phase */
        config->sq.recovery_phase = SQUALL_RECOVERY_REDO;
        do_log_mgr_redo(redo_start_lsn, min_tranid);

        /* 2) undo phase */
        config->sq.recovery_phase = SQUALL_RECOVERY_UNDO;
        do_log_mgr_undo();

        /* 3) do checkpoint */
        config->sq.recovery_phase = SQUALL_RECOVERY_CHKPT;
        do_checkpoint(CHKPT_BY_REQUEST);

        free(log_gl.reco_logrec);
        log_gl.reco_logrec = NULL;

        /* buffer sanity check */
        /* if this check is useless, remove it */
        buf_sanity_check();
    }

    tran_mgr_set_nxt_tranid(log_gl.log_anchor.next_tranid);
    config->sq.recovery_phase = SQUALL_RECOVERY_DONE;
}

void log_mgr_checkpoint(void)
{
    if (log_gl.initialized) {
        do_checkpoint(CHKPT_BY_REQUEST);
    }
}

/* Log Manager Init and Fianl */

static void do_log_global_init(void)
{
    log_gl.log_path = config->sq.logs_path;

    LOGSN_SET_NULL(&log_gl.write_lsn);
    LOGSN_SET_NULL(&log_gl.flush_lsn);
    LOGSN_SET_NULL(&log_gl.fsync_lsn);
    log_gl.fsync_time.tv_sec = 0;
    log_gl.fsync_time.tv_usec = 0;

    pthread_mutex_init(&log_gl.write_lock, NULL);
    pthread_mutex_init(&log_gl.flush_lock, NULL);
    pthread_mutex_init(&log_gl.write_lsn_lock, NULL);
    pthread_mutex_init(&log_gl.flush_lsn_lock, NULL);
    pthread_mutex_init(&log_gl.fsync_lsn_lock, NULL);
    pthread_mutex_init(&log_gl.chkpt_lock, NULL);
}

static void do_log_struct_final(void)
{
    do_log_buff_final();
    do_log_file_final();
    do_log_anch_final();
}

static int do_log_struct_init(void)
{
    int ret = 0;

#ifdef SQUALL_USE_MMAP
    log_gl.log_buffer.data = MAP_FAILED;
#else
    log_gl.log_buffer.data = NULL;
#endif
    log_gl.log_buffer.fque = NULL;

    do {
        if ((ret = do_log_anch_init()) != 0) {
            break;
        }
        if ((ret = do_log_file_init(log_gl.log_anchor.last_filenum)) != 0) {
            break;
        }
        if ((ret = do_log_buff_init(config->sq.log_buffer_size*1024*1024)) != 0) {
            break;
        }
    } while(0);

    if (ret != 0) {
        do_log_struct_final();
    }
    return ret;
}

int log_mgr_init(void *conf, void *srvr)
{
    server = (SERVER_HANDLE_V1 *)srvr;
    config = (struct config *)conf;
    logger = server->log->get_logger();

    /* log record manager init */
    lrec_mgr_init(conf, srvr);

    log_gl.initialized = false;

    /* validation check */
    if (config->sq.log_buffer_size >= config->sq.log_file_size) {
        fprintf(stderr,  "log buffer size is too large. (>= log file size)\n");
        return -1; /* too large log buffer */
    }

    do_log_global_init();
    if (do_log_struct_init() != 0) {
        return -1;
    }

    do_log_flush_thread_init();
    do_log_chkpt_thread_init();

    log_gl.initialized = true;
    return 0;
}

void log_mgr_final(void)
{
    /* release log data structures */
    do_log_struct_final();
}

int log_mgr_flush_thread_start(void)
{
    return do_log_flush_thread_start();
}

void log_mgr_flush_thread_stop(void)
{
    do_log_flush_thread_stop();
}

int log_mgr_chkpt_thread_start(void)
{
    return do_log_chkpt_thread_start();
}

void log_mgr_chkpt_thread_stop(void)
{
    do_log_chkpt_thread_stop();
}
