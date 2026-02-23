#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/time.h>

#include "default_engine.h"

#ifdef ENABLE_PERSISTENCE
#include "cmdlogbuf.h"
#include "cmdlogfile.h"
#include "cdclogfile.h"
#include "disk.h"

#define ENABLE_DEBUG 0
#define MAX_FILE_SIZE (1024*1024)
#define MAX_FILE_INDEX 999999

/* log file structure */
typedef struct _log_file {
    char      path[MAX_FILEPATH_LENGTH];
    char      next_path[MAX_FILEPATH_LENGTH];
    int       prev_fd;
    int       fd;
    int       next_fd;
    size_t    size;
    size_t    next_size;
} log_FILE;

/* cdc log file global struct */
struct cdc_file_global {
    log_FILE log_file;
    LogSN nxt_fsync_lsn;
    int fidx_bgn;
    int fidx_end;
    int fidx_dw_bgn;
    int fidx_dw_end;
    pthread_mutex_t log_fsync_lock;
    pthread_mutex_t fsync_lsn_lock;
    pthread_mutex_t file_access_lock;
    volatile bool initialized;
};

/* global data */
static struct engine_config *config = NULL;
static EXTENSION_LOGGER_DESCRIPTOR *logger = NULL;
static struct cdc_file_global cdc_file_gl;

bool next_cdclog_path(char *path, char *next_path)
{
    char *slash = strrchr(path, '/');
    if (!slash || slash == path) return false;

    char *base = slash + 1;

    // cdclog_<time>_<number>
    // base: "cdclog_"(7) + time(14) + '_'(1) + num(6)
    char *time = base+ 7;
    char *num = base + 22;

    int n = atoi(num)+1;
    size_t dirlen = (size_t)(slash - path);

    memcpy(next_path, path, dirlen);
    next_path[dirlen] = '\0';

    snprintf(next_path+dirlen, MAX_FILEPATH_LENGTH-dirlen, "/cdclog_%.14s_%06d", time, n);
    return true;
}

bool cdclog_file_deletable()
{
    bool ret;
    pthread_mutex_lock(&cdc_file_gl.file_access_lock);
    ret = (cdc_file_gl.fidx_bgn < cdc_file_gl.fidx_end);
    pthread_mutex_unlock(&cdc_file_gl.file_access_lock);
    return ret;
}

void cdclog_file_delete(const char *path)
{
    if (unlink(path) < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to remove cdclog file. path: %s, error: %s\n",
                    path, strerror(errno));
        return;
    }

    pthread_mutex_lock(&cdc_file_gl.file_access_lock);
    cdc_file_gl.fidx_bgn += 1;
    if (cdc_file_gl.fidx_bgn > MAX_FILE_INDEX)
        cdc_file_gl.fidx_bgn = 1;
    pthread_mutex_unlock(&cdc_file_gl.file_access_lock);
}

static int create_new_cdclog(log_FILE **logfile, int next_fidx, bool dual_write)
{
    int fd;
    char newtime[15];
    const char *logfile_path = (dual_write ? (*logfile)->next_path : (*logfile)->path);
    const char *slash = strrchr(logfile_path, '/');
    const char *base  = (slash ? slash + 1 : logfile_path);
    const char *time = base + 7;
    char newpath[MAX_FILEPATH_LENGTH];

    memcpy(newtime, time, 14);
    newtime[14] = '\0';

    snprintf(newpath, MAX_FILEPATH_LENGTH,"%.*s/%s%s_%06d",
            (int)(slash-logfile_path), logfile_path,"cdclog_", newtime, next_fidx);

    if ((fd = open(newpath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP))<0) {
        logger->log(EXTENSION_LOG_WARNING, "Fail to open file : %s\n", newpath);
        return -1;
    }

    if (dual_write) {
        (*logfile)->next_fd = fd;
        (*logfile)->next_size = 0;
        snprintf((*logfile)->next_path, MAX_FILEPATH_LENGTH, "%s", newpath);
    } else {
        (*logfile)->fd = fd;
        (*logfile)->size = 0;
        snprintf((*logfile)->path, MAX_FILEPATH_LENGTH, "%s", newpath);
    }
    return 0;
}

static size_t cdclog_file_fit(log_FILE *logfile, char **log_ptr, uint32_t log_size)
{
    size_t bytes_to_write = 0;
    ssize_t nwrite;
    while (bytes_to_write < log_size) {
        LogRec *logrec = (LogRec*)(*log_ptr + bytes_to_write);
        LogHdr *loghdr = &logrec->header;
        size_t log_len = sizeof(LogHdr)+loghdr->body_length;

        if (logfile->size+bytes_to_write+log_len > MAX_FILE_SIZE) {
            break;
        }
        bytes_to_write += log_len;
    }

    if (bytes_to_write > 0) {
        nwrite = disk_write(logfile->fd, *log_ptr, bytes_to_write);
        if (nwrite != bytes_to_write) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                "curr log file(%d) write - write(%ld!=%ld) error=(%d:%s)\n",
                logfile->fd, nwrite, (ssize_t)bytes_to_write,
                errno, strerror(errno));
        }

        *log_ptr += bytes_to_write;
        log_size -= bytes_to_write;
        logfile->size += bytes_to_write;
    }

    return log_size;
}

void cdclog_file_write(char *log_ptr, uint32_t log_size, bool dual_write)
{
    log_FILE *logfile = &cdc_file_gl.log_file;
    ssize_t nwrite;
    uint32_t dual_log_size = log_size;
    char *dual_log_ptr = log_ptr;
    if (logfile->fd == -1)
        return;

    pthread_mutex_lock(&cdc_file_gl.file_access_lock);
    while (logfile->size+log_size > MAX_FILE_SIZE) {
        log_size = cdclog_file_fit(logfile, &log_ptr, log_size);
        pthread_mutex_lock(&cdc_file_gl.log_fsync_lock);
        pthread_mutex_unlock(&cdc_file_gl.file_access_lock);
        if (!config->async_logging)
            disk_fsync(logfile->fd);
        disk_close(logfile->fd);
        pthread_mutex_lock(&cdc_file_gl.file_access_lock);
        cdc_file_gl.fidx_end+=1;
        if (cdc_file_gl.fidx_end > MAX_FILE_INDEX)
            cdc_file_gl.fidx_end = 1;
        create_new_cdclog(&logfile, cdc_file_gl.fidx_end, false);
        pthread_mutex_unlock(&cdc_file_gl.log_fsync_lock);
    }
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
        while (logfile->next_size+dual_log_size > MAX_FILE_SIZE) {
            dual_log_size = cdclog_file_fit(logfile, &dual_log_ptr, dual_log_size);
            pthread_mutex_unlock(&cdc_file_gl.file_access_lock);
            if (!config->async_logging)
                disk_fsync(logfile->next_fd);
            disk_close(logfile->next_fd);
            pthread_mutex_lock(&cdc_file_gl.file_access_lock);
            cdc_file_gl.fidx_dw_end += 1;
            create_new_cdclog(&logfile, cdc_file_gl.fidx_dw_end, true);
        }
        /* The log data is appended */
        nwrite = disk_write(logfile->next_fd, dual_log_ptr, dual_log_size);
        if (nwrite != dual_log_size) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "next log file(%d) write - write(%ld!=%ld) error=(%d:%s)\n",
                        logfile->next_fd, nwrite, (ssize_t)dual_log_size,
                        errno, strerror(errno));
        }
        /* FIXME::need error handling */
        assert(nwrite == dual_log_size);
        logfile->next_size += dual_log_size;
    }
    pthread_mutex_unlock(&cdc_file_gl.file_access_lock);
}

void cdclog_file_complete_dual_write(void)
{
    log_FILE *logfile = &cdc_file_gl.log_file;

    pthread_mutex_lock(&cdc_file_gl.file_access_lock);
    if (logfile->next_fd != -1) {
        logfile->prev_fd   = logfile->fd;
        logfile->fd        = logfile->next_fd;
        logfile->size      = logfile->next_size;
        logfile->next_fd   = -1;
        logfile->next_size = 0;
        snprintf(logfile->path, MAX_FILEPATH_LENGTH, "%s", logfile->next_path);
        logfile->next_path[0] = '\0';
        cdc_file_gl.fidx_bgn = cdc_file_gl.fidx_dw_bgn;
        cdc_file_gl.fidx_end = cdc_file_gl.fidx_dw_end;
        cdc_file_gl.fidx_dw_bgn = -1;
        cdc_file_gl.fidx_dw_end = -1;

        if (config->async_logging) {
            (void)disk_close(logfile->prev_fd);
            logfile->prev_fd = -1;
        }
    }
    pthread_mutex_unlock(&cdc_file_gl.file_access_lock);
}

bool cdclog_file_dual_write_finished(void)
{
    log_FILE *logfile = &cdc_file_gl.log_file;
    bool finished = false;
    pthread_mutex_lock(&cdc_file_gl.file_access_lock);
    if (logfile->next_fd == -1 && logfile->prev_fd == -1) {
        finished = true;
    }
    pthread_mutex_unlock(&cdc_file_gl.file_access_lock);
    return finished;
}

int cdclog_file_sync(void)
{
    log_FILE *logfile = &cdc_file_gl.log_file;
    LogSN now_flush_lsn;
    int fd;
    int prev_fd = -1;
    int next_fd = -1;
    int ret = 0;

    pthread_mutex_lock(&cdc_file_gl.log_fsync_lock);

    /* get current flush lsn */
    cmdlog_get_flush_lsn(&now_flush_lsn);

    /* get current fd info */
    pthread_mutex_lock(&cdc_file_gl.file_access_lock);
    if (logfile->prev_fd != -1) {
        prev_fd = logfile->prev_fd;
        logfile->prev_fd = -1;
    }
    fd = logfile->fd;
    if (logfile->next_fd != -1) {
        next_fd = logfile->next_fd;
    }
    pthread_mutex_unlock(&cdc_file_gl.file_access_lock);

    if (prev_fd != -1) {
        (void)disk_fsync(prev_fd);
        (void)disk_close(prev_fd);
    }

    if (LOGSN_IS_GT(&now_flush_lsn, &cdc_file_gl.nxt_fsync_lsn)) {
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
            pthread_mutex_lock(&cdc_file_gl.fsync_lsn_lock);
            cdc_file_gl.nxt_fsync_lsn = now_flush_lsn;
            pthread_mutex_unlock(&cdc_file_gl.fsync_lsn_lock);
        } while(0);
    }
    pthread_mutex_unlock(&cdc_file_gl.log_fsync_lock);

    return ret;
}

int cdclog_file_open(char *path)
{
    log_FILE *logfile = &cdc_file_gl.log_file;
    int fd, ret = 0;

    pthread_mutex_lock(&cdc_file_gl.file_access_lock);
    do {
        fd = disk_open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP);
        if (fd < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to open the cmdlog file. path=%s err=%s\n",
                        logfile->path, strerror(errno));
            ret = -1; break;
        }
        if (logfile->fd == -1) {
            logfile->fd = fd;
            cdc_file_gl.fidx_bgn = 1;
            cdc_file_gl.fidx_end = 1;
            snprintf(logfile->path, MAX_FILEPATH_LENGTH, "%s", path);
        } else {
            /* fd != -1 means that a new cdclog file is created by checkpoint */
            logfile->next_fd = fd;
            cdc_file_gl.fidx_dw_bgn = 1;
            cdc_file_gl.fidx_dw_end = 1;
            snprintf(logfile->next_path, MAX_FILEPATH_LENGTH, "%s", path);
        }
    } while(0);
    pthread_mutex_unlock(&cdc_file_gl.file_access_lock);

    return ret;
}

void cdclog_file_close(void)
{
    log_FILE *logfile = &cdc_file_gl.log_file;
    int remove_fd;

    /* We hold log_fsync_lock to prevent fsync() call
     * on the file being closed. See cmdlog_file_sync().
     */
    pthread_mutex_lock(&cdc_file_gl.log_fsync_lock);
    pthread_mutex_lock(&cdc_file_gl.file_access_lock);
    if (logfile->next_fd != -1) {
        remove_fd = logfile->next_fd;
        logfile->next_fd = -1;
        logfile->next_size = 0;
        logfile->next_path[0] = '\0';
    } else { /* the first checkpoint */
        assert(logfile->fd != -1);
        remove_fd = logfile->fd;
        logfile->fd = -1;
    }
    pthread_mutex_unlock(&cdc_file_gl.file_access_lock);
    pthread_mutex_unlock(&cdc_file_gl.log_fsync_lock);

    assert(remove_fd != -1);
    (void)disk_close(remove_fd);
}

void cdclog_file_init(struct default_engine* engine)
{
    config = &engine->config;
    logger = engine->server.log->get_logger();

    /* cdc file global init */
    memset(&cdc_file_gl, 0, sizeof(cdc_file_gl));

    cdc_file_gl.nxt_fsync_lsn.filenum = 1;
    cdc_file_gl.nxt_fsync_lsn.roffset = 0;

    pthread_mutex_init(&cdc_file_gl.log_fsync_lock, NULL);
    pthread_mutex_init(&cdc_file_gl.fsync_lsn_lock, NULL);
    pthread_mutex_init(&cdc_file_gl.file_access_lock, NULL);

    /* cdc file init */
    log_FILE *logfile = &cdc_file_gl.log_file;
    logfile->path[0]   = '\0';
    logfile->next_path[0] = '\0';
    logfile->prev_fd   = -1;
    logfile->fd        = -1;
    logfile->next_fd   = -1;
    logfile->size      = 0;
    logfile->next_size = 0;

    cdc_file_gl.fidx_bgn = -1;
    cdc_file_gl.fidx_end = -1;
    cdc_file_gl.fidx_dw_bgn = -1;
    cdc_file_gl.fidx_dw_end = -1;
    cdc_file_gl.initialized = true;
    logger->log(EXTENSION_LOG_INFO, NULL, "CDCLOG FILE module initialized.\n");
}

void cdclog_file_final(void)
{
    log_FILE *logfile = &cdc_file_gl.log_file;

    if (cdc_file_gl.initialized == false) {
        return;
    }

    if (logfile->fd != -1) {
        (void)disk_fsync(logfile->fd);
        (void)disk_close(logfile->fd);
        logfile->fd = -1;
    }
    if (logfile->next_fd != -1) {
        (void)disk_close(logfile->next_fd);
        logfile->fd = -1;
    }

    pthread_mutex_destroy(&cdc_file_gl.log_fsync_lock);
    pthread_mutex_destroy(&cdc_file_gl.fsync_lsn_lock);
    pthread_mutex_destroy(&cdc_file_gl.file_access_lock);

    cdc_file_gl.initialized = false;
    logger->log(EXTENSION_LOG_INFO, NULL, "CDCLOG FILE module destroyed.\n");
}
size_t cdclog_file_getsize(void)
{
    log_FILE *logfile = &cdc_file_gl.log_file;
    size_t size;
    pthread_mutex_lock(&cdc_file_gl.file_access_lock);
    size = logfile->size;
    pthread_mutex_unlock(&cdc_file_gl.file_access_lock);

    return size;
}

void cdclog_get_fsync_lsn(LogSN *lsn)
{
    pthread_mutex_lock(&cdc_file_gl.fsync_lsn_lock);
    *lsn = cdc_file_gl.nxt_fsync_lsn;
    pthread_mutex_unlock(&cdc_file_gl.fsync_lsn_lock);
}
#endif
