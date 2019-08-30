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
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <dirent.h>
#include <ctype.h>
#include <stdlib.h>
#include <time.h>

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE_03_PERSISTENCE_FILE
#include "psfile.h"
#include "cmdlogrec.h"

#define PS_MAX_FILENAME_LENGTH        255
#define PS_DIRPATH                    "backup"
#define PS_CHECKPOINT_NAME_FORMAT     "%s/%s%d"      /* %d : time */
#define PS_SINGLE_CMDLOG_NAME_FORMAT  "%s/%s%d_%d"   /* %d_%d : time_seqnum */
#define PS_SNAPSHOT_PREFIX            "snapshot_"
#define PS_CMDLOG_PREFIX              "cmdlog_"

/* persistence file main structure */
typedef struct _psfile_st {
    int last_checkpoint_time;  /* last checkpoint time */
    int snapshot_fd;           /* snapshot file descriptor */
    int cmdlog_fd;             /* command log file descriptor */
    char path[PS_MAX_FILENAME_LENGTH+1]; /* file path */
} psfile_st;

/* global data */
static EXTENSION_LOGGER_DESCRIPTOR* logger = NULL;
static psfile_st ps_anch;

/* check that a SnapshotTailLog record exists at the end of the file. */
static int do_psfile_check_snapshot_taillog(int fd)
{
    assert(fd > 0);

    SnapshotTailLog log;
    off_t offset;
    ssize_t nread;

    offset = lseek(fd, -sizeof(log), SEEK_END);
    if (offset < 0) {
        return -1;
    }

    nread = read(fd, &log, sizeof(log));
    if (nread != sizeof(log)) {
        return -1;
    }

    /* it can be true by accident. */
    if (log.header.logtype == LOG_SNAPSHOT_TAIL &&
        log.header.updtype == UPD_NONE &&
        log.header.body_length == 0) {
        return 0;
    }
    return -1;
}

static int do_psfile_check_cmdlog_consecutive(int idx, struct dirent **cmdloglist, int log_count, int time)
{
    /* try to open all consecutive cmdlog files */
    /* set ps->cmdlog_fd with last cmdlog file and delete incompleted command log bytes to write new log. */
    /* ex. cmdlog_{time}_1, cmdlog_{time}_2, cmdlog{time}_3(= ps->cmdlog_fd) */
    return 0;
}

/* #1. Find sequence number of log file is 1. cmdlog_{time}_1.
 * #2. Check sequence numbers of log files having same time value are consecutive. cmdlog_{time}_1 ~ 3.
*/
static int do_psfile_check_cmdlog(psfile_st *ps, struct dirent **cmdloglist, int log_count)
{
    int eidx, time, seqnum;
    struct dirent *ent;
    char *ptr;

    for (eidx = log_count - 1; eidx >= 0; eidx--) {
        ent = cmdloglist[eidx];
        /* time */
        ptr = strchr(ent->d_name, '_');
        time = atoi(ptr + 1);

        /* sequence number */
        ptr = strchr(ptr + 1, '_');
        /* checkpoint cmdlog file. */
        if (ptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "snapshot file might be disappeared or invalid."
                        "path : %s/%s%d\n", PS_DIRPATH, PS_SNAPSHOT_PREFIX, time);
            continue;
        }
        seqnum = atoi(ptr + 1);
        if (seqnum == 1) {
            if (do_psfile_check_cmdlog_consecutive(eidx, cmdloglist, log_count, time) < 0) {
                return -1;
            } else {
                return 0;
            }        
        }
    }
    return -1;
}

/* create cmdlog file */
static int do_psfile_create_cmdlog(psfile_st *ps)
{
    return 0;
}

/* create empty snapshot and cmdlog file */
static int do_psfile_create_set(psfile_st *ps)
{
    return 0;
}

static int do_psfile_snapshotfilter(const struct dirent *dirent)
{
    return (strncmp(dirent->d_name, PS_SNAPSHOT_PREFIX, strlen(PS_SNAPSHOT_PREFIX)) == 0);
}

static int do_psfile_cmdlogfilter(const struct dirent *dirent)
{
    return (strncmp(dirent->d_name, PS_CMDLOG_PREFIX, strlen(PS_CMDLOG_PREFIX)) == 0);
}

static int do_psfile_check(psfile_st *ps)
{
    /* check backup directory exists */
    DIR *dir;
    if ((dir = opendir(PS_DIRPATH)) == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to open backup directory. "
                    "path : %s. error : %s\n", PS_DIRPATH, strerror(errno));
        return -1;
    }
    closedir(dir);

    int ret = 0;
    struct dirent **snapshotlist;
    struct dirent **cmdloglist;
    int snapshot_count, cmdlog_count;

    /* Sort files in alphabetical order */
    snapshot_count = scandir(PS_DIRPATH, &snapshotlist, *do_psfile_snapshotfilter, alphasort);
    cmdlog_count = scandir(PS_DIRPATH, &cmdloglist, *do_psfile_cmdlogfilter, alphasort);
    assert(snapshot_count >= 0 && cmdlog_count >= 0);
    if (snapshot_count + cmdlog_count == 0) {
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "There are no backup files in %s.\n", PS_DIRPATH);
        ret = 0; goto freelist;
    }

    struct dirent *ent;
    int lastidx = snapshot_count - 1;
    while (lastidx >= 0) {
        /* Find last snapshot file. */
        ent = snapshotlist[lastidx];
        ps->last_checkpoint_time = atoi(strchr(ent->d_name, '_') + 1);
        sprintf(ps->path, PS_CHECKPOINT_NAME_FORMAT, PS_DIRPATH, PS_SNAPSHOT_PREFIX, ps->last_checkpoint_time);
        ps->snapshot_fd = open(ps->path, O_RDONLY);
        if (ps->snapshot_fd < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to open snapshot file. "
                        "path : %s, error : %s\n", ent->d_name, strerror(errno));
            ret = -1; goto freelist;
        }

        /* Check this snapshot file is valid. */
        if (do_psfile_check_snapshot_taillog(ps->snapshot_fd) < 0) {
            close(ps->snapshot_fd);
            lastidx--; continue;
        } else {
            sprintf(ps->path, PS_CHECKPOINT_NAME_FORMAT, PS_DIRPATH, PS_CMDLOG_PREFIX, ps->last_checkpoint_time);
            /* FIXME: Distinguish normal snapshot or checkpoint snapshot */
            if (access(ps->path, F_OK) < 0) {
                /* if checkpoint snapshot */
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "cmdlog file might be disappeared. path : %s\n", ps->path);
            } else {
                ps->cmdlog_fd = open(ps->path, O_RDONLY);
                if (ps->cmdlog_fd < 0) {
                    logger->log(EXTENSION_LOG_WARNING, NULL,
                                "Failed to open cmdlog file. "
                                "path : %s, error : %s\n", ps->path, strerror(errno));
                    close(ps->snapshot_fd);
                    ret = -1; goto freelist;
                }
            }
            close(ps->snapshot_fd);
            ret = 0; goto freelist;
        }
    }
    ps->snapshot_fd = -1;

    /* There is no snapshot file.
     * Check that recovery is possible with only log files.
     */
    if (cmdlog_count != 0 && do_psfile_check_cmdlog(ps, cmdloglist, cmdlog_count) < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "There are no files available for backup. Check the files.\n");
        ret = -1;
    }

freelist:
    for(int idx = 0; idx < snapshot_count; idx++) {
        free(snapshotlist[idx]);
    }
    for(int idx = 0; idx < cmdlog_count; idx++) {
        free(cmdloglist[idx]);
    }
    free(snapshotlist);
    free(cmdloglist);
    return ret;
}

static void do_psfile_init(psfile_st *ps)
{
    ps->last_checkpoint_time = -1;
    ps->snapshot_fd = -1;
    ps->cmdlog_fd = -1;
    ps->path[0] = '\0';

    logger->log(EXTENSION_LOG_INFO, NULL, "PERSISTENCE FILE module initialized.\n");
}

static int do_psfile_create(psfile_st *ps)
{
    if (ps->snapshot_fd < 0 && ps->cmdlog_fd < 0) {
        if (do_psfile_create_set(ps) < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "The fileset create function has failed.\n");
            return -1;
        } else {
            return 0;
        }
    }

    if (ps->snapshot_fd > 0 && ps->cmdlog_fd < 0) {
        if (do_psfile_create_cmdlog(ps) < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "The cmdlog create function has failed.\n");
            return -1;
        } else {
            return 0;
        }
    }
    /* TODO: if mode is immediately restart, it has to create cmdlog file. */
    return -1;
}

static int do_psfile_sweep(psfile_st *ps)
{
    return 0;
}

/*
 * External Function
 */
ENGINE_ERROR_CODE psfile_init_and_prepare(struct default_engine *engine)
{
    logger = engine->server.log->get_logger();

    do_psfile_init(&ps_anch);
    if (do_psfile_check(&ps_anch) < 0) {
        return ENGINE_FAILED;
    }

    if (do_psfile_create(&ps_anch) < 0) {
        return ENGINE_FAILED;
    }

    if (do_psfile_sweep(&ps_anch) < 0) {
        return ENGINE_FAILED;
    }

    return ENGINE_SUCCESS;
}
#endif
