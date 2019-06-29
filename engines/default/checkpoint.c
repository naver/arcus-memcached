/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2019 JaM2in Corp.
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
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/time.h>

#include "default_engine.h"

#ifdef ENABLE_PERSISTENCE_03_CHECKPOINT
#include "checkpoint.h"
#include "mc_snapshot.h"

#define CHKPT_MAX_FILENAME_LENGTH   255
#define CHKPT_DIRPATH               "backup"
#define CHKPT_FILE_NAME_FORMAT      "%s/%s%d"
#define CHKPT_SNAPSHOT_PREFIX       "snapshot_"
#define CHKPT_CMDLOG_PREFIX         "command_"

#define CHKPT_DEFAULT_SLEEP         1 /* 1 second */
#define CHKPT_ERROR_SLEEP           5 /* 5 seconds */

enum chkpt_error_code {
    CHKPT_SUCCESS = 0,
    CHKPT_ERROR,
    CHKPT_ERROR_FILE_REMOVE
};

/* checkpoint main structure */
typedef struct _chkpt_st {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    void    *engine;
    bool     start;       /* checkpoint module start */
    bool     stop;        /* stop to do checkpoint */
    bool     sleep;       /* checkpoint thread sleep */
    int      interval;    /* checkpoint execution interval */
    int      bgntime;     /* checkpoint begin time */
    int      pretime;     /* previous time of bgntime */
    char path[CHKPT_MAX_FILENAME_LENGTH+1]; /* file path for checkpoint */
} chkpt_st;

/* global data */
static EXTENSION_LOGGER_DESCRIPTOR* logger = NULL;
static chkpt_st chkpt_anch;

static int getnowtime(void)
{
    int ltime;
    time_t clock;
    struct tm *date;

    clock = time(0);
    date = localtime(&clock);
    ltime = date->tm_hour * 10000;
    ltime += date->tm_min * 100;
    ltime += date->tm_sec;
    return(ltime);
}

/* will create files for next checkpoint : snapshot_(cs->bgntime) */
static int do_chkpt_create_files(chkpt_st *cs)
{
    int nowtime = getnowtime();
    sprintf(cs->path, CHKPT_FILE_NAME_FORMAT, CHKPT_DIRPATH, CHKPT_SNAPSHOT_PREFIX, nowtime);
    int fd = open(cs->path, O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP);
    if (fd < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create file in checkpoint. "
                    "path : %s, error : %s\n", cs->path, strerror(errno));
        return -1;
    }
    close(fd);
    cs->pretime = cs->bgntime;
    cs->bgntime = nowtime;
    return 0;
}

/* will remove files : snapshot_(cs->pretime) */
static int do_chkpt_remove_files(chkpt_st *cs)
{
    sprintf(cs->path, CHKPT_FILE_NAME_FORMAT, CHKPT_DIRPATH, CHKPT_SNAPSHOT_PREFIX, cs->pretime);
    if (unlink(cs->path) < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to remove file in checkpoint. "
                    "path : %s, error : %s\n", cs->path, strerror(errno));
        return -1;
    }
    return 0;
}

static void do_chkpt_sleep(chkpt_st *cs, int ret)
{
    int sleep_sec;
    if (ret != CHKPT_SUCCESS) {
        sleep_sec = 5;
    }
    else {
        sleep_sec = 1;
    }

    struct timeval tv;
    struct timespec to;

    gettimeofday(&tv, NULL);
    to.tv_sec = tv.tv_sec + sleep_sec;
    to.tv_nsec = tv.tv_usec * 1000;

    pthread_mutex_lock(&cs->lock);
    cs->sleep = true;
    pthread_cond_timedwait(&cs->cond, &cs->lock, &to);
    cs->sleep = false;
    pthread_mutex_unlock(&cs->lock);
}

static void do_chkpt_wakeup(chkpt_st *cs)
{
    pthread_mutex_lock(&cs->lock);
    if (cs->sleep) {
        pthread_cond_signal(&cs->cond);
    }
    pthread_mutex_unlock(&cs->lock);
}


/* FIXME : Error handling(Disk I/O etc) */
static int do_checkpoint(chkpt_st *cs)
{
    if (do_chkpt_create_files(cs) < 0) {
        return CHKPT_ERROR;
    }

    if (mc_snapshot_direct(MC_SNAPSHOT_MODE_DATA, NULL, 0, cs->path) != ENGINE_SUCCESS) {
        return CHKPT_ERROR;
    }

    if (cs->pretime != -1) {
        if (do_chkpt_remove_files(cs) < 0) {
            return CHKPT_ERROR_FILE_REMOVE;
        }
    }
    return CHKPT_SUCCESS;
}

static void do_chkpt_init(chkpt_st *cs, struct default_engine *engine)
{
    pthread_mutex_init(&cs->lock, NULL);
    pthread_cond_init(&cs->cond, NULL);
    cs->engine = (void*)engine;
    cs->start = false;
    cs->stop = false;
    cs->sleep = false;
    cs->interval = 60;
    cs->bgntime = -1;
    cs->pretime = -1;
    cs->path[0] = '\0';
}

/* Delete all files that failed to erase. */
static bool do_chkpt_sweep_files(chkpt_st *cs)
{
    int slen = strlen(CHKPT_SNAPSHOT_PREFIX); /* 9 */
    int clen = strlen(CHKPT_CMDLOG_PREFIX);   /* 8 */
    DIR* dir;
    struct dirent* ent;
    if ((dir = opendir(CHKPT_DIRPATH)) != NULL) {
        while ((ent = readdir(dir)) != NULL) {
            char* ptr = ent->d_name;
            if (strncmp(CHKPT_SNAPSHOT_PREFIX, ptr, slen) == 0) {
                ptr += slen;
            }
            else if (strncmp(CHKPT_CMDLOG_PREFIX, ptr, clen) == 0) {
                ptr += clen;
            }

            if (cs->pretime != atoi(ptr) || cs->bgntime != atoi(ptr)) {
                if (unlink(ent->d_name) == -1) {
                    logger->log(EXTENSION_LOG_WARNING, NULL,
                                "Failed to remove file. path : %s. error : %s\n", ent->d_name, strerror(errno));
                    return true;
                }
            }
        }
        closedir(dir);
    }
    else {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to open backup directory. path : %s. error : %s\n", CHKPT_DIRPATH, strerror(errno));
        assert(1);
    }
    closedir(dir);
    return false;
}

static void* chkpt_thread_main(void* arg)
{
    chkpt_st *cs = (chkpt_st *)arg;
    struct default_engine *engine = cs->engine;
    size_t elapsed_time = 0; /* unit : second */
    bool next_retry = false;
    bool need_remove = false;
    int ret = 0;

    assert(cs->start == true);
    logger->log(EXTENSION_LOG_INFO, NULL, "chkpt thread has started.\n");

    while (engine->initialized) {
        if (cs->stop) {
            logger->log(EXTENSION_LOG_INFO, NULL, "Stop the current checkpoint.\n");
            break;
        }

        do_chkpt_sleep(cs, ret);

        if (need_remove) {
            need_remove = do_chkpt_sweep_files(cs);
        }

        elapsed_time += 1;
        if (next_retry || elapsed_time >= cs->interval) {
            ret = do_checkpoint(cs);
            if (ret != CHKPT_SUCCESS) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "Failed in checkpoint\n");
            }
            if (ret == CHKPT_ERROR_FILE_REMOVE) {
                need_remove = true;
            }
            else if (ret == CHKPT_ERROR) {
                next_retry = true;
                continue;
            }
            next_retry = false;
            elapsed_time = 0;
        }
    }
    cs->start = false;
    return NULL;
}

static int do_chkpt_start(chkpt_st *cs)
{
    pthread_t tid;

    cs->start = true;
    if (pthread_create(&tid, NULL, chkpt_thread_main, cs) != 0) {
        cs->start = false;
        logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to create chkpt thread\n");
        return -1;
    }
    return 0;
}

/*
 * External Functions
 */
ENGINE_ERROR_CODE chkpt_init_and_start(struct default_engine* engine)
{
    logger = engine->server.log->get_logger();
    do_chkpt_init(&chkpt_anch, engine);
    logger->log(EXTENSION_LOG_INFO, NULL, "CHECKPOINT module initialized.\n");

    if (do_chkpt_start(&chkpt_anch) != 0) {
        return ENGINE_FAILED;
    }
    return ENGINE_SUCCESS;
}

void chkpt_stop_and_final(void)
{
    pthread_mutex_lock(&chkpt_anch.lock);
    while (chkpt_anch.start) {
        chkpt_anch.stop = true;
        pthread_mutex_unlock(&chkpt_anch.lock);

        if (chkpt_anch.sleep) {
            do_chkpt_wakeup(&chkpt_anch);
        }
        usleep(1000); /* sleep 1ms */
        pthread_mutex_lock(&chkpt_anch.lock);
    }
    pthread_mutex_unlock(&chkpt_anch.lock);
    pthread_mutex_destroy(&chkpt_anch.lock);
    pthread_cond_destroy(&chkpt_anch.cond);
    logger->log(EXTENSION_LOG_INFO, NULL, "CHECKPOINT module destroyed\n");
}

#endif
