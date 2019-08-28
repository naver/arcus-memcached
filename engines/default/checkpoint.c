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
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/time.h>

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE
#include "checkpoint.h"
#include "mc_snapshot.h"
#include "cmdlogbuf.h"

#define CHKPT_MAX_FILENAME_LENGTH  255
#define CHKPT_FILE_NAME_FORMAT     "%s/%s%d"
#define CHKPT_DIRPATH              "backup"
#define CHKPT_SNAPSHOT_PREFIX      "snapshot_"
#define CHKPT_CMDLOG_PREFIX        "cmdlog_"

#define CHKPT_SWEEP_INTERVAL 5

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
    int      lasttime;    /* last checkpoint time */
    char     path[CHKPT_MAX_FILENAME_LENGTH+1]; /* file path for checkpoint */
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

/* Delete all backup files except last checkpoint file. */
static bool do_chkpt_sweep_files(chkpt_st *cs)
{
    int ret = true;
    int slen = strlen(CHKPT_SNAPSHOT_PREFIX); /* 9 */
    DIR *dir;
    struct dirent *ent;

    if ((dir = opendir(CHKPT_DIRPATH)) == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to open backup directory. path : %s. error : %s\n",
                    CHKPT_DIRPATH, strerror(errno));
        return false;
    }

    while ((ent = readdir(dir)) != NULL) {
        char *ptr = ent->d_name;
        if (strncmp(CHKPT_SNAPSHOT_PREFIX, ptr, slen) != 0) {
            continue;
        }
        ptr += slen;

        if (cs->lasttime != atoi(ptr)) {
            if (unlink(ent->d_name) < 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "Failed to remove file. name : %s. error : %s\n",
                            ent->d_name, strerror(errno));
                ret = false; break;
            }
        }
    }
    closedir(dir);
    return ret;
}

/* create files for next checkpoint : snapshot_(newtime), cmdlog_(newtime) */
static int do_chkpt_create_files(chkpt_st *cs, int newtime)
{
    sprintf(cs->path, CHKPT_FILE_NAME_FORMAT, CHKPT_DIRPATH, CHKPT_CMDLOG_PREFIX, newtime);
    int fd = open(cs->path, O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP);
    if (fd < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create file in checkpoint. "
                    "path : %s, error : %s\n", cs->path, strerror(errno));
        return -1;
    }
    close(fd);
    if (cmdlog_file_open(cs->path) < 0) {
        return -1;
    }

    sprintf(cs->path, CHKPT_FILE_NAME_FORMAT, CHKPT_DIRPATH, CHKPT_SNAPSHOT_PREFIX, newtime);
    fd = open(cs->path, O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP);
    if (fd < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create file in checkpoint. "
                    "path : %s, error : %s\n", cs->path, strerror(errno));
        /* FIXME: need error handling - delete old file for open failure */
        return -1;
    }
    close(fd);

    return 0;
}

/* remove files : snapshot_(oldtime), cmdlog_(oldtime) */
static int do_chkpt_remove_files(chkpt_st *cs, int oldtime)
{
    sprintf(cs->path, CHKPT_FILE_NAME_FORMAT, CHKPT_DIRPATH, CHKPT_SNAPSHOT_PREFIX, oldtime);
    if (unlink(cs->path) < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to remove file in checkpoint. "
                    "path : %s, error : %s\n", cs->path, strerror(errno));
        return -1;
    }
    cmdlog_file_close();
    sprintf(cs->path, CHKPT_FILE_NAME_FORMAT, CHKPT_DIRPATH, CHKPT_CMDLOG_PREFIX, oldtime);
    if (unlink(cs->path) < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to remove file in checkpoint. "
                    "path : %s, error : %s\n", cs->path, strerror(errno));
        return -1;
    }
    return 0;
}

static int do_chkpt_sleep(chkpt_st *cs, int sleep_sec)
{
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

    return sleep_sec;
}

static void do_chkpt_wakeup(chkpt_st *cs, bool lock_hold)
{
    if (lock_hold)
        pthread_mutex_lock(&cs->lock);
    if (cs->sleep) {
        pthread_cond_signal(&cs->cond);
    }
    if (lock_hold)
        pthread_mutex_unlock(&cs->lock);
}

/* FIXME : Error handling(Disk I/O etc) */
static int do_checkpoint(chkpt_st *cs, bool *need_remove)
{
    int ret;
    int oldtime = cs->lasttime;
    int newtime = getnowtime();

    if (do_chkpt_create_files(cs, newtime) < 0) {
        return CHKPT_ERROR;
    }

    if (mc_snapshot_direct(MC_SNAPSHOT_MODE_DATA, NULL, -1, cs->path) == ENGINE_SUCCESS) {
        cs->lasttime = newtime;
        ret = CHKPT_SUCCESS;
    } else {
        oldtime = newtime;
        ret = CHKPT_ERROR;
    }

    if (oldtime != -1) {
        if (do_chkpt_remove_files(cs, oldtime) < 0) {
            *need_remove = true;
        }
    }
    return ret;
}

static void* chkpt_thread_main(void* arg)
{
    chkpt_st *cs = (chkpt_st *)arg;
    struct default_engine *engine = cs->engine;
    size_t elapsed_time = 0; /* unit : second */
    size_t flsweep_time = 0; /* unit : second */
    bool need_remove = false;
    int ret = CHKPT_SUCCESS;

    logger->log(EXTENSION_LOG_INFO, NULL, "chkpt thread has started.\n");

    while (engine->initialized) {
        elapsed_time += do_chkpt_sleep(cs, 1);

        if (cs->stop) {
            logger->log(EXTENSION_LOG_INFO, NULL, "Stop the current checkpoint.\n");
            break;
        }

        if (need_remove) {
            if (++flsweep_time >= CHKPT_SWEEP_INTERVAL) {
                /* sweep checkpoint files in each CHKPT_SWEEP_INTERVAL second */
                flsweep_time = 0;
                need_remove = !do_chkpt_sweep_files(cs);
            }
        }

        if (elapsed_time >= cs->interval) {
            ret = do_checkpoint(cs, &need_remove);
            if (ret == CHKPT_SUCCESS) {
                elapsed_time = 0;
            } else {
                logger->log(EXTENSION_LOG_WARNING, NULL, "Failed in checkpoint. "
                            "Retry checkpoint in 5 seconds.\n");
                elapsed_time -= 5;
            }
        }
    }
    cs->start = false;
    return NULL;
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
    cs->lasttime = -1;
    cs->path[0] = '\0';
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

    if (do_chkpt_start(&chkpt_anch) < 0) {
        return ENGINE_FAILED;
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "CHECKPOINT module initialized.\n");
    return ENGINE_SUCCESS;
}

void chkpt_stop_and_final(void)
{
    pthread_mutex_lock(&chkpt_anch.lock);
    while (chkpt_anch.start) {
        chkpt_anch.stop = true;
        if (chkpt_anch.sleep) {
            do_chkpt_wakeup(&chkpt_anch, false); /* false: doesn't hold lock */
        }
        pthread_mutex_unlock(&chkpt_anch.lock);
        usleep(5000); /* sleep 5ms */
        pthread_mutex_lock(&chkpt_anch.lock);
    }
    pthread_mutex_unlock(&chkpt_anch.lock);
    pthread_mutex_destroy(&chkpt_anch.lock);
    pthread_cond_destroy(&chkpt_anch.cond);
    logger->log(EXTENSION_LOG_INFO, NULL, "CHECKPOINT module destroyed\n");
}
#endif
