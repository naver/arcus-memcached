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
#include <sys/time.h> /* gettimeofday() */

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE
#include "checkpoint.h"
#include "cmdlogfile.h"

#define CHKPT_FILE_NAME_FORMAT     "%s/%s%"PRId64
#define CHKPT_SNAPSHOT_PREFIX      "snapshot_"
#define CHKPT_CMDLOG_PREFIX        "cmdlog_"

#define CHKPT_CHECK_INTERVAL 5
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
    void    *config;
    bool     sleep;               /* checkpoint thread sleep */
    int64_t  prevtime;            /* previous checkpoint time */
    int64_t  lasttime;            /* last checkpoint time */
    size_t   lastsize;            /* last snapshot log file size */
    char     snapshot_path[MAX_FILEPATH_LENGTH]; /* snapshot file path */
    char     cmdlog_path[MAX_FILEPATH_LENGTH];   /* cmdlog file path */
    char    *data_path;           /* snapshot directory path */
    char    *logs_path;           /* command log directory path */
    volatile uint8_t running;     /* Is it running, now ? */
    volatile bool    reqstop;     /* stop to do checkpoint */
    volatile bool    initialized; /* checkpoint module init */
} chkpt_st;

typedef struct _chkpt_last_st {
    time_t     recovery_elapsed_time_sec;
    bool       last_chkpt_in_progress;
    int        last_chkpt_failure_count;
    int64_t    last_chkpt_start_time;
    time_t     last_chkpt_elapsed_time_sec;
} chkpt_last_st;

/* global data */
static EXTENSION_LOGGER_DESCRIPTOR* logger = NULL;
static chkpt_st chkpt_anch;
static chkpt_last_st chkpt_last_stat;

static void chkpt_last_stat_init(void) {
    chkpt_last_stat.recovery_elapsed_time_sec = 0;
    chkpt_last_stat.last_chkpt_in_progress = false;
    chkpt_last_stat.last_chkpt_failure_count = 0;
    chkpt_last_stat.last_chkpt_start_time = 0;
    chkpt_last_stat.last_chkpt_elapsed_time_sec = 0;
}
static int64_t getnowtime(void)
{
    char buf[20] = {0};
    int64_t ltime;
    time_t clock = time(0);
    struct tm *date = localtime(&clock);

    /* year(YYYY) month(01-12) day(01-31) hour(00-23) minute(00-59) second(00-61). */
    strftime(buf, 20, "%Y%m%d%H%M%S", date);
    sscanf(buf, "%" SCNd64, &ltime);
    return ltime;
}

/* Delete all backup files except last checkpoint file. */
static bool do_chkpt_sweep_files(chkpt_st *cs)
{
    DIR *dir;
    struct dirent *ent;
    int plen; /* file prefix name length */
    int ret = true;

    /* delete snapshot files. */
    if ((dir = opendir(cs->data_path)) == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to open snapshot directory. path: %s, error: %s\n",
                    cs->data_path, strerror(errno));
        ret = false;
    } else {
        plen = strlen(CHKPT_SNAPSHOT_PREFIX); /* 9 */
        while ((ent = readdir(dir)) != NULL) {
            char *ptr = ent->d_name;
            if (strncmp(CHKPT_SNAPSHOT_PREFIX, ptr, plen) != 0 ||
                cs->lasttime == atoi(ptr + plen)) {
                continue;
            }
            sprintf(cs->snapshot_path, "%s/%s", cs->data_path, ent->d_name);
            if (unlink(cs->snapshot_path) < 0 && errno != ENOENT) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "Failed to sweep snapshot file. path: %s, error: %s\n",
                            cs->snapshot_path, strerror(errno));
                ret = false; break;
            }
        }
        closedir(dir);
    }

    /* delete command log files. */
    if ((dir = opendir(cs->logs_path)) == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to open cmdlog directory. path: %s, error: %s\n",
                    cs->logs_path, strerror(errno));
        ret = false;
    } else {
        plen = strlen(CHKPT_CMDLOG_PREFIX); /* 7 */
        while ((ent = readdir(dir)) != NULL) {
            char *ptr = ent->d_name;
            if (strncmp(CHKPT_CMDLOG_PREFIX, ptr, plen) != 0 ||
                cs->lasttime == atoi(ptr + plen)) {
                continue;
            }
            sprintf(cs->cmdlog_path, "%s/%s", cs->logs_path, ent->d_name);
            if (unlink(cs->cmdlog_path) < 0 && errno != ENOENT) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "Failed to sweep cmdlog file. path: %s, error: %s\n",
                            cs->cmdlog_path, strerror(errno));
                ret = false; break;
            }
        }
        closedir(dir);
    }

    return ret;
}

/* create files for next checkpoint : snapshot_(newtime), cmdlog_(newtime) */
static int do_chkpt_create_files(chkpt_st *cs, int64_t newtime)
{
    int fd;

    sprintf(cs->snapshot_path, CHKPT_FILE_NAME_FORMAT,
            cs->data_path, CHKPT_SNAPSHOT_PREFIX, newtime);
    fd = open(cs->snapshot_path, O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP);
    if (fd < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create snapshot file. path: %s, error: %s\n",
                    cs->snapshot_path, strerror(errno));
        return CHKPT_ERROR;
    }
    close(fd);

    sprintf(cs->cmdlog_path, CHKPT_FILE_NAME_FORMAT,
            cs->logs_path, CHKPT_CMDLOG_PREFIX, newtime);
    fd = open(cs->cmdlog_path, O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP);
    if (fd < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create cmdlog file. path: %s, error: %s\n",
                    cs->cmdlog_path, strerror(errno));

        /* remove the snapshot file created here */
        if (unlink(cs->snapshot_path) < 0 && errno != ENOENT) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to remove the created file. path: %s, error: %s\n",
                        cs->snapshot_path, strerror(errno));
            return CHKPT_ERROR_FILE_REMOVE;
        }
        return CHKPT_ERROR;
    }
    close(fd);

    return 0;
}

/* remove files : snapshot_(oldtime), cmdlog_(oldtime) */
static int do_chkpt_remove_files(chkpt_st *cs, int64_t oldtime)
{
    sprintf(cs->snapshot_path, CHKPT_FILE_NAME_FORMAT,
            cs->data_path, CHKPT_SNAPSHOT_PREFIX, oldtime);
    if (unlink(cs->snapshot_path) < 0 && errno != ENOENT) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to remove snapshot file. path: %s, error: %s\n",
                    cs->snapshot_path, strerror(errno));
        return -1;
    }

    sprintf(cs->cmdlog_path, CHKPT_FILE_NAME_FORMAT,
            cs->logs_path, CHKPT_CMDLOG_PREFIX, oldtime);
    if (unlink(cs->cmdlog_path) < 0 && errno != ENOENT) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to remove cmdlog file. path: %s, error: %s\n",
                    cs->cmdlog_path, strerror(errno));
        return -1;
    }
    return 0;
}

static int do_chkpt_thread_sleep(chkpt_st *cs, int sleep_sec)
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

static void do_chkpt_thread_wakeup(chkpt_st *cs)
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
    logger->log(EXTENSION_LOG_INFO, NULL, "Checkpoint started.\n");
    int64_t newtime = getnowtime();
    chkpt_last_stat.last_chkpt_in_progress = true;
    chkpt_last_stat.last_chkpt_start_time = newtime;
    time_t start, end;
    start = time(NULL);
    int ret;

    if ((ret = do_chkpt_create_files(cs, newtime)) != 0) {
        chkpt_last_stat.last_chkpt_failure_count += 1;
        chkpt_last_stat.last_chkpt_in_progress = false;
        return ret; /* CHKPT_ERROR or CHKPT_ERROR_FILE_REMOVE */
    }

    if ((ret = cmdlog_file_open(cs->cmdlog_path)) != 0) {
        ret = CHKPT_ERROR;
    } else {
        if (chkpt_snapshot_direct(CHKPT_SNAPSHOT_MODE_CHKPT, NULL, -1,
                                  cs->snapshot_path,
                                  &cs->lastsize) == ENGINE_SUCCESS) {
            ret = CHKPT_SUCCESS;
            cs->prevtime = cs->lasttime;
            cs->lasttime = newtime;

            logger->log(EXTENSION_LOG_INFO, NULL, "Checkpoint has been done.\n");
            /* We will remove the previous checkpoint files
             * after those files are closed by log file module.
             * See cmdlog_file_dual_write_finished().
             */
        } else {
            ret = CHKPT_ERROR;
            /* Close the current log file. Don't close the previous log file.
             * See cmdlog_file_sync().
             */
            cmdlog_file_close();
        }
    }

    if (ret != CHKPT_SUCCESS) {
        /* remove the checkpoint files, created in this failed checkpoint. */
        if (do_chkpt_remove_files(cs, newtime) < 0) {
            ret = CHKPT_ERROR_FILE_REMOVE;
        }
        chkpt_last_stat.last_chkpt_failure_count += 1;
    } else {
        chkpt_last_stat.last_chkpt_failure_count = 0;
    }
    end = time(NULL);
    chkpt_last_stat.last_chkpt_elapsed_time_sec = end - start;
    chkpt_last_stat.last_chkpt_in_progress = false;
    return ret;
}

static bool do_checkpoint_needed(chkpt_st *cs)
{
    struct engine_config *config = cs->config;
    size_t snapshot_file_size = cs->lastsize;
    size_t cmdlog_file_size   = cmdlog_file_getsize();
    size_t min_logsize        = config->chkpt_interval_min_logsize;
    int    pct_snapshot       = config->chkpt_interval_pct_snapshot;

    if ((cmdlog_file_size < min_logsize) ||
        (cmdlog_file_size < (snapshot_file_size + (snapshot_file_size*(pct_snapshot*0.01))))) {
        return false;
    }
    return true;
}

static void* chkpt_thread_main(void* arg)
{
    chkpt_st *cs = (chkpt_st *)arg;
    size_t elapsed_time = 0; /* unit : second */
    size_t flsweep_time = 0; /* unit : second */
    bool need_remove = false;
    int ret = CHKPT_SUCCESS;

    cs->running = RUNNING_STARTED;
    while (1) {
        elapsed_time += do_chkpt_thread_sleep(cs, 1);

        if (cs->reqstop) {
            logger->log(EXTENSION_LOG_INFO, NULL, "Checkpoint thread recognized stop request.\n");
            break;
        }

        if (need_remove) {
            if (++flsweep_time >= CHKPT_SWEEP_INTERVAL) {
                /* sweep checkpoint files in each CHKPT_SWEEP_INTERVAL second */
                flsweep_time = 0;
                need_remove = !do_chkpt_sweep_files(cs);
            }
        }

        if (elapsed_time >= CHKPT_CHECK_INTERVAL) {
            /* check previous checkpoint is completed. */
            if (cs->prevtime != -1) {
                if (cmdlog_file_dual_write_finished()) {
                    /* remove previous checkpoint files. */
                    if (do_chkpt_remove_files(cs, cs->prevtime) < 0) {
                        need_remove = true;
                    }
                    cs->prevtime = -1;
                }
            }
            if (cs->prevtime == -1 && do_checkpoint_needed(cs)) {
                ret = do_checkpoint(cs);
                if (ret != CHKPT_SUCCESS) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "Failed in checkpoint. "
                                "Retry checkpoint in 5 seconds.\n");
                    if (ret == CHKPT_ERROR_FILE_REMOVE) need_remove = true;
                }
            }
            elapsed_time = 0;
        }
    }
    cs->running = RUNNING_STOPPED;
    return NULL;
}

/*
 * External Functions
 */

/* Recovery Functions */

static int chkptsnapshotfilter(const struct dirent *ent)
{
    return (strncmp(ent->d_name, CHKPT_SNAPSHOT_PREFIX, strlen(CHKPT_SNAPSHOT_PREFIX)) == 0);
}

int chkpt_recovery_analysis(void)
{
    chkpt_st *cs = &chkpt_anch;
    /* Sort snapshot files in alphabetical order. */
    struct dirent **snapshotlist;
    int snapshot_count = scandir(cs->data_path, &snapshotlist, chkptsnapshotfilter, alphasort);
    if (snapshot_count < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to scan snapshot directory. path: %s, error: %s\n",
                    cs->data_path, strerror(errno));
        return -1;
    }

    struct dirent *ent;
    int lastidx = snapshot_count -1;
    int ret = 0;

    /* Find valid last snapshot file and get lasttime. */
    while (lastidx >= 0) {
        ent = snapshotlist[lastidx];
        sprintf(cs->snapshot_path, "%s/%s", cs->data_path, ent->d_name);
        int snapshot_fd = open(cs->snapshot_path, O_RDONLY);
        if (snapshot_fd < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to open snapshot file. path: %s, error: %s\n",
                        cs->snapshot_path, strerror(errno));
            ret = -1; break;
        }

        logger->log(EXTENSION_LOG_INFO, NULL,
                    "Check that %s is valid snapshot file for recovery.\n",
                    cs->snapshot_path);
        if (chkpt_snapshot_check_file_validity(snapshot_fd, &cs->lastsize) == 0) {
            cs->lasttime = atoll(strchr(ent->d_name, '_') + 1);
            assert(cs->lasttime != 0);
            close(snapshot_fd);
            break;
        }
        close(snapshot_fd);
        lastidx--;
    }

    for (int i = 0; i < snapshot_count; i++) {
        free(snapshotlist[i]);
    }
    free(snapshotlist);

    return ret;
}

int chkpt_recovery_redo(void)
{
    chkpt_st *cs = &chkpt_anch;

    if (cs->lasttime > 0) {
        time_t start, end;
        start = time(NULL);
        /* apply snapshot log records. */
        if (chkpt_snapshot_file_apply(cs->snapshot_path) < 0) {
            return -1;
        }
        sprintf(cs->cmdlog_path, CHKPT_FILE_NAME_FORMAT,
                cs->logs_path, CHKPT_CMDLOG_PREFIX, cs->lasttime);
        if (cmdlog_file_open(cs->cmdlog_path) < 0) {
            return -1;
        }
        /* apply cmd log records if they exist. */
        if (cmdlog_file_apply() < 0) {
            return -1;
        }
         end = time(NULL);
         chkpt_last_stat.recovery_elapsed_time_sec = end - start;
    } else {
        /* create empty checkpoint snapshot and create/open cmdlog file. */
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "There are no files needed for recovery. "
                    "Do checkpoint to create checkpoint file set.\n");
        if (do_checkpoint(cs) != CHKPT_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Checkpoint failed in chkpt_recovery_redo().\n");
            return -1;
        }
    }
    return 0;
}

/* Checkpoint Functions */

ENGINE_ERROR_CODE chkpt_init(struct default_engine* engine)
{
    logger = engine->server.log->get_logger();

    chkpt_last_stat_init();
    pthread_mutex_init(&chkpt_anch.lock, NULL);
    pthread_cond_init(&chkpt_anch.cond, NULL);
    chkpt_anch.config = (void*)&engine->config;
    chkpt_anch.sleep = false;
    chkpt_anch.prevtime = -1;
    chkpt_anch.lasttime = -1;
    chkpt_anch.lastsize = 0;
    chkpt_anch.snapshot_path[0] = '\0';
    chkpt_anch.cmdlog_path[0] = '\0';
    chkpt_anch.data_path = engine->config.data_path;
    chkpt_anch.logs_path = engine->config.logs_path;
    chkpt_anch.running = RUNNING_UNSTARTED;
    chkpt_anch.reqstop = false;
    chkpt_anch.initialized = true;
    logger->log(EXTENSION_LOG_INFO, NULL, "CHECKPOINT module initialized.\n");
    return ENGINE_SUCCESS;
}
void chkpt_persistence_stats(struct default_engine *engine, ADD_STAT add_stat, const void *cookie)
{
     struct engine_config *conf = &engine->config;

     add_statistics(cookie, add_stat, NULL, -1, "use_persistence", "%s", conf->use_persistence ? "on" : "off");
     if (conf->use_persistence) {
         add_statistics(cookie, add_stat, NULL, -1, "data_path", "%s", conf->data_path);
         add_statistics(cookie, add_stat, NULL, -1, "logs_path", "%s", conf->logs_path);
         add_statistics(cookie, add_stat, NULL, -1, "async_logging", "%s", conf->async_logging? "true" : "false");
         add_statistics(cookie, add_stat, NULL, -1, "chkpt_interval_pct_snapshot", "%u", conf->chkpt_interval_pct_snapshot);
         add_statistics(cookie, add_stat, NULL, -1, "chkpt_interval_min_logsize", "%u", conf->chkpt_interval_min_logsize);
         add_statistics(cookie, add_stat, NULL, -1, "recovery_elapsed_time_sec", "%ld", (long)chkpt_last_stat.recovery_elapsed_time_sec);
         add_statistics(cookie, add_stat, NULL, -1, "last_chkpt_in_progress", "%s",
                        chkpt_last_stat.last_chkpt_in_progress? "true" : "false");
         add_statistics(cookie, add_stat, NULL, -1, "last_chkpt_failure_count", "%d", chkpt_last_stat.last_chkpt_failure_count);
         add_statistics(cookie, add_stat, NULL, -1, "last_chkpt_start_time", "%"PRIu64, chkpt_last_stat.last_chkpt_start_time);
         add_statistics(cookie, add_stat, NULL, -1, "last_chkpt_elapsed_time_sec", "%ld", (long)chkpt_last_stat.last_chkpt_elapsed_time_sec);
         add_statistics(cookie, add_stat, NULL, -1, "last_chkpt_snapshot_filesize_bytes", "%u", chkpt_anch.lastsize);
         add_statistics(cookie, add_stat, NULL, -1, "current_command_log_filesize_bytes", "%u", cmdlog_file_getsize());
     }
}
ENGINE_ERROR_CODE chkpt_thread_start(void)
{
    pthread_t tid;
    chkpt_anch.running = RUNNING_UNSTARTED;
    /* create checkpoint thread */
    if (pthread_create(&tid, NULL, chkpt_thread_main, &chkpt_anch) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create checkpoint thread. error=%s\n", strerror(errno));
        return ENGINE_FAILED;
    }
    /* wait until checkpoint thread starts */
    while (chkpt_anch.running == RUNNING_UNSTARTED) {
        usleep(5000); /* sleep 5ms */
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "Checkpoint thread started.\n");
    return ENGINE_SUCCESS;
}

void chkpt_thread_stop(void)
{
    if (chkpt_anch.running == RUNNING_UNSTARTED) {
        return;
    }
    while (chkpt_anch.running == RUNNING_STARTED) {
        chkpt_anch.reqstop = true;
        do_chkpt_thread_wakeup(&chkpt_anch);
        usleep(5000); /* sleep 5ms */
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "Checkpoint thread stopped.\n");
}

void chkpt_final(void)
{
    if (chkpt_anch.initialized == false) {
        return;
    }
    pthread_mutex_destroy(&chkpt_anch.lock);
    pthread_cond_destroy(&chkpt_anch.cond);
    chkpt_anch.initialized = false;
    logger->log(EXTENSION_LOG_INFO, NULL, "CHECKPOINT module destroyed.\n");
}

int64_t chkpt_get_lasttime(void)
{
    return chkpt_anch.lasttime;
}
#endif
