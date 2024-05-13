/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2015 JaM2in Co., Ltd.
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
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <memcached/util.h>

#include "cmdlog.h"

#define CMDLOG_INPUT_SIZE 400
#define CMDLOG_BUFFER_SIZE  (10 * 1024 * 1024)   /* 10MB */
#define CMDLOG_WRITE_SIZE   (4 * 1024)           /* 4KB */

#define CMDLOG_FILE_MAXSIZE (10 * 1024 * 1024)   /* 10MB : log at most CMDLOG_INPUT_SIZE * N commands in one file */
#define CMDLOG_FILE_MAXNUM   10                  /* # of cmdlog files */
#define CMDLOG_DIRPATH_LENGTH  128               /* directory path's length */
#define CMDLOG_FILENAME_LENGTH CMDLOG_DIRPATH_LENGTH + 128
#define CMDLOG_FILENAME_FORMAT "%s/command_%d_%d_%d_%d.log"

/* cmdlog state */
#define CMDLOG_NOT_STARTED   0  /* not started */
#define CMDLOG_EXPLICIT_STOP 1  /* stop by user request */
#define CMDLOG_OVERFLOW_STOP 2  /* stop by command log overflow */
#define CMDLOG_FLUSHERR_STOP 3  /* stop by flush operation error */
#define CMDLOG_RUNNING       4  /* running */

bool cmdlog_in_use = false; /* true or false : logging start condition */

static int mc_port;
static EXTENSION_LOGGER_DESCRIPTOR *mc_logger;

/* command log buffer structure */
struct cmd_log_buffer {
    pthread_mutex_t lock;
    char *data;
    uint32_t size;
    uint32_t head;
    uint32_t tail;
    uint32_t last;
};

/*command log flush structure */
struct cmd_log_flush {
    pthread_t tid;        /* flush thread id */
    pthread_attr_t attr;  /* flush thread mode */
    pthread_mutex_t lock; /* flush thread sleep and wakeup */
    pthread_cond_t cond;  /* flush thread sleep and wakeup */
    bool sleep;
};

/* command log stats structure */
struct cmd_log_stats {
    int bgndate, bgntime;
    int enddate, endtime;
    int file_count;
    volatile int state;        /* command log module state */
    uint32_t entered_commands; /* number of entered command */
    uint32_t skipped_commands; /* number of skipped command */
    char dirpath[CMDLOG_DIRPATH_LENGTH];
};

/* command log global structure */
struct cmd_log_global {
    pthread_mutex_t lock;
    struct cmd_log_buffer buffer;
    struct cmd_log_flush flush;
    struct cmd_log_stats stats;
    volatile bool reqstop;
};
struct cmd_log_global cmdlog;


static void do_cmdlog_flush_sleep(void)
{
    struct timeval tv;
    struct timespec to;

    /* 50 milli seconds sleep */
    pthread_mutex_lock(&cmdlog.flush.lock);

    gettimeofday(&tv, NULL);
    tv.tv_usec += 50000;
    if (tv.tv_usec >= 1000000) {
        tv.tv_sec += 1;
        tv.tv_usec -= 1000000;
    }
    to.tv_sec = tv.tv_sec;
    to.tv_nsec = tv.tv_usec * 1000;

    cmdlog.flush.sleep = true;
    pthread_cond_timedwait(&cmdlog.flush.cond, &cmdlog.flush.lock, &to);
    cmdlog.flush.sleep = false;

    pthread_mutex_unlock(&cmdlog.flush.lock);
}

static void do_cmdlog_flush_wakeup(void)
{
    /* wake up flush thread */
    pthread_mutex_lock(&cmdlog.flush.lock);
    if (cmdlog.flush.sleep == true) {
        pthread_cond_signal(&cmdlog.flush.cond);
    }
    pthread_mutex_unlock(&cmdlog.flush.lock);
}

static void do_cmdlog_stop(int cause)
{
    /* cmdlog lock has already been held */
    cmdlog.stats.state = cause;
    cmdlog.stats.enddate = getnowdate_int();
    cmdlog.stats.endtime = getnowtime_int();
    cmdlog_in_use = false;
}

static void *cmdlog_flush_thread(void *arg)
{
    struct cmd_log_buffer *buffer = &cmdlog.buffer;
    char fname[CMDLOG_FILENAME_LENGTH];
    uint32_t cur_tail;
    uint32_t cur_last;
    int fd = -1;
    int err = 0;
    int writelen;
    int nwritten;
    int nwtotal = 0;

    while (!cmdlog.reqstop)
    {
        if (fd < 0) { /* open command log file */
            sprintf(fname, CMDLOG_FILENAME_FORMAT, cmdlog.stats.dirpath,
                    mc_port, cmdlog.stats.bgndate, cmdlog.stats.bgntime,
                    cmdlog.stats.file_count);
            if ((fd = open(fname, O_WRONLY | O_CREAT | O_APPEND, 0644)) < 0) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Can't open command log file: %s, error: %s\n", fname, strerror(errno));
                err = -1; break;
            }
            cmdlog.stats.file_count++;
        }

        pthread_mutex_lock(&buffer->lock);
        cur_last = buffer->last;
        cur_tail = buffer->tail;
        pthread_mutex_unlock(&buffer->lock);

        if (buffer->head <= cur_tail) {
            assert(cur_last == 0);
            if (cmdlog_in_use) {
                if ((cur_tail - buffer->head) < CMDLOG_WRITE_SIZE) {
                    do_cmdlog_flush_sleep(); /* flush thread sleeps 50ms. */
                    continue;
                }
            } else {
                if (buffer->head == cur_tail) {
                    break; /* stop flushing */
                }
            }
            writelen = (cur_tail - buffer->head) < CMDLOG_WRITE_SIZE
                     ? (cur_tail - buffer->head) : CMDLOG_WRITE_SIZE;
            if (writelen > 0) {
                nwritten = write(fd, buffer->data + buffer->head, writelen);
                if (nwritten != writelen) {
                    mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                                   "write command log error: nwritten(%d) != writelen(%d)\n",
                                   nwritten, writelen);
                    err = -1; break;
                }
                nwtotal += nwritten;
                pthread_mutex_lock(&buffer->lock);
                buffer->head += writelen;
                pthread_mutex_unlock(&buffer->lock);
            }
        } else { /* buffer->head > cur_tail */
            assert(cur_last > 0);
            writelen = cur_last - buffer->head;
            if (writelen > 0) {
                nwritten = write(fd, buffer->data + buffer->head, writelen);
                if (nwritten != writelen) {
                    mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                                   "write command log error: nwritten(%d) != writelen(%d)\n",
                                   nwritten, writelen);
                    err = -1; break;
                }
                nwtotal += nwritten;
                pthread_mutex_lock(&buffer->lock);
                buffer->last = 0;
                buffer->head = 0;
                pthread_mutex_unlock(&buffer->lock);
            }
        }
        if (nwtotal >= CMDLOG_FILE_MAXSIZE) { /* rotate file */
            close(fd); fd = -1;
            nwtotal = 0;
            if (cmdlog.stats.file_count >= CMDLOG_FILE_MAXNUM) {
                break; /* do internal stop: overflow stop */
            }
        }
    }

    if (cmdlog_in_use) { /* do internal stop */
        pthread_mutex_lock(&cmdlog.lock);
        do_cmdlog_stop(err == -1 ? CMDLOG_FLUSHERR_STOP : CMDLOG_OVERFLOW_STOP);
        pthread_mutex_unlock(&cmdlog.lock);
    }
    if (fd > 0) close(fd);
    if (cmdlog.reqstop) {
        // cmdlog_final() checks flush thread is terminated
        cmdlog.stats.state = CMDLOG_NOT_STARTED;
    }
    return NULL;
}

void cmdlog_init(int port, EXTENSION_LOGGER_DESCRIPTOR *logger)
{
    mc_port = port;
    mc_logger = logger;

    cmdlog_in_use = false;
    pthread_mutex_init(&cmdlog.lock, NULL);

    pthread_mutex_init(&cmdlog.buffer.lock, NULL);
    cmdlog.buffer.size = CMDLOG_BUFFER_SIZE;
    cmdlog.buffer.data = NULL;

    pthread_mutex_init(&cmdlog.flush.lock, NULL);
    pthread_cond_init(&cmdlog.flush.cond, NULL);
    cmdlog.flush.sleep = false;

    memset(&cmdlog.stats, 0, sizeof(struct cmd_log_stats));
}

void cmdlog_final(void)
{
    while (cmdlog.stats.state == CMDLOG_RUNNING) {
        cmdlog.reqstop = true;
        do_cmdlog_flush_wakeup(); /* wake up flush thread */
        usleep(5000); /* sleep 5ms */
    }
    pthread_mutex_destroy(&cmdlog.buffer.lock);
    pthread_mutex_destroy(&cmdlog.lock);
    pthread_mutex_destroy(&cmdlog.flush.lock);
    pthread_cond_destroy(&cmdlog.flush.cond);

    if (cmdlog.buffer.data != NULL) {
        free(cmdlog.buffer.data);
    }
}

int cmdlog_start(char *file_path, bool *already_started)
{
    char fname[CMDLOG_FILENAME_LENGTH];
    int ret = 0;
    int fd = -1;

    *already_started = false;

    pthread_mutex_lock(&cmdlog.lock);
    do {
        if (cmdlog_in_use) {
            *already_started = true;
            break;
        }
        /* check the length of file_path */
        if (file_path != NULL && strlen(file_path) > CMDLOG_DIRPATH_LENGTH) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                           "Too long cmdlog file path.\n");
            ret = -1; break;
        }
        /* prepare command logging buffer */
        if (cmdlog.buffer.data == NULL) {
            if ((cmdlog.buffer.data = malloc(CMDLOG_BUFFER_SIZE)) == NULL) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Can't allocate command log buffer\n");
                ret = -1; break;
            }
        }
        cmdlog.buffer.head = 0;
        cmdlog.buffer.tail = 0;
        cmdlog.buffer.last = 0;

        /* prepare command logging stats */
        memset(&cmdlog.stats, 0, sizeof(struct cmd_log_stats));
        cmdlog.stats.bgndate = getnowdate_int();
        cmdlog.stats.bgntime = getnowtime_int();

        sprintf(cmdlog.stats.dirpath, "%s",
                (file_path != NULL ? file_path : "command_log"));

        /* open log file */
        sprintf(fname, CMDLOG_FILENAME_FORMAT, cmdlog.stats.dirpath,
                mc_port, cmdlog.stats.bgndate, cmdlog.stats.bgntime,
                cmdlog.stats.file_count);
        if ((fd = open(fname, O_WRONLY | O_CREAT | O_APPEND, 0644)) < 0) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                           "Can't open command log file: %s, error: %s\n", fname, strerror(errno));
            ret = -1; break;
        } else {
            close(fd);
        }

        /* enable command logging */
        cmdlog_in_use = true;
        cmdlog.stats.state = CMDLOG_RUNNING;

        /* start the flush thread to write command log to disk */
        if (pthread_attr_init(&cmdlog.flush.attr) != 0 ||
            pthread_attr_setdetachstate(&cmdlog.flush.attr, PTHREAD_CREATE_DETACHED) != 0 ||
            (ret = pthread_create(&cmdlog.flush.tid, &cmdlog.flush.attr, cmdlog_flush_thread, NULL)) != 0)
        {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                           "Can't create command log flush thread: %s\n", strerror(ret));
            cmdlog_in_use = false; // disable it */
            if (remove(fname) != 0 && errno != ENOENT) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Can't remove command log file: %s, error: %s\n", fname, strerror(errno));
            }
            cmdlog.stats.state = CMDLOG_NOT_STARTED;
            ret = -1; break;
        }
    } while(0);
    pthread_mutex_unlock(&cmdlog.lock);

    return ret;
}

void cmdlog_stop(bool *already_stopped)
{
    *already_stopped = false;

    pthread_mutex_lock(&cmdlog.lock);
    if (cmdlog_in_use == true) {
        do_cmdlog_stop(CMDLOG_EXPLICIT_STOP);
    } else {
        *already_stopped = true;
    }
    pthread_mutex_unlock(&cmdlog.lock);
}

char *cmdlog_stats(void)
{
    char *str = (char*)malloc(CMDLOG_INPUT_SIZE);
    if (str) {
        char *state_str[5] = { "Not started",                      // CMDLOG_NOT_STARTED
                               "stopped by explicit request",      // CMDLOG_EXPLICIT_STOP
                               "stopped by command log overflow",  // CMDLOG_OVERFLOW_STOP
                               "stopped by disk flush error",      // CMDLOG_FLUSHERR_STOP
                               "running" };                        // CMDLOG_RUNNING

        struct cmd_log_stats *stats = &cmdlog.stats;
        if (cmdlog_in_use) {
            stats->enddate = getnowdate_int();
            stats->endtime = getnowtime_int();
        }

        snprintf(str, CMDLOG_INPUT_SIZE,
                "\t" "Command logging state : %s" "\n"
                "\t" "The last running time : %d_%d ~ %d_%d" "\n"
                "\t" "The number of entered commands : %d" "\n"
                "\t" "The number of skipped commands : %d" "\n"
                "\t" "The number of log files : %d" "\n"
                "\t" "The log file name: %s/command_%d_%d_%d_{n}.log" "\n",
                (stats->state >= 0 && stats->state <= 4 ?
                 state_str[stats->state] : "unknown"),
                stats->bgndate, stats->bgntime, stats->enddate, stats->endtime,
                stats->entered_commands, stats->skipped_commands,
                stats->file_count,
                stats->dirpath, mc_port, stats->bgndate, stats->bgntime);
    }
    return str;
}

void cmdlog_write(char client_ip[], char* command)
{
    struct tm *ptm;
    struct timeval val;
    struct cmd_log_buffer *buffer = &cmdlog.buffer;
    char inputstr[CMDLOG_INPUT_SIZE];
    int inputlen;
    int nwritten;

    gettimeofday(&val, NULL);
    ptm = localtime(&val.tv_sec);

    nwritten = snprintf(inputstr, CMDLOG_INPUT_SIZE, "%02d:%02d:%02d.%06ld %s %s\n",
                       ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (long)val.tv_usec, client_ip, command);
    /* truncated ? */
    if (nwritten > CMDLOG_INPUT_SIZE) {
        inputstr[CMDLOG_INPUT_SIZE-4] = '.';
        inputstr[CMDLOG_INPUT_SIZE-3] = '.';
        inputstr[CMDLOG_INPUT_SIZE-2] = '\n';
    }
    inputlen = strlen(inputstr);

    pthread_mutex_lock(&buffer->lock);
    cmdlog.stats.entered_commands += 1;
    if (buffer->head <= buffer->tail && inputlen >= (buffer->size - buffer->tail)) {
        buffer->last = buffer->tail;
        buffer->tail = 0;
    }
    if (buffer->head <= buffer->tail) {
        assert(buffer->last == 0);
        assert(inputlen < (buffer->size - buffer->tail));
        memcpy(buffer->data + buffer->tail, inputstr, inputlen);
        buffer->tail += inputlen;
        if ((buffer->tail - buffer->head) >= CMDLOG_WRITE_SIZE) {
            do_cmdlog_flush_wakeup(); /* wake up flush thread */
        }
    } else { /* buffer->head > buffer->tail */
        assert(buffer->last > 0);
        if (inputlen < (buffer->head - buffer->tail)) {
            memcpy(buffer->data + buffer->tail, inputstr, inputlen);
            buffer->tail += inputlen;
        } else {
            cmdlog.stats.skipped_commands += 1;
        }
        do_cmdlog_flush_wakeup(); /* wake up flush thread */
    }
    pthread_mutex_unlock(&buffer->lock);
}
