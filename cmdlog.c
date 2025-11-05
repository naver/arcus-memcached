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
#include "cmdlog.h"

#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>

#include "memcached/util.h"

#define CMDLOG_INPUT_SIZE 400
#define CMDLOG_BUFFER_SIZE  (10 * 1024 * 1024)   /* 10MB */
#define CMDLOG_WRITE_SIZE   (4 * 1024)           /* 4KB */

#define CMDLOG_FILE_MAXSIZE (10 * 1024 * 1024)   /* 10MB : log at most CMDLOG_INPUT_SIZE * N commands in one file */
#define CMDLOG_FILE_MAXNUM   10                  /* # of cmdlog files */
#define CMDLOG_DIRPATH_LENGTH  128               /* directory path's length */
#define CMDLOG_FILENAME_LENGTH CMDLOG_DIRPATH_LENGTH + 128
#define CMDLOG_FILENAME_FORMAT "%s/command_%d_%d_%d_%d.log"

#define CMDLOG_FILTER_MAXNUM 10
#define CMDLOG_FILTER_CMD_MAXLEN 30
#define CMDLOG_FILTER_KEY_MAXLEN 1000

/* cmdlog state */
#define CMDLOG_NOT_STARTED   0  /* not started */
#define CMDLOG_OVERFLOW_STOP 1  /* stop by command log overflow */
#define CMDLOG_FLUSHERR_STOP 2  /* stop by flush operation error */
#define CMDLOG_RUNNING       3  /* running */

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

/* command log filter structure */
struct cmd_log_filter {
    char command[CMDLOG_FILTER_CMD_MAXLEN + 1];
    char subcommand[CMDLOG_FILTER_CMD_MAXLEN + 1];
    char key[CMDLOG_FILTER_KEY_MAXLEN + 1];
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
    struct cmd_log_filter filters[CMDLOG_FILTER_MAXNUM];
    int nfilters;
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

static void do_cmdlog_stop(void)
{
    /* cmdlog lock has already been held */
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
    int stop_state = CMDLOG_NOT_STARTED;
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
                stop_state = CMDLOG_FLUSHERR_STOP; break;
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
                    stop_state = CMDLOG_FLUSHERR_STOP; break;
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
                    stop_state = CMDLOG_FLUSHERR_STOP; break;
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
                stop_state = CMDLOG_OVERFLOW_STOP; break; /* do internal stop: overflow stop */
            }
        }
    }

    if (fd > 0) close(fd);

    pthread_mutex_lock(&cmdlog.lock);
    if (cmdlog_in_use) {
        do_cmdlog_stop();
    }
    cmdlog.stats.state = stop_state;
    pthread_mutex_unlock(&cmdlog.lock);
    return NULL;
}

static int do_cmdlog_start(char *file_path)
{
    char fname[CMDLOG_FILENAME_LENGTH];
    int fd = -1;
    int err_ret = 0;  /* for creating log flush thread */

    /* check the previous cmdlog flush thread state */
    if (cmdlog.stats.state == CMDLOG_RUNNING) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "The previous cmdlog flush thread has not been finished yet.\n");
        return -1;
    }
    /* check the length of file_path */
    if (file_path != NULL && strlen(file_path) > CMDLOG_DIRPATH_LENGTH) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Too long cmdlog file path.\n");
        return -1;
    }
    /* prepare command logging buffer */
    if (cmdlog.buffer.data == NULL) {
        if ((cmdlog.buffer.data = malloc(CMDLOG_BUFFER_SIZE)) == NULL) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Can't allocate command log buffer\n");
            return -1;
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
        return -1;
    } else {
        close(fd);
    }

    /* enable command logging */
    cmdlog_in_use = true;
    cmdlog.stats.state = CMDLOG_RUNNING;

    /* start the flush thread to write command log to disk */
    if (pthread_attr_init(&cmdlog.flush.attr) != 0 ||
        pthread_attr_setdetachstate(&cmdlog.flush.attr, PTHREAD_CREATE_DETACHED) != 0 ||
        (err_ret = pthread_create(&cmdlog.flush.tid, &cmdlog.flush.attr, cmdlog_flush_thread, NULL)) != 0)
    {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Can't create command log flush thread: %s\n", strerror(err_ret));
        cmdlog_in_use = false; // disable it */
        if (remove(fname) != 0 && errno != ENOENT) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Can't remove command log file: %s, error: %s\n", fname, strerror(errno));
        }
        cmdlog.stats.state = CMDLOG_NOT_STARTED;
        return -1;
    }

    return 0;
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
    int ret = 0;
    *already_started = false;

    pthread_mutex_lock(&cmdlog.lock);
    if (cmdlog_in_use) {
        *already_started = true;
    } else {
        ret = do_cmdlog_start(file_path);
    }
    pthread_mutex_unlock(&cmdlog.lock);
    return ret;
}

void cmdlog_stop(bool *already_stopped)
{
    *already_stopped = false;

    pthread_mutex_lock(&cmdlog.lock);
    if (cmdlog_in_use) {
        do_cmdlog_stop();
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
                "\t" "The number of entered commands : %u" "\n"
                "\t" "The number of skipped commands : %u" "\n"
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

void cmdlog_write(char *client_ip, token_t *tokens, size_t ntokens)
{
    struct tm *ptm;
    struct timeval val;
    struct cmd_log_buffer *buffer = &cmdlog.buffer;
    char inputstr[CMDLOG_INPUT_SIZE];
    int inputlen;

    gettimeofday(&val, NULL);
    ptm = localtime(&val.tv_sec);

    inputlen = snprintf(inputstr, CMDLOG_INPUT_SIZE, "%02d:%02d:%02d.%06ld %s ",
                        ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (long)val.tv_usec, client_ip);

    for (int i = 0; i < ntokens - 1; i++) {
        inputlen += snprintf(inputstr + inputlen, CMDLOG_INPUT_SIZE - inputlen, "%s", tokens[i].value);
        if (i < ntokens - 2) inputstr[inputlen++] = ' ';

        if (inputlen >= CMDLOG_INPUT_SIZE) {
            inputstr[CMDLOG_INPUT_SIZE-4] = '.';
            inputstr[CMDLOG_INPUT_SIZE-3] = '.';
            inputlen = CMDLOG_INPUT_SIZE - 2;
            break;
        }

    }
    inputstr[inputlen++] = '\n';
    inputstr[inputlen] = '\0';

    pthread_mutex_lock(&buffer->lock);
    cmdlog.stats.entered_commands += 1;
    /* If wrap-around required, write tail to last. */
    if (buffer->head <= buffer->tail && inputlen >= (buffer->size - buffer->tail)) {
        buffer->last = buffer->tail;
        buffer->tail = 0;
    }

    if (buffer->head <= buffer->tail) {  /* wrap-around don`t occur */
        assert(buffer->last == 0);
        assert(inputlen < (buffer->size - buffer->tail));
        memcpy(buffer->data + buffer->tail, inputstr, inputlen);
        buffer->tail += inputlen;
        if ((buffer->tail - buffer->head) >= CMDLOG_WRITE_SIZE) {
            do_cmdlog_flush_wakeup(); /* wake up flush thread */
        }
    } else { /* wrap-around occur */
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

bool is_cmdlog_filter_match(const token_t *cmd, const token_t *subcmd, const token_t *key, size_t ntokens)
{
    int mkey_idx = 0;
    bool multi_key = false;
    bool matched = false;

    if (cmdlog.nfilters == 0) {
        return true;
    }

    if (ntokens < 2) {
        return false;
    }

    pthread_mutex_lock(&cmdlog.lock);
    for (int i = 0; i < cmdlog.nfilters; ++i) {
        struct cmd_log_filter *filter = &cmdlog.filters[i];

        if (filter->command[0] == '\0') {
            matched = true;
        } else if (filter->subcommand[0] == '\0') {
            matched = strcmp(cmd->value, filter->command) == 0 ? true : false;
        } else if (subcmd) {
            matched = strcmp(cmd->value, filter->command) == 0 && strcmp(subcmd->value, filter->subcommand) == 0 ?
                      true : false;
        } else {
            continue;
        }

        if (matched) {
            if (filter->key[0] == '\0') {
                matched = true;
            } else if (key) {
                do {
                    matched = string_pattern_match(key[mkey_idx].value, key[mkey_idx].length, filter->key, strlen(filter->key)) ?
                        true : false;
                    if (matched == false && multi_key) mkey_idx++;
                } while (matched == false && multi_key && mkey_idx < ntokens - 1);
            } else {
                matched = false;
            }
        }

        if (matched) break;
    }
    pthread_mutex_unlock(&cmdlog.lock);

    return matched;
}

int cmdlog_filter_add(const token_t *cmd, const token_t *subcmd, const token_t *key)
{
    if (cmdlog.nfilters >= CMDLOG_FILTER_MAXNUM) {
        return 0;
    }

    if ((cmd && cmd->length > CMDLOG_FILTER_CMD_MAXLEN) ||
        (subcmd && subcmd->length > CMDLOG_FILTER_CMD_MAXLEN) ||
        (key && key->length > CMDLOG_FILTER_KEY_MAXLEN)) {
        return -1;
    }

    pthread_mutex_lock(&cmdlog.lock);
    strcpy(cmdlog.filters[cmdlog.nfilters].command, cmd ? cmd->value : "");
    strcpy(cmdlog.filters[cmdlog.nfilters].subcommand, subcmd ? subcmd->value : "");
    strcpy(cmdlog.filters[cmdlog.nfilters].key, key ? key->value : "");
    cmdlog.nfilters++;
    pthread_mutex_unlock(&cmdlog.lock);

    return 1;
}

int cmdlog_filter_remove(int idx, bool remove_all)
{
    if (remove_all == false && (idx < 0 || idx >= cmdlog.nfilters)) {
        return -1;
    }

    pthread_mutex_lock(&cmdlog.lock);
    if (remove_all) {
        cmdlog.nfilters = 0;
    } else {
        for (int i = idx; i < cmdlog.nfilters - 1; ++i) {
            cmdlog.filters[i] = cmdlog.filters[i + 1];
        }
        cmdlog.nfilters--;
    }
    pthread_mutex_unlock(&cmdlog.lock);

    return 0;
}

char *cmdlog_filter_list(void)
{
    char *buf = (char *)malloc((cmdlog.nfilters + 1) * CMDLOG_INPUT_SIZE);
    int nwritten = 0;

    if (!buf) {
        return NULL;
    }

    pthread_mutex_lock(&cmdlog.lock);
    nwritten = snprintf(buf, CMDLOG_INPUT_SIZE, "\t(%d / %d)\n", cmdlog.nfilters, CMDLOG_FILTER_MAXNUM);
    for (int i = 0; i < cmdlog.nfilters; ++i) {
        struct cmd_log_filter *filter = &cmdlog.filters[i];

        nwritten += snprintf(buf + nwritten, CMDLOG_INPUT_SIZE, "\t%d. ", i);
        if (filter->command[0] != '\0') {
            nwritten += snprintf(buf + nwritten, CMDLOG_INPUT_SIZE - nwritten, "command = %s%s%s",
                                 filter->command,
                                 filter->subcommand[0] != '\0' ? " " : "",
                                 filter->subcommand[0] != '\0' ? filter->subcommand : "");
        }

        if (filter->key[0] != '\0') {
            nwritten += snprintf(buf + nwritten, CMDLOG_INPUT_SIZE - nwritten, "%skey = %s",
                                 filter->command[0] != '\0' ? ", " : "",
                                 filter->key);
        }

        nwritten += snprintf(buf + nwritten, CMDLOG_INPUT_SIZE - nwritten, "\n");
    }
    pthread_mutex_unlock(&cmdlog.lock);

    return buf;
}
