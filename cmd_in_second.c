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
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/uio.h>
#include <pthread.h>
#include <stdlib.h>
#include <fcntl.h>

#include "cmd_in_second.h"

#define LOG_LENGTH 400
#define IP_LENGTH 16
#define KEY_LENGTH 256
#define CMD_STR_LEN 30

static EXTENSION_LOGGER_DESCRIPTOR *mc_logger;

typedef enum cmd_in_second_state {
    NOT_STARTED,
    ON_LOGGING,
    ON_FLUSHING,
    STOPPED
} state;

typedef struct cmd_in_second_log {
    char key[KEY_LENGTH];
    char client_ip[IP_LENGTH];
} cmd_in_second_log;

typedef struct cmd_in_second_buffer {
    cmd_in_second_log* ring;
    int front;
    int rear;
    int capacity;
} cmd_in_second_buffer;

typedef struct cmd_in_second_timer {
    struct timeval* ring;
    int front;
    int rear;
    int capacity;
    int last_elem_idx;
    int circular_counter;
} cmd_in_second_timer;

typedef struct cmd_in_second_flush_thread {
    pthread_t tid;
    pthread_attr_t attr;
    pthread_cond_t cond;
    pthread_mutex_t lock;
    bool sleep;
} flush_thread;

struct cmd_in_second_global {
    int operation;
    char cmd_str[50];
    struct cmd_in_second_buffer buffer;
    cmd_in_second_timer timer;
    int bulk_limit;
    int log_per_timer;
    state cur_state;
    flush_thread flush;
    pthread_mutex_t lock;
};
static struct cmd_in_second_global cmd_in_second;

static bool is_bulk_cmd()
{
    bool timer_empty = cmd_in_second.timer.front == cmd_in_second.timer.rear;

    if (timer_empty) {
        return false;
    }

    struct timeval* front_time = &cmd_in_second.timer.ring[cmd_in_second.timer.front];
    struct timeval* last_time = &cmd_in_second.timer.ring[cmd_in_second.timer.last_elem_idx];

    return last_time->tv_sec - front_time->tv_sec <= 1;
}

static void do_flush_sleep() {
    struct timeval now;
    struct timespec timeout;

    pthread_mutex_lock(&cmd_in_second.flush.lock);
    gettimeofday(&now, NULL);

    now.tv_usec += 50000;

    if (now.tv_usec >= 1000000) {
        now.tv_sec += 1;
        now.tv_usec -= 1000000;
    }

    timeout.tv_sec = now.tv_sec;
    timeout.tv_nsec = now.tv_usec * 1000;


    cmd_in_second.flush.sleep = true;
    pthread_cond_timedwait(&cmd_in_second.flush.cond, &cmd_in_second.flush.lock, &timeout);
    cmd_in_second.flush.sleep = false;

    pthread_mutex_unlock(&cmd_in_second.flush.lock);
}

static void do_flush_wakeup(){
    pthread_mutex_lock(&cmd_in_second.flush.lock);
    if (cmd_in_second.flush.sleep) {
        pthread_cond_signal(&cmd_in_second.flush.cond);
    }
    pthread_mutex_unlock(&cmd_in_second.flush.lock);
}

static void* do_flush_write()
{
    int fd = open("cmd_in_second.log", O_CREAT | O_WRONLY | O_TRUNC,  0644);

    cmd_in_second_buffer* buffer = &cmd_in_second.buffer;
    cmd_in_second_timer* timer = &cmd_in_second.timer;

    while(1) {
        if (fd < 0) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                           "Can't open cmd_in_second.log");
            break;
        }

        if (cmd_in_second.cur_state == STOPPED) {
            break;
        }

        if (cmd_in_second.cur_state != ON_FLUSHING) {
            do_flush_sleep();
            continue;
        }


        char* log_str = (char*)malloc(LOG_LENGTH * cmd_in_second.bulk_limit * sizeof(char));

        if (log_str == NULL) {
            break;
        }

        const size_t cmd_len = strlen(cmd_in_second.cmd_str);
        const int whitespaces = 3;

        size_t expected_write_length = 0;
        int circular_log_counter = 0;

        bool buffer_empty = buffer->front == buffer->rear;

        while (!buffer_empty) {

            cmd_in_second_log front = buffer->ring[buffer->front];
            buffer->front = (buffer->front+1) % buffer->capacity;
            buffer_empty = buffer->front == buffer->rear;

            char time_str[50] = "";

            if (circular_log_counter == 0) {

                struct timeval* front_time = &timer->ring[timer->front];
                struct tm* lt = localtime((time_t*)&front_time->tv_sec);

                timer->front = (timer->front+1) % timer->capacity;

                if (lt == NULL) {
                    mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                           "localtime failed");
                    continue;
                }

                sprintf(time_str, "%04d-%02d-%02d %02d:%02d:%02d.%06d\n", lt ->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday,
                        lt->tm_hour, lt->tm_min, lt->tm_sec, (int)front_time->tv_usec);
                expected_write_length += 27;
            }

            char log[LOG_LENGTH] = "";
            snprintf(log, LOG_LENGTH, "%s%s %s %s\n", time_str, cmd_in_second.cmd_str, front.key, front.client_ip);
            strncat(log_str, log, LOG_LENGTH);

            expected_write_length += cmd_len + strlen(front.key) + strlen(front.client_ip) + whitespaces;
            circular_log_counter = (circular_log_counter+1) % cmd_in_second.log_per_timer;
        }

        int written_length = write(fd, log_str, expected_write_length);
        free(log_str);

        if (written_length != expected_write_length) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                           "write length is difference to expectation");
            break;
        }

        break;
    }

    if (fd >= 0) {
        close(fd);
    }

    free(buffer->ring);
    buffer->ring = NULL;

    free(timer->ring);
    timer->ring = NULL;

    cmd_in_second.cur_state = NOT_STARTED;

    return NULL;
}

static void do_buffer_add(const char* key, const char* client_ip)
{

    cmd_in_second_buffer* buffer = &cmd_in_second.buffer;

    snprintf(buffer->ring[buffer->rear].key,
             KEY_LENGTH, "%s", key);
    snprintf(buffer->ring[buffer->rear].client_ip,
             IP_LENGTH, "%s", client_ip);

    buffer->rear = (buffer->rear+1) % buffer->capacity;

    bool buffer_full = (buffer->rear+1) % buffer->capacity ==
                        buffer->front;

    if (buffer_full) {
        if (is_bulk_cmd()) {
            cmd_in_second.cur_state = ON_FLUSHING;
            do_flush_wakeup();
            return;
        }
        buffer->front = (buffer->front+1) % buffer->capacity;
    }
}

static void do_timer_add()
{
    struct cmd_in_second_timer* timer = &cmd_in_second.timer;

    if (gettimeofday(&timer->ring[timer->rear], NULL) == -1) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                       "gettimeofday failed");
        return;
    }

    timer->rear = (timer->rear+1) % timer->capacity;
    timer->last_elem_idx = timer->rear;
    bool timer_full = (timer->rear+1) % timer->capacity == timer->front;

    if (timer_full) {
        timer->front = (timer->front+1) % timer->capacity;
    }
}

void cmd_in_second_write(int operation, const char* key, const char* client_ip)
{
    pthread_mutex_lock(&cmd_in_second.lock);

    if (cmd_in_second.cur_state != ON_LOGGING ||
        cmd_in_second.operation != operation) {
        pthread_mutex_unlock(&cmd_in_second.lock);
        return;
    }

    if (cmd_in_second.timer.circular_counter == 0) {
        do_timer_add();
    }

    do_buffer_add(key, client_ip);
    cmd_in_second.timer.circular_counter = (cmd_in_second.timer.circular_counter+1) %
                                            cmd_in_second.log_per_timer;

    pthread_mutex_unlock(&cmd_in_second.lock);
    return;
}

void cmd_in_second_init(EXTENSION_LOGGER_DESCRIPTOR *logger)
{
    mc_logger = logger;
    cmd_in_second.cur_state = NOT_STARTED;

    pthread_mutex_init(&cmd_in_second.lock, NULL);
    pthread_attr_init(&cmd_in_second.flush.attr);
    pthread_mutex_init(&cmd_in_second.flush.lock, NULL);
    pthread_cond_init(&cmd_in_second.flush.cond, NULL);

    cmd_in_second.buffer.ring = NULL;
    cmd_in_second.timer.ring = NULL;
}

int cmd_in_second_start(int operation, const char cmd_str[], int bulk_limit)
{

    pthread_mutex_lock(&cmd_in_second.lock);

    if (cmd_in_second.cur_state != NOT_STARTED) {
        pthread_mutex_unlock(&cmd_in_second.lock);
        return CMD_IN_SECOND_STARTED_ALREADY;
    }

    int fd = open("cmd_in_second.log", O_CREAT | O_WRONLY | O_TRUNC,  0644);

    if (fd < 0) {
        pthread_mutex_unlock(&cmd_in_second.lock);
        return CMD_IN_SECOND_FILE_FAILED;
    }

    close(fd);

    if (pthread_attr_init(&cmd_in_second.flush.attr) != 0 ||
        pthread_attr_setdetachstate(&cmd_in_second.flush.attr, PTHREAD_CREATE_DETACHED) != 0 ||
        (pthread_create(&cmd_in_second.flush.tid, &cmd_in_second.flush.attr,
                        do_flush_write, NULL)) != 0)
    {
        pthread_mutex_unlock(&cmd_in_second.lock);
        return CMD_IN_SECOND_THREAD_FAILED;
    }

    cmd_in_second.operation = operation;
    cmd_in_second.bulk_limit = bulk_limit;
    snprintf(cmd_in_second.cmd_str, CMD_STR_LEN, "%s", cmd_str);

    cmd_in_second.buffer.front = 0;
    cmd_in_second.buffer.rear = 0;
    cmd_in_second.buffer.capacity = bulk_limit+1;
    cmd_in_second.buffer.ring = (cmd_in_second_log*)malloc(cmd_in_second.buffer.capacity *
                                sizeof(cmd_in_second_log));

    if (cmd_in_second.buffer.ring == NULL) {
        pthread_mutex_unlock(&cmd_in_second.lock);
        return CMD_IN_SECOND_NO_MEM;
    }

    cmd_in_second.log_per_timer = bulk_limit / 10 + (bulk_limit % 10 != 0);

    cmd_in_second.timer.front = 0;
    cmd_in_second.timer.rear = 0;
    cmd_in_second.timer.circular_counter = 0;
    cmd_in_second.timer.last_elem_idx = 0;
    cmd_in_second.timer.capacity = 12;
    cmd_in_second.timer.ring = (struct timeval*) malloc (cmd_in_second.timer.capacity *
                                                         sizeof(struct timeval));

    if (cmd_in_second.timer.ring == NULL) {
        free(cmd_in_second.buffer.ring);
        pthread_mutex_unlock(&cmd_in_second.lock);
        return CMD_IN_SECOND_NO_MEM;
    }

    cmd_in_second.cur_state = ON_LOGGING;

    pthread_mutex_unlock(&cmd_in_second.lock);

    return CMD_IN_SECOND_START;
}

void cmd_in_second_stop(bool* already_stop) {

    pthread_mutex_lock(&cmd_in_second.lock);

    if (cmd_in_second.cur_state != ON_LOGGING) {
        *already_stop = true;
        pthread_mutex_unlock(&cmd_in_second.lock);
        return;
    }

    *already_stop = false;

    cmd_in_second.cur_state = STOPPED;

    pthread_mutex_unlock(&cmd_in_second.lock);
}
