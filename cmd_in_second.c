#include "cmd_in_second.h"
#include "include/memcached/extension.h"
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/uio.h>
#include <pthread.h>
#include <stdlib.h>
#include <fcntl.h>

#define LOG_LENGTH 400
#define IP_LENGTH 16
#define KEY_LENGTH 256

typedef enum cmd_in_second_state {
    NOT_STARTED,
    ON_LOGGING,
    ON_FLUSHING
} state;

typedef struct cmd_in_second_log {
    char key[KEY_LENGTH];
    char client_ip[IP_LENGTH];
} logtype;

typedef struct cmd_in_second_buffer {
    logtype* ring;
    int32_t front;
    int32_t rear;
    int32_t capacity;
} buffertype;

typedef struct cmd_in_second_timer {
    struct timeval* ring;
    int32_t front;
    int32_t rear;
    int32_t capacity;
    int32_t circular_counter;
} timertype;

struct cmd_in_second {
    char cmd[20];
    char collection_name[10];
    struct cmd_in_second_buffer buffer;
    timertype timer;
    int32_t bulk_limit;
    int32_t log_per_timer;
    state cur_state;
};

static struct cmd_in_second this;

static bool is_bulk_cmd()
{
    const bool timer_empty = this.timer.front == this.timer.rear;

    if (timer_empty) {
        return false;
    }

    const struct timeval* front_time = &this.timer.ring[this.timer.front];
    const struct timeval* rear_time = &this.timer.ring[this.timer.rear];

    return rear_time->tv_sec - front_time->tv_sec <= 1;
}

static void get_whole_cmd(char* whole_cmd)
{
    if (strlen(this.collection_name)) {
        snprintf(whole_cmd, 20, "%s %s", this.collection_name, this.cmd);
        return;
    }
    snprintf(whole_cmd, 15, "%s", this.cmd);
}

static bool buffer_empty()
{
    return this.buffer.front == this.buffer.rear;
}

static void* buffer_flush_thread()
{
    const int32_t fd = open("cmd_in_second.log", O_CREAT | O_WRONLY | O_TRUNC,  0644);

    if (fd < 0) {
        perror("Can't open cmd_in_second log file: cmd_in_second.log");
        return NULL;
    }

    buffertype* const buffer = &this.buffer;
    timertype* const timer = &this.timer;

    char* log_str = (char*)malloc(LOG_LENGTH * this.bulk_limit * sizeof(char));

    if (log_str == NULL) {
        perror("Can't allocate memory");
        return NULL;
    }

    char whole_cmd[20] = "";
    get_whole_cmd(whole_cmd);

    const size_t whole_cmd_len = strlen(whole_cmd);
    const int32_t whitespaces = 3;

    size_t expected_write_length = 0;
    int32_t circular_log_counter = 0;

    while (!buffer_empty()) {

        const logtype front = buffer->ring[buffer->front];
        buffer->front = (buffer->front+1) % buffer->capacity;

        char time_str[50] = "";

        if (circular_log_counter == 0) {

            const struct timeval* front_time = &timer->ring[timer->front];
            const struct tm* lt = localtime((time_t*)&front_time->tv_sec);

            timer->front = (timer->front+1) % timer->capacity;

            if (lt == NULL) {
                perror("localtime failed");
                continue;
            }

            sprintf(time_str, "%04d-%02d-%02d %02d:%02d:%02d.%06d\n", lt ->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday,
                                                                      lt->tm_hour, lt->tm_min, lt->tm_sec, (int32_t)front_time->tv_usec);
            expected_write_length += 27;
        }

        char log[LOG_LENGTH] = "";
        snprintf(log, LOG_LENGTH, "%s%s %s %s\n", time_str, whole_cmd, front.key, front.client_ip);
        strncat(log_str, log, LOG_LENGTH);

        expected_write_length += whole_cmd_len + strlen(front.key) + strlen(front.client_ip) + whitespaces;
        circular_log_counter = (circular_log_counter+1) % this.log_per_timer;
    }

    if (write(fd, log_str, expected_write_length) != expected_write_length) {
        perror("write length is difference to expectation.");
    }

    close(fd);

    free(log_str);

    free(this.timer.ring);
    this.timer.ring = NULL;

    free(this.buffer.ring);
    this.buffer.ring = NULL;

    this.cur_state = NOT_STARTED;

    return NULL;
}

static int32_t buffer_flush()
{
    this.cur_state = ON_FLUSHING;

    pthread_t tid;
    pthread_attr_t attr;

    int32_t ret = 0;

    if (pthread_attr_init(&attr) != 0 ||
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
        (ret = pthread_create(&tid, &attr, buffer_flush_thread, NULL)) != 0)
    {
        perror("Can't create buffer flush thread");
        return ret;
    }

    return ret;
}

static void buffer_add(const logtype* log)
{

    struct cmd_in_second_buffer* const buffer = &this.buffer;

    buffer->ring[buffer->rear] = *log;
    buffer->rear = (buffer->rear+1) % buffer->capacity;

    const bool buffer_full = (buffer->rear+1) % buffer->capacity == buffer->front;

    if (buffer_full) {
        if (is_bulk_cmd()) {
            buffer_flush();
            return;
        }
        buffer->front = (buffer->front+1) % buffer->capacity;
    }

}

static void timer_add()
{
    struct cmd_in_second_timer* const timer = &this.timer;

    const bool timer_full = (timer->rear+1) % timer->capacity == timer->front;

    if (timer_full) {
        timer->front = (timer->front+1) % timer->capacity;
    }

    if (gettimeofday(&timer->ring[timer->rear], NULL) == -1) {
        perror("gettimeofday failed");
        return;
    };

    timer->rear = (timer->rear+1) % timer->capacity;
}

static bool is_cmd_to_log(const char* collection_name, const char* cmd)
{
    return strcmp(this.collection_name, collection_name) == 0 &&
           strcmp(this.cmd, cmd) == 0;
}

bool cmd_in_second_write(const char* collection_name, const char* cmd,
                         const char* key, const char* client_ip)
{
    if (this.cur_state != ON_LOGGING || !is_cmd_to_log(collection_name, cmd)) {
        return false;
    }

    logtype log = {"", ""};
    snprintf(log.client_ip, IP_LENGTH, "%s", client_ip);
    snprintf(log.key, KEY_LENGTH, "%s", key);

    if (this.timer.circular_counter == 0) {
        timer_add();
    }

    buffer_add(&log);
    this.timer.circular_counter = (this.timer.circular_counter+1) % this.log_per_timer;

    return true;
}

int32_t cmd_in_second_start(const char* collection_name, const char* cmd,
                            const int32_t bulk_limit)
{

    if (this.cur_state != NOT_STARTED) {
        return CMD_IN_SECOND_STARTED_ALREADY;
    }

    this.bulk_limit = bulk_limit;

    this.buffer.capacity = bulk_limit+1;
    this.buffer.front = 0;
    this.buffer.rear = 0;
    this.buffer.ring = (logtype*)malloc(this.buffer.capacity * sizeof(logtype));

    if (this.buffer.ring == NULL) {
        return CMD_IN_SECOND_NO_MEM;
    }

    this.log_per_timer = bulk_limit / 10 + (bulk_limit % 10 != 0);
    this.timer.capacity = this.log_per_timer + 1;
    this.timer.front = 0;
    this.timer.rear = 0;
    this.timer.circular_counter = 0;

    this.timer.ring = (struct timeval*)malloc(this.timer.capacity * sizeof(struct timeval));

    if (this.timer.ring == NULL) {
        free(this.buffer.ring);
        return CMD_IN_SECOND_NO_MEM;
    }

    snprintf(this.collection_name, 5, "%s", collection_name);
    snprintf(this.cmd, 15, "%s", cmd);

    this.cur_state = ON_LOGGING;

    return CMD_IN_SECOND_START;
}
