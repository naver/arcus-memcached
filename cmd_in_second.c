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

#define LOG_PER_TIMER 500
#define LOG_LENGTH 400
#define IP_LENGTH 16
#define KEY_LENGTH 256

typedef enum cmd_in_second_state{
    NOT_STARTED,
    ON_LOGGING,
    ON_FLUSHING
}state;

typedef struct cmd_in_second_log{
    char key[KEY_LENGTH];
    char client_ip[IP_LENGTH];
    int32_t timer_idx;
}logtype;

typedef struct cmd_in_second_buffer {
    logtype *ring;
    int32_t front;
    int32_t rear;
    int32_t capacity;
}buffertype;

typedef struct cmd_in_second_timer{
    struct timeval* times;
    int32_t size;
    int32_t counter;
}timertype;

struct cmd_in_second {
    char cmd[10];
    char collection_name[4];
    struct cmd_in_second_buffer buffer;
    int32_t bulk_limit;
    timertype timer;
    state cur_state;
};

static EXTENSION_LOGGER_DESCRIPTOR *mc_logger;
static struct cmd_in_second this;

static bool is_bulk_cmd()
{
    const logtype* front = &this.buffer.ring[this.buffer.front];
    const logtype* rear = &this.buffer.ring[this.buffer.rear];

    struct timeval front_time = this.timer.times[front->timer_idx];
    struct timeval rear_time = this.timer.times[rear->timer_idx];

    return rear_time.tv_sec - front_time.tv_sec <= 1;
}

static void get_whole_cmd(char* whole_cmd)
{
    if (strlen(this.collection_name)) {
        sprintf(whole_cmd, "%s %s", this.collection_name, this.cmd);
        return;
    }
    sprintf(whole_cmd, "%s", this.cmd);
}

static bool buffer_empty()
{
    return this.buffer.front == this.buffer.rear;
}

static void* buffer_flush_thread()
{
    int32_t fd = open("cmd_in_second.log", O_CREAT | O_WRONLY | O_TRUNC,  0644);

    if (fd < 0) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Can't open cmd_in_second log file: %s\n", "cmd_in_second.log");
        return NULL;
    }

    char whole_cmd[20] = "";
    get_whole_cmd(whole_cmd);

    buffertype* buffer = &this.buffer;
    timertype* timer = &this.timer;

    int32_t timer_idx = -1;

    char* log_str = (char*)malloc(LOG_LENGTH * this.bulk_limit * sizeof(char));

    if (log_str == NULL) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL, "Can't allocate memory");
        return NULL;
    }

    int32_t expected_write_length = 0;

    while (!buffer_empty()) {

        logtype front = buffer->ring[buffer->front++];

        char time_str[50] = "";

        if (front.timer_idx != timer_idx) {

            const struct timeval* front_time = &timer->times[front.timer_idx];
            const struct tm *lt = localtime((time_t*)&front_time->tv_sec);

            sprintf(time_str, "%04d-%02d-%02d %02d:%02d:%02d.%06d\n", lt ->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday, lt->tm_hour, lt->tm_min, lt->tm_sec, (int32_t)front_time->tv_usec);
            timer_idx = front.timer_idx;

            expected_write_length += 27;
        }
        char log[LOG_LENGTH] = "";
        sprintf(log, "%s%s %s %s\n", time_str, whole_cmd, front.key, front.client_ip);
        strncat(log_str, log, LOG_LENGTH);

        expected_write_length += LOG_LENGTH;
    }

    if (write(fd, log_str, expected_write_length) != expected_write_length) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL, "write length is difference to expectation.");
    }

    close(fd);

    free(log_str);

    free(this.timer.times);
    this.timer.times = NULL;

    free(this.buffer.ring);
    this.buffer.ring = NULL;

    this.cur_state = NOT_STARTED;

    return NULL;
}

static int32_t buffer_flush()
{

    this.cur_state = ON_FLUSHING;

    int32_t ret = 0;
    pthread_t tid;
    pthread_attr_t attr;

    if (pthread_attr_init(&attr) != 0 ||
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
        (ret = pthread_create(&tid, &attr, buffer_flush_thread, NULL)) != 0)
    {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                       "Can't create buffer flush thread: %s\n", strerror(ret));
    }

    return ret;
}

static void buffer_add(const logtype* log)
{

    struct cmd_in_second_buffer* buffer = &this.buffer;

    const bool buffer_full = (buffer->rear+1) % buffer->capacity == buffer->front;

    if (buffer_full) {
        if (is_bulk_cmd()) {
            buffer_flush();
            return;
        }
        buffer->front = (buffer->front+1) % buffer->capacity;
    }

    buffer->ring[buffer->rear] = *log;
    buffer->rear = (buffer->rear+1) % buffer->capacity;
}

static bool is_cmd_to_log(const char* collection_name, const char* cmd)
{
    return strcmp(this.collection_name, collection_name) == 0 && strcmp(this.cmd, cmd) == 0;
}

bool cmd_in_second_write(const char* collection_name, const char* cmd, const char* key, const char* client_ip)
{
    if (this.cur_state != ON_LOGGING || !is_cmd_to_log(collection_name, cmd)) {
        return false;
    }

    timertype *timer = &this.timer;

    logtype log = {"", "", 0};
    snprintf(log.client_ip, IP_LENGTH, "%s", client_ip);
    snprintf(log.key, KEY_LENGTH, "%s", key);

    if (timer->counter == 0) {
        timer->size++;
        gettimeofday(&timer->times[timer->size-1], NULL);
    }

    log.timer_idx = timer->size-1;

    buffer_add(&log);
    timer->counter = (timer->counter+1) % LOG_PER_TIMER;

    return true;
}

int32_t cmd_in_second_start(const char* collection_name, const char* cmd, const int32_t bulk_limit)
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

    this.timer.size = 0;
    this.timer.counter = 0;
    this.timer.times = (struct timeval*)malloc(bulk_limit * sizeof(struct timeval));

    if (this.timer.times == NULL) {
        free(this.buffer.ring);
        return CMD_IN_SECOND_NO_MEM;
    }

    sprintf(this.collection_name, "%s", collection_name);
    sprintf(this.cmd, "%s", cmd);

    this.cur_state = ON_LOGGING;

    return CMD_IN_SECOND_START;
}
