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

#include "cmdlog.h"

#define CMDLOG_BUFFER_SIZE     (10 * 1024 * 1024)  /* 10 * MB */
#define CMDLOG_WRITE_SIZE      (4 * 1024)          /* 4 * KB */
#define CMDLOG_BUFFER_NUM 10  /* number of log files */
#define CMDLOG_FILE_DIR "command_log/%d_%d_%d.log"  /* arcus/scripts/command_log */
#define CMDLOG_FILENAME_LENGTH 128  /* filename plus path's length */
#define CMDLOG_EXPLICIT_STOP 1      /* stop by user request */
#define CMDLOG_OVERFLOW_STOP 2      /* stop by command log overflow */
#define CMDLOG_FLUSHERR_STOP 3      /* stop by flush operation error */

EXTENSION_LOGGER_DESCRIPTOR *mc_logger;

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
struct cmd_log_flush
{
    pthread_t tid;        /* flush thread id */
    pthread_attr_t attr;  /* flush thread mode */
    pthread_mutex_t lock; /* flush thread sleep and wakeup */
    pthread_cond_t cond;  /* flush thread sleep and wakeup */
    bool sleep;
};

/* command log global structure */
struct cmd_log_global {
    pthread_mutex_t lock;
    struct cmd_log_buffer buffer;
    struct cmd_log_flush flush;
    struct cmd_log_stats stats;
    bool on_logging; /* true or false : logging start condition */
};
struct cmd_log_global cmdlog;


/* command log function */
static int getnowdate(void)
{
    int ldate;
    time_t clock;
    struct tm *date;

    clock = time(0);
    date = localtime(&clock);
    ldate = date->tm_year * 100000;
    ldate += (date->tm_mon + 1) * 1000;
    ldate += date->tm_mday * 10;
    ldate += date->tm_wday;
    ldate += 190000000;
    ldate /= 10;
    return(ldate);
}

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

static void do_cmdlog_flush_sleep()
{
    struct timeval tv;
    struct timespec to;

    /* 50 mili second sleep */
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

static void do_cmdlog_flush_wakeup()
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
    cmdlog.stats.stop_cause = cause;
    cmdlog.stats.enddate = getnowdate();
    cmdlog.stats.endtime = getnowtime();
    cmdlog.on_logging = false;
}

static void *cmdlog_flush_thread()
{
    struct cmd_log_buffer *buffer = &cmdlog.buffer;
    char fname[CMDLOG_FILENAME_LENGTH];
    uint32_t cur_tail;
    uint32_t cur_last;
    int fd = -1;
    int err = 0;
    int writelen;
    int nwritten;

    while (1)
    {
        if (fd < 0) { /* open log file */
            sprintf(fname, CMDLOG_FILE_DIR, cmdlog.stats.bgndate, cmdlog.stats.bgntime,
                                            cmdlog.stats.file_count);
            if ((fd = open(fname, O_WRONLY | O_CREAT | O_APPEND, 0644)) < 0) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL, "Can't open %d.log file\n",
                               cmdlog.stats.file_count);
                err = -1; break;
            }
        }

        pthread_mutex_lock(&buffer->lock);
        cur_last = buffer->last;
        cur_tail = buffer->tail;
        pthread_mutex_unlock(&buffer->lock);

        if (buffer->head <= cur_tail) {
            assert(cur_last == 0);
            if (cmdlog.on_logging) {
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
                    mc_logger->log(EXTENSION_LOG_WARNING, NULL, "flush thread write error\n");
                    err = -1; break;
                }
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
                    mc_logger->log(EXTENSION_LOG_WARNING, NULL, "flush thread write error\n");
                    err = -1; break;
                    break;
                }
                pthread_mutex_lock(&buffer->lock);
                buffer->last = 0;
                buffer->head = 0;
                pthread_mutex_unlock(&buffer->lock);
            }
            close(fd); fd = -1;
            if (++cmdlog.stats.file_count >= CMDLOG_BUFFER_NUM) {
                break; /* do internal stop: overflow stop */
            }
        }
    }

    if (cmdlog.on_logging) { /* do internal stop */
        pthread_mutex_lock(&cmdlog.lock);
        do_cmdlog_stop(err == -1 ? CMDLOG_FLUSHERR_STOP : CMDLOG_OVERFLOW_STOP);
        pthread_mutex_unlock(&cmdlog.lock);
    }
    if (fd > 0) close(fd);
    return NULL;
}

void cmdlog_init(EXTENSION_LOGGER_DESCRIPTOR *logger)
{
    mc_logger = logger;
    cmdlog.on_logging = false;
    pthread_mutex_init(&cmdlog.lock, NULL);

    pthread_mutex_init(&cmdlog.buffer.lock, NULL);
    cmdlog.buffer.size = CMDLOG_BUFFER_SIZE;
    cmdlog.buffer.data = NULL;

    pthread_mutex_init(&cmdlog.flush.lock, NULL);
    pthread_cond_init(&cmdlog.flush.cond, NULL);
    cmdlog.flush.sleep = false;

    memset(&cmdlog.stats, 0, sizeof(struct cmd_log_stats));
}

void cmdlog_final()
{
    pthread_mutex_destroy(&cmdlog.buffer.lock);
    pthread_mutex_destroy(&cmdlog.lock);
    pthread_mutex_destroy(&cmdlog.flush.lock);
    pthread_cond_destroy(&cmdlog.flush.cond);

    if (cmdlog.buffer.data != NULL) {
        free(cmdlog.buffer.data);
    }
}

int cmdlog_start(bool *already_started)
{
    int ret = 0;

    pthread_mutex_lock(&cmdlog.lock);
    do {
        if (cmdlog.on_logging) {
            *already_started = true;
            break;
        }
        /* prepare command logging buffer */
        if (cmdlog.buffer.data == NULL) {
            if ((cmdlog.buffer.data = malloc(CMDLOG_BUFFER_SIZE)) == NULL) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Can't allocate buffer\n");
                ret = -1; break;
            }
        }
        cmdlog.buffer.head = 0;
        cmdlog.buffer.tail = 0;
        cmdlog.buffer.last = 0;

        /* prepare comand logging stats */
        memset(&cmdlog.stats, 0, sizeof(struct cmd_log_stats));
        cmdlog.stats.bgndate = getnowdate();
        cmdlog.stats.bgntime = getnowtime();

        /* enable command logging */
        cmdlog.on_logging = true;

        /* start the flush thread to write command log to disk */
        if (pthread_attr_init(&cmdlog.flush.attr) != 0 ||
            pthread_attr_setdetachstate(&cmdlog.flush.attr, PTHREAD_CREATE_DETACHED) != 0 ||
            (ret = pthread_create(&cmdlog.flush.tid, &cmdlog.flush.attr, cmdlog_flush_thread, NULL)) != 0)
        {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                           "Can't create thread: %s\n", strerror(ret));
            cmdlog.on_logging = false; // disable it */
            ret = -1; break;
        }
        *already_started = false;
    } while(0);
    pthread_mutex_unlock(&cmdlog.lock);

    return ret;
}

int cmdlog_stop(bool *already_stopped)
{
    int ret = 0;

    pthread_mutex_lock(&cmdlog.lock);
    if (cmdlog.on_logging == true) {
        /* stop bu user request */
        do_cmdlog_stop(CMDLOG_EXPLICIT_STOP);
        *already_stopped = false;
        ret = 0;
    } else {
        *already_stopped = true;
        ret = 0;
    }
    pthread_mutex_unlock(&cmdlog.lock);

    return ret;
}

struct cmd_log_stats *cmdlog_stats()
{
    struct cmd_log_stats *stats = &cmdlog.stats;

    if (cmdlog.on_logging) {
        stats->enddate = getnowdate();
        stats->endtime = getnowtime();
    }

    return stats;
}

bool cmdlog_write(char client_ip[], char* command)
{
    struct tm *ptm;
    struct timeval val;
    struct cmd_log_buffer *buffer = &cmdlog.buffer;
    char inputstr[CMDLOG_INPUT_SIZE];
    int inputlen;

    if (! cmdlog.on_logging) {
        return false;
    }

    gettimeofday(&val, NULL);
    ptm = localtime(&val.tv_sec);

    snprintf(inputstr, CMDLOG_INPUT_SIZE, "%02d:%02d:%02d.%06ld %s %s\n",
             ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (long)val.tv_usec, client_ip, command);
    inputlen = strlen(inputstr);

    pthread_mutex_lock(&buffer->lock);
    if (buffer->head <= buffer->tail && inputlen >= (buffer->size - buffer->tail)) {
        buffer->last = buffer->tail;
        buffer->tail = 0;
    }
    cmdlog.stats.entered_commands += 1;
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

    return true;
}
