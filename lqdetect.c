#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>

#include "lqdetect.h"

static EXTENSION_LOGGER_DESCRIPTOR *mc_logger;
static char *command_str[LONGQ_COMMAND_NUM] = {"sop get", "lop insert", "lop delete", "lop get",
                            "bop delete", "bop get", "bop count", "bop gbp"};

/* lqdetect global structure */
struct lq_detect_global {
    pthread_mutex_t lock;
    struct lq_detect_argument arg[LONGQ_COMMAND_NUM][LONGQ_SAVE_CNT];
    struct lq_detect_buffer buffer[LONGQ_COMMAND_NUM];
    struct lq_detect_stats stats;
    int overflow_cnt;
    bool on_detecting;
};
struct lq_detect_global lqdetect;

/* lqdetect function */
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

static void do_lqdetect_stop(int cause)
{
    /* detect long query lock has already been held */
    lqdetect.stats.stop_cause = cause;
    lqdetect.stats.enddate = getnowdate();
    lqdetect.stats.endtime = getnowtime();
    lqdetect.on_detecting = false;
}

int lqdetect_init()
{
    int ii, jj;
    pthread_mutex_init(&lqdetect.lock, NULL);
    lqdetect.on_detecting = false;

    memset(lqdetect.buffer, 0, LONGQ_COMMAND_NUM * sizeof(struct lq_detect_buffer));
    for(ii = 0; ii < LONGQ_COMMAND_NUM; ii++) {
        lqdetect.buffer[ii].data = malloc(LONGQ_SAVE_CNT * LONGQ_INPUT_SIZE);
        if (lqdetect.buffer[ii].data == NULL) {
            for(jj = 0; jj < ii; jj++) {
                free(lqdetect.buffer[jj].data);
            }
            return -1;
        }
        memset(lqdetect.arg[ii], 0, LONGQ_SAVE_CNT * sizeof(struct lq_detect_argument));
    }

    memset(&lqdetect.stats, 0, sizeof(struct lq_detect_stats));
    lqdetect_refcount = 0;
    return 0;
}

void lqdetect_final()
{
    int ii;
    for(ii = 0; ii < LONGQ_COMMAND_NUM; ii++) {
        free(lqdetect.buffer[ii].data);
    }
}

int lqdetect_start(uint32_t lqdetect_standard, bool *already_started)
{
    int ii, ret = 0;

    pthread_mutex_lock(&lqdetect.lock);
    do {
        if (lqdetect.on_detecting) {
            *already_started = true;
            break;
        }

        /* if lqdetect showing, start is fail */
        if (lqdetect_refcount != 0) {
            ret = -1;
            break;
        }

        /* prepare detect long query buffer, argument and counts*/
        for(ii = 0; ii < LONGQ_COMMAND_NUM; ii++) {
            memset(lqdetect.buffer[ii].data, 0, LONGQ_SAVE_CNT * LONGQ_INPUT_SIZE);
            memset(lqdetect.buffer[ii].keypos, 0, LONGQ_SAVE_CNT * sizeof(uint32_t));
            memset(lqdetect.buffer[ii].keylen, 0, LONGQ_SAVE_CNT * sizeof(uint32_t));
            memset(lqdetect.arg[ii], 0, LONGQ_SAVE_CNT * sizeof(struct lq_detect_argument));
            lqdetect.buffer[ii].ntotal = 0;
            lqdetect.buffer[ii].nsaved = 0;
            lqdetect.buffer[ii].offset = 0;
        }

        /* prepare detect long query stats */
        memset(&lqdetect.stats, 0, sizeof(struct lq_detect_stats));
        lqdetect.stats.bgndate = getnowdate();
        lqdetect.stats.bgntime = getnowtime();
        lqdetect.stats.stop_cause = LONGQ_RUNNING;
        lqdetect.stats.standard = lqdetect_standard;

        lqdetect.overflow_cnt = 0;
        lqdetect.on_detecting = true;
        ret = 0;
    } while(0);
    pthread_mutex_unlock(&lqdetect.lock);

    return ret;
}

void lqdetect_stop(bool *already_stopped)
{
    pthread_mutex_lock(&lqdetect.lock);
    if (lqdetect.on_detecting == true) {
        do_lqdetect_stop(LONGQ_EXPLICIT_STOP);
    } else {
        *already_stopped = true;
    }
    pthread_mutex_unlock(&lqdetect.lock);
}

struct lq_detect_stats *lqdetect_stats()
{
    int ii;
    struct lq_detect_stats *stats = &lqdetect.stats;

    if (lqdetect.on_detecting) {
        stats->enddate = getnowdate();
        stats->endtime = getnowtime();
    }

    stats->total_lqcmds = 0;
    for (ii = 0; ii < LONGQ_COMMAND_NUM; ii++) {
        stats->total_lqcmds += lqdetect.buffer[ii].ntotal;
    }
    return stats;
}

struct lq_detect_buffer *lqdetect_buffer(int cmd)
{
    struct lq_detect_buffer *buffer = &lqdetect.buffer[cmd];
    return buffer;
}

void lqdetect_buffer_hold()
{
    pthread_mutex_lock(&lqdetect.lock);
    lqdetect_refcount++;
    pthread_mutex_unlock(&lqdetect.lock);
}

void lqdetect_buffer_release()
{
    pthread_mutex_lock(&lqdetect.lock);
    if (lqdetect_refcount != 0) {
        lqdetect_refcount--;
    }
    pthread_mutex_unlock(&lqdetect.lock);
}

static bool lqdetect_dupcheck(char *key, enum lqdetect_command cmd, struct lq_detect_argument *arg)
{
    int ii;
    int count = lqdetect.buffer[cmd].nsaved;

    switch (cmd) {
    case LQCMD_LOP_INSERT:
    case LQCMD_LOP_DELETE:
    case LQCMD_LOP_GET:
    case LQCMD_BOP_GBP:
        for(ii = 0; ii < count; ii++) {
            if (strcmp(lqdetect.arg[cmd][ii].range, arg->range) == 0) {
                return true;
            }
        }
        break;
    case LQCMD_BOP_GET:
    case LQCMD_BOP_COUNT:
    case LQCMD_BOP_DELETE:
        for(ii = 0; ii < count; ii++) {
            if ((strcmp(lqdetect.arg[cmd][ii].range, arg->range) == 0) &&
                (lqdetect.arg[cmd][ii].offset == arg->offset) &&
                (lqdetect.arg[cmd][ii].count == arg->count)) {
                return true;
            }
        }
        break;
    case LQCMD_SOP_GET:
        for(ii = 0; ii < count; ii++) {
            if (arg->count == 0) {
                uint32_t offset = lqdetect.buffer[cmd].keypos[ii];
                uint32_t cmplen = lqdetect.buffer[cmd].keylen[ii];
                if (cmplen != strlen(key)) {
                    return true;
                } else {
                    if (strncmp(lqdetect.buffer[cmd].data+offset, key, cmplen) == 0) {
                        return true;
                    }
                }
            } else {
                if (strcmp(lqdetect.arg[cmd][ii].range, arg->range) == 0) {
                    return true;
                }
            }
        }
        break;
    }
    lqdetect.arg[cmd][ii] = *arg;
    return false;
}

static bool lqdetect_write(char client_ip[], char *key, enum lqdetect_command cmd)
{
    struct   tm *ptm;
    struct   timeval val;
    struct   lq_detect_buffer *buffer = &lqdetect.buffer[cmd];
    uint32_t offset = buffer->offset;
    int      count = buffer->nsaved;
    struct   lq_detect_argument *arg = &lqdetect.arg[cmd][count];
    char     *bufptr = buffer->data + offset;

    gettimeofday(&val, NULL);
    ptm = localtime(&val.tv_sec);

    snprintf(bufptr, LONGQ_INPUT_SIZE, "%02d:%02d:%02d.%06ld %s <%d> %s ",
        ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (long)val.tv_usec, client_ip,
        arg->overhead, command_str[cmd]);

    buffer->keypos[count] = offset + strlen(bufptr);
    buffer->keylen[count] = strlen(key);
    bufptr += strlen(bufptr);

    switch (cmd) {
    case LQCMD_LOP_INSERT:
    case LQCMD_SOP_GET:
        snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s\n", key, arg->range);
        break;
    case LQCMD_LOP_DELETE:
        if (arg->delete_or_drop == 2) {
            snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %s\n", key, arg->range, "drop");
        } else {
            snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s\n", key, arg->range);
        }
    case LQCMD_LOP_GET:
        if (arg->delete_or_drop != 0) {
            snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %s\n", key, arg->range,
                       (arg->delete_or_drop == 2 ? "drop" : "delete"));
        } else {
            snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s\n", key, arg->range);
        }
        break;
    case LQCMD_BOP_GBP:
        snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %s\n", key, arg->range, (arg->asc_or_desc == 2 ? "desc" : "asc"));
        break;
    case LQCMD_BOP_GET:
        if (arg->delete_or_drop != 0) {
            snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %d %d %s\n", key, arg->range,
                        arg->offset, arg->count, (arg->delete_or_drop == 2 ? "drop" : "delete"));
        } else {
            snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %d %d\n", key, arg->range,
                        arg->offset, arg->count);
        }
        break;
    case LQCMD_BOP_COUNT:
        snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s\n", key, arg->range);
        break;
    case LQCMD_BOP_DELETE:
        if (arg->delete_or_drop == 2) {
            snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %d %s\n", key, arg->range, arg->count, "drop");
        } else {
            snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %d\n", key, arg->range, arg->count);
        }
        break;
    }

    buffer->offset = buffer->keypos[count] + strlen(bufptr);
    buffer->nsaved++;

    /* buffer overflow check */
    if (buffer->offset + LONGQ_INPUT_SIZE > LONGQ_SAVE_CNT * LONGQ_INPUT_SIZE) {
        return false;
    }

    return true;
}

bool lqdetect_discriminant(uint32_t count)
{
    if (count >= lqdetect.stats.standard) {
        return true;
    } else {
        return false;
    }
}

bool lqdetect_save_cmd(char client_ip[], char* key,
                                enum lqdetect_command cmd, struct lq_detect_argument *arg)
{
    bool ret = true;

    if (cmd < LQCMD_SOP_GET || cmd > LQCMD_BOP_GBP) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                       "lqdetect error: entered non target command");
        return true;
    }

    pthread_mutex_lock(&lqdetect.lock);
    do {
        if (! lqdetect.on_detecting) {
            ret = false;
            break;
        }

        lqdetect.buffer[cmd].ntotal++;
        if (lqdetect.buffer[cmd].nsaved >= LONGQ_SAVE_CNT) break;

        if (! lqdetect_dupcheck(key, cmd, arg)) {
            if (! lqdetect_write(client_ip, key, cmd)) {
                do_lqdetect_stop(LONGQ_BUFFER_OVERFLOW_STOP);
                ret = false;
                break;
            }
        } else {
            break;
        }

        /* internal stop */
        if (lqdetect.buffer[cmd].nsaved >= LONGQ_SAVE_CNT) {
            lqdetect.overflow_cnt++;
            if (lqdetect.overflow_cnt >= LONGQ_COMMAND_NUM) {
                do_lqdetect_stop(LONGQ_COUNT_OVERFLOW_STOP);
                ret = false;
            }
        }
    } while(0);
    pthread_mutex_unlock(&lqdetect.lock);

    return ret;
}
