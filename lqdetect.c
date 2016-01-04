#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>

#include "lqdetect.h"

#define LONGQ_INPUT_SIZE    500    /* the size of input(time, ip, command, argument) */

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
            memset(lqdetect.buffer[ii].keyoffset, 0, LONGQ_SAVE_CNT * sizeof(uint32_t));
            memset(lqdetect.buffer[ii].lenkey, 0, LONGQ_SAVE_CNT * sizeof(uint32_t));
            memset(lqdetect.arg[ii], 0, LONGQ_SAVE_CNT * sizeof(struct lq_detect_argument));
            lqdetect.buffer[ii].detect_count = 0;
            lqdetect.buffer[ii].save_count = 0;
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

    stats->total_ndetdcmd = 0;
    for (ii = 0; ii < LONGQ_COMMAND_NUM; ii++) {
        stats->total_ndetdcmd += lqdetect.buffer[ii].detect_count;
    }
    return stats;
}

struct lq_detect_buffer *lqdetect_buffer(int cmd)
{
    struct lq_detect_buffer *buffer = &lqdetect.buffer[cmd];
    return buffer;
}

void lqdetect_refcount_lock(bool locking)
{
    pthread_mutex_lock(&lqdetect.lock);
    lqdetect_refcount = locking ? 1 : 0;
    pthread_mutex_unlock(&lqdetect.lock);
}

static bool lqdetect_dupcheck(char *key, enum lqdetect_command cmd, struct lq_detect_argument *arg)
{
    int ii;
    int count = lqdetect.buffer[cmd].save_count;

    switch (cmd) {
    case lop_insert:
    case lop_delete:
    case lop_get:
    case bop_gbp:
        for(ii = 0; ii < count; ii++) {
            if (strcmp(lqdetect.arg[cmd][ii].range, arg->range) == 0) {
                return false;
            }
        }
        break;
    case bop_get:
    case bop_count:
    case bop_delete:
        for(ii = 0; ii < count; ii++) {
            if ((strcmp(lqdetect.arg[cmd][ii].range, arg->range) == 0) &&
                (lqdetect.arg[cmd][ii].offset == arg->offset) &&
                (lqdetect.arg[cmd][ii].input_count == arg->input_count)) {
                return false;
            }
        }
        break;
    case sop_get:
        for(ii = 0; ii < count; ii++) {
            if (arg->input_count == 0) {
                uint32_t offset = lqdetect.buffer[cmd].keyoffset[ii];
                uint32_t cmplen = lqdetect.buffer[cmd].lenkey[ii];
                if (strncmp(lqdetect.buffer[cmd].data+offset, key, cmplen) == 0) {
                    return false;
                }
            } else {
                if (strcmp(lqdetect.arg[cmd][ii].range, arg->range) == 0) {
                    return false;
                }
            }
        }
        break;
    default:
        return false;
    }
    lqdetect.arg[cmd][ii] = *arg;
    return true;
}

static void lqdetect_write(char client_ip[], char *key, enum lqdetect_command cmd)
{
    char *command_str[8] = {"sop get", "lop insert", "lop delete", "lop get",
                            "bop delete", "bop get", "bop count", "bop gbp"};

    struct   tm *ptm;
    struct   timeval val;
    struct   lq_detect_buffer *buffer = &lqdetect.buffer[cmd];
    uint32_t offset = buffer->offset;
    int      count = buffer->save_count;
    struct   lq_detect_argument *arg = &lqdetect.arg[cmd][count];
    char     *bufptr = buffer->data + offset;

    gettimeofday(&val, NULL);
    ptm = localtime(&val.tv_sec);

    snprintf(bufptr, LONGQ_INPUT_SIZE, "%02d:%02d:%02d.%06ld %s <%d> %s ",
        ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (long)val.tv_usec, client_ip,
        arg->access_count, command_str[cmd]);

    buffer->keyoffset[count] = offset + strlen(bufptr);
    buffer->lenkey[count] = strlen(key);
    bufptr += strlen(bufptr);

    switch (cmd) {
    case lop_insert:
    case sop_get:
        snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s\n", key, arg->range);
        break;
    case lop_delete:
    case lop_get:
        snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %s\n", key, arg->range, (arg->delete_or_drop != 0 ?
                   (arg->delete_or_drop == 2 ? "drop" : "delete") : "notdel"));
        break;
    case bop_gbp:
        snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %s\n", key, arg->range, (arg->asc_or_desc == 2 ? "desc" : "asc"));
        break;
    case bop_get:
        snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %s %d %d %s\n", key, arg->range, (arg->efilter ? "efilter" : "nofilter"),
                   arg->offset, arg->input_count, (arg->delete_or_drop != 0 ?
                   (arg->delete_or_drop == 2 ? "drop" : "delete") : "notdel"));
        break;
    case bop_count:
        snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %s\n", key, arg->range, (arg->efilter ? "efilter" : "nofilter"));
        break;
    case bop_delete:
        snprintf(bufptr, LONGQ_INPUT_SIZE, "%s %s %s %d %s\n", key, arg->range, (arg->efilter ? "efilter" : "nofilter"),
                   arg->input_count, (arg->delete_or_drop != 0 ?
                   (arg->delete_or_drop == 2 ? "drop" : "delete") : "notdel"));
        break;
    default:
        break;
    }

    /* cmdstring overflow check */
    if (strlen(bufptr) >= LONGQ_INPUT_SIZE) {
        do_lqdetect_stop(LONGQ_BUFFER_OVERFLOW_STOP);
        return;
    }

    buffer->offset = buffer->keyoffset[count] + strlen(bufptr);
    buffer->save_count++;
}

bool lqdetect_discriminant(uint32_t count)
{
    if (count >= lqdetect.stats.standard) {
        return true;
    } else {
        return false;
    }
}

bool lqdetect_process_targetcmd(char client_ip[], char* key,
                                enum lqdetect_command cmd, struct lq_detect_argument *arg)
{
    bool ret = true;

    pthread_mutex_lock(&lqdetect.lock);
    do {
        if (! lqdetect.on_detecting) {
            ret = false;
            break;
        }

        lqdetect.buffer[cmd].detect_count++;
        if (lqdetect.buffer[cmd].save_count >= LONGQ_SAVE_CNT) break;

        if (lqdetect_dupcheck(key, cmd, arg)) {
            lqdetect_write(client_ip, key, cmd);
        } else {
            break;
        }

        /* internal stop */
        if (lqdetect.buffer[cmd].save_count >= LONGQ_SAVE_CNT) {
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
