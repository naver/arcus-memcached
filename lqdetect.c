#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>

#include "lqdetect.h"

#define LONGQ_SAVE_CNT     20     /* save key count */
#define LONGQ_INPUT_SIZE   500    /* the size of input(time, ip, command, argument) */

static EXTENSION_LOGGER_DESCRIPTOR *mc_logger;
static char *command_str[LONGQ_COMMAND_NUM] = {"sop get","mop delete", "mop get",
                                               "lop insert", "lop delete", "lop get",
                                               "bop delete", "bop get", "bop count", "bop gbp"};
/* lqdetect buffer structure */
struct lq_detect_buffer {
    char *data;
    uint32_t offset;
    uint32_t ntotal;
    uint32_t nsaved;
    uint32_t keypos[LONGQ_SAVE_CNT];
    uint32_t keylen[LONGQ_SAVE_CNT];
};

/* lqdetect global structure */
struct lq_detect_global {
    pthread_mutex_t lock;
    struct lq_detect_argument arg[LONGQ_COMMAND_NUM][LONGQ_SAVE_CNT];
    struct lq_detect_buffer buffer[LONGQ_COMMAND_NUM];
    struct lq_detect_stats stats;
    int overflow_cnt;
    bool on_detecting;
    uint16_t refcount; /* lqdetect show reference count */
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
    lqdetect.refcount = 0;
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
        if (lqdetect.refcount != 0) {
            ret = -1;
            break;
        }

        /* prepare detect long query buffer, argument and counts*/
        for(ii = 0; ii < LONGQ_COMMAND_NUM; ii++) {
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

void lqdetect_get_stats(char* str)
{
    int ii;
    char *stop_cause_str[3] = {"stopped by explicit request",     // LONGQ_EXPLICIT_STOP
                               "stopped by long query overflow", // LONGQ_OVERFLOW_STOP
                               "running"}; // LONGQ_RUNNING
    struct lq_detect_stats stats = lqdetect.stats;

    if (lqdetect.on_detecting) {
        stats.enddate = 0;
        stats.endtime = 0;
    }

    stats.total_lqcmds = 0;
    for (ii=0; ii < LONGQ_COMMAND_NUM; ii++) {
        stats.total_lqcmds += lqdetect.buffer[ii].ntotal;
    }

    snprintf(str, LONGQ_STAT_STRLEN,
            "\t" "Long query detection stats : %s" "\n"
            "\t" "The last running time : %d_%d ~ %d_%d" "\n"
            "\t" "The number of total long query commands : %d" "\n"
            "\t" "The detection standard : %u" "\n",
            (stats.stop_cause >= 0 && stats.stop_cause <= 2 ?
             stop_cause_str[stats.stop_cause] : "unknown"),
            stats.bgndate, stats.bgntime, stats.enddate, stats.endtime,
            stats.total_lqcmds, stats.standard);
}

char *lqdetect_buffer_get(int cmd, uint32_t *length, uint32_t *cmdcnt)
{
    pthread_mutex_lock(&lqdetect.lock);

    char *data = lqdetect.buffer[cmd].data;
    *length = lqdetect.buffer[cmd].offset;
    *cmdcnt = lqdetect.buffer[cmd].ntotal;
    lqdetect.refcount++;

    pthread_mutex_unlock(&lqdetect.lock);
    return data;
}

void lqdetect_buffer_release(int bufcnt)
{
    pthread_mutex_lock(&lqdetect.lock);
    lqdetect.refcount = (lqdetect.refcount-bufcnt) < 0 ?
                              0 : lqdetect.refcount-bufcnt;
    pthread_mutex_unlock(&lqdetect.lock);
}

static bool lqdetect_dupcheck(char *key, enum lq_detect_command cmd, struct lq_detect_argument *arg)
{
    int ii = 0;
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
    case LQCMD_MOP_DELETE:
    case LQCMD_MOP_GET:
    case LQCMD_SOP_GET:
        for(ii = 0; ii < count; ii++) {
            if (arg->count == 0) {
                uint32_t offset = lqdetect.buffer[cmd].keypos[ii];
                uint32_t cmplen = lqdetect.buffer[cmd].keylen[ii];
                if (cmplen == strlen(key) &&
                    memcmp(lqdetect.buffer[cmd].data+offset, key, cmplen) == 0) {
                    return true;
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

static void lqdetect_write(char client_ip[], char *key, enum lq_detect_command cmd)
{
    struct   tm *ptm;
    struct   timeval val;
    struct   lq_detect_buffer *buffer = &lqdetect.buffer[cmd];
    uint32_t offset = buffer->offset;
    uint32_t nsaved = buffer->nsaved;
    char     *bufptr = buffer->data + buffer->offset;
    struct   lq_detect_argument *arg = &lqdetect.arg[cmd][nsaved];
    uint32_t nwrite;
    uint32_t length;

    gettimeofday(&val, NULL);
    ptm = localtime(&val.tv_sec);
    length = ((nsaved+1) * LONGQ_INPUT_SIZE) - offset - 1;

    snprintf(bufptr, length, "%02d:%02d:%02d.%06ld %s <%u> %s ",
        ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (long)val.tv_usec, client_ip,
        arg->overhead, command_str[cmd]);

    nwrite = strlen(bufptr);
    buffer->keypos[nsaved] = offset + nwrite;
    buffer->keylen[nsaved] = strlen(key);
    length -= nwrite;
    bufptr += nwrite;

    switch (cmd) {
    case LQCMD_LOP_INSERT:
        snprintf(bufptr, length, "%s %s\n", key, arg->range);
        break;
    case LQCMD_MOP_DELETE:
    case LQCMD_LOP_DELETE:
        if (arg->delete_or_drop == 2) {
            snprintf(bufptr, length, "%s %s %s\n", key, arg->range, "drop");
        } else {
            snprintf(bufptr, length, "%s %s\n", key, arg->range);
        }
        break;
    case LQCMD_SOP_GET:
    case LQCMD_MOP_GET:
    case LQCMD_LOP_GET:
        if (arg->delete_or_drop != 0) {
            snprintf(bufptr, length, "%s %s %s\n", key, arg->range,
                       (arg->delete_or_drop == 2 ? "drop" : "delete"));
        } else {
            snprintf(bufptr, length, "%s %s\n", key, arg->range);
        }
        break;
    case LQCMD_BOP_GBP:
        snprintf(bufptr, length, "%s %s %s\n", key, arg->range, (arg->asc_or_desc == 2 ? "desc" : "asc"));
        break;
    case LQCMD_BOP_GET:
        if (arg->delete_or_drop != 0) {
            snprintf(bufptr, length, "%s %s %u %u %s\n", key, arg->range,
                        arg->offset, arg->count, (arg->delete_or_drop == 2 ? "drop" : "delete"));
        } else {
            snprintf(bufptr, length, "%s %s %u %u\n", key, arg->range,
                        arg->offset, arg->count);
        }
        break;
    case LQCMD_BOP_COUNT:
        snprintf(bufptr, length, "%s %s\n", key, arg->range);
        break;
    case LQCMD_BOP_DELETE:
        if (arg->delete_or_drop == 2) {
            snprintf(bufptr, length, "%s %s %u %s\n", key, arg->range, arg->count, "drop");
        } else {
            snprintf(bufptr, length, "%s %s %u\n", key, arg->range, arg->count);
        }
        break;
    }

    nwrite += strlen(bufptr);
    buffer->offset += nwrite;
    buffer->nsaved += 1;
}

static bool lqdetect_discriminant(uint32_t overhead)
{
    if (overhead >= lqdetect.stats.standard) {
        return true;
    } else {
        return false;
    }
}

static bool lqdetect_save_cmd(char client_ip[], char* key,
                              enum lq_detect_command cmd, struct lq_detect_argument *arg)
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

        if (lqdetect_dupcheck(key, cmd, arg))
            break; /* duplication query */

        /* write to buffer */
        lqdetect_write(client_ip, key, cmd);

        /* internal stop */
        if (lqdetect.buffer[cmd].nsaved >= LONGQ_SAVE_CNT) {
            lqdetect.overflow_cnt++;
            if (lqdetect.overflow_cnt >= LONGQ_COMMAND_NUM) {
                do_lqdetect_stop(LONGQ_OVERFLOW_STOP);
                ret = false;
            }
        }
    } while(0);
    pthread_mutex_unlock(&lqdetect.lock);

    return ret;
}

static void lqdetect_make_bkeystring(const unsigned char* from_bkey, const unsigned char* to_bkey,
                                     const int from_nbkey, const int to_nbkey,
                                     const eflag_filter *efilter, char *bufptr) {
    char *tmpptr = bufptr;

    /* bkey */
    if (from_nbkey > 0) {
        memcpy(tmpptr, "0x", 2); tmpptr += 2;
        safe_hexatostr(from_bkey, from_nbkey, tmpptr);
        tmpptr += strlen(tmpptr);
        if (to_bkey != NULL) {
            memcpy(tmpptr, "..0x", 4); tmpptr += 4;
            safe_hexatostr(to_bkey, to_nbkey, tmpptr);
            tmpptr += strlen(tmpptr);
        }
    } else {
        sprintf(tmpptr, "%"PRIu64"", *(uint64_t*)from_bkey);
        tmpptr += strlen(tmpptr);
        if (to_bkey != NULL) {
            sprintf(tmpptr, "..%"PRIu64"", *(uint64_t*)to_bkey);
            tmpptr += strlen(tmpptr);
        }
    }
    /* efilter */
    if (efilter != NULL) {
        strcpy(tmpptr, " efilter");
    }
}

bool lqdetect_lop_insert(char *client_ip, char *key, int coll_index)
{
    uint32_t overhead = coll_index >= 0 ? coll_index+1 : -(coll_index);
    if (lqdetect_discriminant(overhead)) {
        struct lq_detect_argument argument;
        char *bufptr = argument.range;

        snprintf(bufptr, 16, "%d", coll_index);
        argument.overhead = overhead;

        if (! lqdetect_save_cmd(client_ip, key, LQCMD_LOP_INSERT, &argument)) {
            return false;
        }
    }
    return true;
}

bool lqdetect_lop_delete(char *client_ip, char *key, uint32_t del_count,
                         int32_t from_index, int32_t to_index, const int delete_or_drop)
{
    uint32_t overhead = del_count + (from_index >= 0 ? from_index+1 : -(from_index));
    if (lqdetect_discriminant(overhead)) {
        struct lq_detect_argument argument;
        char *bufptr = argument.range;

        snprintf(bufptr, 36, "%d..%d", from_index, to_index);
        argument.overhead = overhead;
        argument.delete_or_drop = delete_or_drop;

        if (! lqdetect_save_cmd(client_ip, key, LQCMD_LOP_DELETE, &argument)) {
            return false;
        }
    }
    return true;
}

bool lqdetect_lop_get(char *client_ip, char *key, uint32_t elem_count,
                      int32_t from_index, int32_t to_index, const int delete_or_drop)
{
    uint32_t overhead = elem_count + (from_index >= 0 ? from_index+1 : -(from_index));
    if (lqdetect_discriminant(overhead)) {
        struct lq_detect_argument argument;
        char *bufptr = argument.range;

        snprintf(bufptr, 36, "%d..%d", from_index, to_index);
        argument.overhead = overhead;
        argument.delete_or_drop = delete_or_drop;

        if (! lqdetect_save_cmd(client_ip, key, LQCMD_LOP_GET, &argument)) {
            return false;
        }
    }
    return true;
}

bool lqdetect_sop_get(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t count, const int delete_or_drop)
{
    if (lqdetect_discriminant(elem_count)) {
        struct lq_detect_argument argument;
        char *bufptr = argument.range;

        if (count == 0) {
            snprintf(bufptr, 4, "all");
        } else {
            snprintf(bufptr, 16, "%u", count);
        }
        argument.overhead = elem_count;
        argument.count = count;
        argument.delete_or_drop = delete_or_drop;

        if (! lqdetect_save_cmd(client_ip, key, LQCMD_SOP_GET, &argument)) {
            return false;
        }
    }
    return true;
}

bool lqdetect_mop_get(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t coll_numkeys, const int delete_or_drop)
{
    if (lqdetect_discriminant(elem_count)) {
        struct lq_detect_argument argument;
        char *bufptr = argument.range;

        if (coll_numkeys == 0) {
            snprintf(bufptr, 4, "all");
        } else {
            snprintf(bufptr, 16, "%u", coll_numkeys);
        }
        argument.overhead = elem_count;
        argument.count = coll_numkeys;
        argument.delete_or_drop = delete_or_drop;

        if (! lqdetect_save_cmd(client_ip, key, LQCMD_MOP_GET, &argument)) {
            return false;
        }
    }
    return true;
}

bool lqdetect_mop_delete(char *client_ip, char *key, uint32_t del_count,
                         uint32_t coll_numkeys, const int delete_or_drop)
{
    if (lqdetect_discriminant(del_count)) {
        struct lq_detect_argument argument;
        char *bufptr = argument.range;

        if (coll_numkeys == 0) {
            snprintf(bufptr, 4, "all");
        } else {
            snprintf(bufptr, 16, "%u", coll_numkeys);
        }
        argument.overhead = del_count;
        argument.count = coll_numkeys;
        argument.delete_or_drop = delete_or_drop;

        if (! lqdetect_save_cmd(client_ip, key, LQCMD_MOP_DELETE, &argument)) {
            return false;
        }
    }
    return true;
}

bool lqdetect_bop_gbp(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t from_posi, uint32_t to_posi, int order)
{
    if (lqdetect_discriminant(elem_count)) {
        struct lq_detect_argument argument;
        char *bufptr = argument.range;

        snprintf(bufptr, 36, "%u..%u", from_posi, to_posi);
        argument.overhead = elem_count;
        argument.asc_or_desc = order;

        if (! lqdetect_save_cmd(client_ip, key, LQCMD_BOP_GBP, &argument)) {
            return false;
        }
    }
    return true;
}

bool lqdetect_bop_get(char *client_ip, char *key, uint32_t access_count,
                      const bkey_range *bkrange, const eflag_filter *efilter,
                      uint32_t offset, uint32_t count, const int delete_or_drop)
{
    if (lqdetect_discriminant(access_count)) {
        struct lq_detect_argument argument;
        char *bufptr = argument.range;

        lqdetect_make_bkeystring(bkrange->from_bkey, bkrange->to_bkey,
                                 bkrange->from_nbkey, bkrange->to_nbkey,
                                 efilter, bufptr);
        argument.overhead = access_count;
        argument.offset = offset;
        argument.count = count;
        argument.delete_or_drop = delete_or_drop;

        if (! lqdetect_save_cmd(client_ip, key, LQCMD_BOP_GET, &argument)) {
            return false;
        }
    }
    return true;
}

bool lqdetect_bop_count(char *client_ip, char *key, uint32_t access_count,
                        const bkey_range *bkrange, const eflag_filter *efilter)
{
    if (lqdetect_discriminant(access_count)) {
        struct lq_detect_argument argument;
        char *bufptr = argument.range;

        lqdetect_make_bkeystring(bkrange->from_bkey, bkrange->to_bkey,
                                 bkrange->from_nbkey, bkrange->to_nbkey,
                                 efilter, bufptr);
        argument.overhead = access_count;

        if (! lqdetect_save_cmd(client_ip, key, LQCMD_BOP_COUNT, &argument)) {
            return false;
        }
    }
    return true;
}

bool lqdetect_bop_delete(char *client_ip, char *key, uint32_t access_count,
                         const bkey_range *bkrange, const eflag_filter *efilter,
                         uint32_t count, const int delete_or_drop)
{
    if (lqdetect_discriminant(access_count)) {
        struct lq_detect_argument argument;
        char *bufptr = argument.range;

        lqdetect_make_bkeystring(bkrange->from_bkey, bkrange->to_bkey,
                                 bkrange->from_nbkey, bkrange->to_nbkey,
                                 efilter, bufptr);
        argument.overhead = access_count;
        argument.count = count;
        argument.delete_or_drop = delete_or_drop;

        if (! lqdetect_save_cmd(client_ip, key, LQCMD_BOP_DELETE, &argument)) {
            return false;
        }
    }
    return true;
}
