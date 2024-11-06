#include "lqdetect.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <assert.h>

#include "memcached/util.h"

#define LQ_THRESHOLD_DEFAULT 4000
#define LQ_QUERY_SIZE  (64*2+64) /* bop get (longest query) : "<longest bkey>..<longest bkey> efilter <offset> <count> delete" */
#define LQ_KEY_SIZE    250       /* the max size of key string */
#define LQ_SAVE_CNT    20        /* save key count */
#define LQ_INPUT_SIZE  500       /* the size of input(time, ip, command, argument) */
#define LQ_STAT_STRLEN 300       /* max length of stats */

/* lqdetect state */
#define LQ_EXPLICIT_STOP 0       /* stop by user request */
#define LQ_OVERFLOW_STOP 1       /* stop by detected command overflow (buffer or count) */
#define LQ_RUNNING       2       /* long query is running */

/* lqdetect command */
enum lq_detect_command {
    LQCMD_SOP_GET=0,
    LQCMD_MOP_DELETE,
    LQCMD_MOP_GET,
    LQCMD_LOP_INSERT,
    LQCMD_LOP_DELETE,
    LQCMD_LOP_GET,
    LQCMD_BOP_DELETE,
    LQCMD_BOP_GET,
    LQCMD_BOP_COUNT,
    LQCMD_BOP_GBP
};
#define LQ_CMD_NUM (LQCMD_BOP_GBP+1) /* the number of command to detect */

bool lqdetect_in_use = false;

static EXTENSION_LOGGER_DESCRIPTOR *mc_logger;
static const char *command_str[LQ_CMD_NUM] = {
    "sop get","mop delete", "mop get",
    "lop insert", "lop delete", "lop get",
    "bop delete", "bop get", "bop count", "bop gbp"
};

/* lqdetect stats structure */
struct lq_detect_stats {
    int bgndate, bgntime;
    int enddate, endtime;
    int total;  /* number of total long query command */
    int state;  /* lqdetect state */
    uint32_t threshold;
};

/* lqdetect buffer structure */
struct lq_detect_buffer {
    char *data;
    char *keyptr[LQ_SAVE_CNT];
    char *queryptr[LQ_SAVE_CNT];
    uint32_t offset;
    uint32_t ntotal;
    uint32_t nsaved;
    uint32_t keylen[LQ_SAVE_CNT];
    uint32_t querylen[LQ_SAVE_CNT];
};

/* lqdetect global structure */
struct lq_detect_global {
    pthread_mutex_t lock;
    struct lq_detect_buffer buffer[LQ_CMD_NUM];
    struct lq_detect_stats stats;
    int overflow_cnt;
};
struct lq_detect_global lqdetect;

static bool is_command_duplicated(char *key, int keylen, char *query, int querylen,
                                  enum lq_detect_command cmd, bool check_key)
{
    int nsaved = lqdetect.buffer[cmd].nsaved;
    struct lq_detect_buffer *buf = &lqdetect.buffer[cmd];

    switch (cmd) {
    case LQCMD_LOP_INSERT:
    case LQCMD_LOP_DELETE:
    case LQCMD_LOP_GET:
    case LQCMD_BOP_GBP:
    case LQCMD_BOP_GET:
    case LQCMD_BOP_COUNT:
    case LQCMD_BOP_DELETE:
        for (int ii = 0; ii < nsaved; ii++) {
            if (buf->querylen[ii] == querylen &&
                memcmp(buf->queryptr[ii], query, querylen) == 0) {
                return true;
            }
        }
        break;
    case LQCMD_MOP_DELETE:
    case LQCMD_MOP_GET:
    case LQCMD_SOP_GET:
        for (int ii = 0; ii < nsaved; ii++) {
            if (buf->querylen[ii] == querylen &&
                memcmp(buf->queryptr[ii], query, querylen) == 0) {
                if (!check_key) return true;
                if (buf->keylen[ii] == keylen &&
                    memcmp(buf->keyptr[ii], key, keylen) == 0) {
                    return true;
                }
            }
        }
        break;
    }
    return false;
}

static void do_lqdetect_write(char *client_ip, char *key, char *query,
                              enum lq_detect_command cmd, uint32_t overhead, bool check_key)
{
    struct   tm *ptm;
    struct   timeval val;
    struct   lq_detect_buffer *buffer = &lqdetect.buffer[cmd];
    uint32_t nsaved = buffer->nsaved;
    uint32_t length = ((nsaved+1) * LQ_INPUT_SIZE);
    uint32_t keylen = strlen(key);
    uint32_t querylen = strlen(query);
    char keybuf[LQ_KEY_SIZE + 1];
    char *keyptr = key;

    if (keylen > LQ_KEY_SIZE) { /* long key string */
        keylen = snprintf(keybuf, sizeof(keybuf), "%.*s...%.*s",
                          124, key, 123, (key + keylen - 123));
        keyptr = keybuf;
    }

    if (is_command_duplicated(keyptr, keylen, query, querylen, cmd, check_key) == true) {
        return;
    }

    gettimeofday(&val, NULL);
    ptm = localtime(&val.tv_sec);

    snprintf(buffer->data + buffer->offset, length - buffer->offset,
             "%02d:%02d:%02d.%06ld %s <%u> %s ",
             ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (long)val.tv_usec,
             client_ip, overhead, command_str[cmd]);
    buffer->offset += strlen(buffer->data + buffer->offset);
    buffer->keyptr[nsaved] = buffer->data + buffer->offset;
    buffer->keylen[nsaved] = keylen;

    snprintf(buffer->keyptr[nsaved], length - buffer->offset,
             "%s ", keyptr);
    buffer->offset += strlen(buffer->keyptr[nsaved]);
    buffer->queryptr[nsaved] = buffer->data + buffer->offset;
    buffer->querylen[nsaved] = strlen(query);

    snprintf(buffer->queryptr[nsaved], length - buffer->offset,
             "%s\n", query);
    buffer->offset += strlen(buffer->data + buffer->offset);
    buffer->nsaved += 1;
}

static void do_lqdetect_stop(int cause)
{
    /* detect long query lock has already been held */
    lqdetect.stats.state = cause;
    lqdetect.stats.enddate = getnowdate_int();
    lqdetect.stats.endtime = getnowtime_int();
    lqdetect_in_use = false;
}

static void do_lqdetect_save_cmd(char *client_ip, char* key, char *query,
                                 enum lq_detect_command cmd, uint32_t overhead, bool check_key)
{
    assert(cmd >= LQCMD_SOP_GET && cmd <= LQCMD_BOP_GBP);
    pthread_mutex_lock(&lqdetect.lock);
    if (lqdetect_in_use) {
        lqdetect.buffer[cmd].ntotal++;
        if (lqdetect.buffer[cmd].nsaved < LQ_SAVE_CNT) {
            /* write to buffer */
            do_lqdetect_write(client_ip, key, query, cmd, overhead, check_key);
            /* internal stop */
            if (lqdetect.buffer[cmd].nsaved >= LQ_SAVE_CNT) {
                lqdetect.overflow_cnt++;
                if (lqdetect.overflow_cnt >= LQ_CMD_NUM) {
                    do_lqdetect_stop(LQ_OVERFLOW_STOP);
                }
            }
        }
    }
    pthread_mutex_unlock(&lqdetect.lock);
}

static int do_make_bkeystring(char *buffer, const bkey_range *bkrange, const eflag_filter *efilter) {
    char *bufptr = buffer;
    /* bkey */
    if (bkrange->from_nbkey > 0) { /* hexadecimal */
        memcpy(bufptr, "0x", 2); bufptr += 2;
        safe_hexatostr(bkrange->from_bkey, bkrange->from_nbkey, bufptr);
        bufptr += strlen(bufptr);
        if (bkrange->to_nbkey != BKEY_NULL) { /* range */
            memcpy(bufptr, "..0x", 4); bufptr += 4;
            safe_hexatostr(bkrange->to_bkey, bkrange->to_nbkey, bufptr);
            bufptr += strlen(bufptr);
        }
    } else { /* 64bit unsigned integer */
        const unsigned char* bkptr = bkrange->from_bkey;
        bufptr += sprintf(bufptr, "%"PRIu64"", *(uint64_t*)bkptr);
        if (bkrange->to_nbkey != BKEY_NULL) { /* range */
            bkptr = bkrange->to_bkey;
            bufptr += sprintf(bufptr, "..%"PRIu64"", *(uint64_t*)bkptr);
        }
    }
    /* efilter */
    if (efilter != NULL) {
        strcpy(bufptr, " efilter");
        bufptr += strlen(bufptr);
    }
    return (bufptr - buffer);
}

/* external functions */
int lqdetect_init(EXTENSION_LOGGER_DESCRIPTOR *logger)
{
    mc_logger = logger;
    pthread_mutex_init(&lqdetect.lock, NULL);
    lqdetect_in_use = false;

    memset(lqdetect.buffer, 0, LQ_CMD_NUM * sizeof(struct lq_detect_buffer));
    memset(&lqdetect.stats, 0, sizeof(struct lq_detect_stats));
    for (int ii = 0; ii < LQ_CMD_NUM; ii++) {
        lqdetect.buffer[ii].data = malloc(LQ_SAVE_CNT * LQ_INPUT_SIZE);
        if (lqdetect.buffer[ii].data == NULL) {
            while (--ii >= 0) {
                free(lqdetect.buffer[ii].data);
            }
            return -1;
        }
    }
    return 0;
}

void lqdetect_final(void)
{
    for (int ii = 0; ii < LQ_CMD_NUM; ii++) {
        free(lqdetect.buffer[ii].data);
        lqdetect.buffer[ii].data = NULL;
    }
}

int lqdetect_start(uint32_t threshold, bool *already_started)
{
    int ret = 0;
    pthread_mutex_lock(&lqdetect.lock);
    do {
        if (lqdetect_in_use) {
            *already_started = true;
            break;
        }

        /* prepare detect long query buffer, argument and counts*/
        for (int ii = 0; ii < LQ_CMD_NUM; ii++) {
            lqdetect.buffer[ii].ntotal = 0;
            lqdetect.buffer[ii].nsaved = 0;
            lqdetect.buffer[ii].offset = 0;
        }

        /* prepare detect long query stats */
        memset(&lqdetect.stats, 0, sizeof(struct lq_detect_stats));
        lqdetect.stats.bgndate = getnowdate_int();
        lqdetect.stats.bgntime = getnowtime_int();
        lqdetect.stats.state = LQ_RUNNING;
        lqdetect.stats.threshold = (threshold == 0 ? LQ_THRESHOLD_DEFAULT : threshold);

        lqdetect.overflow_cnt = 0;
        lqdetect_in_use = true;
        ret = 0;
    } while(0);
    pthread_mutex_unlock(&lqdetect.lock);
    return ret;
}

void lqdetect_stop(bool *already_stopped)
{
    pthread_mutex_lock(&lqdetect.lock);
    if (lqdetect_in_use == true) {
        do_lqdetect_stop(LQ_EXPLICIT_STOP);
    } else {
        *already_stopped = true;
    }
    pthread_mutex_unlock(&lqdetect.lock);
}

char *lqdetect_stats(void)
{
    char *str = (char*)malloc(LQ_STAT_STRLEN);
    if (str) {
        char *state_str[3] = {
            "stopped by explicit request",          // LQ_EXPLICIT_STOP
            "stopped by internal buffer overflow",  // LQ_OVERFLOW_STOP
            "running"                               // LQ_RUNNING
        };
        struct lq_detect_stats stats = lqdetect.stats;

        if (lqdetect_in_use) {
            stats.enddate = 0;
            stats.endtime = 0;
        }

        stats.total = 0;
        for (int i=0; i < LQ_CMD_NUM; i++) {
            stats.total += lqdetect.buffer[i].ntotal;
        }

        snprintf(str, LQ_STAT_STRLEN,
                "\t" "Long query detection stats : %s" "\n"
                "\t" "The last running time : %d_%d ~ %d_%d" "\n"
                "\t" "The number of total long query commands : %d" "\n"
                "\t" "The detection threshold : %u" "\n",
                (stats.state >= 0 && stats.state <= 2 ?
                 state_str[stats.state] : "unknown"),
                stats.bgndate, stats.bgntime, stats.enddate, stats.endtime,
                stats.total, stats.threshold);
    }
    return str;
}

char *lqdetect_result_get(int *size)
{
    int offset = 0;
    int length = 32 * LQ_CMD_NUM; // header length
    char *str;

    pthread_mutex_lock(&lqdetect.lock);
    for (int i = 0; i < LQ_CMD_NUM; i++) {
        length += lqdetect.buffer[i].offset;
    }
    str = (char*)malloc(length);
    if (str != NULL) {
        for (int i = 0; i < LQ_CMD_NUM; i++) {
            struct lq_detect_buffer *ldb = &lqdetect.buffer[i];
            offset += snprintf(str + offset, length - offset, "%s : %u\n", command_str[i], ldb->ntotal);
            if (ldb->ntotal > 0) {
                offset += snprintf(str + offset, length - offset, "%s", ldb->data);
            }
        }
    }
    pthread_mutex_unlock(&lqdetect.lock);

    *size = offset;
    return str;
}

void lqdetect_lop_insert(char *client_ip, char *key, int coll_index)
{
    uint32_t overhead = coll_index >= 0 ? coll_index+1 : -(coll_index);
    if (overhead >= lqdetect.stats.threshold) {
        char query[LQ_QUERY_SIZE];
        snprintf(query, LQ_QUERY_SIZE, "%d", coll_index);
        do_lqdetect_save_cmd(client_ip, key, query, LQCMD_LOP_INSERT, overhead, false);
    }
}

void lqdetect_lop_delete(char *client_ip, char *key, uint32_t del_count,
                         int32_t from_index, int32_t to_index, const bool drop_if_empty)
{
    uint32_t overhead = del_count + (from_index >= 0 ? from_index+1 : -(from_index));
    if (overhead >= lqdetect.stats.threshold) {
        char query[LQ_QUERY_SIZE];
        snprintf(query, LQ_QUERY_SIZE, "%d..%d%s", from_index, to_index,
                 drop_if_empty ? " drop" : "");
        do_lqdetect_save_cmd(client_ip, key, query, LQCMD_LOP_DELETE, overhead, false);
    }
}

void lqdetect_lop_get(char *client_ip, char *key, uint32_t elem_count,
                      int32_t from_index, int32_t to_index, const bool delete, const bool drop_if_empty)
{
    uint32_t overhead = elem_count + (from_index >= 0 ? from_index+1 : -(from_index));
    if (overhead >= lqdetect.stats.threshold) {
        char query[LQ_QUERY_SIZE];
        snprintf(query, LQ_QUERY_SIZE, "%d..%d%s", from_index, to_index,
                 drop_if_empty ? " drop" : (delete ? " delete" : ""));
        do_lqdetect_save_cmd(client_ip, key, query, LQCMD_LOP_GET, overhead, false);
    }
}

void lqdetect_sop_get(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t count, const bool delete, const bool drop_if_empty)
{
    if (elem_count >= lqdetect.stats.threshold) {
        char query[LQ_QUERY_SIZE];
        snprintf(query, LQ_QUERY_SIZE, "%u%s", count,
                 drop_if_empty ? " drop" : (delete ? " delete" : ""));
        do_lqdetect_save_cmd(client_ip, key, query, LQCMD_SOP_GET, elem_count, (count == 0));
    }
}

void lqdetect_mop_get(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t coll_numkeys, const bool delete, const bool drop_if_empty)
{
    if (elem_count >= lqdetect.stats.threshold) {
        char query[LQ_QUERY_SIZE];
        snprintf(query, LQ_QUERY_SIZE, "%u%s", coll_numkeys,
                 drop_if_empty ? " drop" : (delete ? " delete" : ""));
        do_lqdetect_save_cmd(client_ip, key, query, LQCMD_MOP_GET, elem_count, (coll_numkeys == 0));
    }
}

void lqdetect_mop_delete(char *client_ip, char *key, uint32_t del_count,
                         uint32_t coll_numkeys, const bool drop_if_empty)
{
    if (del_count >= lqdetect.stats.threshold) {
        char query[LQ_QUERY_SIZE];
        snprintf(query, LQ_QUERY_SIZE, "%u%s", coll_numkeys,
                 drop_if_empty ? " drop" : "");
        do_lqdetect_save_cmd(client_ip, key, query, LQCMD_MOP_DELETE, del_count, (coll_numkeys == 0));
    }
}

void lqdetect_bop_gbp(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t from_posi, uint32_t to_posi, ENGINE_BTREE_ORDER order)
{
    if (elem_count >= lqdetect.stats.threshold) {
        char query[LQ_QUERY_SIZE];
        snprintf(query, LQ_QUERY_SIZE, "%u..%u %s", from_posi, to_posi,
                 order == BTREE_ORDER_ASC ? "asc" : "desc");
        do_lqdetect_save_cmd(client_ip, key, query, LQCMD_BOP_GBP, elem_count, false);
    }
}

void lqdetect_bop_get(char *client_ip, char *key, uint32_t access_count,
                      const bkey_range *bkrange, const eflag_filter *efilter,
                      uint32_t offset, uint32_t count, const bool delete, const bool drop_if_empty)
{
    if (access_count >= lqdetect.stats.threshold) {
        char query[LQ_QUERY_SIZE];
        int nwrite = do_make_bkeystring(query, bkrange, efilter);
        snprintf(query + nwrite, LQ_QUERY_SIZE - nwrite, " %u %u%s",
                 offset, count, drop_if_empty ? " drop" : (delete ? " delete" : ""));
        do_lqdetect_save_cmd(client_ip, key, query, LQCMD_BOP_GET, access_count, false);
    }
}

void lqdetect_bop_count(char *client_ip, char *key, uint32_t access_count,
                        const bkey_range *bkrange, const eflag_filter *efilter)
{
    if (access_count >= lqdetect.stats.threshold) {
        char query[LQ_QUERY_SIZE];
        do_make_bkeystring(query, bkrange, efilter);
        do_lqdetect_save_cmd(client_ip, key, query, LQCMD_BOP_COUNT, access_count, false);
    }
}

void lqdetect_bop_delete(char *client_ip, char *key, uint32_t access_count,
                         const bkey_range *bkrange, const eflag_filter *efilter,
                         uint32_t count, const bool drop_if_empty)
{
    if (access_count >= lqdetect.stats.threshold) {
        char query[LQ_QUERY_SIZE];
        int nwrite = do_make_bkeystring(query, bkrange, efilter);
        snprintf(query + nwrite, LQ_QUERY_SIZE - nwrite, " %u%s",
                 count, drop_if_empty ? " drop" : "");
        do_lqdetect_save_cmd(client_ip, key, query, LQCMD_BOP_DELETE, access_count, false);
    }
}
