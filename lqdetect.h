#ifndef LQDETECT_H
#define LQDETECT_H

#include "memcached/extension_loggers.h"

#define DETECT_LONG_QUERY
#define LONGQ_COMMAND_NUM       LQCMD_BOP_GBP+1       /* detectiong command count */
#define LONGQ_SAVE_CNT          20                    /* save key count */
#define LONGQ_INPUT_SIZE        500                   /* the size of input(time, ip, command, argument) */
#define LONGQ_STANDARD_DEFAULT  4000                  /* defulat detect standard */
#define LONGQ_RANGE_SIZE        31*2+10*2


#define LONGQ_EXPLICIT_STOP          0    /* stop by user request */
#define LONGQ_BUFFER_OVERFLOW_STOP   1    /* stop by detected command buffer overflow */
#define LONGQ_COUNT_OVERFLOW_STOP    2    /* stop by detected command count obferflow */
#define LONGQ_RUNNING                3    /* long query is running */

/* detect long query target command */
enum lqdetect_command {
    LQCMD_SOP_GET=0,
    LQCMD_LOP_INSERT,
    LQCMD_LOP_DELETE,
    LQCMD_LOP_GET,
    LQCMD_BOP_DELETE,
    LQCMD_BOP_GET,
    LQCMD_BOP_COUNT,
    LQCMD_BOP_GBP
};

/* lqdetect buffer structure */
struct lq_detect_buffer {
    char *data;
    uint32_t offset;
    uint32_t ntotal;
    uint32_t nsaved;
    uint32_t keypos[LONGQ_SAVE_CNT];
    uint32_t keylen[LONGQ_SAVE_CNT];
};

/* lqdetect argument structure */
struct lq_detect_argument {
    char range[LONGQ_RANGE_SIZE];
    uint32_t overhead;
    uint32_t offset;
    uint32_t count;
    int delete_or_drop;
    int asc_or_desc;
};

/* detectiong long query stats structure */
struct lq_detect_stats {
    int bgndate, bgntime;
    int enddate, endtime;
    int total_lqcmds; /* number of total long query command */
    int stop_cause; /* how stopped */
    uint32_t standard;
};
uint16_t lqdetect_refcount; /* lqdetect show refrence count */

int lqdetect_init(void);
void lqdetect_final(void);
void lqdetect_buffer_hold(void);
void lqdetect_buffer_release(void);
int lqdetect_start(uint32_t lqdetect_base, bool *already_started);
void lqdetect_stop(bool *already_stopped);
struct lq_detect_stats *lqdetect_stats(void);
struct lq_detect_buffer *lqdetect_buffer(int cmd);
bool lqdetect_discriminant(uint32_t count);
bool lqdetect_save_cmd(char client_ip[], char *key,
                      enum lqdetect_command cmd, struct lq_detect_argument *arg);
#endif
