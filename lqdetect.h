#ifndef LQDETECT_H
#define LQDETECT_H

#define DETECT_LONG_QUERY
#define LONGQ_COMMAND_NUM       8       /* detectiong command count */
#define LONGQ_SAVE_CNT          25      /* save key count */
#define LONGQ_STANDARD_BASE     4000    /* basic detect standard */
#define LONGQ_RANGE_SIZE        31*2+10


#define LONGQ_EXPLICIT_STOP          0  /* stop by user request */
#define LONGQ_BUFFER_OVERFLOW_STOP   1  /* stop by detected command buffer overflow */
#define LONGQ_COUNT_OVERFLOW_STOP    2  /* stop by detected command count obferflow */
#define LONGQ_RUNNING                3  /* long query is running */

/* detect long query target command */
enum lqdetect_command {
    sop_get,
    lop_insert,
    lop_delete,
    lop_get,
    bop_delete,
    bop_get,
    bop_count,
    bop_gbp
};

/* lqdetect buffer structure */
struct lq_detect_buffer {
    char *data;
    uint32_t offset;
    uint32_t detect_count;
    uint32_t save_count;
    uint32_t keyoffset[LONGQ_SAVE_CNT];
    uint32_t lenkey[LONGQ_SAVE_CNT];
};

/* lqdetect argument structure */
struct lq_detect_argument {
    char range[LONGQ_RANGE_SIZE];
    uint32_t access_count;
    uint32_t offset;
    int lenskip;
    int input_count;
    int delete_or_drop;
    int asc_or_desc;
    bool efilter;
};

/* detectiong long query stats structure */
struct lq_detect_stats {
    int bgndate, bgntime;
    int enddate, endtime;
    int total_ndetdcmd; /* number of total long query command */
    int stop_cause; /* how stopped */
    uint32_t standard;
};
uint16_t lqdetect_refcount; /* lqdetect show refrence count */

int lqdetect_init(void);
void lqdetect_final(void);
void lqdetect_refcount_lock(bool locking);
int lqdetect_start(uint32_t lqdetect_base, bool *already_started);
void lqdetect_stop(bool *already_stopped);
struct lq_detect_stats *lqdetect_stats(void);
struct lq_detect_buffer *lqdetect_buffer(int cmd);
bool lqdetect_discriminant(uint32_t count);
bool lqdetect_process_targetcmd(char client_ip[], char *key,
                      enum lqdetect_command cmd, struct lq_detect_argument *arg);
#endif
