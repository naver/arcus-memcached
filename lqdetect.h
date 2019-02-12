#ifndef LQDETECT_H
#define LQDETECT_H

#include "memcached/extension_loggers.h"
#include "memcached/util.h"

#define DETECT_LONG_QUERY
#define LONGQ_STAT_STRLEN       300
#define LONGQ_STANDARD_DEFAULT  4000        /* defulat detect standard */
#define LONGQ_RANGE_SIZE        (31*2+10*2)

#define LONGQ_EXPLICIT_STOP     0    /* stop by user request */
#define LONGQ_OVERFLOW_STOP     1    /* stop by detected command overflow (buffer or count)*/
#define LONGQ_RUNNING           2    /* long query is running */

/* detect long query target command */
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
#define LONGQ_COMMAND_NUM   (LQCMD_BOP_GBP+1)  /* detectiong command count */

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

int lqdetect_init(void);
void lqdetect_final(void);
char *lqdetect_buffer_get(int cmd, uint32_t *length, uint32_t *cmdcnt);
void lqdetect_buffer_release(int bufcnt);
int lqdetect_start(uint32_t lqdetect_base, bool *already_started);
void lqdetect_stop(bool *already_stopped);
void lqdetect_get_stats(char* str);

bool lqdetect_lop_insert(char *client_ip, char *key, int coll_index);
bool lqdetect_lop_delete(char *client_ip, char *key, uint32_t del_count,
                         int32_t from_index, int32_t to_index, const int delete_or_drop);
bool lqdetect_lop_get(char *client_ip, char *key, uint32_t elem_count,
                      int32_t from_index, int32_t to_index, const int delete_or_drop);
bool lqdetect_sop_get(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t count, const int delete_or_drop);
bool lqdetect_mop_get(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t coll_numkeys, const int delete_or_drop);
bool lqdetect_mop_delete(char *client_ip, char *key, uint32_t del_count,
                         uint32_t coll_numkeys, const int delete_or_drop);
bool lqdetect_bop_gbp(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t from_posi, uint32_t to_posi, int order);
bool lqdetect_bop_get(char *client_ip, char *key, uint32_t access_count,
                      const bkey_range *bkrange, const eflag_filter *efilter,
                      uint32_t offset, uint32_t count, const int delete_or_drop);
bool lqdetect_bop_count(char *client_ip, char *key, uint32_t access_count,
                        const bkey_range *bkrange, const eflag_filter *efilter);
bool lqdetect_bop_delete(char *client_ip, char *key, uint32_t access_count,
                         const bkey_range *bkrange, const eflag_filter *efilter,
                         uint32_t count, const int delete_or_drop);

#endif
