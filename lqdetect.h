#ifndef LQDETECT_H
#define LQDETECT_H

#include "memcached/extension_loggers.h"
#include "memcached/util.h"

#define DETECT_LONG_QUERY
#define LQ_STANDARD_DEFAULT  4000 /* default detect standard */
/* longest range: "<longest bkey>..<longest bkey> efilter" */
#define LQ_RANGE_SIZE        (64*2+16)

/* lqdetect argument structure */
struct lq_detect_argument {
    char range[LQ_RANGE_SIZE];
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
    int total_lqcmds;     /* number of total long query command */
    int state;            /* lqdetect state */
    uint32_t standard;
};

int lqdetect_init(EXTENSION_LOGGER_DESCRIPTOR *logger);
void lqdetect_final(void);
field_t *lqdetect_result_get(int *size);
void lqdetect_result_release(field_t *results);
int lqdetect_start(uint32_t lqdetect_base, bool *already_started);
void lqdetect_stop(bool *already_stopped);
char *lqdetect_stats(void);

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
