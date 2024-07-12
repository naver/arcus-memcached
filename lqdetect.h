#ifndef LQDETECT_H
#define LQDETECT_H

#include "memcached/extension_loggers.h"
#include "memcached/util.h"

#define DETECT_LONG_QUERY

extern bool lqdetect_in_use;

int lqdetect_init(EXTENSION_LOGGER_DESCRIPTOR *logger);
void lqdetect_final(void);
char *lqdetect_result_get(int *size);
int lqdetect_start(uint32_t threshold, bool *already_started);
void lqdetect_stop(bool *already_stopped);
char *lqdetect_stats(void);

void lqdetect_lop_insert(char *client_ip, char *key, int coll_index);
void lqdetect_lop_delete(char *client_ip, char *key, uint32_t del_count,
                         int32_t from_index, int32_t to_index, const bool drop_if_empty);
void lqdetect_lop_get(char *client_ip, char *key, uint32_t elem_count,
                      int32_t from_index, int32_t to_index, const bool delete, const bool drop_if_empty);
void lqdetect_sop_get(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t count, const bool delete, const bool drop_if_empty);
void lqdetect_mop_get(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t coll_numkeys, const bool delete, const bool drop_if_empty);
void lqdetect_mop_delete(char *client_ip, char *key, uint32_t del_count,
                         uint32_t coll_numkeys, const bool drop_if_empty);
void lqdetect_bop_gbp(char *client_ip, char *key, uint32_t elem_count,
                      uint32_t from_posi, uint32_t to_posi, ENGINE_BTREE_ORDER order);
void lqdetect_bop_get(char *client_ip, char *key, uint32_t access_count,
                      const bkey_range *bkrange, const eflag_filter *efilter,
                      uint32_t offset, uint32_t count, const bool delete, const bool drop_if_empty);
void lqdetect_bop_count(char *client_ip, char *key, uint32_t access_count,
                        const bkey_range *bkrange, const eflag_filter *efilter);
void lqdetect_bop_delete(char *client_ip, char *key, uint32_t access_count,
                         const bkey_range *bkrange, const eflag_filter *efilter,
                         uint32_t count, const bool drop_if_empty);
#endif
