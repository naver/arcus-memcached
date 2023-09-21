#ifndef NETWORK_UTIL_H
#define NETWORK_UTIL_H

#include "memcached.h"
#ifdef ENABLE_ZK_INTEGRATION
#include "arcus_zk.h"
#include "arcus_hb.h"
#endif

void ritem_set_first(conn *c, int rtype, int vleng);

/* stats */
void server_stats(ADD_STAT add_stats, conn *c, bool aggregate);
void process_stats_settings(ADD_STAT add_stats, void *c);
#ifdef ENABLE_ZK_INTEGRATION
void process_stats_zookeeper(ADD_STAT add_stats, void *c);
#endif
void update_stat_cas(conn *c, ENGINE_ERROR_CODE ret);
void stats_reset(const void *cookie);

/* message */
int add_msghdr(conn *c);
void write_and_free(conn *c, char *buf, int bytes);
bool grow_dynamic_buffer(conn *c, size_t needed);

inline char *get_ovflaction_str(uint8_t ovflact)
{
    if (ovflact == OVFL_HEAD_TRIM)          return "head_trim";
    else if (ovflact == OVFL_TAIL_TRIM)     return "tail_trim";
    else if (ovflact == OVFL_SMALLEST_TRIM) return "smallest_trim";
    else if (ovflact == OVFL_LARGEST_TRIM)  return "largest_trim";
    else if (ovflact == OVFL_SMALLEST_SILENT_TRIM) return "smallest_silent_trim";
    else if (ovflact == OVFL_LARGEST_SILENT_TRIM)  return "largest_silent_trim";
    else if (ovflact == OVFL_ERROR)         return "error";
    else                                    return "unknown";
}

inline char get_item_type_char(uint8_t type)
{
    if (type == ITEM_TYPE_KV)          return 'K';
    else if (type == ITEM_TYPE_LIST)   return 'L';
    else if (type == ITEM_TYPE_SET)    return 'S';
    else if (type == ITEM_TYPE_MAP)    return 'M';
    else if (type == ITEM_TYPE_BTREE)  return 'B';
    else                               return 'A';
}

inline char *get_item_type_str(uint8_t type)
{
    if (type == ITEM_TYPE_KV)          return "kv";
    else if (type == ITEM_TYPE_LIST)   return "list";
    else if (type == ITEM_TYPE_SET)    return "set";
    else if (type == ITEM_TYPE_MAP)    return "map";
    else if (type == ITEM_TYPE_BTREE)  return "b+tree";
    else                               return "unknown";
}

#endif
