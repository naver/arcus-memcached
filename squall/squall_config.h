/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Summary: Engine Configuration
 *
 * Copy: See Copyright for the status of this software.
 *
 * Author: JunHyun Park <junhyun.park@nhn.com>
 */
#ifndef MEMCACHED_SQUALL_CONFIG_H
#define MEMCACHED_SQUALL_CONFIG_H

#include "config.h"
#include "memcached/engine.h"

/* collection internal configurations */
#define MAX_ELEMENT_BYTES     (4*1024)
#define MAX_SM_VALUE_SIZE     8000
#ifdef SUPPORT_BOP_SMGET
#define MAX_SMGET_REQ_COUNT   2000
#endif

#define MAX_LIST_SIZE      50000
#define MAX_SET_SIZE       50000
#define MAX_BTREE_SIZE     50000

#define DEFAULT_LIST_SIZE   4000
#define DEFAULT_SET_SIZE    4000
#define DEFAULT_BTREE_SIZE  4000

/* squall engine recovery phase */
enum squall_recovery_phase {
    SQUALL_RECOVERY_NONE = 0,
    SQUALL_RECOVERY_ANALYSIS,
    SQUALL_RECOVERY_REDO,
    SQUALL_RECOVERY_UNDO,
    SQUALL_RECOVERY_CHKPT,
    SQUALL_RECOVERY_DONE
};

/* squall engine specific configuration */
struct squall_conf {
    char    *data_path;             /* data file path */
    char    *logs_path;             /* log file path */
    char    *mesg_path;             /* [NOT USED] message file path */
    size_t  data_file_size;         /* unit: MB (default: 2048) */
    size_t  data_page_size;         /* unit: Bytes (default: 4096) */
    size_t  data_buffer_count;      /* unit: page count (default: 262144) */
    size_t  log_file_size;          /* unit: MB (default: 2048) */
    size_t  log_buffer_size;        /* unit: MB (default: 10) */
    size_t  chkpt_interval;         /* unit: seconds (default: 1800) */
    size_t  tran_commit_mode;       /* 1 : memory commit
                                       2 : system commit
                                       3 : disk commit every second
                                       4 : disk commit every command (default)
                                    */
    size_t  max_prefix_count;       /* [INTERNAL USE] max # of prefixes */
    int     dbidx;                  /* [INTERNAL USE] database index: 0 or 1 */
    int     recovery_phase;         /* [INTERNAL USE] recovery phase */
    bool    use_recovery;           /* recovery support */
    bool    use_directio;           /* direct io on data pages */
    bool    use_dbcheck;            /* db check at server start */
};

/* engine general configuration */
struct config {
   /* engine general conf */
    bool        use_cas;
    size_t      verbose;
    rel_time_t  oldest_live;
    bool        evict_to_free;
    size_t      num_threads;       /* # of worker threads */
    size_t      maxbytes;
    size_t      sticky_limit;
    size_t      junk_item_time;
    bool        preallocate;
    float       factor;
    size_t      chunk_size;
    size_t      item_size_max;
    bool        ignore_vbucket;
    char        prefix_delimiter;
    bool        vb0;
#if 1 // ENABLE_SQUALL_ENGINE
    /* squall engine specific config */
    struct squall_conf sq;
#endif
};
#endif
