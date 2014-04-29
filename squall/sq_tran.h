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
#ifndef SQ_TRAN_H
#define SQ_TRAN_H

#include "sq_types.h"

/* transaction end request */
typedef enum _tran_command {
    TRAN_COM_COMMIT = 1,
    TRAN_COM_ABORT
} TranCommand;

/* transaction entry state */
typedef enum _tran_state {
    TRAN_STATE_FREE = 0,
    TRAN_STATE_ALLOC,
    TRAN_STATE_BEGIN,
    TRAN_STATE_COMMIT,
    TRAN_STATE_ABORT
} TranState;

/* transaction entry structure */
typedef struct _trans {
    uint64_t    tranid;     /* transaction id */
    LogSN       first_lsn;
    LogSN       last_lsn;
    LogSN       undonxt_lsn;
    int16_t     curr_eid;   /* curr entry id */
    int16_t     prev_eid;   /* next entry id */
    int16_t     next_eid;   /* next entry id */
    uint8_t     implicit;   /* is implicit transaction ? */
    uint8_t     state;      /* tran entry state */
} Trans;

#define TRAN_STAT_MAX_TRANS 32

struct tran_stat {
    uint64_t    nxt_tranid;
    uint16_t    max_trans;
    uint16_t    cur_trans;
    uint16_t    imp_trans;
    uint16_t    num_trans;
    Trans       tran_info[TRAN_STAT_MAX_TRANS];
};

/* external transaction functions */
Trans   *tran_find(int entryid);
Trans   *tran_search(uint64_t tranid);
Trans   *tran_first(void);
Trans   *tran_next(Trans *tran);
Trans   *tran_alloc(bool implicit_tran);
void     tran_free(Trans *tran, TranCommand com);
void     tran_start(Trans *tran);
void     tran_stop(Trans *tran, TranCommand com);

Trans   *tran_reco_alloc(uint64_t tranid);
void     tran_reco_free(Trans *tran);

void     tran_stat_get(struct tran_stat *stats);

uint64_t tran_mgr_get_min_tranid(void);
uint64_t tran_mgr_get_nxt_tranid(void);
void     tran_mgr_set_nxt_tranid(uint64_t nxt_tranid);

int      tran_mgr_init(void *conf, void *srvr);
void     tran_mgr_final(void);
#endif
