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
#ifndef SQ_BUFFER_H
#define SQ_BUFFER_H

#include "sq_page.h"

/* buffer configuration */
struct buffer_conf {
    uint32_t ini_buffers;
    uint32_t max_buffers;
    uint32_t max_threads;
    uint32_t page_size;
};

/* buffer latch mode */
enum buf_latch_mode {
    BUF_MODE_NONE = 0, /* internal use only */
    BUF_MODE_READ,
    BUF_MODE_WRITE
};

/* buffer priority */
enum buf_priority {
    BUF_PRIO_LOW = 0,
    BUF_PRIO_HIGH
};

/* buffer statistics */
struct buffer_stat {
    uint64_t total_fix_count;
    uint64_t total_hit_count;
    uint32_t fixed_buffer_count;
    uint32_t buffer_used_count;
    uint32_t buffer_free_count;
    uint32_t hash_table_size;
    uint32_t hash_chain_max_size;
    uint32_t lru_perm_count;
    uint32_t lru_warm_count;
    uint32_t lru_middle_count;
    uint32_t lru_cold_dirty_count;
    uint32_t lru_cold_clean_count;
    uint64_t buffer_replace_count;
    uint64_t disk_read_count;
    uint64_t disk_write_count;
};

#ifdef __cplusplus
extern "C" {
#endif

int   buf_mgr_init(void *conf, void *srvr);
void  buf_mgr_final(void);
int   buf_mgr_page_flusher_start(void);
void  buf_mgr_page_flusher_stop(void);

Page *buf_fix(const PageID pgid, enum buf_latch_mode mode, bool dotry, bool newpg);
Page *buf_fix_if_exist(const PageID pgid, enum buf_latch_mode mode);
void  buf_unfix(Page *pgptr);
void  buf_unfix_invalidate(Page *pgptr);
void  buf_unlatch_only(Page *pgptr);
void  buf_relatch_only(Page *pgptr, enum buf_latch_mode mode);
void  buf_uplatch_only(Page *pgptr, enum buf_latch_mode mode);
void  buf_dwlatch_only(Page *pgptr, enum buf_latch_mode mode);
Page *buf_pin_only(const PageID pgid, bool dotry, bool newpg);
void  buf_unpin_only(Page *pgptr);

Page *buf_permanent_pin(const PageID pgid, bool newpg);
void  buf_permanent_unpin(Page *pgptr);
void  buf_permanent_latch(Page *pgptr, enum buf_latch_mode mode);
void  buf_permanent_unlatch(Page *pgptr);

void  buf_invalidate_all(int pfxid);
void  buf_flush_all(int pfxid);
void  buf_flush_checkpoint(LogSN *last_chkpt_lsn, LogSN *min_oldest_lsn);

bool  buf_sanity_check(void);

int   buf_get_latch_mode(Page *pgptr);

void  buf_set_lsn(Page *pgptr, LogSN lsn);
void  buf_set_dirty(Page *pgptr);

bool  buf_is_the_only_writer(Page *pgptr);

void  buf_stat_start(void);
void  buf_stat_stop(void);
void  buf_stat_get(struct buffer_stat *stats);
void  buf_stat_reset(void);

#ifdef __cplusplus
}
#endif

#endif
