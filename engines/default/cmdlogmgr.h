/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2019 JaM2in Co., Ltd.
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
#ifndef CMDLOGMGR_H
#define CMDLOGMGR_H

#include "cmdlogrec.h"

/* undefine PERSISTENCE_ACTION macros */
#undef PERSISTENCE_ACTION_BEGIN
#undef PERSISTENCE_ACTION_END

/* redefine PERSISTENCE_ACTION macros (FIXME: define the error code) */
#define PERSISTENCE_ACTION_BEGIN(c,u) \
    do { \
        if (config->use_persistence) { \
            if (cmdlog_waiter_begin((c),(u)) == NULL) \
                return ENGINE_ENOMEM; \
        } \
    } while(0)
#define PERSISTENCE_ACTION_END(r) \
    do { \
        log_waiter_t *waiter = cmdlog_get_my_waiter(); \
        if (waiter) cmdlog_waiter_end(waiter, &r); \
    } while(0)

/* command log manager entry structure */
typedef struct _log_waiter {
    struct _log_waiter *wait_next;
    struct _log_waiter *free_next;
    LogSN               lsn;
    uint8_t             updtype;
    bool                elem_insert_with_create;
    bool                elem_delete_with_drop;
    bool                generated_range_clog;
    const void         *cookie;
} log_waiter_t;

/* external command log manager functions */
log_waiter_t      *cmdlog_waiter_begin(const void *cookie, uint8_t updtype);
void               cmdlog_waiter_end(log_waiter_t *waiter, ENGINE_ERROR_CODE *result);
log_waiter_t      *cmdlog_get_my_waiter(void);
ENGINE_ERROR_CODE  cmdlog_waiter_init(struct default_engine *engine);
void               cmdlog_waiter_final(void);

ENGINE_ERROR_CODE  cmdlog_mgr_init(struct default_engine *engine_ptr);
void               cmdlog_mgr_final(void);

/* Generate Log Record Functions */
void cmdlog_generate_link_item(hash_item *it);
void cmdlog_generate_unlink_item(hash_item *it);
void cmdlog_generate_flush_item(const char *prefix, const int nprefix, const time_t when);
void cmdlog_generate_setattr(hash_item *it, const ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt);
void cmdlog_generate_list_elem_insert(hash_item *it, const uint32_t total, const int index, list_elem_item *elem);
void cmdlog_generate_list_elem_delete(hash_item *it, const uint32_t total, const int index, const uint32_t count);
void cmdlog_generate_map_elem_insert(hash_item *it, map_elem_item *elem);
void cmdlog_generate_map_elem_delete(hash_item *it, map_elem_item *elem);
void cmdlog_generate_set_elem_insert(hash_item *it, set_elem_item *elem);
void cmdlog_generate_set_elem_delete(hash_item *it, set_elem_item *elem);
void cmdlog_generate_btree_elem_insert(hash_item *it, btree_elem_item *elem);
void cmdlog_generate_btree_elem_delete(hash_item *it, btree_elem_item *elem);
void cmdlog_generate_btree_elem_delete_logical(hash_item *it, const bkey_range *bkrange,
                                               const eflag_filter *efilter, uint32_t offset, uint32_t reqcount);
void cmdlog_generate_operation_range(bool begin);

void cmdlog_set_chkpt_scan(void *scanp);
void cmdlog_reset_chkpt_scan(bool chkpt_success);

#endif
