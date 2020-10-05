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
#ifndef CMDLOGBUF_H
#define CMDLOGBUF_H

#include "cmdlogmgr.h"
#include "cmdlogrec.h"

/* external log buffer functions */
void cmdlog_buff_write(LogRec *logrec, log_waiter_t *waiter, bool dual_write);
void cmdlog_buff_flush(LogSN *upto_lsn);
#ifdef ENABLE_PERSISTENCE_03_REQUEST_FLUSH_IN_SYNC
void cmdlog_buff_flush_request(LogSN *upto_lsn);
#endif
void cmdlog_buff_complete_dual_write(bool success);

void cmdlog_get_write_lsn(LogSN *lsn);
void cmdlog_get_flush_lsn(LogSN *lsn);

ENGINE_ERROR_CODE cmdlog_buf_init(struct default_engine *engine);
void              cmdlog_buf_final(void);
ENGINE_ERROR_CODE cmdlog_buf_flush_thread_start(void);
void              cmdlog_buf_flush_thread_stop(void);

#endif
