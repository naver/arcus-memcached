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

/* external log functions */
void log_record_write(LogRec *logrec, log_waiter_t *waiter, bool dual_write);
void log_file_sync(LogSN *prev_flush_lsn);

/* FIXME: remove later, if not used */
//void log_get_write_lsn(LogSN *lsn);
void log_get_flush_lsn(LogSN *lsn);
void log_get_fsync_lsn(LogSN *lsn);

int               cmdlog_file_open(char *path);
void              cmdlog_file_close(bool shutdown);
#ifdef ENABLE_PERSISTENCE_CHKPT_INIT
int               cmdlog_file_trim_incompleted_command(void);
#endif
ENGINE_ERROR_CODE cmdlog_buf_init(struct default_engine *engine);
void              cmdlog_buf_final(void);
ENGINE_ERROR_CODE cmdlog_buf_flush_thread_start(void);
void              cmdlog_buf_flush_thread_stop(void);
#ifdef ENABLE_PERSISTENCE_CHKPT_INIT
ENGINE_ERROR_CODE cmdlog_recovery(const char *path);
#endif

#endif
