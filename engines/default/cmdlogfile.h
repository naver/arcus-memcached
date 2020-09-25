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
#ifndef CMDLOGFILE_H
#define CMDLOGFILE_H

#include "cmdlogmgr.h"

/* external log file functions */
void cmdlog_file_write(char *log_ptr, uint32_t log_size, bool dual_write);
void cmdlog_file_complete_dual_write(void);
bool cmdlog_file_dual_write_finished(void);
void cmdlog_file_sync(void);

int    cmdlog_file_open(char *path);
void   cmdlog_file_close(void);
void   cmdlog_file_init(struct default_engine* engine);
void   cmdlog_file_final(void);
int    cmdlog_file_apply(void);
size_t cmdlog_file_getsize(void);

void   cmdlog_get_fsync_lsn(LogSN *lsn);
int    cmdlog_get_next_fd(void);
#endif
