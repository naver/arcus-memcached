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
#ifndef CHECKPOINT_H
#define CHECKPOINT_H

#include "chkpt_snapshot.h"

#ifdef ENABLE_PERSISTENCE
/* Recovery Functions */
int chkpt_recovery_analysis(void);
int chkpt_recovery_redo(void);

ENGINE_ERROR_CODE chkpt_init(struct default_engine* engine);
ENGINE_ERROR_CODE chkpt_thread_start(void);

void chkpt_thread_stop(void);
void chkpt_final(void);

int64_t chkpt_get_lasttime(void);
#ifdef STATS_PERSISTENCE

typedef struct _chkpt_last {
    double   recovery_elapsed_time_sec;
    bool     last_chkpt_in_progress;
    int      last_chkpt_failure_count;
    int64_t  last_chkpt_start_time;
    double   last_chkpt_elapsed_time_sec;
    size_t   last_chkpt_snapshot_filesize_bytes;/* last snapshot log file size */
    size_t   current_command_log_filesize_bytes;/* current_command_log file size */
} chkpt_last;

ENGINE_ERROR_CODE chkpt_last_init(void);
chkpt_last get_chkpt_last(void);

#endif
#endif

#endif
