/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2015 JaM2in Co., Ltd.
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
#ifndef CMDLOG_H
#define CMDLOG_H

#include "memcached/extension_loggers.h"

#define COMMAND_LOGGING
#define CMDLOG_INPUT_SIZE 400
#define CMDLOG_FILENAME_LENGTH 256 /* filename plus path's length */
#define CMDLOG_DIRPATH_LENGTH 128 /* directory path's length */

#define CMDLOG_NOT_STARTED   0  /* not started */
#define CMDLOG_EXPLICIT_STOP 1  /* stop by user request */
#define CMDLOG_OVERFLOW_STOP 2  /* stop by command log overflow */
#define CMDLOG_FLUSHERR_STOP 3  /* stop by flush operation error */
#define CMDLOG_RUNNING       4  /* running */

/* command log stats structure */
struct cmd_log_stats {
    int bgndate, bgntime;
    int enddate, endtime;
    int file_count;
    volatile int state;        /* command log module state */
    uint32_t entered_commands; /* number of entered command */
    uint32_t skipped_commands; /* number of skipped command */
    char dirpath[CMDLOG_DIRPATH_LENGTH];
};

void cmdlog_init(int port, EXTENSION_LOGGER_DESCRIPTOR *logger);
void cmdlog_final(void);
int cmdlog_start(char *file_path, bool *already_started);
void cmdlog_stop(bool *already_stopped);
struct cmd_log_stats *cmdlog_stats(void);
bool cmdlog_write(char client_ip[], char *command);
#endif
