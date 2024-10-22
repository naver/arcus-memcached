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

extern bool cmdlog_in_use;

void cmdlog_init(int port, EXTENSION_LOGGER_DESCRIPTOR *logger);
void cmdlog_final(void);
int cmdlog_start(char *file_path, bool *already_started);
void cmdlog_stop(bool *already_stopped);
char *cmdlog_stats(void);
void cmdlog_write(char *client_ip, char *command);
#endif
