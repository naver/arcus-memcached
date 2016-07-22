/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
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
#ifndef MEMCACHED_CLUSTER_CONFIG_H
#define MEMCACHED_CLUSTER_CONFIG_H

#include "memcached/extension_loggers.h"

struct cluster_config;

struct   cluster_config *cluster_config_init(const char *hostport, size_t hostport_len,
                                             EXTENSION_LOGGER_DESCRIPTOR *logger, int verbose);
void     cluster_config_free(struct cluster_config *config);

uint32_t cluster_config_self_id(struct cluster_config *config);
int      cluster_config_num_servers(struct cluster_config *config);
int      cluster_config_num_continuum(struct cluster_config *conifg);

int      cluster_config_reconfigure(struct cluster_config *config,
                                    char **server_list, size_t num_servers);
int      cluster_config_key_is_mine(struct cluster_config *config,
                                    const char *key, size_t key_len, bool *mine,
                                    uint32_t *key_id, uint32_t *self_id);
uint32_t cluster_config_ketama_hash(struct cluster_config *config,
                                    const char *key, size_t nkey);
int      cluster_config_ketama_hslice(struct cluster_config *config,
                                      uint32_t hvalue);
/**** OLD CODE ****
uint32_t cluster_config_ketama_hash(struct cluster_config *config,
                                    const char *key, size_t nkey, int *hslice);
*******************/
#endif
