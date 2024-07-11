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
/*
 * Detailed statistics management. For simple stats like total number of
 * "get" requests, we use inline code in memcached.c and friends, but when
 * stats detail mode is activated, the code here records more information.
 *
 * Author:
 *   Steven Grimm <sgrimm@facebook.com>
 */
#include "stats.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef STATS_TCP_RETRANS

static void rtrim(char **src) {
    if (src == NULL || *src == NULL || strlen(*src) == 0) {
        return;
    }

    char *str = *src;
    for (int i = strlen(str) - 1; i >= 0; i--) {
        if (!isspace(str[i])) {
            break;
        }

        str[i] = '\0';
    }

    *src = str;
}

int64_t stats_tcp_retrans(void) {
    char *key_line = NULL;
    char *value_line = NULL;
    int64_t tcp_retrans = -1;

    FILE *fp = fopen("/proc/net/snmp", "r");
    if (fp == NULL) {
        goto done;
    }

    size_t key_len = 0;
    size_t value_len = 0;
    ssize_t key_read = -1;
    ssize_t value_read = -1;

    char *prefix = "Tcp:";
    size_t prefix_len = strlen(prefix);

    while ((key_read = getline(&key_line, &key_len, fp)) != -1) {
        if (strncmp(prefix, key_line, prefix_len) != 0) {
            continue;
        }

        if ((value_read = getline(&value_line, &value_len, fp)) != -1) {
            break;
        }
    }

    if (key_read < 0 || value_read < 0) {
        goto done;
    }

    /*
     * below lines are examples of key_line and value_line.
     * for examples below, tcp_retrans = 9814784
     *
     * Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors
     * Tcp: 1 200 120000 -1 231273424 45758374 43975115 12045288 360 9853401239 10104984649 9814784 12765 17318799 883
     */

    char *delimiter = " ";
    char *retrans_segs = "RetransSegs";

    char *key_next = NULL;
    char *value_next = NULL;

    char *key_token = strtok_r(key_line, delimiter, &key_next);
    char *value_token = strtok_r(value_line, delimiter, &value_next);

    // first token is always "Tcp:"
    key_token = strtok_r(NULL, delimiter, &key_next);
    value_token = strtok_r(NULL, delimiter, &value_next);

    while (key_token && value_token) {
        // last token always ends with "\n"
        rtrim(&key_token);
        rtrim(&value_token);

        if (strcmp(key_token, retrans_segs) == 0) {
            char *end = NULL;
            errno = 0;
            tcp_retrans = strtoll(value_token, &end, 10);

            if (errno > 0 || *end != '\0') {
                tcp_retrans = -1;
            }
            goto done;
        }

        key_token = strtok_r(NULL, delimiter, &key_next);
        value_token = strtok_r(NULL, delimiter, &value_next);
    }

done:
    if (fp != NULL) {
        fclose(fp);
    }

    if (key_line != NULL) {
        free(key_line);
    }

    if (value_line != NULL) {
        free(value_line);
    }

    return tcp_retrans;
}
#endif
