/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2016 JaM2in Co., Ltd.
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

#include "config.h"
//#include <syslog.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <memcached/extension.h>
#include <memcached/engine.h>
#include <time.h>
#include <errno.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdlib.h>
#include "protocol_extension.h"

#define NEW_DO_USERLOG 1

static EXTENSION_LOG_LEVEL current_log_level = EXTENSION_LOG_WARNING;
SERVER_HANDLE_V1 *sapi;

static const char *get_name(void) {
    return "userlog";
}

/* Instead of syslog which needs su authority, make and use user's log files : 2016.02
 * make & use 5 log files, rotate the files, append the created date to the file name
 * make a directory containing log files, named "/ARCUSlog" under the current directory */
/* Remove the duplicate check, instead, cut off over RATE_LIMIT msgs per 1 sec. like Syslog : 2016.03 */
/* definitions and data */
#define NUM_LOGFILE 5
#define MAX_LOGFILE_SIZE (1024*1024*20)  // for now, 20M
#define DEFAULT_LOGFILE_NAME  "arcus"
#define LOGDIRECTORY          "./ARCUSlog"
#define DATE_LEN              16          // by Syslog format
#define DEFAULT_RATELIMIT     200         // 200 default log rate limit burst per interval
#define DEFAULT_INTERVAL      5           // 5 default log rate limit interval(second)
#define LOGLIMIT_MAX_INTERVAL 60          // Maximum log rate limit interval
#define LOGLIMIT_MAX_BURST    50000       // Maximum log rate limit burst per interval

pthread_mutex_t log_lock;           // userlog thread lock
static char logfile_name[NUM_LOGFILE][40];
static int  current_file;           // currently which file is used among NUM_LOGFILE
static FILE *current_fp;
static int  current_flength;        // current file length
static char hname_pid[100];         // hostname + pid for prefix
static int  hname_len;              // length of hname_pid
static int  prefix_len;             // length of date_time + hname_pid
#ifdef NEW_DO_USERLOG
#else
static time_t prev_time;            // save the previous time
static int    msg_cnt;              // # of inputted messages in the interval
static int    drop_cnt;             // # of messages to be dropped in a second
static bool   drop_mode;            // indicate the DROP mode when over RATE_LIMIT
static int    samelog_cnt;
#endif
static bool   reduction_mode;       // duplication reduction or not. user setting or default(=False)
static char   prevtime_str[200];
static char   prev_log[2048];
static int    prev_len;
static unsigned int loglimit_interval; // log rate limit time interval. user setting or default value
static unsigned int loglimit_burst;    // log rate limit burst per interval. user setting or default value

static void do_make_logname(int filenum, char *ret_name)
{
    /* log file name : DIR-NAME + "/" + DEFAULT-NAME + sequence no(0~4) + date + ".log" */
    if (ret_name == NULL || filenum < 0 || filenum > NUM_LOGFILE) return;
    struct tm *day;
    time_t clock = time(0);
    day = localtime(&clock);
    sprintf(ret_name, "%s/%s%d_%04d%02d%02d", LOGDIRECTORY, DEFAULT_LOGFILE_NAME,
            filenum, day->tm_year + 1900, day->tm_mon + 1, day->tm_mday);
}

static void do_make_prefix(char *ret_string, time_t clock)
{
    /* prefix : syslog type : time(month(Eng) day hr:min:sec) + hostname + pid */
    struct tm *now;
    now = localtime(&clock);
    (void) strftime(ret_string, 50, "%h %e %T ", now);
    memcpy((void *)&ret_string[DATE_LEN], hname_pid, hname_len);
    ret_string[prefix_len] = 0;
}

static void do_print_msg(char *head_str, int head_len, char *body_str, int body_len)
{
    fprintf(current_fp, "%s%s", head_str, body_str);
    fflush(current_fp);
    current_flength += head_len + body_len;
}

static void do_print_dup(char *time_str, int dup_cnt)
{
    if (dup_cnt == 1) {  // The Syslog do like this.
        fprintf(current_fp, "%s%s", time_str, prev_log);
        current_flength += prefix_len + prev_len;
    } else {
        fprintf(current_fp, "%slast messages repeated %d times\n", time_str, dup_cnt);
        current_flength += prefix_len + 35;
    }
    fflush(current_fp);
}

#ifdef NEW_DO_USERLOG
#else
static void do_process_dup(char *time_str, int dup_cnt)
{
    do_print_dup(time_str, dup_cnt);
    samelog_cnt = 1;
}
#endif

static void do_save_log(char *log_buf, int len)
{
    memcpy(prev_log, log_buf, len);
    prev_log[len] = 0;
    prev_len = len;
}

#ifdef NEW_DO_USERLOG
static void do_print_drop_begin_message(time_t cur_time)
{
    char *tmpstr = " user_logger begins to drop messages due to rate-limiting\n";
    do_make_prefix(prevtime_str, cur_time);
    do_print_msg(prevtime_str, prefix_len, tmpstr, 60);
}

static void do_print_drop_stat_message(time_t cur_time, int drop_cnt)
{
    char tmpbuf[128];
    int tmplen = sprintf(tmpbuf, " user_logger lost %d messages due to rate-limiting (%d)\n",
                         drop_cnt, loglimit_burst);
    do_make_prefix(prevtime_str, cur_time);
    do_print_msg(prevtime_str, prefix_len, tmpbuf, tmplen);
}

static bool do_userlog_internal(time_t cur_time, char *body_buf, int len)
{
    if (reduction_mode) {
        static time_t samelog_time = 0;
        static int samelog_cnt = 0;
        if (len == prev_len && memcmp(body_buf, prev_log, len) == 0) {
            samelog_time = cur_time;
            samelog_cnt++; // Just increase the count and save the time
            return false;
        }

        // Two log messages are different. Print the count and the previous time,
        // then restart the count.
        if (samelog_cnt > 0) {
            do_make_prefix(prevtime_str, samelog_time);
            do_print_dup(prevtime_str, samelog_cnt);
            samelog_cnt = 0;
        }
        do_save_log(body_buf, len);
    }

    do_make_prefix(prevtime_str, cur_time);
    do_print_msg(prevtime_str, prefix_len, body_buf, len);
    return true;
}

static void do_userlog_basic(char *body_buf, int len)
{
    time_t cur_time = time(0);
    (void)do_userlog_internal(cur_time, body_buf, len);
}

static void do_userlog_ratelimit(char *body_buf, int len)
{
    static bool drop_mode = false;
    static int drop_cnt = 0;
    static int msg_cnt = 0;
    static time_t prev_time = 0;
    time_t cur_time = time(0);
    double time_diff = difftime(cur_time, prev_time);

    if (time_diff > (double)loglimit_interval) {
        if (drop_mode) {
            if (drop_cnt > 0) {
                do_print_drop_stat_message(cur_time, drop_cnt);
                drop_cnt = 0;
            }
            drop_mode = false;
        }
        prev_time = cur_time;
        msg_cnt = 0;
    } else {
        if (drop_mode) {
            drop_cnt++;
            return;
        }
    }

    if (do_userlog_internal(cur_time, body_buf, len)) {
        msg_cnt++;
        if (msg_cnt >= loglimit_burst) { // if duplicates remains, print that.
            do_print_drop_begin_message(cur_time);
            drop_cnt = 0;
            drop_mode = true;
        }
    }
}
#endif

#ifdef NEW_DO_USERLOG
#else
static void do_userlog(char *body_buf, int len)
{
    char prefix_buf[200];
    double time_diff;
    time_t cur_time = time(0);
    if (loglimit_interval == 0) { // 1. No Log Rate Limiting. Process all logs regardless of speed.
        if (reduction_mode == false) { // 1.1 No Reduction. The simplest case.
            do_make_prefix(prefix_buf, cur_time);
            do_print_msg(prefix_buf, prefix_len, body_buf, len);
        } else {                       // 1.2 Reduction-mode.
            if ((len != prev_len) || strcmp(body_buf, prev_log) != 0) {
                // Two log messages are different. Print the count and the previous time,
                // then restart the count.
                if (samelog_cnt > 1) do_process_dup(prevtime_str, samelog_cnt - 1);
                do_make_prefix(prevtime_str, cur_time);
                do_print_msg(prevtime_str, prefix_len, body_buf, len);
                do_save_log(body_buf, len);
            } else {              // The current log 'body_buf' and the previous log are same
                samelog_cnt++;    // Just increase the count and save the time
                do_make_prefix(prevtime_str, cur_time);
            }
        }
    } else {     // 2. Check Log Rate Limit
        time_diff = difftime(cur_time, prev_time);
        do_make_prefix(prefix_buf, cur_time);
        if (time_diff <= (double)loglimit_interval) { // check interval and burst rate.
            if (drop_mode == false) {  // not yet reach the limit
                if (reduction_mode == false) {
                    do_print_msg(prefix_buf, prefix_len, body_buf, len);
                } else {        // The most complex case : Reduction-ON, Log Limit-ON
                    if ((len != prev_len) || strcmp(body_buf, prev_log) != 0) {
                        if (samelog_cnt > 1) do_process_dup(prevtime_str, samelog_cnt - 1);
                        do_print_msg(prefix_buf, prefix_len, body_buf, len);
                        do_save_log(body_buf, len);
                    } else {    // The current log 'body_buf' and the previous log are same
                        samelog_cnt++;
                    }
                    memcpy(prevtime_str, prefix_buf, prefix_len);
                    prevtime_str[prefix_len] = 0;
                }
                msg_cnt++;
                if (msg_cnt >= loglimit_burst) { // if duplicates remains, print that.
                    if (samelog_cnt > 1) do_process_dup(prevtime_str, samelog_cnt - 1);
                    char *tmpstr = " user_logger begins to drop messages due to rate-limiting\n";
                    do_print_msg(prefix_buf, prefix_len, tmpstr, 60);
                    drop_mode = true;
                    drop_cnt = 0;
                }
            } else {  // drop_mode = true
                drop_cnt++;  // Increase the dropped log, regardless of Reduction-mode.
            }
        } else {   // 'loglimit_interval' seconds passed.
            if (drop_mode == true) {   // Start a new interval regardless of Reduction-mode.
                if (drop_cnt > 0) {
                    fprintf(current_fp, "%s user_logger lost %d messages due to rate-limiting (%d)\n",
                            prefix_buf, drop_cnt, loglimit_burst);
                    fflush(current_fp);
                    current_flength += prefix_len + 55;
                    drop_cnt = 0;
                }       // We have to consider the special case that drop_mode=true && drop_cnt=0
                do_print_msg(prefix_buf, prefix_len, body_buf, len);
                drop_mode = false;
                if (reduction_mode == true) {
                    samelog_cnt = 1;
                    do_save_log(body_buf, len);
                }
            } else {   // Normal status.
                if (reduction_mode == false)
                    do_print_msg(prefix_buf, prefix_len, body_buf, len);
                else {
                    if ((len != prev_len) || strcmp(body_buf, prev_log) != 0) {
                        if (samelog_cnt > 1) do_process_dup(prevtime_str, samelog_cnt - 1);
                        do_print_msg(prefix_buf, prefix_len, body_buf, len);
                        do_save_log(body_buf, len);
                    } else {  // The current log 'body_buf' and the previous log are same
                        samelog_cnt++;
                    }
                }
            }
            msg_cnt = 1;
            prev_time = cur_time;
            memcpy(prevtime_str, prefix_buf, prefix_len);
            prevtime_str[prefix_len] = 0;
        }
    }
}
#endif

static void logger_log(EXTENSION_LOG_LEVEL severity,
                       const void* client_cookie,
                       const char *fmt, ...)
{
    (void)client_cookie;
    if (severity >= current_log_level) {
        static rel_time_t last_time_log_open_failed = 0;
        char log_buf[2048];
        va_list ap;
        va_start(ap, fmt);
        int len = vsnprintf(log_buf, sizeof(log_buf), fmt, ap);
        va_end(ap);

        pthread_mutex_lock(&log_lock);
        if (current_fp == NULL) {
            if (last_time_log_open_failed < sapi->core->get_current_time()) { // 1 sec elapsed
                current_fp = fopen(logfile_name[current_file], "w");
                if (current_fp == NULL) {
                    last_time_log_open_failed = sapi->core->get_current_time();
                    fprintf(stderr, "\n FATAL error : can't open user log file: %s\n",
                            logfile_name[current_file]);
                    pthread_mutex_unlock(&log_lock);
                    return;
                }
            } else {
               pthread_mutex_unlock(&log_lock);
               return;
            }
        }

#ifdef NEW_DO_USERLOG
        if (loglimit_interval == 0) {
            do_userlog_basic(log_buf, len);
        } else {
            do_userlog_ratelimit(log_buf, len);
        }
#else
        do_userlog(log_buf, len); // userlog codes
#endif

        if (current_flength >= MAX_LOGFILE_SIZE) {
            fclose(current_fp);
            current_file++;
            if (current_file == NUM_LOGFILE) current_file = 0;
            remove(logfile_name[current_file]);    // To maintain the total # of log files
            do_make_logname(current_file, logfile_name[current_file]);
            current_fp = fopen(logfile_name[current_file], "w");
            current_flength = 0;
            if (current_fp == NULL) {
                last_time_log_open_failed = sapi->core->get_current_time();
                fprintf(stderr, "\n FATAL error : can't open user log file: %s\n", logfile_name[current_file]);
                pthread_mutex_unlock(&log_lock);
                return;
            }
        }
        pthread_mutex_unlock(&log_lock);
    }
}

static EXTENSION_LOGGER_DESCRIPTOR descriptor = {
    .get_name = get_name,
    .log = logger_log
};

static void on_log_level(const void *cookie, ENGINE_EVENT_TYPE type,
                         const void *event_data, const void *cb_data) {
    if (sapi != NULL) {
        current_log_level = sapi->log->get_level();
    }
}

MEMCACHED_PUBLIC_API
EXTENSION_ERROR_CODE memcached_extensions_initialize(const char *config,
                                                     GET_SERVER_API get_server_api) {

    sapi = get_server_api();
    if (sapi == NULL) {
        return EXTENSION_FATAL;
    }
    current_log_level = sapi->log->get_level();

    /* userlog codes */
    char buf[50];
    if (mkdir(LOGDIRECTORY, 0744) == -1) {
        if (errno != EEXIST) {
            fprintf(stderr, "\n FATAL error : can't make log Directory: %s\n", LOGDIRECTORY);
            return EXTENSION_FATAL;
        }
    }
    do_make_logname(0, logfile_name[0]);
    if ((current_fp = fopen(logfile_name[0], "w")) == NULL) {
        fprintf(stderr, "\n FATAL error : can't make log file: %s\n", logfile_name[0]);
        return EXTENSION_FATAL;
    }
    current_file = 0;
    current_flength = 0;
    if (gethostname(buf, sizeof(buf)-1) != 0) buf[0] = 0;
    sprintf(hname_pid, "%s memcached[%d]: ", buf, getpid());
    hname_len = strlen(hname_pid);
    prefix_len = DATE_LEN + hname_len;
    pthread_mutex_init(&log_lock, NULL);
    // User option
    char *env;
    unsigned int env_value;
    loglimit_interval = DEFAULT_INTERVAL;
    env = getenv("UserLogRateLimitInterval");
    if (env != NULL) {
        if (strlen(env) == 1 && env[0] == '0') {
            loglimit_interval = 0;
        } else {
            env_value = (unsigned int)atoi(env);
            if (env_value > 0 && env_value <= LOGLIMIT_MAX_INTERVAL)
                loglimit_interval = env_value;
        }
    }
    if (loglimit_interval != 0) {
#ifdef NEW_DO_USERLOG
#else
        prev_time = time(0);
        msg_cnt = 0;
        drop_cnt = 0;
        drop_mode = false;
#endif
        loglimit_burst = DEFAULT_RATELIMIT;
        env = getenv("UserLogRateLimitBurst");
        if (env != NULL) {
            env_value = (unsigned int)atoi(env);
            if (env_value > 0 && env_value <= LOGLIMIT_MAX_BURST)
                loglimit_burst = env_value;
        }
    }
    reduction_mode = false;
    env = getenv("UserLogReduction");
    if (env != NULL && strcmp(env, "on") == 0)
        reduction_mode = true;
    if (reduction_mode == true) {  // default is false
#ifdef NEW_DO_USERLOG
#else
        samelog_cnt = 1;
#endif
        prev_log[0] = 0;
        prev_len = 0;
        prevtime_str[0] = 0;
    }
    /* end of userlog codes */

    if (!sapi->extension->register_extension(EXTENSION_LOGGER, &descriptor)) {
        return EXTENSION_FATAL;
    }

    sapi->callback->register_callback(NULL, ON_LOG_LEVEL, on_log_level, NULL);
    return EXTENSION_SUCCESS;
}
