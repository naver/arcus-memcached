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
#include "config.h"
#include <stdio.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <time.h>

#include <memcached/util.h>

bool safe_strtoull(const char *str, uint64_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    unsigned long long ull = strtoull(str, &endptr, 10);
    if (errno == ERANGE)
        return false;
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long long) ull < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }
        *out = ull;
        return true;
    }
    return false;
}

bool safe_strtoll(const char *str, int64_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    long long ll = strtoll(str, &endptr, 10);
    if (errno == ERANGE)
        return false;
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = ll;
        return true;
    }
    return false;
}

bool safe_strtoul(const char *str, uint32_t *out) {
    char *endptr = NULL;
    unsigned long l = 0;
    assert(out);
    assert(str);
    *out = 0;
    errno = 0;

    l = strtoul(str, &endptr, 10);
    if (errno == ERANGE) {
        return false;
    }

    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long) l < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }
        *out = l;
        return true;
    }

    return false;
}

bool safe_strtol(const char *str, int32_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    long l = strtol(str, &endptr, 10);
    if (errno == ERANGE)
        return false;
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = l;
        return true;
    }
    return false;
}

bool safe_strtof(const char *str, float *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    float l = strtof(str, &endptr);
    if (errno == ERANGE)
        return false;
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = l;
        return true;
    }
    return false;
}

bool safe_strtohexa(const char *str, unsigned char *bin, const int size) {
    assert(bin != NULL);
    int  slen = strlen(str);
    char ch1, ch2;

    if (slen <= 0 || slen > (2*size) || (slen%2) != 0) {
        return false;
    }
    for (int i=0; i < (slen/2); i++) {
        ch1 = str[2*i]; ch2 = str[2*i+1];
        if      (ch1 >= '0' && ch1 <= '9') bin[i] = (ch1 - '0');
        else if (ch1 >= 'A' && ch1 <= 'F') bin[i] = (ch1 - 'A' + 10);
        else if (ch1 >= 'a' && ch1 <= 'f') bin[i] = (ch1 - 'a' + 10);
        else return false;
        if      (ch2 >= '0' && ch2 <= '9') bin[i] = (bin[i] << 4) + (ch2 - '0');
        else if (ch2 >= 'A' && ch2 <= 'F') bin[i] = (bin[i] << 4) + (ch2 - 'A' + 10);
        else if (ch2 >= 'a' && ch2 <= 'f') bin[i] = (bin[i] << 4) + (ch2 - 'a' + 10);
        else return false;
    }
    return true;
}

void safe_hexatostr(const unsigned char *bin, const int size, char *str) {
    assert(str != NULL);

    for (int i=0; i < size; i++) {
        str[(i*2)  ] = (bin[i] & 0xF0) >> 4;
        str[(i*2)+1] = (bin[i] & 0x0F);
        if (str[(i*2)  ] < 10) str[(i*2)  ] += ('0');
        else                   str[(i*2)  ] += ('A' - 10);
        if (str[(i*2)+1] < 10) str[(i*2)+1] += ('0');
        else                   str[(i*2)+1] += ('A' - 10);
    }
    str[size*2] = '\0';
}

/* prefix name check */
static inline bool mc_isnamechar(int c) {
    return ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') ||
            (c == '_') || (c == '-') || (c == '+') || (c == '.'));
}

static inline bool mc_ishyphon(int c) {
    return (c == '-');
}

bool mc_isvalidname(const char *str, int len) {
    bool valid;

    if (mc_ishyphon(str[0])) {
        return false;
    }
    valid = true;
    for (int i = 0; i < len; i++) {
        if (!mc_isnamechar(str[i])) {
            valid = false; break;
        }
    }
    return valid;
}

void vperror(const char *fmt, ...) {
    int old_errno = errno;
    char buf[1024];
    va_list ap;

    va_start(ap, fmt);
    if (vsnprintf(buf, sizeof(buf), fmt, ap) == -1) {
        buf[sizeof(buf) - 1] = '\0';
    }
    va_end(ap);

    errno = old_errno;

    perror(buf);
}

#ifndef HAVE_HTONLL
static uint64_t mc_swap64(uint64_t in) {
#ifndef WORDS_BIGENDIAN
    /* Little endian, flip the bytes around until someone makes a faster/better
    * way to do this. */
    int64_t rv = 0;
    int i = 0;
     for(i = 0; i<8; i++) {
        rv = (rv << 8) | (in & 0xff);
        in >>= 8;
     }
    return rv;
#else
    /* big-endian machines don't need byte swapping */
    return in;
#endif
}

uint64_t mc_ntohll(uint64_t val) {
   return mc_swap64(val);
}

uint64_t mc_htonll(uint64_t val) {
   return mc_swap64(val);
}
#endif

/* integer represents format : YYYYMMDDHHMMSS (ex. 20210806152049) */
int64_t getnowdatetime_int(void)
{
    char buf[24] = {0};
    int64_t res;
    time_t clock = time(0);
    struct tm *lctime = localtime(&clock);

    /* year(YYYY) month(01-12) day(01-31) hour(00-23) minute(00-59) second(00-61). */
    strftime(buf, 24, "%Y%m%d%H%M%S", lctime);
    sscanf(buf, "%" SCNd64, &res);
    return res;
}

/* integer represents format : YYYYMMDD (ex. 20210806) */
int getnowdate_int(void)
{
    char buf[16] = {0};
    int res;
    time_t clock = time(0);
    struct tm *lctime = localtime(&clock);

    /* year(YYYY) month(01-12) day(01-31). */
    strftime(buf, 16, "%Y%m%d", lctime);
    sscanf(buf, "%d", &res);
    return res;
}

/* integer represents format : HHMMSS (ex. 152049) */
int getnowtime_int(void)
{
    char buf[8] = {0};
    int res;
    time_t clock = time(0);
    struct tm *lctime = localtime(&clock);

    /* hour(00-23) minute(00-59) second(00-61). */
    strftime(buf, 8, "%H%M%S", lctime);
    sscanf(buf, "%d", &res);
    return res;
}

/*
 * glob pattern('?', '*') string match algorithm from :
 * https://www.codeproject.com/Articles/5163931/Fast-String-Matching-with-Wildcards-Globs-and-Giti
 *
 * The execution time of this algorithm is linear in the length of the pattern and string for typical cases.
 * In the worst case, this algorithm takes quadratic time (to see why, consider pattern
 * *aaa…aaab with ½n a’s and string aaa…aaa with n a’s, which requires ¼n2 comparisons before failing.)
 */
bool string_pattern_match(const char *text, int text_len, const char *pattern, int pattern_len)
{
    const char *text_backup = NULL;
    const char *patt_backup = NULL;
    int tlen_backup = 0;
    int plen_backup = 0;
    while (text_len)
    {
        if (*pattern == '\\' && *(pattern+1) == *text) {
            text++;
            pattern += 2;
            text_len--;
            pattern_len -= 2;
        }
        else if (*pattern == '*') {
            /* new star-loop: backup positions in pattern and text. */
            text_backup = text;
            patt_backup = ++pattern;
            tlen_backup = text_len;
            plen_backup = --pattern_len;
        }
        else if (*pattern == '?' || *pattern == *text) {
            /* ? match any character or match the current non-NULL character */
            text++;
            pattern++;
            text_len--;
            pattern_len--;
        }
        else {
            /* if no stars we fail to match */
            if (patt_backup == NULL) {
                return false;
            }
            /* star-loop: backtrack to the last * by restoring the backup positions
             * in the pattern and text
             */
            text        = ++text_backup;
            pattern     = patt_backup;
            text_len    = --tlen_backup;
            pattern_len = plen_backup;
        }
    }
    /* ignore trailing stars */
    while (pattern_len && *pattern == '*') {
        pattern++;
        pattern_len--;
    }
    /* at end of text means success if nothing else is left to match */
    return (pattern_len == 0);
}
