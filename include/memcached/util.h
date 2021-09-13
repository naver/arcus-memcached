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
#ifndef UTIL_H
#define UTIL_H
/*
 * Wrappers around strtoull/strtoll that are safer and easier to
 * use.  For tests and assumptions, see internal_tests.c.
 *
 * str   a NULL-terminated base decimal 10 unsigned integer
 * out   out parameter, if conversion succeeded
 *
 * returns true if conversion succeeded.
 */
#include <config.h>
#include <memcached/visibility.h>
#ifdef __cplusplus
extern "C" {
#endif

MEMCACHED_PUBLIC_API bool safe_strtoull(const char *str, uint64_t *out);
MEMCACHED_PUBLIC_API bool safe_strtoll(const char *str, int64_t *out);
MEMCACHED_PUBLIC_API bool safe_strtoul(const char *str, uint32_t *out);
MEMCACHED_PUBLIC_API bool safe_strtol(const char *str, int32_t *out);
MEMCACHED_PUBLIC_API bool safe_strtof(const char *str, float *out);
MEMCACHED_PUBLIC_API bool safe_strtohexa(const char *str, unsigned char *bin, const int size);
MEMCACHED_PUBLIC_API void safe_hexatostr(const unsigned char *bin, const int size, char *str);
MEMCACHED_PUBLIC_API bool mc_isvalidname(const char *str, int len);
MEMCACHED_PUBLIC_API bool string_pattern_match(const char *text, int text_len,
                                               const char *pattern, int pattern_len);

MEMCACHED_PUBLIC_API int64_t getnowdatetime_int(void);
MEMCACHED_PUBLIC_API int     getnowdate_int(void);
MEMCACHED_PUBLIC_API int     getnowtime_int(void);

#ifndef HAVE_HTONLL
#define htonll mc_htonll
#define ntohll mc_ntohll

MEMCACHED_PUBLIC_API extern uint64_t mc_htonll(uint64_t);
MEMCACHED_PUBLIC_API extern uint64_t mc_ntohll(uint64_t);
#endif

#ifdef __GCC
# define __gcc_attribute__ __attribute__
#else
# define __gcc_attribute__(x)
#endif

/**
 * Vararg variant of perror that makes for more useful error messages
 * when reporting with parameters.
 *
 * @param fmt a printf format
 */
MEMCACHED_PUBLIC_API void vperror(const char *fmt, ...)
    __gcc_attribute__ ((format (printf, 1, 2)));

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif
