/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
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
#ifndef _UTIL_COMMON_H_
#define _UTIL_COMMON_H_

/* Utility functions.  Only the replication code uses them. */

/* logger is actually EXTENSION_LOGGER_DESCRIPTOR.  Use void to avoid having
 * to include memcached header in files like msg_chan.c.  FIXME.
 */
void print_log_init(void *logger);

void print_log(const char *fmt, ...)
  __attribute__ ((__format__ (__printf__, 1, 2)));

void print_errlog(const char *file, int line, int err, const char *fmt, ...)
  __attribute__ ((__format__ (__printf__, 4, 5)));

void print_log_hexdump(const char *data, int len, const char *fmt, ...)
  __attribute__ ((__format__ (__printf__, 3, 4)));

void print_hexdump(const char *data, int len);

void gettime(uint64_t *msec, struct timeval *ptv, struct timespec *pts);
uint64_t getusec(void);
int fill_sockaddr(const char *host, struct sockaddr_in *addr);
int parse_hostport(const char *addr_str, struct sockaddr_in *addr, char **host);

#define ERRLOG(err, ...) do {                         \
  print_errlog(__FILE__, __LINE__, err, __VA_ARGS__); \
} while(0)

#define ERROR_DIE(...) do {                         \
  printf(__VA_ARGS__);                              \
  printf(" die %s:%d\n", __FILE__, __LINE__);       \
  abort();                                          \
} while(0)

#define PERROR_DIE(...) do {                                                 \
  printf(__VA_ARGS__);                                                       \
  printf(" %s(%d) die %s:%d\n", strerror(errno), errno, __FILE__, __LINE__); \
  abort();                                                                   \
} while(0)

#endif /* !defined(_UTIL_COMMON_H_) */
