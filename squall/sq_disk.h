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
#ifndef SQ_DISK_H
#define SQ_DISK_H

#include <fcntl.h>
#include <aio.h>

#ifdef __cplusplus
extern "C" {
#endif

int     disk_open(const char *fname, int flags, int mode);
int     disk_close(int fd);
off_t   disk_lseek(int fd, off_t offset, int whence);
int     disk_fsync(int fd);
int     disk_unlink(const char *fname);

ssize_t disk_page_pwrite(int fd, void *page, uint32_t pgnum, size_t pgsiz);
ssize_t disk_page_pread(int fd, void *page, uint32_t pgnum, size_t pgsiz);
ssize_t disk_byte_pwrite(int fd, void *buf, size_t count, off_t offset);
ssize_t disk_byte_pread(int fd, void *buf, size_t count, off_t offset);
ssize_t disk_byte_write(int fd, void *buf, size_t count);
ssize_t disk_byte_read(int fd, void *buf, size_t count);

int     disk_page_aio_read(int fd, void *buf, uint32_t pgnum, uint32_t pgsiz, struct aiocb *task);
int     disk_page_aio_write(int fd, void *buf, uint32_t pgnum, uint32_t pgsiz, struct aiocb *task);
int     disk_aio_sync(int fd, struct aiocb *task);
int     disk_aio_wait(struct aiocb *task);

#ifdef __cplusplus
}
#endif

#endif // SQ_DISK_H
