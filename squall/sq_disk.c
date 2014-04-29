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
#define _XOPEN_SOURCE 500
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <strings.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "squall_config.h"
#include "sq_disk.h"

/*
 * Disk Public Functions
 */

int disk_open(const char *fname, int flags, int mode)
{
    int fd;
    while (1) {
        if ((fd = open(fname, flags, mode)) == -1) {
            if (errno == EINTR) continue;
        }
        break;
    }
    return fd;
}

int disk_close(int fd)
{
    while (1) {
        if (close(fd) != 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        break;
    }
    return 0;
}

off_t disk_lseek(int fd, off_t offset, int whence)
{
    off_t ret = lseek(fd, offset, whence);
    return ret;
}

int disk_fsync(int fd)
{
    if (fsync(fd) != 0) {
        return -1;
    }
    return 0;
}

int disk_unlink(const char *fname)
{
    if (unlink(fname) != 0) {
        return -1;
    }
    return 0;
}

/* Synchronous IO */

ssize_t disk_page_pwrite(int fd, void *page, uint32_t pgnum, size_t pgsiz)
{
    char   *bfptr = (char*)page;
    off_t   nskip = pgnum*pgsiz;
    ssize_t nleft = pgsiz;
    ssize_t nwrite;

    while (nleft > 0) {
        nwrite = pwrite(fd, bfptr, nleft, nskip);
        if (nwrite == 0) break;
        if (nwrite <  0) {
            if (errno == EINTR) continue;
            return nwrite;
        }
        nleft -= nwrite;
        bfptr += nwrite;
        nskip += nwrite;
    }
    return (pgsiz - nleft);
}

ssize_t disk_page_pread(int fd, void *page, uint32_t pgnum, size_t pgsiz)
{
    char   *bfptr = (char*)page;
    off_t   nskip = pgnum*pgsiz;
    ssize_t nleft = pgsiz;
    ssize_t nread;

    while (nleft > 0) {
        nread = pread(fd, bfptr, nleft, nskip);
        if (nread == 0) break;
        if (nread <  0) {
            if (errno == EINTR) continue;
            return nread;
        }
        nleft -= nread;
        bfptr += nread;
        nskip += nread;
    }
    return (pgsiz - nleft);
}

ssize_t disk_byte_pwrite(int fd, void *buf, size_t count, off_t offset)
{
    char   *bfptr = (char*)buf;
    off_t   nskip = offset;
    ssize_t nleft = count;
    ssize_t nwrite;

    while (nleft > 0) {
        nwrite = pwrite(fd, bfptr, nleft, nskip);
        if (nwrite == 0) break;
        if (nwrite <  0) {
            if (errno == EINTR) continue;
            return nwrite;
        }
        nleft -= nwrite;
        bfptr += nwrite;
        nskip += nwrite;
    }
    return (count - nleft);
}

ssize_t disk_byte_pread(int fd, void *buf, size_t count, off_t offset)
{
    char   *bfptr = (char*)buf;
    off_t   nskip = offset;
    ssize_t nleft = count;
    ssize_t nread;

    while (nleft > 0) {
        nread = pread(fd, bfptr, nleft, nskip);
        if (nread == 0) break;
        if (nread <  0) {
            if (errno == EINTR) continue;
            return nread;
        }
        nleft -= nread;
        bfptr += nread;
        nskip += nread;
    }
    return (count - nleft);
}

ssize_t disk_byte_write(int fd, void *buf, size_t count)
{
    char   *bfptr = (char*)buf;
    ssize_t nleft = count;
    ssize_t nwrite;

    while (nleft > 0) {
        nwrite = write(fd, bfptr, nleft);
        if (nwrite == 0) break;
        if (nwrite <  0) {
            if (errno == EINTR) continue;
            return nwrite;
        }
        nleft -= nwrite;
        bfptr += nwrite;
    }
    return (count - nleft);
}

ssize_t disk_byte_read(int fd, void *buf, size_t count)
{
    char   *bfptr = (char*)buf;
    ssize_t nleft = count;
    ssize_t nread;

    while (nleft > 0) {
        nread = read(fd, bfptr, nleft);
        if (nread == 0) break;
        if (nread <  0) {
            if (errno == EINTR) continue;
            return nread;
        }
        nleft -= nread;
        bfptr += nread;
    }
    return (count - nleft);
}

/* POSIX Asynchronous IO */

int disk_page_aio_read(int fd, void *buf, uint32_t pgnum, uint32_t pgsiz, struct aiocb *task)
{
    task->aio_fildes = fd;
    task->aio_buf    = buf;
    task->aio_nbytes = pgsiz;
    task->aio_offset = (off_t)(pgnum*pgsiz);

    if (aio_read(task) != 0) {
        return -1;
    }
    return 0;
}

int disk_page_aio_write(int fd, void *buf, uint32_t pgnum, uint32_t pgsiz, struct aiocb *task)
{
    task->aio_fildes = fd;
    task->aio_buf    = buf;
    task->aio_nbytes = pgsiz;
    task->aio_offset = (off_t)(pgnum*pgsiz);

    if (aio_write(task) != 0) {
        return -1;
    }
    return 0;
}

int disk_aio_sync(int fd, struct aiocb *task)
{
    task->aio_fildes = fd;
    task->aio_nbytes = 0;

    if (aio_fsync(O_SYNC, task) != 0) {
        return -1;
    }
    return 0;
}

int disk_aio_wait(struct aiocb *task)
{
    const struct aiocb *list[1];
    list[0] = task;

    while (1) {
        if (aio_suspend(list, 1, NULL) != 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        break;
    }
    if (aio_return(task) != task->aio_nbytes) {
        return -1;
    }
    return 0;
}
