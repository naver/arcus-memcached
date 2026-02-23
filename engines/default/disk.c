#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "disk.h"

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

off_t disk_lseek(int fd, off_t offset, int whence)
{
    off_t ret = lseek(fd, offset, whence);
    return ret;
}

ssize_t disk_read(int fd, void *buf, size_t count)
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

ssize_t disk_write(int fd, void *buf, size_t count)
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

int disk_fsync(int fd)
{
    if (fsync(fd) != 0) {
        return -1;
    }
    return 0;
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
