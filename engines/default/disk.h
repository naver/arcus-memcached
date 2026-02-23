#ifndef DISK_H
#define DISK_H

int disk_open(const char *fname, int flags, int mode);
off_t disk_lseek(int fd, off_t offset, int whence);
ssize_t disk_read(int fd, void *buf, size_t count);
ssize_t disk_write(int fd, void *buf, size_t count);
int disk_fsync(int fd);
int disk_close(int fd);

#endif
