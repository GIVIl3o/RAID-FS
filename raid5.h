#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

int clone_data_raid5(int server_pos);

int raid5_getattr(const char *path, struct stat *stbuf);

int raid5_open(const char* path, struct fuse_file_info* fi);

int raid5_read(const char *path, char *to, size_t size, off_t offset, struct fuse_file_info* fi);

int raid5_write(const char* path, const char* from, size_t size, off_t offset, struct fuse_file_info* fi);

int raid5_release(const char* path, struct fuse_file_info* fi);
