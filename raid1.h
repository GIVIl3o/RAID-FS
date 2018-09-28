#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

int server_cnt;

struct server{
	char ip[4096];
	int port;
	
	int status[2];
	char* mem;
	int cur_reserved;
	int cur_pos;
};

struct server servs[20000];

void raid1_clone_data(struct server* srv_from,struct server* srv_to);

int raid1_getattr(const char *path, struct stat *stbuf);

int raid1_open(const char* path, struct fuse_file_info* fi);

int raid1_read(const char *path, char *to, size_t size, off_t offset, struct fuse_file_info* fi);

int raid1_write(const char* path, const char* from, size_t size, off_t offset, struct fuse_file_info* fi);

int raid1_release(const char* path, struct fuse_file_info* fi);
