
#include "include/uthash.h"


int cache_size;

int cache_blocks_cnt;

struct block{
	char* path;
	int path_size;

	int data_size;
	char* data;
	int malloced_size;

	UT_hash_handle hh;

	struct block* prev;
	struct block* next;
};

char* cache;

struct block* list;
struct block* hash;


void save_atribute(const char* path,struct stat* st);
int get_atribute(const char* path,struct stat* st);
void update_atribute(const char* path,long long index);

void save_file_content(const char* path,char* from,long long offset);
void update_file_content(const char* path,char* from,long long offset,int size);
int get_file_content(const char* path,char* to,long long offset,int size);

void save_directory_content(const char* path,char* from,int cnt);
int get_directory_content(const char* path,char* to);
int cache_dir_el_cnt(const char* path,char* response);

void remove_from_dir(const char* path);
void add_to_dir(const char* path);
void set_rename(const char* name,const char* replace);
void cache_delete_file(const char* path);
void cache_mkdir(const char* path);
