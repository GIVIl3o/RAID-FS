#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>


#include "include/uthash.h"
#include "include/utlist.h"
#include "cache.h"

void destroy_element(struct block* el){
	DL_DELETE(list,el);
	HASH_DEL(hash,el);

	cache_size+=el->malloced_size;

	free(el);
}

char* get_memory(int size){
	while(1){
		if(size<cache_size)break;

		destroy_element(list);
	}

	cache_size-=size;

	return malloc(size);
}

void* fill_block(const char* path,char* data,int data_size){
	struct block* cur=(struct block*)get_memory(sizeof(struct block)+strlen(path)+1+data_size);
	if(cur==NULL)return NULL;

	cur->path_size=strlen(path)+1;
	cur->path=(char*)cur+sizeof(struct block);
	cur->data_size=data_size;
	cur->data=(char*)cur+sizeof(struct block)+strlen(path)+1;
	cur->malloced_size=sizeof(struct block)+strlen(path)+1+data_size;

	strcpy(cur->path,path);
	memcpy(cur->data,data,data_size);

	DL_APPEND(list,cur);
	HASH_ADD_STR(hash,path,cur);
	return cur;
}

void save_atribute(const char* path,struct stat* st){
	struct block* cur=NULL;

	HASH_FIND_STR(hash,path,cur);
	if(cur!=NULL){
		memcpy(cur->data,st,sizeof(struct stat));
		DL_DELETE(list,cur);
		DL_APPEND(list,cur);
		return;
	}
	fill_block(path,(char*)st,sizeof(struct stat));
}

int get_atribute(const char* path,struct stat* st){
	struct block* cur=NULL;
	HASH_FIND_STR(hash,path,cur);

	if(cur==NULL)return 0;

	memcpy(st,cur->data,sizeof(struct stat));

	DL_DELETE(list,cur);
	DL_APPEND(list,cur);

	return 1;
}


void update_atribute(const char* path,long long index){
	struct block* cur;
	HASH_FIND_STR(hash,path,cur);
	if(cur==NULL)return;

	if(((struct stat*)(cur->data))->st_size<index){
		((struct stat*)(cur->data))->st_size=index;}

	DL_DELETE(list,cur);
	DL_APPEND(list,cur);
}

void save_file_content(const char* path,char* from,long long offset){
	char new_path[4233];sprintf(new_path,"%lld%s",offset,path);

	fill_block(new_path,from,4*4096);

}

void update_file_content(const char* path,char* from,long long offset,int size){
	char new_path[4233];sprintf(new_path,"%lld%s",offset-offset%4096,path);

	struct block* cur;
	HASH_FIND_STR(hash,new_path,cur);

	if(cur==NULL)return ;

	memcpy(cur->data+offset%(4096*4),from,size);
	DL_DELETE(list,cur);
	DL_APPEND(list,cur);
}

int get_file_content(const char* path,char* to,long long offset,int size){
	char new_path[4233];sprintf(new_path,"%lld%s",offset-offset%4096,path);

	struct block* cur;
	HASH_FIND_STR(hash,new_path,cur);

	if(cur==NULL)return 0;

	if(size+offset%(4096*4)>4096*4)size=4096*4-offset%(4096*4);

	memcpy(to,cur->data+offset%(4096*4),size);
	DL_DELETE(list,cur);
	DL_APPEND(list,cur);

	return 1;
}


void save_directory_content(const char* path,char* from,int cnt){
	char new_path[4233];sprintf(new_path,"readdir%s",path);if(new_path[strlen(new_path)-1]=='/')new_path[strlen(new_path)-1]='\0';

	struct block* cur=NULL;
	HASH_FIND_STR(hash,new_path,cur);
	if(cur!=NULL)return;

	fill_block(new_path,from,cnt);
}

int get_directory_content(const char* path,char* to){
	char new_path[4233];sprintf(new_path,"readdir%s",path);if(new_path[strlen(new_path)-1]=='/')new_path[strlen(new_path)-1]='\0';

	struct block* cur;
	HASH_FIND_STR(hash,new_path,cur);

	if(cur==NULL)return 0;

	memcpy(to,cur->data,cur->data_size);
	DL_DELETE(list,cur);
	DL_APPEND(list,cur);

	return 1;
}

int cache_dir_el_cnt(const char* path,char* response){
	char new_path[4233];sprintf(new_path,"readdir%s",path);if(new_path[strlen(new_path)-1]=='/')new_path[strlen(new_path)-1]='\0';

	struct block* cur;
	HASH_FIND_STR(hash,new_path,cur);

	if(cur==NULL)return 0;

	*(int*)response=0;

	int cnt=0;

	int pos=sizeof(int);
	char* cur_ptr=cur->data+2*sizeof(int);

	while(pos!=cur->data_size){
		pos+=sizeof(int)+strlen(cur_ptr)+1;
		cur_ptr+=sizeof(int)+strlen(cur_ptr)+1;
		cnt++;
		if(cnt==10)break;
	}
	*(int*)(response+sizeof(int))=cnt;

	DL_DELETE(list,cur);
	DL_APPEND(list,cur);

	return 1;
}

void get_file_name(const char* buffer,char* response){
	int i=strlen(buffer);
	while(buffer[i]!='/')i--;
	strcpy(response,buffer+i+1);
}

void get_upper_folder(const char* buffer,char* response){
	int i=strlen(buffer);
	while(i>=0&&buffer[i]!='/')i--;
	memcpy(response,buffer,i);
	
	if(i==0)response[i]='\0';
	else response[i]='\0';
}

struct block* get_upper_directory(const char* path){
	struct block* cur=NULL;

	char test[strlen(path)+1+strlen("readdir")+1];
	get_upper_folder(path,test);

	char upper[strlen(path)+1+strlen("readdir")+1];
	sprintf(upper,"readdir%s",test);

	HASH_FIND_STR(hash,upper,cur);
	return cur;
}

void remove_from_dir(const char* path){
	struct block* cur=NULL;
	HASH_FIND_STR(hash,path,cur);
	if(cur!=NULL)destroy_element(cur);

	cur=NULL;

	cur=get_upper_directory(path);
	if(cur==NULL)return;

	char file_name[300];
	get_file_name(path,file_name);

	int pos=2*sizeof(int);
	char* cur_ptr=cur->data+2*sizeof(int);
	while(1){
		if(strcmp(cur_ptr,file_name)==0)break;
		else {pos+=sizeof(int)+strlen(cur_ptr)+1;cur_ptr+=sizeof(int)+strlen(cur_ptr)+1;}}

	int prev_siz=strlen(cur_ptr)+1;
	memmove(cur->data+pos-sizeof(int),cur->data+pos+prev_siz,cur->data_size-pos-prev_siz);
	cur->data_size-=prev_siz;cur->data_size-=sizeof(int);

	DL_DELETE(list,cur);
	DL_APPEND(list,cur);

}

void add_to_dir(const char* path){
	struct block* cur=get_upper_directory(path);
	if(cur==NULL)return;

	char file_name[300];
	get_file_name(path,file_name);

	char content[cur->data_size+sizeof(int)+strlen(file_name)+1];
	memcpy(content,cur->data,cur->data_size);

	int data_size=cur->data_size;
	int cc=strlen(file_name)+1;
	memcpy(content+data_size,&cc,sizeof(int));
	memcpy(content+data_size+sizeof(int),file_name,cc);

	destroy_element(cur);
	
	char new_hash[strlen(path)+strlen("readdir")+1];
	memcpy(new_hash,"readdir",strlen("readdir"));
	get_upper_folder(path,new_hash+strlen("readdir"));

	fill_block(new_hash,content,data_size+sizeof(int)+cc);

}

void set_rename(const char* name,const char* replace){
	
	struct block* elt;
	struct block* tmp;
	struct block* cur;

	HASH_FIND_STR(hash,name,cur);
	if(cur!=NULL){// change atribute
		char content[cur->data_size];
		int data_size=cur->data_size;
		memcpy(content,cur->data,data_size);
		destroy_element(cur);

		fill_block(replace,content,data_size);
	}

	remove_from_dir(name);
	add_to_dir(replace);

	DL_FOREACH_SAFE(list,elt,tmp){	// update inner files

		int j=0;	char* aa="readdir";

		while(elt->path[j]==aa[j])j++;
		while(elt->path[j]>='0'&&elt->path[j]<='9')j++;

		//if(strcmp(elt->path+j,name)!=0)continue;
		if(strstr(elt->path+j,name)!=elt->path+j)continue;

		char new_path[strlen(replace)+51];
		int i=0;
		while(elt->path[i]!='\0'&&elt->path[i]!='/'){
			new_path[i]=elt->path[i];
			i++;
		}
		strcpy(new_path+i,replace);
		strcat(new_path,elt->path+i+strlen(name));

		char new_data[elt->data_size];
		int data_size=elt->data_size;
		memcpy(new_data,elt->data,elt->data_size);

		destroy_element(elt);
		fill_block(new_path,new_data,data_size);
	}
}


void cache_delete_file(const char* path){
	remove_from_dir(path);

	struct block* elt;
	struct block* tmp;

	DL_FOREACH_SAFE(list,elt,tmp){

		char* start=strstr(elt->path,path);
		if(start==NULL)continue;

		if(start[strlen(path)]!='/'&&start[strlen(path)]!='\0')continue;

		destroy_element(elt);
	}

}


void cache_mkdir(const char* path){
	add_to_dir(path);
	int siz=0;
	char content[300];
	memcpy(content,&siz,sizeof(int));siz=strlen(".")+1;
	memcpy(content+sizeof(int),&siz,sizeof(int));
	strcpy(content+2*sizeof(int),".");siz=strlen("..")+1;
	memcpy(content+2*sizeof(int)+strlen(".")+1,&siz,sizeof(int));
	strcpy(content+3*sizeof(int)+strlen(".")+1,"..");

	save_directory_content(path,content,3*sizeof(int)+strlen(".")+strlen("..")+2);
}
