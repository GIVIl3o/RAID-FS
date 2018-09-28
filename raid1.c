#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>


#include "raid1.h"

static void copy_file(const char* path,struct server* to,struct server* from){

	log_message("found damaged file",to);

	int cfd_from=get_server_fd(from);
	int cfd_to=get_server_fd(to);

	char buffer[4296];
	strcpy(buffer,"get_file");
	*(int*)(buffer+strlen("get_file")+1)=strlen(path)+1;

	strcpy(buffer+strlen("get_file")+1+sizeof(int),path);
	write(cfd_from,buffer,strlen("get_file")+2+strlen(path)+sizeof(int));

	buffer[0]='s';
	write(cfd_to,buffer,strlen("set_file")+2+strlen(path)+sizeof(int));

	while(1){
		int cnt=read(cfd_from,buffer,4096);
		if(cnt==0)break;
		write(cfd_to,buffer,cnt);
	}
	close(cfd_from);
	close(cfd_to);
}

void raid1_clone_data(struct server* srv_from,struct server* srv_to){
	char response[4296];
	int fd_from=get_server_fd(srv_from);
	int fd_to=get_server_fd(srv_to);

	write(fd_from,"get clone",strlen("get clone")+1);
	write(fd_to,"set clone",strlen("set clone")+1);
	while(1){
		int read_cnt=read(fd_from,response,4096);

		if(read_cnt<=0)break;

		write(fd_to,response,read_cnt);
	}
	close(fd_from);
	close(fd_to);
}


int raid1_getattr(const char *path, struct stat *stbuf){
	if(get_atribute(path,stbuf)==1)return 0;

	char buffer[4096];
	char response[4096];
	strcpy(buffer,"getattr");
	strcpy(buffer+strlen("getattr")+1,path);
	int read_cnt=server_connect(buffer,strlen("getattr")+strlen(path)+2,response,1);

	if(read_cnt!=sizeof(struct stat)+sizeof(int)){
		//dbg("CLIENT ERROR:hello_getattr");
	}

	memcpy(stbuf,response,sizeof(struct stat));
	if(*(int*)(response+sizeof(struct stat))!=0){
		return -ENOENT;
	}
	save_atribute(path,stbuf);
	return 0;

}

int raid1_open(const char* path, struct fuse_file_info* fi){
	char buffer[4096];
	strcpy(buffer,"open");
	strcpy(buffer+strlen("open")+1,path);

	int i=0;

	int siz[server_cnt];
	char hash[server_cnt][1024];

	for(;i<server_cnt;i++){
		siz[i]=send_request(buffer,strlen("open")+strlen(path)+2,hash[i],&servs[i],1,1);

		if(siz[i]==-1)return 0;

		if(*(int*)hash[i]==1){copy_file(path,&servs[i],&servs[1-i]);return 0;}
	}

	if(siz[0]!=siz[1]){
		copy_file(path,&servs[1],&servs[0]);return 0;}
	i=0;
	for(;i<siz[0];i++)
		if(hash[0][i]!=hash[1][i]){
			copy_file(path,&servs[1],&servs[0]);
			return 0;
		}

	return 0;
}


int raid1_read(const char *path, char *to, size_t size, off_t offset, struct fuse_file_info* fi){
	long long to_return=size;

	while(get_file_content(path,to,offset,size)==1){
		to-=(offset%(4096*4));
		size+=(offset%(4096*4));
		offset-=(offset%(4096*4));
		offset+=4096*4;
		size-=4096*4;
		to+=4096*4;
		if((long long)size<=0)return to_return;
	}

	long long my_read=offset-offset%(4096*4);
	long long full_read=((my_read+size+4096*4-1)/(4096*4))*(4096*4)-my_read;

	char buffer[4096];
	char response[full_read+1000];
	int cur_pos=0;
	strcpy(buffer,"read");cur_pos+=strlen("read")+1;
	strcpy(buffer+cur_pos,path);cur_pos+=strlen(path)+1;
	memcpy(buffer+cur_pos,&(fi->fh),sizeof(uint64_t));cur_pos+=sizeof(uint64_t);
	memcpy(buffer+cur_pos,&full_read,sizeof(size_t));cur_pos+=sizeof(size_t);
	memcpy(buffer+cur_pos,&my_read,sizeof(off_t));cur_pos+=sizeof(off_t);

	int read_cnt=server_connect(buffer,cur_pos,response,1);

	memcpy(to,response+offset%(4096*4),size);

	int i=0;
	for(;i<full_read/(4096*4);i++){
		save_file_content(path,response+i*4096*4,my_read+i*4096*4);
	}
	return to_return;
}

int raid1_write(const char* path, const char* from, size_t size, off_t offset, struct fuse_file_info* fi){
	fi->fh=1;

	char buffer[size+4096];
	char response[4096];
	int cur_pos=0;
	strcpy(buffer,"write");cur_pos+=strlen("write")+1;
	strcpy(buffer+cur_pos,path);cur_pos+=strlen(path)+1;
	memcpy(buffer+cur_pos,&(fi->fh),sizeof(uint64_t));cur_pos+=sizeof(uint64_t);
	memcpy(buffer+cur_pos,&size,sizeof(size_t));cur_pos+=sizeof(size_t);
	memcpy(buffer+cur_pos,&offset,sizeof(off_t));cur_pos+=sizeof(off_t);
	memcpy(buffer+cur_pos,from,size);cur_pos+=size;

	server_connect(buffer,cur_pos,response,0);

	update_atribute(path,offset+size);
	update_file_content(path,from,offset,size);

	return size;
}

int raid1_release(const char* path, struct fuse_file_info* fi){
	char buffer[4096];
	char response[4096];
	strcpy(buffer,"release");

	memcpy(buffer+strlen("release")+1,path,strlen(path)+1);

	server_connect(buffer,strlen("release")+2+strlen(path),response,0);	

	return 0;
}



