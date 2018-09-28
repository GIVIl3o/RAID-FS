
#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/wait.h>

#include "parser.h"
#include "raid1.h"
#include "raid5.h"
#include "hello.h"
#include "cache.h"

int raid;

char error_write[4096];
char storage_name[4096];
int timeout;

int send_request(char* write_buffer,int write_cnt,char* response,struct server* srv,int fill,int read);

void dbg(char* buf);

void log_message(char* message,struct server* srv){
	char final_message[40960];

	int fd=open(error_write,O_RDWR|O_APPEND|O_CREAT,S_IRWXO|S_IRWXG|S_IRWXU);

	time_t current_time=time(NULL);

	char* cur_time=ctime(&current_time);
	cur_time[strlen(cur_time)-1]='\0';

	sprintf(final_message,"[%s] %s %s:%d %s\n",cur_time,storage_name,srv->ip,srv->port,message);
	write(fd,final_message,strlen(final_message));

	close(fd);
}

int get_server_fd(struct server* srv){
	int ip;
	int sfd = socket(AF_INET, SOCK_STREAM, 0);
	inet_pton(AF_INET,srv->ip, &ip);
	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_port = htons(srv->port);
	addr.sin_addr.s_addr = ip;

	int cur=connect(sfd,(struct sockaddr *) &addr,sizeof(struct sockaddr_in));

	if(cur==-1){close(sfd);return -1;}
	return sfd;
}

int clone_data(char* write_buffer,int write_cnt,struct server* srv){
	log_message("server declared as lost",srv);

	if(servs[server_cnt].port==-1){log_message("hotswap server can't be used twice",srv);return -1;}

	if(srv->cur_reserved!=0)free(srv->mem);

	int i=0;

	if(raid==1){
		for(;i<server_cnt;i++){
			if(strcmp(servs[i].ip,srv->ip)==0&&servs[i].port==srv->port)break;
		}
		raid1_clone_data(&servs[1-i],&servs[server_cnt]);
	}
	else if(raid==5){
		for(;i<server_cnt;i++){
			if(strcmp(servs[i].ip,srv->ip)==0&&servs[i].port==srv->port)break;
		}
		clone_data_raid5(i);
	}

	strcpy(servs[i].ip,servs[server_cnt].ip);
	servs[i].port=servs[server_cnt].port;
	close(servs[i].status[0]);servs[i].status[0]=0;
	close(servs[i].status[1]);servs[i].status[1]=0;
	servs[i].cur_reserved=0;
	servs[i].cur_pos=0;

	servs[server_cnt].port=-1;

	log_message("hot swap server added",&servs[i]);

	char test[4097];
	send_request(write_buffer,write_cnt,test,&servs[i],0,0);

	return -1;
}

void timeout_listener(struct server* srv){
	time_t start;
	time(&start);

	time_t cur_time;

	int answer=1;

	while(1){
		time(&cur_time);
		if(difftime(cur_time,start)>timeout)break;

		int srv_fd=get_server_fd(srv);
		if(srv_fd!=-1){
			write(srv_fd,"nothing",strlen("nothing")+1);
			close(srv_fd);
			answer=2;
			break;
		}
	}
	write(srv->status[1],&answer,sizeof(int));
	close(srv->status[0]);
	close(srv->status[1]);
	exit(0);
}

void send_delayed_requests(struct server* srv){
	close(srv->status[0]);
	close(srv->status[1]);

	int i=0;
	int write_cnt;
	int to_read_size;

	log_message("established new connection with server, sending previous write requests",srv);

	while(1){
		if(i==srv->cur_pos){
			free(srv->mem);
			srv->cur_reserved=0;
			srv->cur_pos=0;
			srv->status[0]=0;
			srv->status[1]=0;
			log_message("server is up to date",srv);
			return;
		}

		int fd=get_server_fd(srv);
		if(fd==-1)break;

		write_cnt=write(fd,srv->mem+i+sizeof(int),*(int*)(srv->mem+i));
		if(write_cnt!=*(int*)(srv->mem+i))break;

		write_cnt=read(fd,&to_read_size,sizeof(int));
		if(write_cnt!=sizeof(int))break;

		int buffer[to_read_size+5];
		write_cnt=read(fd,buffer,to_read_size);
		if(to_read_size!=write_cnt)break;

		i+=sizeof(int)+*(int*)(srv->mem+i);
		close(fd);
	}

	log_message("lost connection, write requests are not finished",srv);

	memmove(srv->mem,srv->mem+i,srv->cur_pos-i);
	srv->cur_pos=srv->cur_pos-i;

	pipe(srv->status);

	fcntl(srv->status[0],F_SETFL,O_NONBLOCK);

	if(fork()==0){
		timeout_listener(srv);
	}

}

int start_timeout(char* write_buffer,int writer_cnt,struct server* srv,int read_request){

	if(read_request==0){
		if(srv->cur_pos==0){
			srv->cur_reserved=4296*5;
			srv->mem=malloc(srv->cur_reserved);
		}
		if(srv->cur_reserved<srv->cur_pos+sizeof(int)+writer_cnt){
			srv->cur_reserved+=4296*5;
			srv->mem=realloc(srv->mem,srv->cur_reserved);
		}
			
		memcpy(srv->mem+srv->cur_pos,&writer_cnt,sizeof(int));
		memcpy(srv->mem+srv->cur_pos+sizeof(int),write_buffer,writer_cnt);
		srv->cur_pos+=sizeof(int)+writer_cnt;
	}

	if(srv->status[0]!=0){
		int cc=0;
		if(read(srv->status[0],&cc,sizeof(int))==sizeof(int)){
			wait(NULL);
			if(cc==1)return clone_data(write_buffer,writer_cnt,srv);
			else{
				send_delayed_requests(srv);
			}
		}else{
			
			
		}
	}else{
		log_message("connection to server is lost",srv);
		pipe(srv->status);

		fcntl(srv->status[0],F_SETFL,O_NONBLOCK);

		if(fork()==0){
			timeout_listener(srv);
		}
	}

	return -1;
}


// fill: fill response? read: request type
int send_request(char* write_buffer,int write_cnt,char* response,struct server* srv,int fill,int read_request){

	if(srv->status[0]!=0)
		return start_timeout(write_buffer,write_cnt,srv,read_request);

	int read_cnt=0;
	int sfd=get_server_fd(srv);
	if(sfd==-1)return start_timeout(write_buffer,write_cnt,srv,read_request);
	else{
 		int cur=write(sfd,write_buffer,write_cnt);
		if(cur!=write_cnt){close(sfd);return start_timeout(write_buffer,write_cnt,srv,read_request);}
		else{
			int to_read_number=0;
			if(read(sfd,&to_read_number,sizeof(int))<=0){close(sfd);
				return start_timeout(write_buffer,write_cnt,srv,read_request);}

			while(1){
				int ww=read_cnt;
				if(fill==0)ww=0;

				int cnt=read(sfd,response+ww,4096);

				read_cnt+=cnt;
				if(cnt==0)break;
			}
			if(to_read_number!=-1&&to_read_number!=read_cnt){close(sfd);
				return start_timeout(write_buffer,write_cnt,srv,read_request);}
		}
	}
	close(sfd);

	return read_cnt;
}

void dbg(char* buf){
	char a[4096];
	strcpy(a,"debug");
	strcpy(a+strlen("debug")+1,buf);

	struct server test;
	strcpy(test.ip,"127.0.0.1");
	test.port=5007;
	test.status[0]=0;
	test.status[1]=0;

	send_request(a,strlen("debug")+strlen(buf)+2,NULL,&test,1,0);
}

int server_connect(char* write_buffer,int write_cnt,char* response,int read){
	int i=0;
	int read_cnt=-1;
	int fill=1;
	char test[4097];
	int saved_read_cnt;

	for(;i<server_cnt;i++){
		if(fill==1)read_cnt=send_request(write_buffer,write_cnt,response,&servs[i],fill,read);
		else read_cnt=send_request(write_buffer,write_cnt,test,&servs[i],fill,read);

		if(read_cnt>0){fill=0;saved_read_cnt=read_cnt;}

		if(read==1&&read_cnt>0)
			return saved_read_cnt;
	}
	return saved_read_cnt;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi){

	char buffer[4096];
	char response[fi->fh*(300+sizeof(int))];
	strcpy(buffer,"readdir");
	strcpy(buffer+strlen("readdir")+1,path);

	if(get_directory_content(path,response)!=1)
		server_connect(buffer,strlen("readdir")+strlen(path)+2,response,1);

	if((*(int*)response)!=0)return -(*(int*)response);

	int i=0;
	int pos=sizeof(int);
	int full_size=sizeof(int);
	for(;i<fi->fh;i++){
		int size=*(int*)(response+pos);
		full_size+=size+sizeof(int);
		filler(buf,response+pos+sizeof(int),NULL,0);
		pos+=sizeof(int)+size;
	}
	save_directory_content(path,response,full_size);
	return 0;
}

static int hello_rename(const char* name, const char* replace){
	char buffer[4096];
	char response[4096];
	strcpy(buffer,"rename");
	strcpy(buffer+strlen("rename")+1,name);
	strcpy(buffer+strlen("rename")+strlen(name)+2,replace);

	server_connect(buffer,strlen("rename")+strlen(name)+strlen(replace)+3,response,0);

	if(*(int*)response==0)set_rename(name,replace);

	//if(read_cnt!=sizeof(int))dbg("CLIENT ERROR:hello_rename");

	return *(int*)response;
}

static int hello_mkdir(const char* name,mode_t s){
	char buffer[4096];
	char response[4096];
	strcpy(buffer,"mkdir");
	strcpy(buffer+strlen("mkdir")+1,name);
	memcpy(buffer+strlen("mkdir")+strlen(name)+2,&s,sizeof(mode_t));

	server_connect(buffer,strlen("mkdir")+strlen(name)+2+sizeof(mode_t),response,0);

	//if(*(int*)response==0)add_to_dir(name);
	if(*(int*)response==0)cache_mkdir(name);

	return *(int*)response;
}

static int hello_rmdir(const char* name){
	char buffer[4096];
	char response[4096];
	strcpy(buffer,"rmdir");
	strcpy(buffer+strlen("rmdir")+1,name);

	server_connect(buffer,strlen("mkdir")+strlen(name)+2,response,0);

	if(*(int*)response==0)cache_delete_file(name);

	return *(int*)response;
}

static int hello_opendir(const char* path, struct fuse_file_info * info){
	char buffer[4096];
	char response[4096];
	strcpy(buffer,"opendir");
	strcpy(buffer+strlen("opendir")+1,path);

	if(cache_dir_el_cnt(path,response)==0)
		server_connect(buffer,strlen("opendir")+strlen(path)+2,response,1);


	int item_number_cnt=*(response+sizeof(int));
	info->fh=item_number_cnt;

	return *(int*)response;
}

static int hello_releasedir(const char* path, struct fuse_file_info* fi){
	return 0;
}

static int hello_create(const char* path, mode_t s, struct fuse_file_info* fi){
	char buffer[4096];
	char response[4096];
	strcpy(buffer,"create");
	strcpy(buffer+strlen("create")+1,path);
	memcpy(buffer+strlen("create")+strlen(path)+2,&s,sizeof(mode_t));

	server_connect(buffer,strlen("create")+strlen(path)+2+sizeof(mode_t),response,0);

	if(*(int*)response!=-1)add_to_dir(path);

	fi->fh=*(int*)response;

	if(fi->fh==-1)
		return EEXIST;

	return 0;
}

static int hello_unlink(const char* path){
	char buffer[4096];
	char response[4096];
	strcpy(buffer,"unlink");
	strcpy(buffer+strlen("unlink")+1,path);

	server_connect(buffer,strlen("unlink")+strlen(path)+2,response,0);
	
	if(*(int*)response==0)cache_delete_file(path);

	return *(int*)response;
}



static struct fuse_operations raid1_oper={
	.getattr	= raid1_getattr,
	.open		= raid1_open,
	.read		= raid1_read,
	.write		= raid1_write,
	.release	= raid1_release,	
	.rename		= hello_rename,
	.unlink		= hello_unlink,
	.rmdir		= hello_rmdir,
	.mkdir		= hello_mkdir,
	.opendir	= hello_opendir, 	//?
	.releasedir	= hello_releasedir, 	//?
	.create		= hello_create,
	.readdir	= hello_readdir,
};

static struct fuse_operations raid5_oper={
	.getattr	= raid5_getattr,
	.open		= raid5_open,
	.read		= raid5_read,
	.write		= raid5_write,
	.release	= raid5_release,	
	.rename		= hello_rename,
	.unlink		= hello_unlink,
	.rmdir		= hello_rmdir,
	.mkdir		= hello_mkdir,
	.opendir	= hello_opendir, 	//?
	.releasedir	= hello_releasedir, 	//?
	.create		= hello_create,
	.readdir	= hello_readdir,
};

int main(int argc, char *argv[]){
	int line_max_length=4096;

	FILE* f;
	f=fopen(argv[1],"r");
	if(f==NULL){
		printf("can't find config file");
		exit(1);
	}

	char buffer[line_max_length];
	char key[line_max_length];
	char value[line_max_length];

	char mount_point[line_max_length];

	server_cnt=0;

	while(fgets(buffer,line_max_length,f)!=NULL){
		set_key(buffer,key);
		set_value(buffer,value);

		if(strcmp(key,"errorlog")==0)set_value(buffer,error_write);
		if(strcmp(key,"diskname")==0)set_value(buffer,storage_name);
		if(strcmp(key,"mountpoint")==0)set_value(buffer,mount_point);
		if(strcmp(key,"timeout")==0)timeout=get_timeout(buffer);
		if(strcmp(key,"cache_size")==0)cache_size=get_timeout(buffer)*1024*1024;
		if(strcmp(key,"raid")==0)raid=value[0]-'0';
		if(strcmp(key,"servers")==0){
			int i=0;
			int pos=0;
			for(;buffer[i]!='=';i++){}pos=i+2;

			for(i=0;1;i++){
				if(buffer[i]==','||buffer[i]=='\0'){
					char test[4096];
					get_ip(buffer+pos,test);
					strcpy(servs[server_cnt].ip,test);
					servs[server_cnt].port=get_port(buffer+pos);
					servs[server_cnt].status[0]=0;
					servs[server_cnt].status[1]=0;
					servs[server_cnt].cur_pos=0;
					
					server_cnt++;
					pos=i+2;
				}
				if(buffer[i]=='\0')break;
			}
		}
		if(strcmp(key,"hotswap")==0){
			char test[4096];
			int j=0;
			for(;buffer[j]!='=';j++){}

			get_ip(buffer+j+2,test);
			strcpy(servs[server_cnt].ip,test);
			servs[server_cnt].port=get_port(buffer);
			servs[server_cnt].status[0]=0;
			servs[server_cnt].status[1]=0;
			servs[server_cnt].cur_pos=0;

			argv[1]=mount_point;

			if(fork()==0)
				if(raid==1){
					return fuse_main(argc,argv,&raid1_oper, NULL);}
				else if(raid==5)
					return fuse_main(argc,argv,&raid5_oper, NULL);
				else
					printf("unknown raid!\n");
			else{}
			server_cnt=0;
		}
	}
	
	
	fclose(f);

	return 0;
}
