#include <stdio.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>

#include <openssl/md5.h>
#include <sys/xattr.h>
#include <arpa/inet.h>

#define BACKLOG 10

#define malloc_size 70*1024*1024

static char path[40096];
static char real_path[4096];

const char* atribute_name="user.hash";
const char* size_name="user.fsize";


void get_file_hash(int fd,unsigned char* write){
	MD5_CTX hash;
	MD5_Init(&hash);
	char buff[4096];

	lseek(fd,0,SEEK_SET);

	while(1){
		int read_cnt=read(fd,buff,4096);
		if(read_cnt==0)break;
		MD5_Update(&hash,buff,read_cnt);
	}
	MD5_Final(write,&hash);
}

void update_hash(int fd){
	unsigned char hash[MD5_DIGEST_LENGTH];
	get_file_hash(fd,hash);
	fsetxattr(fd,atribute_name,hash,MD5_DIGEST_LENGTH,0);

}


int read_dir(char* cur_path,char* buff){
	strcpy(path+strlen(real_path),cur_path);
	int cnt_to_write=0;
	int starting_size=malloc_size;

	struct dirent *dp;

	DIR* dir = opendir(path);
	
	if(dir==NULL){
		int err=errno;
		memcpy(buff,&err,sizeof(int));
		return sizeof(int);
	}
	int err=0;
	memcpy(buff,&err,sizeof(int));

	cnt_to_write+=sizeof(int);	

	while(1){
		dp=readdir(dir);
		if(dp==NULL)break;
		int cnt=strlen(dp->d_name)+1;

		if(starting_size<cnt_to_write+sizeof(int)+cnt){
			starting_size+=sizeof(int)+cnt;
			buff=realloc(buff,starting_size);
		}

		memcpy(buff+cnt_to_write,&cnt,sizeof(int));
		cnt_to_write+=sizeof(int);

		memcpy(buff+cnt_to_write,dp->d_name,cnt);
		cnt_to_write+=cnt;
	}
	closedir(dir);

	return cnt_to_write;
}

int get_attr(char* cur_path,char* buff,int raid5){
	strcpy(path+strlen(real_path),cur_path);

	int ret=stat(path,(struct stat*)buff);
	*(int*)(buff+sizeof(struct stat))=ret;

	if(raid5==1){
		long long real_size=0;
		getxattr(path,size_name,&real_size,sizeof(long long));
		((struct stat*)buff)->st_size=real_size;
	}

	return sizeof(struct stat)+sizeof(int);
}

int sys_rename(char* input,char* buff){
	char cur1[4096];
	char cur2[4096];
	strcpy(path+strlen(real_path),input);
	strcpy(cur1,path);

	strcpy(path+strlen(real_path),input+strlen(input)+1);
	strcpy(cur2,path);

	*(int*)buff=rename(cur1,cur2);
	return sizeof(int);
}

int sys_mkdir(char* input,char* buff){
	strcpy(path+strlen(real_path),input);
	mode_t s=*(mode_t*)(input+strlen(input)+1);

	*(int*)buff=mkdir(path,s);
	return sizeof(int);
}

int sys_rmdir(char* input,char* buff){
	strcpy(path+strlen(real_path),input);

	*(int*)buff=rmdir(path);
	return sizeof(int);
}

int sys_opendir(char* input,char* buff){
	strcpy(path+strlen(real_path),input);

	int number_of_items=0;

	struct dirent *dp;

	DIR* dir=opendir(path);
	
	if(dir==NULL){
		*(int*)buff=errno;
		return sizeof(int);
	}

	*(int*)buff=0;

	while(1){
		dp=readdir(dir);
		if(dp==NULL)break;
		number_of_items++;
	}
	closedir(dir);

	*((int*)(buff+sizeof(int)))=number_of_items;

	return 2*sizeof(int);
}

int sys_create(char* input,char* buff){
	strcpy(path+strlen(real_path),input);
	mode_t s=*(mode_t*)(input+strlen(input)+1);

	*(int*)buff=creat(path,s);

	long long a=0;
	fsetxattr(*(int*)buff,size_name,&a,sizeof(long long),0);

	return sizeof(int);
}

int sys_ulink(char* input,char* buff){
	strcpy(path+strlen(real_path),input);

	*(int*)buff=remove(path);

	return sizeof(int);
}

int sys_open(char* input,char* buff){
	strcpy(path+strlen(real_path),input);

	*(int*)buff=open(path,O_RDWR);

	if(*(int*)buff==-1){
		*(int*)buff=-errno;
	}else{
		unsigned char test[MD5_DIGEST_LENGTH];
		get_file_hash(*(int*)buff,test);
		
		unsigned char a[MD5_DIGEST_LENGTH];
		fgetxattr(*(int*)buff,atribute_name,a,MD5_DIGEST_LENGTH);
		close(*(int*)buff);
		int i=0;
		for(;i<MD5_DIGEST_LENGTH;i++)
			if(test[i]!=a[i]){*(int*)buff=1;break;}// hash mismatch
	}
	

	return sizeof(int);
}

int sys_read(char* input,char* buff,int raid5){

	strcpy(path+strlen(real_path),input);

	long long fd=*(long long*)(input+strlen(input)+1);
	long long size=*(long long*)(input+strlen(input)+1+sizeof(long long));
	long long offset=*(long long*)(input+strlen(input)+1+2*sizeof(long long));

	long long cpy_size=0;

	fd=open(path,O_RDWR);
	int cc=lseek(fd,offset,SEEK_SET);

	if(cc==-1){close(fd);return 0;}

	cpy_size+=read(fd,buff+cpy_size,size);

	if(raid5==1){
		long long real_size=0;
		getxattr(path,size_name,&real_size,sizeof(long long));
		memcpy(buff+cpy_size,&real_size,sizeof(long long));
		cpy_size+=sizeof(long long);
	}

	close(fd);

	return cpy_size;
}

void update_file_size(long long writing_at,int file_fd){

	long long cur_value;
	
	fgetxattr(file_fd,size_name,&cur_value,sizeof(long long));
	

	if(writing_at>cur_value){
		fsetxattr(file_fd,size_name,&writing_at,sizeof(long long),0);
	}
}

int sys_write(char* input,char* buffer,int raid5){
	strcpy(path+strlen(real_path),input);

	long long fd,size;
	off_t offset;

	memcpy(&fd,input+strlen(input)+1,sizeof(long long));

	memcpy(&size,input+strlen(input)+1+sizeof(long long),sizeof(long long));

	memcpy(&offset,input+strlen(input)+1+2*sizeof(long long),sizeof(off_t));

	fd=open(path,O_RDWR);
	lseek(fd,offset,SEEK_SET);

	*(int*)buffer=write(fd,input+strlen(input)+1+2*sizeof(long long)+sizeof(off_t),size);

	int pos=strlen(input)+1+2*sizeof(long long)+sizeof(off_t)+size;


	if(raid5==1)update_file_size(*(long long*)(input+pos),fd);

	close(fd);

	return sizeof(int);
}


int sys_close(char* input,char* buff){
	strcpy(path+strlen(real_path),input);
	int fd=open(path,O_RDONLY);
	update_hash(fd);
	close(fd);

	return 0;
}


void sys_get_clone(int fd,char* relative_path){
	strcpy(path+strlen(real_path),relative_path);

	struct dirent *dp;
	DIR* dir = opendir(path);
	
	if(dir==NULL){
		return;
	}

	char path_was[4096];
	strcpy(path_was,path);

	struct stat cur;

	char next[4096];

	while (1){
		dp=readdir(dir);
		if(dp==NULL)break;

		strcat(path,"/");
		strcat(path,dp->d_name);
		if(strcmp(dp->d_name,".")==0||strcmp(dp->d_name,"..")==0){
			strcpy(path,path_was);
			continue;
		}

		strcpy(next,relative_path);
		strcat(next,"/");
		strcat(next,dp->d_name);

		stat(path,&cur);

		int cc=strlen(next)+1;
		write(fd,&cc,sizeof(int));
		write(fd,next,cc);
		write(fd,&cur.st_mode,sizeof(mode_t));

		if(S_ISREG(cur.st_mode)){
			char buff[4096];

			write(fd,&cur.st_size,sizeof(off_t));

			int new_fd=open(path,O_RDWR);
			long long size=cur.st_size;
			while(1){
				if(size>4096){
					int read_cnt=read(new_fd,buff,4096);
					write(fd,buff,read_cnt);
					size-=read_cnt;
				}else{
					int read_cnt=read(new_fd,buff,size);
					write(fd,buff,read_cnt);
					size-=read_cnt;
				}
				if(size==0)break;
			}
		}
		if(S_ISDIR(cur.st_mode)){
			sys_get_clone(fd,next);
		}
		strcpy(path,path_was);
	}
	closedir(dir);
}

void sys_set_clone(int fd){
	mode_t cur;
	int name_size;
	
	while(1){
		int cnt=read(fd,&name_size,sizeof(int));
		if(cnt<=0)break;

		char name[name_size+5];
		read(fd,name,name_size);

		read(fd,&cur,sizeof(mode_t));

		strcpy(path+strlen(real_path),name);

		if(S_ISDIR(cur)){
			mkdir(path,cur);
		}
		if(S_ISREG(cur)){
			off_t file_size;
			read(fd,&file_size,sizeof(off_t));
			char buff[4096];

			int new_fd=creat(path,cur);

			MD5_CTX hash;
			MD5_Init(&hash);

			while(1){
				int read_cnt;
				if(file_size>4096){
					read_cnt=read(fd,buff,4096);
					write(new_fd,buff,read_cnt);
					file_size-=read_cnt;
				}else{
					read_cnt=read(fd,buff,file_size);
					write(new_fd,buff,read_cnt);
					file_size-=read_cnt;
				}
				MD5_Update(&hash,buff,read_cnt);
				if(file_size==0)break;
			}

			unsigned char res[MD5_DIGEST_LENGTH];
			MD5_Final(res,&hash);
			fsetxattr(new_fd,atribute_name,res,MD5_DIGEST_LENGTH,0);

			close(new_fd);
		}
	}
}
void sys_getfile(char* input,int cfd){
	int path_size;
	read(cfd,&path_size,sizeof(int));
	char buff[4096];
	read(cfd,buff,path_size);

	strcpy(path+strlen(real_path),buff);
	int fd=open(path,O_RDWR);
	char buffer[4096];

	while(1){
		int cnt=read(fd,buffer,4096);
		if(cnt<=0)break;
		write(cfd,buffer,cnt);
	}
	close(fd);
}

void sys_setfile(char* input,int cfd){
	int path_size;
	read(cfd,&path_size,sizeof(int));
	char buff[4096];
	read(cfd,buff,path_size);

	strcpy(path+strlen(real_path),buff);
	int fd=open(path,O_RDWR|O_TRUNC);
	char buffer[4096];

	while(1){
		int cnt=read(cfd,buffer,4096);
		if(cnt<=0)break;
		write(fd,buffer,cnt);
	}
	update_hash(fd);
	close(fd);
}




void sys_get_clone_raid5(int fd,char* relative_path){
	strcpy(path+strlen(real_path),relative_path);

	struct dirent *dp;
	DIR* dir = opendir(path);
	
	if(dir==NULL){
		return;
	}

	char path_was[4096];
	strcpy(path_was,path);

	struct stat cur;

	char next[4096];

	while (1){
		dp=readdir(dir);
		if(dp==NULL)break;

		strcat(path,"/");
		strcat(path,dp->d_name);
		if(strcmp(dp->d_name,".")==0||strcmp(dp->d_name,"..")==0){
			strcpy(path,path_was);
			continue;
		}

		strcpy(next,relative_path);
		strcat(next,"/");
		strcat(next,dp->d_name);

		stat(path,&cur);

		int cc=strlen(next)+1;
		write(fd,&cc,sizeof(int));
		write(fd,next,cc);
		write(fd,&cur.st_mode,sizeof(mode_t));

		if(S_ISREG(cur.st_mode)){
			char buff[4096];

			write(fd,&cur.st_size,sizeof(off_t));
			
			long long test;

			int new_fd=open(path,O_RDWR);
			long long size=cur.st_size;

			fgetxattr(new_fd,size_name,&test,sizeof(long long));
			write(fd,&test,sizeof(long long));

			while(1){
				if(size>4096){
					int read_cnt=read(new_fd,buff,4096);
					write(fd,buff,read_cnt);
					size-=read_cnt;
				}else{
					int read_cnt=read(new_fd,buff,size);
					write(fd,buff,read_cnt);
					size-=read_cnt;
				}
				if(size==0)break;
			}
		}
		if(S_ISDIR(cur.st_mode)){
			sys_get_clone(fd,next);
		}
		strcpy(path,path_was);
	}
	closedir(dir);
}

void sys_set_clone_raid5(int fd){
	mode_t cur;
	int name_size;
	
	while(1){
		int cnt=read(fd,&name_size,sizeof(int));
		if(cnt<=0)break;

		char name[name_size+5];
		read(fd,name,name_size);

		read(fd,&cur,sizeof(mode_t));

		strcpy(path+strlen(real_path),name);

		if(S_ISDIR(cur)){
			mkdir(path,cur);
		}
		if(S_ISREG(cur)){
			off_t file_size;
			read(fd,&file_size,sizeof(off_t));
			char buff[4096];
			char buff2[4096];

			int new_fd=open(path,O_CREAT|O_RDWR,cur);

			MD5_CTX hash;
			MD5_Init(&hash);

			long long pos=0;

			long long cur_max_size=0,max_max_size=0;
			fgetxattr(new_fd,size_name,&max_max_size,sizeof(long long));
			read(fd,&cur_max_size,sizeof(long long));
			
			if(cur_max_size>max_max_size)
				fsetxattr(new_fd,size_name,&cur_max_size,sizeof(long long),0);


			while(1){
				int read_cnt,i=0;
				if(file_size>4096){
					read_cnt=read(fd,buff,4096);

					int g=read(new_fd,buff2,read_cnt);

					for(;i<g;i++)
						buff[i]=buff[i]^buff2[i];

					lseek(new_fd,pos,SEEK_SET);

					write(new_fd,buff,read_cnt);
					file_size-=read_cnt;
				}else{
					read_cnt=read(fd,buff,file_size);

					int g=read(new_fd,buff2,read_cnt);
					
					for(;i<g;i++)
						buff[i]=buff[i]^buff2[i];

					lseek(new_fd,pos,SEEK_SET);

					write(new_fd,buff,read_cnt);
					file_size-=read_cnt;
				}
				MD5_Update(&hash,buff,read_cnt);

				pos+=read_cnt;

				if(file_size==0)break;
			}

			unsigned char res[MD5_DIGEST_LENGTH];
			MD5_Final(res,&hash);
			fsetxattr(new_fd,atribute_name,res,MD5_DIGEST_LENGTH,0);

			close(new_fd);
		}
	}
}


void client_handler(int cfd){
	char* buff=malloc(malloc_size);
	int cnt_to_write=-1;

	char syscall[40096];

	int pos=0;
	while(1){
		read(cfd,syscall+pos,1);
		if(syscall[pos]=='\0')break;
		pos++;
	}


	if(strcmp(syscall,"get clone")==0){free(buff);sys_get_clone(cfd,"");close(cfd);return;}
	if(strcmp(syscall,"get clone raid5")==0){free(buff);sys_get_clone_raid5(cfd,"");close(cfd);return;}

	if(strcmp(syscall,"set clone")==0){free(buff);sys_set_clone(cfd);close(cfd);return;}
	if(strcmp(syscall,"set clone raid5")==0){free(buff);sys_set_clone_raid5(cfd);close(cfd);return;}

	if(strcmp(syscall,"get_file")==0){free(buff);sys_getfile(syscall+strlen("get_file")+1,cfd);close(cfd);return;}
	if(strcmp(syscall,"set_file")==0){free(buff);sys_setfile(syscall+strlen("set_file")+1,cfd);close(cfd);return;}

	if(strcmp(syscall,"nothing")==0){free(buff);close(cfd);return;}

	read(cfd,syscall+strlen(syscall)+1,40096-strlen(syscall)-1);

	if(strcmp(syscall,"debug")==0){printf("CLIENT: %s\n",syscall+strlen("debug")+1);cnt_to_write=0;}
	if(strcmp(syscall,"raid5_getattr")==0)cnt_to_write=get_attr(syscall+strlen("raid5_getattr")+1,buff,1);
	if(strcmp(syscall,"getattr")==0)cnt_to_write=get_attr(syscall+strlen("getattr")+1,buff,0);
	if(strcmp(syscall,"rename")==0)cnt_to_write=sys_rename(syscall+strlen("rename")+1,buff);
	if(strcmp(syscall,"mkdir")==0)cnt_to_write=sys_mkdir(syscall+strlen("mkdir")+1,buff);
	if(strcmp(syscall,"rmdir")==0)cnt_to_write=sys_rmdir(syscall+strlen("rmdir")+1,buff);
	if(strcmp(syscall,"opendir")==0)cnt_to_write=sys_opendir(syscall+strlen("opendir")+1,buff);
	if(strcmp(syscall,"readdir")==0)cnt_to_write=read_dir(syscall+strlen("readdir")+1,buff);
	if(strcmp(syscall,"create")==0)cnt_to_write=sys_create(syscall+strlen("create")+1,buff);
	if(strcmp(syscall,"unlink")==0)cnt_to_write=sys_ulink(syscall+strlen("unlink")+1,buff);

	if(strcmp(syscall,"open")==0)cnt_to_write=sys_open(syscall+strlen("open")+1,buff);
	if(strcmp(syscall,"read")==0)cnt_to_write=sys_read(syscall+strlen("read")+1,buff,0);
	if(strcmp(syscall,"raid5_read")==0)cnt_to_write=sys_read(syscall+strlen("raid5_read")+1,buff,1);
	if(strcmp(syscall,"write")==0)cnt_to_write=sys_write(syscall+strlen("write")+1,buff,0);
	if(strcmp(syscall,"raid5_write")==0)cnt_to_write=sys_write(syscall+strlen("raid5_write")+1,buff,1);
	if(strcmp(syscall,"release")==0)cnt_to_write=sys_close(syscall+strlen("release")+1,buff);

	write(cfd,&cnt_to_write,sizeof(int));
	write(cfd,buff,cnt_to_write);

	close(cfd);

	free(buff);
}

int get_int(char* inp){
	int sign=1;
	if(inp[0]=='-'){sign=-1;inp++;}
	int ret=0;
	while(inp[0]!='\0'){
		ret+=(inp[0]-'0');
		ret*=10;
		inp++;
	}ret/=10;
	return ret*sign;
}

void receive_child(int sign){
	if(sign==SIGCHLD)wait(NULL);
}

int main(int argc, char* argv[])
{

	strcpy(path,argv[3]);
	strcpy(real_path,argv[3]);

	int ip;
	inet_pton(AF_INET,argv[1], &ip);


    int cfd;
    struct sockaddr_in addr;
    struct sockaddr_in peer_addr;

	if(fork()!=0)exit(0);

    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    int optval = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(get_int(argv[2]));
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
	addr.sin_addr.s_addr = ip;

    bind(sfd, (struct sockaddr *) &addr, sizeof(struct sockaddr_in));
    listen(sfd, BACKLOG);
    
	signal(SIGCHLD,receive_child);

    while (1) 
    {
        int peer_addr_size = sizeof(struct sockaddr_in);
        cfd = accept(sfd,(struct sockaddr *)&peer_addr,(socklen_t *)&peer_addr_size);

	//client_handler(cfd);
	//continue;

        switch(fork()) {
            case -1:
                exit(100);
            case 0:
                close(sfd);
                client_handler(cfd);
                exit(0);
            default:
                close(cfd);
        }
    }
    close(sfd);
}
