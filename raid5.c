#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>


#include "raid1.h"
#include "raid5.h"
#include "hello.h"

#define chunk_size 4096		// don't change because of server clone_data side

void xor_array(char* xor_array,const char* xorer,int position,int cnt){
	int i=0;

	for(;i<cnt;i++){
		xor_array[position+i]=xor_array[position+i]^xorer[position+i];
	}

}


int clone_data_raid5(int server_pos){
	char response[chunk_size+5];

	int i;
	for(i=0;i<server_cnt;i++){
		if(i==server_pos)continue;
		
		int fd_from=get_server_fd(&servs[i]);
		int fd_to=get_server_fd(&servs[server_cnt]);

		write(fd_from,"get clone raid5",strlen("get clone raid5")+1);
		write(fd_to,"set clone raid5",strlen("set clone raid5")+1);
		while(1){
			int read_cnt=read(fd_from,response,4096);

			if(read_cnt<=0)break;

			write(fd_to,response,read_cnt);
		}
		close(fd_from);
		close(fd_to);
	}

}


int raid5_open(const char* path, struct fuse_file_info* fi){
	return 0;
}

int get_server_chunk(long long offset){
	offset%=chunk_size;
	return offset%server_cnt;
}

int get_parity_position(long long offset){
	int stripe_cnt=offset/((server_cnt-1)*chunk_size);
	stripe_cnt%=server_cnt;
	return server_cnt-stripe_cnt-1;
}

int get_server(long long offset){
	int parity_pos=get_parity_position(offset);
	offset%=((server_cnt-1)*chunk_size);


	if(offset<(server_cnt-parity_pos-1)*chunk_size)
		return parity_pos+1+offset/chunk_size;
	else 
		return offset/chunk_size-(server_cnt-parity_pos-1);
}


int raid5_getattr(const char *path, struct stat *stbuf){
	if(get_atribute(path,stbuf)==1)return 0;

	char buffer[4096];
	char response[4096];
	strcpy(buffer,"raid5_getattr");
	strcpy(buffer+strlen("raid5_getattr")+1,path);

	int read_cnt=-1;

	int i=0;

	long long file_size=0;
	int gg=0;

	for(;i<server_cnt;i++){
		read_cnt=send_request(buffer,strlen("raid5_getattr")+strlen(path)+2,response,&servs[i],1,0);
		if(read_cnt==-1)continue;

		if(read_cnt!=sizeof(struct stat)+sizeof(int)){
			//dbg("CLIENT ERROR:raid5_getattr");
		}else if(gg==0){
			memcpy(stbuf,response,sizeof(struct stat));
			file_size=stbuf->st_size;
			gg=1;
		}else{
			if(((struct stat*)response)->st_size>file_size)
				file_size=((struct stat*)response)->st_size;
		}
	}

	stbuf->st_size=file_size;


	if(*(int*)(response+sizeof(struct stat))!=0){
		return -ENOENT;
	}

	save_atribute(path,stbuf);
	return 0;
}

int raid5_read_to_server(const char* path,char* to,size_t size,off_t offset,int server_pos,long long* real_size){

	char buffer[4096];
	int cur_pos=0;

	long long nulli=0;

	strcpy(buffer,"raid5_read");cur_pos+=strlen("raid5_read")+1;
	strcpy(buffer+cur_pos,path);cur_pos+=strlen(path)+1;
	memcpy(buffer+cur_pos,&nulli,sizeof(uint64_t));cur_pos+=sizeof(uint64_t); // unused
	memcpy(buffer+cur_pos,&size,sizeof(size_t));cur_pos+=sizeof(size_t);
	memcpy(buffer+cur_pos,&offset,sizeof(off_t));cur_pos+=sizeof(off_t);

	int read_cnt=send_request(buffer,cur_pos,to,&servs[server_pos],1,1);
	
	if(read_cnt<(int)sizeof(long long))return -1;

	long long cur_size;
	memcpy(&cur_size,to+read_cnt-sizeof(long long),sizeof(long long));


	if(cur_size>*real_size)
		*real_size=cur_size;

	return read_cnt-sizeof(long long);
}

int raid5_read_stripe(const char* path,char stripe[server_cnt+1][chunk_size+1+sizeof(long long)],int stripeN,long long size){
	int i=0;
	int server_fail=-1;
	int read_sum=0;

	long long real_size=0;

	for(;i<server_cnt-1;i++){
		long long cur_offset=stripeN*(server_cnt-1)*chunk_size+i*chunk_size;

		int read_cnt=raid5_read_to_server(path,stripe[i],chunk_size,stripeN*chunk_size,get_server(cur_offset),&real_size);

		if(read_cnt!=chunk_size&&read_cnt!=-1){int j;for(j=read_cnt;j<chunk_size;j++)stripe[i][j]=0;}


		if(read_cnt==-1){server_fail=i;size+=chunk_size*(server_cnt+2);}
		else {read_sum+=read_cnt;size-=read_cnt;}

		if(size<=0)break;

	}


	int parity_pos=get_parity_position(1ll*stripeN*(server_cnt-1)*chunk_size);
	int read_cnt=raid5_read_to_server(path,stripe[server_cnt-1],chunk_size,stripeN*chunk_size,parity_pos,&real_size);

	if(read_cnt==-1)server_fail=server_cnt-1;
	else read_sum+=read_cnt;
	
	if(read_cnt!=chunk_size&&read_cnt!=-1){int j;for(j=read_cnt;j<chunk_size;j++)stripe[server_cnt-1][j]=0;}


	if(server_fail!=-1){
		int j=0;
		for(;j<chunk_size;j++)
			stripe[server_fail][j]=0;

		for(j=0;j<server_cnt;j++){
			if(j==server_fail)continue;
			int k=0;
			for(;k<chunk_size;k++)
				stripe[server_fail][k]=stripe[server_fail][k]^stripe[j][k];
		}

		read_sum+=chunk_size; //		NOT SURE!!!!!
		
	}

	if(real_size<=stripeN*(server_cnt-1)*chunk_size){
		read_sum=real_size%((server_cnt-1)*chunk_size);}

	return read_sum;
}


int raid5_read(const char *path, char *to, size_t size, off_t offset, struct fuse_file_info* fi){
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
	long long my_read2=full_read;

	offset=offset-offset%(4*4096);
	char response[full_read+1000];

	char stripe[server_cnt+1][chunk_size+1+sizeof(long long)];

	int i=0;
	int j=0;
	for(;i<server_cnt;i++)
		for(;j<chunk_size;j++)
			stripe[i][j]=0;

	long long cur_offset=0;

	while(full_read>0){
		int cur_stripe=offset/((server_cnt-1)*chunk_size);

		int read_cnt=raid5_read_stripe(path,stripe,cur_stripe,full_read);


		if(read_cnt==0)break;
		if(read_cnt!=(server_cnt)*chunk_size&&full_read>read_cnt-chunk_size)full_read=read_cnt-chunk_size;

		i=0;
		for(;i<server_cnt-1;i++){
			if(full_read==0)break;

			int endi=cur_stripe*(server_cnt-1)*chunk_size+(i+1)*chunk_size;

			if(offset<endi){
				int gg=endi-offset;
				if(gg>full_read)gg=full_read;

				memcpy(response+cur_offset,stripe[i]+(offset-(endi-chunk_size)),gg);

				cur_offset+=gg;
				full_read-=gg;
				offset+=gg;
			}
		}
	}

	memcpy(to,response+my_read%(4096*4),size);

	i=0;
	for(;i<my_read2/(4096*4);i++){
		save_file_content(path,response+i*4096*4,my_read+i*4096*4);
	}

	return to_return;
}

int raid5_write_to_server(const char* path,const char* from,size_t size,off_t server_offset,int server_pos,long long actual_write){
	char buffer[size+4096];
	char response[4096];
	int cur_pos=0;

	long long nulli=0;

	strcpy(buffer,"raid5_write");cur_pos+=strlen("raid5_write")+1;
	strcpy(buffer+cur_pos,path);cur_pos+=strlen(path)+1;
	memcpy(buffer+cur_pos,&nulli,sizeof(uint64_t));cur_pos+=sizeof(uint64_t); // unused
	memcpy(buffer+cur_pos,&size,sizeof(size_t));cur_pos+=sizeof(size_t);
	memcpy(buffer+cur_pos,&server_offset,sizeof(off_t));cur_pos+=sizeof(off_t);
	memcpy(buffer+cur_pos,from,size);cur_pos+=size;
	memcpy(buffer+cur_pos,&actual_write,sizeof(long long));cur_pos+=sizeof(long long);

	int written=send_request(buffer,cur_pos,response,&servs[server_pos],1,0);
	if(written==sizeof(int))memcpy(&written,&response,sizeof(int));

	return written;
}

int raid5_write(const char* path, const char* from, size_t size, off_t offset, struct fuse_file_info* fi){

	long long my_offset=offset;
	long long my_size=size;

	char stripe[server_cnt+1][chunk_size+1+sizeof(long long)];
	
	int cur_at=0;

	while(size>0){

		int cur_stripe=offset/((server_cnt-1)*chunk_size);

		off_t server_offset=cur_stripe*chunk_size+offset%chunk_size;
		

		int p=0;int succ=0;int read_cnt;
		for(;p<server_cnt-1;p++){
			if(get_file_content(path,stripe[p],(server_cnt-1)*chunk_size*cur_stripe+p*chunk_size,chunk_size)!=1){succ=1;break;}
		}

		if(succ==1)read_cnt=raid5_read_stripe(path,stripe,cur_stripe,size);
		else read_cnt=(server_cnt-1)*chunk_size;

		int i=0;
		for(;i<server_cnt-1;i++){
			if(size==0)break;

			int endi=cur_stripe*(server_cnt-1)*chunk_size+(i+1)*chunk_size;

			if(offset<endi){
				int to_write_cnt=endi-offset;
				if(to_write_cnt>size)to_write_cnt=size;
		
				int cur_offset=(offset-(endi-chunk_size));

				xor_array(stripe[server_cnt-1],stripe[i],cur_offset,to_write_cnt);   // remove old values
				xor_array(stripe[server_cnt-1],from+cur_at-cur_offset,cur_offset,to_write_cnt); // put new values

				int write_cnt=raid5_write_to_server(path,from+cur_at,to_write_cnt,cur_stripe*chunk_size+cur_offset,get_server(offset),offset+size);

				offset+=to_write_cnt;
				cur_at+=to_write_cnt;
				size-=to_write_cnt;

			}
		}
		raid5_write_to_server(path,stripe[server_cnt-1],chunk_size,cur_stripe*chunk_size,get_parity_position(offset-1),offset+size);

	}

	update_atribute(path,my_offset+my_size);
	update_file_content(path,from,my_offset,my_size);

	return cur_at;
}

int raid5_release(const char* path, struct fuse_file_info* fi){
	
}
