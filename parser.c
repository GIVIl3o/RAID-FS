#include <stdio.h>
#include <string.h>
#include <stdlib.h>


void set_key(char* buffer,char* key){
	int i=0;
	while(buffer[i]!=' '){
		key[i]=buffer[i];
		i++;
	}key[i]='\0';
}

void set_value(char* buffer,char* value){
	int i=0;
	while(buffer[i]!='=')i++;
	i+=2;
	strcpy(value,buffer+i);
	value[strlen(value)-1]='\0';
}

void get_ip(char* buffer,char* value){
	int i=0;
	while(buffer[i]!=':'){
		value[i]=buffer[i];
		i++;
	}
	value[i]='\0';
}

int get_port(char* buffer){
	int port=0;

	int i=0;
	while(buffer[i]!=':'){
		i++;
	}
	i++;
	while(buffer[i]>='0'&&buffer[i]<='9'){
		port*=10;
		port+=(buffer[i]-'0');
		i++;
	}
	return port;
}

int get_timeout(char* buffer){
	int i=0;
	int value=0;
	while(buffer[i]!='=')i++;
	i+=2;
	while(buffer[i]>='0'&&buffer[i]<='9'){
		value*=10;
		value+=buffer[i]-'0';
		i++;
	}
	return value;
}
