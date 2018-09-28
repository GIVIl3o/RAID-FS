
all: 
	gcc -Wall hello.c `pkg-config fuse --cflags --libs` -o hello.o  -c;
	gcc parser.c -o parser.o -c;
	gcc raid1.c `pkg-config fuse --cflags --libs` -o raid1.o -c;
	gcc raid5.c `pkg-config fuse --cflags --libs` -o raid5.o -c;
	gcc cache.c -o cache.o -c;
	gcc -Wall -o net_raid_client cache.o hello.o parser.o raid1.o raid5.o `pkg-config fuse --cflags --libs`
	
	gcc -g -Wall -o server server.c -lssl -lcrypto
	rm -rf *.o

install:
	sudo apt-get update
	sudo apt-get install libssl-dev
