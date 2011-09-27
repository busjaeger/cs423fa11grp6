obj-m += mp2.o

all:
	make -C /lib/modules/$(shell uname -r)/build SUBDIRS=$(PWD) modules

clean:
	make -C /lib/modules/$(shell uname -r)/build SUBDIRS=$(PWD) clean

factorial: factorial2.c
	gcc -g factorial2.c -o factorial
