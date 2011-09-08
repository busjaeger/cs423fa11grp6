obj-m += mp1.o

all:
	make -C /lib/modules/$(shell uname -r)/build SUBDIRS=$(PWD) modules

clean:
	make -C /lib/modules/$(shell uname -r)/build SUBDIRS=$(PWD) clean

factorial: factorial.c
	gcc -g factorial.c -o factorial
