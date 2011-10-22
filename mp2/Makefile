obj-m += mp2.o

all:
	make -C /lib/modules/$(shell uname -r)/build SUBDIRS=$(PWD) modules

clean:
	make -C /lib/modules/$(shell uname -r)/build SUBDIRS=$(PWD) clean

factorial: factorial2.c
	gcc -g factorial2.c -o factorial

factorial20 : factorial_20ms.c
	gcc -g factorial_20ms.c -o factorial20

factorial10 : factorial_10ms.c
	gcc -g factorial_10ms.c -o factorial10

factorial40 : factorial_40ms.c
	gcc -g factorial_40ms.c -o factorial40
