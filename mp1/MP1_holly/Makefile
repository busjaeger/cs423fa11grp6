obj-m += mp1.o

userapp: userapp.c
	gcc userapp.c -o userapp

factorial: factorial.c
	gcc factorial.c -o factorial

all:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) modules

clean:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
