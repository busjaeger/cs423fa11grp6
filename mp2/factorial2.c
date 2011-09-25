#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>
#include <stdbool.h>

#define PERIOD	1
#define RUNTIME	1
 
unsigned long long fac(unsigned long long in);
bool bContinue = true;
 
 void signal_callback_handler(int signum)
 {
	if(signum==SIGINT);
	{
		printf("Got CTRL+C\n");
		bContinue=false;
	}
 }
 
int main(void)
{
    FILE *fp;
    size_t count;
    int i;
	int in_pid;
	unsigned int in_period, in_runtime;
	bool bFound = false;
	int pid = (int)getpid();
	int ret = EXIT_SUCCESS;
	
	signal(SIGINT, signal_callback_handler);
 
    fp = fopen("/proc/mp2/status", "r+");
    if(fp == NULL) {
        perror("Failed to open proc file!\n");
        ret = EXIT_FAILURE;
		goto cleanup;
    }
    count = fprintf(fp, "R %d %u %u", pid, PERIOD, RUNTIME);
	
	while(fscanf(fp, "%d %u %u", in_pid, in_period, in_runtime)!=EOF)
	{
		if(in_pid == pid)
		{
			bFound = true;
			break;
		}
	}
	
	if(!bFound)
	{
		perror("Did not find registration in proc file!\n");
		ret = EXIT_FAILURE;
		goto cleanup;
	}
	
    printf("Registered successfully. PID is %d\n", count, pid);
	printf("Doing math. Press CTRL+C to stop...\n");
	
	while(bContinue) {
		for(i=0; i<100000000; i++) {
			fac(20LLU);
			if(!bContinue) {
				break;
			}
		}
		count = fprintf(fp, "Y %d", pid);
	}
	
	count = fprintf(fp, "D %d", pid);
	
	printf("Math done. Press enter key to exit...");
	getchar();

cleanup:
	if(fp!=NULL)
		fclose(fp);
    return ret;
}

unsigned long long fac(unsigned long long in)
{
	if(in > 1)
	{
		return in * fac(in-1);
	}
	else
	{
		return 1;
	}
}
