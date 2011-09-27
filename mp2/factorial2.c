#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>
#include <stdbool.h>

#define PERIOD	1
#define RUNTIME	1

#define LOOPS 1000
#define RUNS_PER_LOOP 100000000
#define VAL_TO_FAC 20LLU
 
unsigned long long fac(unsigned long long in);
bool bContinue = true;

// signal handler to check for ctrl-c 
void signal_callback_handler(int signum)
 {
	if(signum==SIGINT) {
		printf("Got CTRL+C\n");
		bContinue=false;
	}
 }
 
 // test app entry point
int main(void)
{
	FILE *fp;
	size_t count;
	int i, j, in_pid;
	unsigned int in_period, in_runtime;
	bool bFound = false;
	bool registered = false;
	int pid = (int)getpid();
	int ret = EXIT_SUCCESS;
	
	signal(SIGINT, signal_callback_handler);
 
	fp = fopen("/proc/mp2/status", "r+");
	if(fp == NULL) {
		perror("Failed to open proc file!\n");
		ret = EXIT_FAILURE;
		goto cleanup;
	}

	//Register app
	count = fprintf(fp, "R %d %u %u", pid, PERIOD, RUNTIME);

	// Check if we registered successfully
	while(fscanf(fp, "%d %u %u", in_pid, in_period, in_runtime)!=EOF) {
		if(in_pid == pid) {
			bFound = true;
			break;
		}
	}
	if(!bFound) {
		perror("Did not find registration in proc file!\n");
		ret = EXIT_FAILURE;
		goto cleanup;
	}
	registered = true;
	printf("Registered successfully. PID is %d\n", count, pid);

	// Tell scheduler we are ready
	count = fprintf(fp, "Y %d", pid);

	printf("Doing math. Press CTRL+C to stop...\n");

	// Start of real time loop
	while(bContinue && j<LOOPS) {
		for(i=0; i<RUNS_PER_LOOP; i++) {
			fac(VAL_TO_FAC);
			if(!bContinue) {
				break;
			}
		}
		j++;
		count = fprintf(fp, "Y %d", pid);
	}
	// End of real time scheduling loop

	printf("Math done. Press enter key to exit...");
	getchar();

cleanup:
	if(registered)
		count = fprintf(fp, "D %d", pid);
	if(fp!=NULL)
		fclose(fp);
	return ret;
}

// calculates factorial
unsigned long long fac(unsigned long long in)
{
	if(in > 1) {
		return in * fac(in-1);
	}
	else {
		return 1;
	}
}
