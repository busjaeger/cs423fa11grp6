#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>
#include <stdbool.h>
#include <sys/time.h>
#include <time.h>

#define PERIOD	240
#define RUNTIME	40

#define LOOPS 12
#define RUNS_PER_LOOP 300000
#define VAL_TO_FAC 20LLU
#define TIME_STRING_LENGTH 255
 
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
	int i, j = 0, in_pid;
	unsigned int in_period, in_runtime;
	bool bFound = false;
	int pid = (int)getpid();
	int ret = EXIT_SUCCESS;
	struct timeval tval;
	char strtime[TIME_STRING_LENGTH];
	time_t second;
	struct timeval tval_actual_runtime;
	time_t second_actual_runtime;
	
	signal(SIGINT, signal_callback_handler);
 
	fp = fopen("/proc/mp2/status", "r+");
	if(fp == NULL) {
		perror("Failed to open proc file!\n");
		ret = EXIT_FAILURE;
		goto cleanup;
	}

	//Register app
	count = fprintf(fp, "R %d %u %u", pid, PERIOD, RUNTIME);
	fflush(fp);

	// Check if we registered successfully
	while(fscanf(fp, "%d %u %u", &in_pid, &in_period, &in_runtime)!=EOF) {
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
	rewind(fp);
	gettimeofday(&tval, NULL);
	second = tval.tv_sec;
	strftime(strtime, TIME_STRING_LENGTH, "%D %T", localtime(&second));
	printf("Registered successfully at: %s.%06ld. PID is %d\n", strtime, tval.tv_usec, pid);

	// Tell scheduler we are ready
	count = fprintf(fp, "Y %d", pid);
	fflush(fp);

	printf("Doing math. Press CTRL+C to stop...\n");

	
	// Start of real time loop
	while(bContinue && j<LOOPS) {
		gettimeofday(&tval, NULL);
		second = tval.tv_sec;
		strftime(strtime, TIME_STRING_LENGTH, "%D %T", localtime(&second));
		printf("Process %d woke at: %s.%06ld\n", pid, strtime, tval.tv_usec);
		for(i=0; i<RUNS_PER_LOOP; i++) {
			fac(VAL_TO_FAC);
			if(!bContinue) {
				break;
			}
		}
		j++;
		gettimeofday(&tval_actual_runtime, NULL);
		second_actual_runtime = tval_actual_runtime.tv_sec;
		strftime(strtime, TIME_STRING_LENGTH, "%D %T", localtime(&second_actual_runtime));
		printf("Took %06ld\n", (tval_actual_runtime.tv_usec - tval.tv_usec));
		count = fprintf(fp, "Y %d", pid);
		fflush(fp);
	}
	
	// End of real time scheduling loop

cleanup:
	if(bFound)
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
