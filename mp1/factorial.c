#include <stdio.h>

#define LIMIT 100000

int main(int argc, char *argv[])
{
  	FILE *fp;
	char line[100];
	int limit, temp;
	double f = 0, i = 0;
	limit = argc > 1 ? atoi(argv[1]) : LIMIT;

	/* Register with mp1 kernel module */
  	fp = fopen("/proc/mp1/status", "w");
	if (fp) {
	 	fprintf(fp, "%d", getpid());
		printf("Factorial program's PID: %d\n", getpid());
		fclose(fp);
	}

	/* Compute some factorials */
	for( temp = 0; temp < limit; temp++) {
		f = 1;
		for(i = 1; i <= temp; i++) {
			f *= i;
		}
	}

	/* Get PIDs and CPU times */
	fp = fopen("/proc/mp1/status", "r");
	if (fp) {
		while (fgets(line, 100, fp) != NULL) {
			printf( "%s\n", line );
		}
		fclose(fp);
	}

  	return 0;
}
