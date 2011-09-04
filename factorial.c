#include <stdio.h>

#define LIMIT 100000

int main()
{
  	FILE *fp;
	char line[100];
	double temp = 0;
	double f = 0;
	double i = 0;

	/* Register with mp1 kernel module */
  	fp = fopen("/proc/mp1/status", "w");
	if (fp)
	{
	 	fprintf(fp, "%d", getpid());
		printf("Factorial program's PID: %d\n", getpid());
		fclose(fp);
	}
  	

	/* Compute some factorials */
	for( temp = 0; temp < LIMIT; temp++)
	{
		f = 1;
		for(i = 1; i <= temp; i++)
		{
			f *= i;
		}
		//printf("Computed factorial for %d to be %d.\n", temp, f);
	}

	/* Get PIDs and CPU times */
	fp = fopen("/proc/mp1/status", "r");
	if (fp)
	{
		while (fgets(line, 100, fp) != NULL)
		{
			printf( "%s\n", line );
		}
		fclose(fp);
	}
	
	
  	return 0;
}
