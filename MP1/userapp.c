#include <stdio.h>

int main()
{
  	FILE *fp;
	char line[100];


  	fp = fopen("/proc/mp1/status", "w");
  	if (fp)
	{
		fprintf(fp, "%d", getpid());
		printf("userapp's PID: %d\n", getpid());
  		fclose(fp);
	}

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
