#include <stdio.h>

int main()
{
  	FILE *fp;
	char line[100];


  	fp = fopen("/proc/mp1/status", "w");
  	fprintf(fp, "%d", getpid());
  	fclose(fp);

	fp = fopen("/proc/mp1/status", "r");
	while (fgets(line, 100, fp) != NULL)
	{
		printf( "%s\n", line );
	}
	fclose(fp);
	
  	return 0;
}
