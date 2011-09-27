#include <stdio.h>
#include <sys/time.h>

#define LOOPS 1000
#define RUNS_PER_LOOP 50000
#define VAL_TO_FAC 20LLU

unsigned long long fac(unsigned long long in)
{
        if(in > 1)
                return in * fac(in-1);
        else
                return 1;
}

static inline long msecs_passed(struct timeval *from, struct timeval *until)
{
        long secs = until->tv_sec - from->tv_sec;
        long micros = until->tv_usec - from->tv_usec;
        return secs * 1000 + micros / 1000;
}

int main(void)
{
        struct timeval before, after;
        int i, j = 0;

        while (j < LOOPS) {
                gettimeofday(&before, NULL);
                for (i=0; i<RUNS_PER_LOOP; i++)
                        fac(VAL_TO_FAC);
                j++;
                gettimeofday(&after, NULL);
                printf("done loop in %ld\n", msecs_passed(&before, &after));
        }

}
