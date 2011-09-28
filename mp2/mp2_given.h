#ifndef __MP2_GIVEN_INCLUDE__
#define __MP2_GIVEN_INCLUDE__

#include <linux/pid.h>
#include <linux/sched.h>

struct task_struct* find_task_by_pid(unsigned int nr)
{
    struct task_struct* task = NULL;
    printk(KERN_INFO "mrs: find_task_by_pid: passed pid %d\n", nr);
    rcu_read_lock();
    task=pid_task(find_vpid(nr), PIDTYPE_PID);
    rcu_read_unlock();
    if (task == NULL)
	printk(KERN_INFO "mrs: find_task_by_pid: couldnt find pid %d\n", nr);

    return task;
}

#endif
