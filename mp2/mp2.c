/*
 * TODO possible considerations
 *  could the task struct become invalid while referenced?
 *  could the mrs_task / task state get inconsistent?
 *  think through locking strategy; streamline locking
 */

#include <linux/init.h>
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/proc_fs.h>
#include <linux/kthread.h>
#include <linux/timer.h>
#include <asm/uaccess.h>
#include "mp2_given.h"

#define DIR_NAME "mp2"
#define FILE_NAME "status"
#define THREAD_NAME "mrs-dispatcher"
#define MAX_PERIOD 0xffffffff

enum mrs_state {NEW, SLEEPING, READY, RUNNING };

struct mrs_task_struct
{
	struct task_struct* task;
	struct timer_list period_timer;
	enum mrs_state state;
	struct list_head list;
	unsigned int period;
	unsigned int runtime;
};

// proc directory stats file
static struct proc_dir_entry *proc_file;
// task list
static LIST_HEAD(mrs_tasks);
// list lock
static DEFINE_MUTEX(mrs_mutex);
// dispatcher thread
static struct task_struct *dispatcher_thread;
// global current task
static struct mrs_task_struct *currently_running_task;


// find task from task list
struct mrs_task_struct *_find_mrs_task(pid_t pid)
{
        struct mrs_task_struct *mrs_task;
        list_for_each_entry(mrs_task, &mrs_tasks, list) {
                if (mrs_task->task->pid == pid)
                        return mrs_task;
        }
        return NULL;
}

// dispatcher thread function
static int dispatch(void *data)
{
	struct mrs_task_struct *position = NULL;
	struct mrs_task_struct *task_with_highest_priority = NULL;
	unsigned int shortest_period = MAX_PERIOD;
	struct sched_param *sparam = NULL;

	while(1) {
		if (kthread_should_stop()) {
			break;
		}
		// find ready job from list w/ highest priority (shortest period)
		position = NULL;
		task_with_highest_priority = NULL;
		shortest_period = MAX_PERIOD;
		sparam = NULL;
		list_for_each_entry(position, &mrs_tasks, list) {
			if (position->state == READY) {
				if (position->period < shortest_period ) {
					task_with_highest_priority = position;
				}
			}
		}
		// set task to FIFO
		// set task to RUNNING
		// if previously RUNNING -> change to NORMAL prio & READY
		// update global task
		if (task_with_highest_priority) {
			// TODO Not sure about the order of these lines:
			wake_up_process(task_with_highest_priority->task);
			sparam->sched_priority = MAX_USER_RT_PRIO - 1;
			task_with_highest_priority->state = RUNNING;
			sched_setscheduler(task_with_highest_priority->task, SCHED_FIFO, 
								sparam);

			sparam->sched_priority = 0;
			currently_running_task->state = READY;
			sched_setscheduler(currently_running_task->task, SCHED_NORMAL, 
								sparam);

			currently_running_task = task_with_highest_priority;
		}
		// else TODO need to do anything in this case?		
	}
	return 0;
}

// timer callback
void period_timeout(unsigned long data)
{
	struct mrs_task_struct *mrs_task = (struct mrs_task_struct *)data;
	switch (mrs_task->state) {
		case NEW:
			printk(KERN_ALERT "mrs: timer triggered for new task %d.\n", 
					mrs_task->task->pid);
			break;
		case SLEEPING:
			mrs_task->state = READY;
			wake_up_process(dispatcher_thread);
			break;
		case READY:
			printk(KERN_ALERT "mrs: timer triggered for ready task %d.\n", 
					mrs_task->task->pid);
			break;
		case RUNNING:
			printk(KERN_WARNING "mrs: timer triggered for running task %d.\n", 
					mrs_task->task->pid);
			break;
	}
}

// allocate and initialize a task
struct mrs_task_struct *create_mrs_task(struct task_struct *task,
				unsigned int period, unsigned int runtime)
{
    struct mrs_task_struct *mrs_task = kmalloc(sizeof(*mrs_task), GFP_KERNEL);
	if (mrs_task) {
	        mrs_task->task = task;
	        mrs_task->state = NEW;
	        mrs_task->period = period;
		mrs_task->runtime = runtime;
	        init_timer(&mrs_task->period_timer);
	        mrs_task->period_timer.function = period_timeout;
	        mrs_task->period_timer.data = (unsigned long)mrs_task;
	}
	return mrs_task;
}

// admission control: returns 0 if can be admitted, 1 if can not be admitted
static int _mrs_admission_control(unsigned int new_task_period, 
									unsigned int new_task_runtime)
{
	struct mrs_task_struct *position = NULL;
	unsigned int acceptance_value = 693;
	int rvalue = 1;
	unsigned int sum_ratio = new_task_runtime / new_task_period;
	list_for_each_entry(position, &mrs_tasks, list) {
		sum_ratio = sum_ratio + (position->runtime / position->period);
	}
	if ((sum_ratio * 1000) <= acceptance_value) {
		rvalue = 0;
	}	
	return rvalue;
}

static int register_mrs_task(pid_t pid, unsigned int period, 
									unsigned int runtime)
{
	struct task_struct *task;
	struct mrs_task_struct *mrs_task;
	if (period == 0 || runtime == 0) {
		printk(KERN_ERR "mrs: invalid arguments.\n");
		return -1;
	}
	task = find_task_by_pid(pid);
	if (!task || task != current) {
		printk(KERN_ERR "mrs: invalid pid %d.\n", pid);
		return -1;
	}
	mutex_lock(&mrs_mutex);
	if (_mrs_admission_control(period, runtime)) {
		printk(KERN_ERR "mrs: pid %d not admitted\n", pid);
		mutex_unlock(&mrs_mutex);
		return -1;
	}
	if (_find_mrs_task(pid)) {
		printk(KERN_ERR "mrs: pid %d already registered.\n", pid);
		mutex_unlock(&mrs_mutex);
		return -1;
	}
	mrs_task = create_mrs_task(task, period, runtime);
	if (!mrs_task) {
		mutex_unlock(&mrs_mutex);
		return -ENOMEM;
	}
	list_add_tail(&mrs_task->list, &mrs_tasks);
	mutex_unlock(&mrs_mutex);
	return 0;
}

static int yield_msr_task(pid_t pid)
{
	struct mrs_task_struct *mrs_task;
	mutex_lock(&mrs_mutex);
	mrs_task = _find_mrs_task(pid);
	mutex_unlock(&mrs_mutex);
	if (!mrs_task) {
		printk(KERN_ERR "mrs: pid %d not registered.\n", pid);
		return -1;
	}
	switch (mrs_task->state) {
		case NEW:
			// TODO locking, weak up dispatcher
			mrs_task->state = READY;
			break;
		case RUNNING:
			// TODO locking, wake up dispatcher, set timer
			mrs_task->state = SLEEPING;
	                __set_current_state(TASK_UNINTERRUPTIBLE);
			schedule();
			break;
		default:
			printk(KERN_ERR "mrs: invalid state transition attempted.\n");
			return -1;
	}
	return 0;
}

static int deregister_mrs_task(pid_t pid)
{
	struct list_head *ptr, *tmp;
	struct mrs_task_struct *task;
	
	mutex_lock(&mrs_mutex);

	list_for_each_safe(ptr, tmp, &mrs_tasks) {
		task = list_entry(ptr, struct mrs_task_struct, list);
		if(task->task->pid == pid) {
			if(task == currently_running_task) {
				currently_running_task = NULL;
			}
			
			list_del(ptr);
			del_timer(&task->period_timer); // should we remove task timer?
			kfree(task);
			break;
		}
	}
		
	mutex_unlock(&mrs_mutex);
	
	return 0;
}

int mrs_write_proc(struct file *file, const char __user *buffer,
                        unsigned long count, void *data)
{
	char *proc_buffer;
	pid_t pid;
	unsigned int period, runtime;

	proc_buffer=kmalloc(count, GFP_KERNEL);
	if (copy_from_user(proc_buffer, buffer, count)) {
		return -EFAULT;
	}
	// TODO error handling
	switch (*proc_buffer) {
		case 'R':
			sscanf(proc_buffer, "R %d %u %u", &pid, &period, &runtime);
			register_mrs_task(pid, period, runtime);
			break;
		case 'Y':
			sscanf(proc_buffer, "Y %d", &pid);
			yield_msr_task(pid);
			break;
		case 'D':
			sscanf(proc_buffer, "D %d", &pid);
			deregister_mrs_task(pid);
			break;
		default:
	                printk(KERN_ERR "mrs: invalid write command.\n");
	                return count = -1;
	}
	kfree(proc_buffer);
	return count;
}

// Print registered process list to proc special file
int mrs_read_proc(char *page, char **start, off_t off,
                        int count, int *eof, void *data)
{
	char *ptr = page;
	struct mrs_task_struct *mrs_task;
	
	mutex_lock(&mrs_mutex);
	
	list_for_each_entry(mrs_task, &mrs_tasks, list)	{
		// Output: PID Period ProccessingTime
		ptr += sprintf( ptr, "%d %d %d\n", mrs_task->task->pid, 
					mrs_task->period, 
					mrs_task->runtime);
		
	}	
	mutex_unlock(&mrs_mutex);
		
	return ptr - page;
}

// module init
static int mrs_init(void)
{
	struct proc_dir_entry *proc_dir;
	// create proc directory and file
	proc_dir = proc_mkdir_mode(DIR_NAME, S_IRUGO | S_IXUGO, NULL);
	if (!proc_dir) {
		printk(KERN_ERR "mrs: unable to create proc directory %s.\n", DIR_NAME);
		return -ENOMEM;
	}
	proc_file = create_proc_entry(FILE_NAME, S_IRUGO | S_IWUGO, proc_dir);
	if (!proc_file) {
		printk(KERN_ERR "mrs: unable to create proc file %s.\n", FILE_NAME);
		remove_proc_entry(DIR_NAME, NULL);
		return -ENOMEM;
	}
	// set proc functions
	proc_file->read_proc = mrs_read_proc;
	proc_file->write_proc = mrs_write_proc;
	// create dispatcher thread (not started here)
	dispatcher_thread = kthread_create(dispatch, NULL, THREAD_NAME);
	if (IS_ERR(dispatcher_thread)) {
		printk(KERN_ERR "mrs: failed to create kernel thread %s.\n", 
				THREAD_NAME);
		remove_proc_entry(FILE_NAME, proc_dir);
		remove_proc_entry(DIR_NAME, NULL);
		return PTR_ERR(dispatcher_thread);
	}
	currently_running_task = NULL;
        return 0;
}

/**
 * releases module resources and frees dynamically allocated data
 */
static void mrs_exit(void)
{
	struct list_head *ptr, *tmp;
	struct mrs_task_struct *task;
	// remove proc directory and file
	if (proc_file) {
		remove_proc_entry(FILE_NAME, proc_file->parent);
		remove_proc_entry(DIR_NAME, NULL);
	}
        // stop kernel thread
        kthread_stop(dispatcher_thread);
	// free stats list
	mutex_lock(&mrs_mutex);
	list_for_each_safe(ptr, tmp, &mrs_tasks) {
		task = list_entry(ptr, struct mrs_task_struct, list);
		list_del(ptr);
		// TODO stop timer? ==> del_timer(&task->period_timer);
		kfree(task);
	}
	mutex_unlock(&mrs_mutex);
}

module_init(mrs_init);
module_exit(mrs_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Group 06");
MODULE_DESCRIPTION("CS423 Fa2011 MP2");
