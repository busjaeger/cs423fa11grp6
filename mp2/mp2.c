#include <linux/init.h>
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/proc_fs.h>
#include <linux/kthread.h>
#include <linux/timer.h>
#include <asm/uaccess.h>
#include <linux/semaphore.h>
#include "mp2_given.h"

#define DIR_NAME "mp2"
#define FILE_NAME "status"
#define THREAD_NAME "mrs-dispatcher"
#define MAX_PERIOD 0xffffffff
#define MRS_PRIO MAX_USER_RT_PRIO - 1
#define MRS_THRESHOLD 693

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
static DEFINE_SPINLOCK(mrs_lock);
// coordination semaphore
static struct semaphore mrs_sem;
// dispatcher thread
static struct task_struct *dispatcher_thread;
// thread control flag
static int should_stop;
// global current task
static struct mrs_task_struct *current_mrs;

// find task from task list
static struct mrs_task_struct *_find_mrs_task(pid_t pid)
{
	struct mrs_task_struct *mrs_task;
	list_for_each_entry(mrs_task, &mrs_tasks, list) {
		if (mrs_task->task->pid == pid)
			return mrs_task;
	}
        return NULL;
}

static struct mrs_task_struct *_remove_mrs_task(pid_t pid)
{
	struct list_head *ptr, *tmp;
	struct mrs_task_struct *mrs_task;
	list_for_each_safe(ptr, tmp, &mrs_tasks) {
		mrs_task = list_entry(ptr, struct mrs_task_struct, list);
		if (mrs_task->task->pid == pid) {
                        list_del(ptr);
			return mrs_task;
                }
        }
	return NULL;
}

// find task ready task with shortest period
static struct mrs_task_struct *_find_next_mrs_task(void)
{
	struct mrs_task_struct *pos, *next_ready = NULL;
	unsigned int sp = MAX_PERIOD;
	list_for_each_entry(pos, &mrs_tasks, list) {
		if (pos->state == READY && pos->period < sp) {
			sp = pos->period;
			next_ready = pos;
                }
	}
	return next_ready;
}

// updates the task timer
static inline int _mod_period_timer(struct mrs_task_struct *mrs_task)
{
	unsigned long expires = jiffies + msecs_to_jiffies(mrs_task->period);
	return mod_timer(&mrs_task->period_timer, expires);
}

// Updates task scheduling policy
static inline int _mrs_sched_setscheduler(struct task_struct *task, int policy,
								int priority)
{
	struct sched_param sparam = { priority };
	return sched_setscheduler(task, policy, &sparam);
}

static void _mrs_schedule(void)
{
	struct mrs_task_struct *next;
	// current task was put to sleep, so reset priority and unset current
	if (current_mrs && current_mrs->state == SLEEPING) {
		_mrs_sched_setscheduler(current_mrs->task, SCHED_NORMAL, 0);
		current_mrs = NULL;
	}
	// if another task with higher priority is ready, switch to it
	next = _find_next_mrs_task();
	if (next && (!current_mrs || current_mrs->period > next->period)) {
		// preempt current task if present
		if (current_mrs) {
			current_mrs->state = READY;
			_mrs_sched_setscheduler(current_mrs->task, SCHED_NORMAL,
				 0);
			// we don't want this task run with normal priority
			set_task_state(current_mrs, TASK_UNINTERRUPTIBLE);
		}
		next->state = RUNNING;
		_mrs_sched_setscheduler(next->task, SCHED_FIFO, MRS_PRIO);
		wake_up_process(next->task);
		current_mrs = next;
	}
}

// dispatcher thread function
static int dispatch(void *data)
{
	unsigned long flags;
	
	printk(KERN_INFO "mrs: dispatcher entry.\n");
	while(1) {
		printk(KERN_INFO "mrs: dispatcher waiting.\n");
		if (down_interruptible(&mrs_sem) == -EINTR)
			break;
		printk(KERN_INFO "mrs: dispatcher awake.\n");
		spin_lock_irqsave(&mrs_lock, flags);
		if (should_stop) {
			spin_unlock_irqrestore(&mrs_lock, flags);
			break;
		}
		_mrs_schedule();
		spin_unlock_irqrestore(&mrs_lock, flags);
	}
	printk(KERN_INFO "mrs: dispatcher exit.\n");
	return 0;
}

// timer callback - wakes dispatcher
void period_timeout(unsigned long data)
{
	struct mrs_task_struct *mrs_task;
	unsigned long flags;
	pid_t pid = (pid_t)data;
	int err;

	printk(KERN_INFO "mrs: %d timer entry.\n", pid);
	spin_lock_irqsave(&mrs_lock, flags);
	mrs_task = _find_mrs_task(pid);
        if (!mrs_task) {
		err = -ESRCH;
                goto error;
	}
	_mod_period_timer(mrs_task);
	switch (mrs_task->state) {
	case SLEEPING:
		mrs_task->state = READY;
		up(&mrs_sem);
		break;
	default:
		err = -EPERM;
		goto error;
	}
        spin_unlock_irqrestore(&mrs_lock, flags);
	printk(KERN_INFO "mrs: %d timer exit.\n", pid);
	return;
error:
        spin_unlock_irqrestore(&mrs_lock, flags);
	printk(KERN_WARNING "mrs: %d timer failed with error %d.\n", pid, err);
}

// allocate and initialize a task
struct mrs_task_struct *_create_mrs_task(struct task_struct *task,
				unsigned int period, unsigned int runtime)
{
	struct mrs_task_struct *mrs_task = kmalloc(sizeof(*mrs_task), GFP_KERNEL);
	if (mrs_task) {
	        mrs_task->task = task;
	        mrs_task->state = NEW;
		INIT_LIST_HEAD(&mrs_task->list);
	        mrs_task->period = period;
		mrs_task->runtime = runtime;
	        init_timer(&mrs_task->period_timer);
	        mrs_task->period_timer.function = period_timeout;
	        mrs_task->period_timer.data = mrs_task->task->pid;
	}
	return mrs_task;
}

// admission control: returns 0 if can be admitted, 1 if can not be admitted
static int _mrs_admission_control(unsigned int period, unsigned int runtime)
{
	struct mrs_task_struct *pos;
	unsigned int sum_ratio = (1000 * runtime) / period;
	list_for_each_entry(pos, &mrs_tasks, list)
		sum_ratio += (1000 * pos->runtime) / pos->period;
	return sum_ratio > MRS_THRESHOLD;
}

/*
 * Verifies that a PID is valid, passes admission control, and isn't already 
 * registered. If verificaiton succeeds, creates a new task entry and adds it
 * to the list.
 */
static int register_mrs_task(pid_t pid, unsigned int period,
				unsigned int runtime)
{
	struct task_struct *task;
	struct mrs_task_struct *mrs_task;
	unsigned long flags;
	int err = -1;
	if (period == 0 || runtime == 0)
		return -EINVAL;
	task = find_task_by_pid(pid);
	if (!task || task != current)
		return -ESRCH;
	// optimistically allocate task outside of critical region
	mrs_task = _create_mrs_task(task, period, runtime);
	if (!mrs_task)
		return -ENOMEM;
	// critical section: add task if admitted and not alrady present
	spin_lock_irqsave(&mrs_lock, flags);
	if (_mrs_admission_control(period, runtime)) {
		err = -EBUSY;
		goto error;
	}
	if (_find_mrs_task(pid)) {
		err = -EEXIST;
		goto error;
	}
	list_add_tail(&mrs_task->list, &mrs_tasks);
	spin_unlock_irqrestore(&mrs_lock, flags);
	return 0;
error:
	spin_unlock_irqrestore(&mrs_lock, flags);
	kfree(mrs_task);
	return err;
}

// Handles yield calls from usermode app. Changes task state and wakes the
// dispatcher thread
static int yield_msr_task(pid_t pid)
{
	struct mrs_task_struct *mrs_task;
	unsigned long flags;
	int err = -1;

	printk(KERN_INFO "mrs: %d yield entry.\n", pid);
	spin_lock_irqsave(&mrs_lock, flags);
	mrs_task = _find_mrs_task(pid);
	if (!mrs_task) {
		err = -ESRCH;
		goto error;
	}
	switch (mrs_task->state) {
	case NEW:
		mrs_task->state = READY;
		_mod_period_timer(mrs_task);
		break;
	case RUNNING:
		mrs_task->state = SLEEPING;
		break;
	default:
		err = -EPERM;
		goto error;
	}
	__set_current_state(TASK_UNINTERRUPTIBLE);
	up(&mrs_sem);
	spin_unlock_irqrestore(&mrs_lock, flags);
	printk(KERN_INFO "mrs: %d yielding.\n", pid);
	schedule();
	printk(KERN_INFO "mrs: %d yield exit.\n", pid);
	return 0;
error:
	spin_unlock_irqrestore(&mrs_lock, flags);
	return err;
}

// Remove a task from the list, delete its timer, and free it
static int deregister_mrs_task(pid_t pid)
{
	struct mrs_task_struct *mrs_task;
	unsigned long flags;
	printk(KERN_INFO "mrs: %d deregistering entry.\n", pid);
	spin_lock_irqsave(&mrs_lock, flags);
	mrs_task =_remove_mrs_task(pid);
	// TODO do we need to let the dispatcher know?
	if (mrs_task == current_mrs)
		current_mrs = NULL;
	spin_unlock_irqrestore(&mrs_lock, flags);
	if (mrs_task) {
		del_timer(&mrs_task->period_timer);
		kfree(mrs_task);
	}
	printk(KERN_INFO "mrs: %d deregistering exit.\n", pid);
	return 0;
}

// Get input from usermode app and take action on it
int mrs_write_proc(struct file *file, const char __user *user_buf,
                        unsigned long count, void *data)
{
	char *buf;
	pid_t pid;
	unsigned int period, runtime;
	int ret;

	buf = kmalloc(count + 1, GFP_KERNEL);
	if (copy_from_user(buf, user_buf, count))
		return -EFAULT;
	buf[count] = '\0';
	printk(KERN_INFO "mrs: command received \"%s\"\n", buf);
	switch (*buf) {
	case 'R':
		if (sscanf(buf, "R %d %u %u", &pid, &period, &runtime) != 3)
			ret = -EINVAL;
		else
			ret = register_mrs_task(pid, period, runtime);
		break;
	case 'Y':
		if (sscanf(buf, "Y %d", &pid) != 1)
			ret = -EINVAL;
		else
			ret = yield_msr_task(pid);
		break;
	case 'D':
		if (sscanf(buf, "D %d", &pid) != 1)
			ret = -EINVAL;
		else
			ret = deregister_mrs_task(pid);
		break;
	default:
		ret = -EINVAL;
		break;
	}
	kfree(buf);
	if (ret < 0)
		printk(KERN_ERR "mrs: proc write failed with error %d.\n", ret);
	else
		ret = count;
	return ret;
}

// Print registered process list to proc special file
int mrs_read_proc(char *page, char **start, off_t off,
                        int count, int *eof, void *data)
{
	char *ptr = page;
	struct mrs_task_struct *mrs_task;
	unsigned long flags;

	spin_lock_irqsave(&mrs_lock, flags);
	list_for_each_entry(mrs_task, &mrs_tasks, list)	{
		// Output: PID Period ProccessingTime
		ptr += sprintf( ptr, "%d %u %u\n", mrs_task->task->pid,
					mrs_task->period, mrs_task->runtime);
	}
	spin_unlock_irqrestore(&mrs_lock, flags);

	return ptr - page;
}

// module init - sets up structes, registeres proc file and creates the
// dispatcher kthread
static int _mrs_init(void)
{
	struct proc_dir_entry *proc_dir;
	int err;
	// create proc directory and file
	proc_dir = proc_mkdir_mode(DIR_NAME, S_IRUGO | S_IXUGO, NULL);
	if (!proc_dir) {
		err = -ENOMEM;
		goto error;
	}
	proc_file = create_proc_entry(FILE_NAME, S_IRUGO | S_IWUGO, proc_dir);
	if (!proc_file) {
		err = -ENOMEM;
		goto error_rmd;
	}
	// set proc functions
	proc_file->read_proc = mrs_read_proc;
	proc_file->write_proc = mrs_write_proc;
	// init semaphore used by dispatcher
	sema_init(&mrs_sem, 0);
	current_mrs = NULL;
	// create dispatcher thread (not started here)
	dispatcher_thread = kthread_create(dispatch, NULL, THREAD_NAME);
        if (IS_ERR(dispatcher_thread)) {
		err = PTR_ERR(dispatcher_thread);
		goto error_rm;
        }
	should_stop = 0;
	_mrs_sched_setscheduler(dispatcher_thread, SCHED_FIFO, MAX_USER_RT_PRIO);
	wake_up_process(dispatcher_thread);
        return 0;
error_rm:
	remove_proc_entry(FILE_NAME, proc_dir);
error_rmd:
	remove_proc_entry(DIR_NAME, NULL);
error:
	printk(KERN_ERR "mrs: module failed to init due to %d.\n", err);
	return err;
}

// releases module resources and frees dynamically allocated data
static void mrs_exit(void)
{
	struct list_head *ptr, *tmp;
	struct mrs_task_struct *task;
	unsigned long flags;
	// remove proc directory and file
	if (proc_file) {
		remove_proc_entry(FILE_NAME, proc_file->parent);
		remove_proc_entry(DIR_NAME, NULL);
	}
	spin_lock_irqsave(&mrs_lock, flags);
	// free stats list
	list_for_each_safe(ptr, tmp, &mrs_tasks) {
		task = list_entry(ptr, struct mrs_task_struct, list);
		list_del(ptr);
		del_timer(&task->period_timer);
		kfree(task);
	}
	current_mrs = NULL;
	// stop kernel thread
	should_stop = 1;
	up(&mrs_sem);
	spin_unlock_irqrestore(&mrs_lock, flags);
	kthread_stop(dispatcher_thread);
}

module_init(_mrs_init);
module_exit(mrs_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Group 06");
MODULE_DESCRIPTION("CS423 Fa2011 MP2");
