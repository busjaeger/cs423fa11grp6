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
	struct mrs_task_struct *mrs_task, *removed;
	list_for_each_safe(ptr, tmp, &mrs_tasks) {
		mrs_task = list_entry(ptr, struct mrs_task_struct, list);
		if(mrs_task->task->pid == pid) {
                        list_del(ptr);
			removed = mrs_task;
                        break;
                }
        }
	return removed;
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
	while(1) {
		down(&mrs_sem);
		spin_lock_irqsave(&mrs_lock, flags);
		if (should_stop)
			break;
		_mrs_schedule();
		spin_unlock_irqrestore(&mrs_lock, flags);
	}
	return 0;
}

// timer callback - wakes dispatcher
void period_timeout(unsigned long data)
{
	struct mrs_task_struct *mrs_task;
	unsigned long flags;
	pid_t pid = (pid_t)data;

	spin_lock_irqsave(&mrs_lock, flags);
	mrs_task = _find_mrs_task(pid);
        if (!mrs_task)
                goto err;
	_mod_period_timer(mrs_task);
	switch (mrs_task->state) {
	case NEW:
	case READY:
	case RUNNING:
		goto err;
	case SLEEPING:
		mrs_task->state = READY;
		up(&mrs_sem); //move after spin unlock?
		break;
	}
        spin_unlock_irqrestore(&mrs_lock, flags);
	return;
err:
        spin_unlock_irqrestore(&mrs_lock, flags);
	printk(KERN_ALERT "mrs: timer triggered for awake task %d.\n", pid);
}

// allocate and initialize a task
struct mrs_task_struct *_create_mrs_task(struct task_struct *task,
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
	struct mrs_task_struct *position;
	const unsigned int acceptance_value = 693;
	unsigned int sum_ratio = (1000 * new_task_runtime) / new_task_period;
	list_for_each_entry(position, &mrs_tasks, list)
		sum_ratio += (1000 * position->runtime) / position->period;
	return sum_ratio <= acceptance_value;
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
	if (_mrs_admission_control(period, runtime))
		goto err;
	if (_find_mrs_task(pid))
		goto err;
	list_add_tail(&mrs_task->list, &mrs_tasks);
	spin_unlock_irqrestore(&mrs_lock, flags);
	return 0;
err:
	spin_unlock_irqrestore(&mrs_lock, flags);
	kfree(mrs_task);
	return -EAGAIN;
}

// Handles yield calls from usermode app. Changes task state and wakes the
// dispatcher thread
static int yield_msr_task(pid_t pid)
{
	struct mrs_task_struct *mrs_task;
	unsigned long flags;

	spin_lock_irqsave(&mrs_lock, flags);
	mrs_task = _find_mrs_task(pid);
	if (!mrs_task)
		goto err;
	switch (mrs_task->state) {
	case NEW:
		mrs_task->state = READY;
		_mod_period_timer(mrs_task);
		break;
	case RUNNING:
		mrs_task->state = SLEEPING;
		break;
	default:
		printk(KERN_ERR "mrs: invalid state transition attempted.\n");
		goto err;
	}
	__set_current_state(TASK_UNINTERRUPTIBLE);
	up(&mrs_sem);// TODO switch order with unlock?
	spin_unlock_irqrestore(&mrs_lock, flags);
	schedule();
	return 0;
err:
	spin_unlock_irqrestore(&mrs_lock, flags);
	return -1;
}

// Remove a task from the list, delete its timer, and free it
static int deregister_mrs_task(pid_t pid)
{
	struct mrs_task_struct *mrs_task;
	unsigned long flags;
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
	return 0;
}

// Get input from usermode app and take action on it
int mrs_write_proc(struct file *file, const char __user *user_buf,
                        unsigned long count, void *data)
{
	char *buf;
	pid_t pid;
	unsigned int period, runtime;
	int ret = count;

	buf = kmalloc(count, GFP_KERNEL);
	if (copy_from_user(buf, user_buf, count))
		return -EFAULT;
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
	if (count < 0)
		printk(KERN_ERR "mrs: write failed with error # %d.\n", ret);
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
					mrs_task->period,
					mrs_task->runtime);
	}
	spin_unlock_irqrestore(&mrs_lock, flags);

	return ptr - page;
}

// module init - sets up structes, registeres proc file and creates the
// dispatcher kthread
static int _mrs_init(void)
{
	struct proc_dir_entry *proc_dir;
	// create proc directory and file
	proc_dir = proc_mkdir_mode(DIR_NAME, S_IRUGO | S_IXUGO, NULL);
	if (!proc_dir) {
		printk(KERN_ERR "mrs: unable to create proc directory %s.\n",
								DIR_NAME);
		return -ENOMEM;
	}
	proc_file = create_proc_entry(FILE_NAME, S_IRUGO | S_IWUGO, proc_dir);
	if (!proc_file) {
		printk(KERN_ERR "mrs: unable to create proc file %s.\n",
								FILE_NAME);
		remove_proc_entry(DIR_NAME, NULL);
		return -ENOMEM;
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
		printk(KERN_ERR "mrs: failed to create kernel thread %s.\n",
				THREAD_NAME);
                remove_proc_entry(FILE_NAME, proc_dir);
                remove_proc_entry(DIR_NAME, NULL);
                return PTR_ERR(dispatcher_thread);
        }
	should_stop = 0;
	_mrs_sched_setscheduler(dispatcher_thread, SCHED_FIFO, MAX_USER_RT_PRIO);
	wake_up_process(dispatcher_thread);
        return 0;
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
