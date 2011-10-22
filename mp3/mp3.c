/*
 * profiler kernel module (pkm)
 */

#include <linux/init.h>
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/proc_fs.h>
#include <linux/timer.h>
#include <asm/uaccess.h>
#include <linux/sched.h>		// PCB structure
#include <linux/workqueue.h>
#include "mp3_given.h"

#define DIR_NAME "mp3"
#define FILE_NAME "status"

struct pkm_task_struct
{
	struct task_struct* task;
	struct list_head list;
	unsigned long major_faults;
	unsigned long minor_faults;
	unsigned long cpu_time;
};

struct pkm_wrkq_struct
{
	struct work_struct my_work;
};

// mutex
static DEFINE_MUTEX(pkm_mutex);
// workqueue
static struct workqueue_struct *pkm_wrkq;  // TODO: this correct?
// proc directory stats file
static struct proc_dir_entry *proc_file;
// task list
static LIST_HEAD(pkm_tasks);



// find task from task list
static struct pkm_task_struct *_find_pkm_task(pid_t pid)
{
	struct pkm_task_struct *pkm_task;
	list_for_each_entry(pkm_task, &pkm_tasks, list) {
		if (pkm_task->task->pid == pid)
			return pkm_task;
	}
    return NULL;
}

static struct pkm_task_struct *_remove_pkm_task(pid_t pid)
{
	struct list_head *ptr, *tmp;
	struct pkm_task_struct *pkm_task;
	list_for_each_safe(ptr, tmp, &pkm_tasks) {
		pkm_task = list_entry(ptr, struct pkm_task_struct, list);
		if (pkm_task->task->pid == pid) {
			list_del(ptr);
			return pkm_task;
        }
    }
	return NULL;
}



// allocate and initialize a task
// returns NULL if get_cpu_use fails to get values
struct pkm_task_struct *_create_pkm_task(struct task_struct *task, pid_t pid)
{
	int ret;
	int major, minor, cpu_time;
	struct pkm_task_struct *pkm_task = NULL;
	ret = get_cpu_use(pid, &minor, &major, &cpu_time);
	if (ret == 0) {
		pkm_task = kmalloc(sizeof(*pkm_task), GFP_KERNEL);
		if (pkm_task) {
			pkm_task->task = task;		
			pkm_task->major_faults = major;
			pkm_task->minor_faults = minor;
			pkm_task->cpu_time = cpu_time;
			INIT_LIST_HEAD(&pkm_task->list);
		}		
	}
	return pkm_task;
}


// Code from mp2
struct task_struct* find_task_by_pid(unsigned int nr)
{
    struct task_struct* task;
    rcu_read_lock();
    task = pid_task(find_vpid(nr), PIDTYPE_PID);
    rcu_read_unlock();

    return task;
}

// Add process to PCB list and create a work queue job if the PCB list is empty
static int register_pkm_task(pid_t pid)
{
	struct task_struct *task;
	struct pkm_task_struct *pkm_task;
	int err = -1;
	
	task = find_task_by_pid(pid);
	if (!task || task != current)
		return -ESRCH;
	// optimistically allocate task outside of critical region
	pkm_task = _create_pkm_task(task, pid);
	if (!pkm_task)
		return -ENOMEM;
	// critical section: add task if not already present
	mutex_lock(&pkm_mutex);
	if (_find_pkm_task(pid) == NULL) {
		err = -EEXIST;
		goto error;
	}
	list_add_tail(&pkm_task->list, &pkm_tasks);
	mutex_unlock(&pkm_mutex);
	// TODO: Create work queue job if PCB list is empty, here or earlier?
	return 0;
error:
	mutex_unlock(&pkm_mutex);
	kfree(pkm_task);
	return err;
}

// Remove a task from the list and free it
static int deregister_pkm_task(pid_t pid)
{
	struct pkm_task_struct *pkm_task;
	printk(KERN_INFO "pkm: %d deregistering entry.\n", pid);
	mutex_lock(&pkm_mutex);
	pkm_task =_remove_pkm_task(pid);
	if (pkm_task == NULL) {
		//printk(KERN_INFO "pkm: Deregister %d could not be found!\n", pid);
		//TODO: call function to delete workqueue
	}
	mutex_unlock(&pkm_mutex);
	if (pkm_task)
		kfree(pkm_task);
	printk(KERN_INFO "pkm: %d deregistering exit.\n", pid);
	return 0;
}

// Get input from usermode app and take action on it
int pkm_write_proc(struct file *file, const char __user *user_buf,
                        unsigned long count, void *data)
{
	char *buf;
	pid_t pid;
	int ret;

	buf = kmalloc(count + 1, GFP_KERNEL);
	if (copy_from_user(buf, user_buf, count))
		return -EFAULT;
	buf[count] = '\0';
	printk(KERN_INFO "pkm: command received \"%s\"\n", buf);
	switch (*buf) {
	case 'R':
		if (sscanf(buf, "R %d", &pid) != 1)
			ret = -EINVAL;
		else
			ret = register_pkm_task(pid);
		break;
	case 'U':
		if (sscanf(buf, "U %d", &pid) != 1)
			ret = -EINVAL;
		else
			ret = deregister_pkm_task(pid);
		break;
	default:
		ret = -EINVAL;
		break;
	}
	kfree(buf);
	if (ret < 0)
		printk(KERN_ERR "pkm: %d proc write failed with error %d.\n",
				current->pid, ret);
	else
		ret = count;
	return ret;
}

// Print registered process list to proc special file
int pkm_read_proc(char *page, char **start, off_t off,
                        int count, int *eof, void *data)
{
	char *ptr = page;
	struct pkm_task_struct *pkm_task;

	mutex_lock(&pkm_mutex);
	list_for_each_entry(pkm_task, &pkm_tasks, list)	{
		// Output: PID
		ptr += sprintf( ptr, "%d\n", pkm_task->task->pid);
	}
	mutex_unlock(&pkm_mutex);

	return ptr - page;
}

// module init - sets up structes, registeres proc file and creates the
// workqueue
static int _pkm_init(void)
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
	proc_file->read_proc = pkm_read_proc;
	proc_file->write_proc = pkm_write_proc;

	//TODO: Create workqueue
	return 0;
error_rm:
	remove_proc_entry(FILE_NAME, proc_dir);
error_rmd:
	remove_proc_entry(DIR_NAME, NULL);
error:
	printk(KERN_ERR "pkm: module failed to init due to %d.\n", err);
	return err;
}

// releases module resources and frees dynamically allocated data
static void pkm_exit(void)
{
	struct list_head *ptr, *tmp;
	struct pkm_task_struct *task;

	// remove proc directory and file
	if (proc_file) {
		remove_proc_entry(FILE_NAME, proc_file->parent);
		remove_proc_entry(DIR_NAME, NULL);
	}
	mutex_lock(&pkm_mutex);
	// free stats list
	list_for_each_safe(ptr, tmp, &pkm_tasks) {
		task = list_entry(ptr, struct pkm_task_struct, list);
		list_del(ptr);
		kfree(task);
	}
	mutex_unlock(&pkm_mutex);
}

module_init(_pkm_init);
module_exit(pkm_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Group 06");
MODULE_DESCRIPTION("CS423 Fa2011 MP3");
