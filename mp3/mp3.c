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
#include <linux/vmalloc.h>
#include <linux/cdev.h>
#include <linux/mm.h>
#include <linux/page-flags.h>
#include "mp3_given.h"

#define DIR_NAME "mp3"
#define FILE_NAME "status"
#define WORKER_FREQ 20
#define PKM_NAME "pkm"
#define BUF_SIZE 131072 * sizeof(unsigned long)         // half MB for 4 bytes

struct pkm_task_struct {
	struct task_struct* task;
	struct list_head list;
	unsigned long major_faults;
	unsigned long minor_faults;
	unsigned long cpu_time;
};

typedef struct {
	struct delayed_work my_work;
} pkm_wrkq_struct;

// task list mutex
static DEFINE_MUTEX(pkm_mutex);
// workqueue
static struct workqueue_struct *pkm_wrkq;
// proc directory stats file
static struct proc_dir_entry *proc_file;
// task list
static LIST_HEAD(pkm_tasks);
// work item pointer
struct delayed_work *work_item;
// memory mapped profiling buffer
static void *pkm_buffer;
// pointers to current and last position in buffer
static unsigned long *pkm_buffer_pos;
static unsigned long *pkm_buffer_last;
// device cdev struct
static struct cdev pkmdev_cdev;
// device file operations
int pkmdev_mmap(struct file *f, struct vm_area_struct *vma);
static struct file_operations pkmdev_fops = {
        .owner = THIS_MODULE,
        .mmap = pkmdev_mmap,
};

// updates jiffies, cpu utilization, and major and minor fault data
// params: none
// returns: bool - true if succeeded, false if failed
bool update_buffer(void)
{
	unsigned long jiffs;
	unsigned long minor=0;
	unsigned long major=0;
	unsigned long cpu=0;

	unsigned long min;
	unsigned long maj;
	unsigned long cpu_use;

	struct pkm_task_struct *pkm_task;

	#ifdef DEBUG
	printk(KERN_INFO "pkm: Entered update_buffer\n");
	#endif

	if (list_empty(&pkm_tasks))
		return false;

	list_for_each_entry(pkm_task, &pkm_tasks, list)	{
		if(get_cpu_use(pkm_task->task->pid, &min, &maj, &cpu_use)!=-1) {
			pkm_task->major_faults += maj;
			pkm_task->minor_faults += min;
			pkm_task->cpu_time += cpu_use;
			
			major+=maj;
			minor+=min;
			cpu+=cpu_use;
		}
	}

        jiffs = jiffies;
        *pkm_buffer_pos++ = jiffs;
        *pkm_buffer_pos++ = minor;
        *pkm_buffer_pos++ = major;
        *pkm_buffer_pos = cpu;
        // wrap around
        if (pkm_buffer_pos == pkm_buffer_last)
                pkm_buffer_pos = pkm_buffer;
        else
                pkm_buffer_pos++;
        return true;
}

// Work queue worker function
// params: delayed_work *work - queued work item pointer
// returns: void
void monitor_worker(struct work_struct *work)
{
	#ifdef DEBUG
	printk(KERN_INFO "pkm: Entered pkm_wrkq worker\n");
	#endif
	
	if(update_buffer())
		schedule_delayed_work(work_item, (long)(HZ/WORKER_FREQ));
	else
		kfree(work);
}

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
	unsigned long major, minor, cpu_time;
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

int pkmdev_mmap(struct file *f, struct vm_area_struct *vma)
{
        return remap_vmalloc_range(vma, pkm_buffer, 0);
}

// Add process to PCB list and create a work queue job if the PCB list is empty
static int register_pkm_task(pid_t pid)
{
	struct task_struct *task;
	struct pkm_task_struct *pkm_task;
	int err = -1;
	bool listwasempty = false;
	
	task = find_task_by_pid(pid);
	if (!task || task != current)
		return -ESRCH;
	// optimistically allocate task outside of critical region
	pkm_task = _create_pkm_task(task, pid);
	if (!pkm_task)
		return -ENOMEM;
	// critical section: add task if not already present
	mutex_lock(&pkm_mutex);
	if (_find_pkm_task(pid)) {
		err = -EEXIST;
		goto error;
	}
	if (list_empty(&pkm_tasks)) {
		listwasempty = true;
	}
	list_add_tail(&pkm_task->list, &pkm_tasks);
	mutex_unlock(&pkm_mutex);
	
	//Create work queue job if PCB list was empty
	//pkm_wrkq_struct *work_item;
	if(listwasempty) {
		work_item = kmalloc(sizeof(struct delayed_work), GFP_ATOMIC);
		if (work_item) {
			INIT_DELAYED_WORK(work_item, monitor_worker);
			schedule_delayed_work(work_item, 0);
		}
		else {
			return -ENOMEM;
                }
	}
	
	return 0;
error:
	mutex_unlock(&pkm_mutex);
	kfree(pkm_task);
	return err;
}

// Remove a task from the list and free it
static int deregister_pkm_task(pid_t pid)
{
	bool listempty = false;
	struct pkm_task_struct *pkm_task;
	printk(KERN_INFO "pkm: %d deregistering entry.\n", pid);
	mutex_lock(&pkm_mutex);
	pkm_task =_remove_pkm_task(pid);
	if (pkm_task == NULL) {
		//printk(KERN_INFO "pkm: Deregister %d could not be found!\n", pid);
	}
	if (list_empty(&pkm_tasks)) {
		listempty = true;
	}
	mutex_unlock(&pkm_mutex);
	if (pkm_task)
		kfree(pkm_task);
	if(listempty) {
		// delete workqueue jobs
		if(work_item) {
			if(cancel_delayed_work(work_item)==0)
				flush_workqueue(pkm_wrkq);
			kfree(work_item);
		}
	}
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
	int err, i;
        dev_t dev;
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
	// Create workqueue
	pkm_wrkq = create_workqueue("pkm_wrkq");
	if (!pkm_wrkq) {
		err = -ENOMEM;
		goto error_rm;
	}
        // allocate buffer
        pkm_buffer = vmalloc(BUF_SIZE);
        if (!pkm_buffer) {
                err = -ENOMEM;
                goto error_dw;
        }
        for(i=0; i<BUF_SIZE; i+=PAGE_SIZE)
                SetPageReserved(vmalloc_to_page(pkm_buffer+i));
        pkm_buffer_pos = pkm_buffer;
        pkm_buffer_last = pkm_buffer + (BUF_SIZE - sizeof(unsigned long));
        // initialize and add device driver
        if ((err = alloc_chrdev_region(&dev, 0, 1, PKM_NAME)))
                goto error_dw;
        cdev_init(&pkmdev_cdev, &pkmdev_fops);
        if ((err = cdev_add(&pkmdev_cdev, dev, 1)))
                goto error_udv;
	return 0;

error_udv:
        unregister_chrdev_region(dev, 1);
error_dw:
        destroy_workqueue(pkm_wrkq);
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
        dev_t dev;

	// remove proc directory and file
	if (proc_file) {
		remove_proc_entry(FILE_NAME, proc_file->parent);
		remove_proc_entry(DIR_NAME, NULL);
	}
	
	// clean up work queue
	if (work_item)
		cancel_delayed_work(work_item);
	if (pkm_wrkq) {
		flush_workqueue(pkm_wrkq);
		destroy_workqueue(pkm_wrkq);
	}
	if (work_item)
		kfree(work_item);
	mutex_lock(&pkm_mutex);
	// free stats list
	list_for_each_safe(ptr, tmp, &pkm_tasks) {
		task = list_entry(ptr, struct pkm_task_struct, list);
		list_del(ptr);
		kfree(task);
	}
	mutex_unlock(&pkm_mutex);
        if (pkm_buffer)
                vfree(pkm_buffer);
        dev = pkmdev_cdev.dev;
        cdev_del(&pkmdev_cdev);
        unregister_chrdev_region(dev, 1);
}

module_init(_pkm_init);
module_exit(pkm_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Group 06");
MODULE_DESCRIPTION("CS423 Fa2011 MP3");
