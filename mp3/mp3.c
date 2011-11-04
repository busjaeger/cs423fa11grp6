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

// device file operations
static int _pkmdev_mmap(struct file *f, struct vm_area_struct *vma);
static int _pkmdev_open(struct inode *inode, struct file *f);
static int _pkmdev_release(struct inode *inode, struct file *f);

// device file operations struct
static struct file_operations pkmdev_fops = {
	.owner = THIS_MODULE,
	.open = _pkmdev_open,
	.release = _pkmdev_release,
	.mmap = _pkmdev_mmap,
};

// module task struct
struct pkm_task_struct {
	struct task_struct* task;
	struct list_head list;
	unsigned long maj_flt;
	unsigned long min_flt;
	unsigned long utime;
	unsigned long stime;
};

// proc directory stats file
static struct proc_dir_entry *proc_file;
// task list
static LIST_HEAD(pkm_tasks);
// task list mutex
static DEFINE_MUTEX(pkm_tasks_mutex);
// worker mutex
static DEFINE_MUTEX(pkm_work_mutex);
// work item pointer
struct delayed_work *pkm_work;
// pointer to first position in memory mapped profiling buffer (immutable)
static unsigned long *pkm_buffer_first;
// pointer to last position in memory mapped profiling buffer (immutable)
static unsigned long *pkm_buffer_last;
// pointers to current and last position in buffer (mutable)
static unsigned long *pkm_buffer_pos;
// device cdev struct
static struct cdev pkmdev_cdev;

// open method for character device
// params:	inode pointer - unused
//			file struct pointer - unused
// returns: 0
static int _pkmdev_open(struct inode *inode, struct file *f)
{
	return 0;
}

// close method for character device
// params:	inode pointer - unused
//			file struct pointer - unused
// returns: 0
static int _pkmdev_release(struct inode *inode, struct file *f)
{
	return 0;
}

// mmap function for character device
// params:	file struct pointer - unused
//			vma - vm_area_struct to map
// returns: 0 on success, otherwise an error code
static int _pkmdev_mmap(struct file *f, struct vm_area_struct *vma)
{
	int ret;
	unsigned long pfn, size;
	unsigned long start = vma->vm_start;
	unsigned long length = vma->vm_end - start;
	void *ptr = pkm_buffer_first;

	printk(KERN_INFO "pkm: mmap: start=%lu, length=%lu.\n", start, length);
	if (vma->vm_pgoff > 0 || length > BUF_SIZE)
			return -EIO;
	while (length > 0) {
		pfn = vmalloc_to_pfn(ptr);
		size = length < PAGE_SIZE ? length : PAGE_SIZE;
                ret = remap_pfn_range(vma, start, pfn, size, PAGE_SHARED);
		if (ret < 0)
			return ret;
		start += PAGE_SIZE;
		ptr += PAGE_SIZE;
		length -= PAGE_SIZE;
	}
	return 0;
}

// updates jiffies, cpu utilization, and major and minor fault data
// params: none
// returns: bool - true if succeeded, false if failed
bool update_buffer(void)
{
	unsigned long total_min_flt=0, total_maj_flt=0, total_time=0;
	unsigned long min_flt, maj_flt,utime, stime;
	struct pkm_task_struct *pkm_task;

#ifdef DEBUG
	printk(KERN_INFO "pkm: Entered update_buffer\n");
#endif

        mutex_lock(&pkm_work_mutex);
	if (list_empty(&pkm_tasks)) {
		mutex_unlock(&pkm_work_mutex);
		return false;
	}
	mutex_lock(&pkm_tasks_mutex);
	list_for_each_entry(pkm_task, &pkm_tasks, list)	{
		if (get_cpu_use(pkm_task->task->pid, &min_flt, &maj_flt,
                                &utime, &stime)==-1)
			continue;
		pkm_task->maj_flt += maj_flt;
		pkm_task->min_flt += min_flt;
		pkm_task->utime += utime;
		pkm_task->stime += stime;

		total_min_flt += min_flt;
		total_maj_flt += maj_flt;
		total_time += utime + stime;
	}
	mutex_unlock(&pkm_tasks_mutex);
        mutex_unlock(&pkm_work_mutex);

	*pkm_buffer_pos++ = jiffies;
	*pkm_buffer_pos++ = total_min_flt;
	*pkm_buffer_pos++ = total_maj_flt;
	*pkm_buffer_pos = total_time;
	// wrap around
	if (pkm_buffer_pos == pkm_buffer_last)
		pkm_buffer_pos = pkm_buffer_first;
	else
		pkm_buffer_pos++;
	*pkm_buffer_pos = -1l; // mark end
	return true;
}

// Work queue worker function
// params: delayed_work *work - queued work item pointer
// returns: void
void monitor_worker(struct work_struct *w)
{
#ifdef DEBUG
	printk(KERN_INFO "pkm: Entered monitor_worker\n");
#endif
	if (update_buffer())
		schedule_delayed_work(pkm_work, (HZ/WORKER_FREQ));
#ifdef DEBUG
	printk(KERN_INFO "pkm: Exiting monitor_worker\n");
#endif
}

// find task from task list
// params: pid to be searched for in task list
// returns: pkm_task_sturct for PID or NULL if task isn't found
static struct pkm_task_struct *_find_pkm_task(pid_t pid)
{
	struct pkm_task_struct *pkm_task;
	list_for_each_entry(pkm_task, &pkm_tasks, list) {
		if (pkm_task->task->pid == pid)
			return pkm_task;
	}
    return NULL;
}

// removes a task from the list
// params: PID of task to remove
// returns: removed task
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
// params: task_struct and pid to be used for new pkm_task_struct
// returns: new pkm_task_sturct or NULL if get_cpu_use fails to get values or
// 			kmalloc fails
struct pkm_task_struct *_create_pkm_task(struct task_struct *task, pid_t pid)
{
	int ret;
	unsigned long min_flt, maj_flt, utime, stime;
	struct pkm_task_struct *pkm_task = NULL;
	ret = get_cpu_use(pid, &min_flt, &maj_flt, &utime, &stime);
	if (ret == 0) {
		pkm_task = kmalloc(sizeof(*pkm_task), GFP_KERNEL);
		if (pkm_task) {
			pkm_task->task = task;
			pkm_task->maj_flt = maj_flt;
			pkm_task->min_flt = min_flt;
			pkm_task->utime = utime;
			pkm_task->stime = stime;
			INIT_LIST_HEAD(&pkm_task->list);
		}		
	}
	return pkm_task;
}

// Add process to PCB list and create a work queue job if the PCB list is empty
// params: PID to begin monitoring
// returns: 0 on success, int error value otherwise
static int register_pkm_task(pid_t pid)
{
	struct task_struct *task;
	struct pkm_task_struct *pkm_task;
	int err = -1;

	printk(KERN_INFO "pkm: %d registering entry at %lu.\n", pid, jiffies);
	if (!(task = find_task_by_pid(pid)))
		return -ESRCH;
	// optimistically allocate task outside of critical region
	if (!(pkm_task = _create_pkm_task(task, pid)))
		return -ENOMEM;
	// critical section: add task if not already present
	mutex_lock(&pkm_tasks_mutex);
	if (_find_pkm_task(pid)) {
		err = -EEXIST;
		goto error;
	}
	if (list_empty(&pkm_tasks)) {
                pkm_work = kmalloc(sizeof(struct delayed_work), GFP_KERNEL);
                if (!pkm_work) {
                        err = -ENOMEM;
                        goto error;
                }
		INIT_DELAYED_WORK(pkm_work, monitor_worker);
		schedule_delayed_work(pkm_work, (HZ/WORKER_FREQ));
        }
	list_add_tail(&pkm_task->list, &pkm_tasks);
	mutex_unlock(&pkm_tasks_mutex);
	return 0;
error:
	mutex_unlock(&pkm_tasks_mutex);
	kfree(pkm_task);
	return err;
}

// Remove a task from the list and free it
// params: PID to deregister
// returns: 0
static int deregister_pkm_task(pid_t pid)
{
	struct pkm_task_struct *pkm_task;
	printk(KERN_INFO "pkm: %d deregistering entry at %lu.\n", pid, jiffies);

        mutex_lock(&pkm_work_mutex);
	mutex_lock(&pkm_tasks_mutex);
	pkm_task =_remove_pkm_task(pid);
        mutex_unlock(&pkm_work_mutex);
        if (!pkm_task) {
                mutex_unlock(&pkm_tasks_mutex);
                return -ESRCH;
        }
	if (list_empty(&pkm_tasks)) {
                cancel_delayed_work_sync(pkm_work);
                kfree(pkm_work);
        }
        mutex_unlock(&pkm_tasks_mutex);
	if (pkm_task)
		kfree(pkm_task);

	printk(KERN_INFO "pkm: %d deregistering exit.\n", pid);
	return 0;
}

// Get input from usermode app and take action on it
// params: 	file - unused file struct pointer
//			user_buf - data to copy in
//			count - size of buffer
//			data - unused
// returns: count on success, otherwise an error code
int _pkm_write_proc(struct file *file, const char __user *user_buf,
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
// params:	page - buffer to use for data
//			start - pointer to pointer to characters (unused)
//			off - offset into file (unused)
//			count - size of buffer
//			eof - end of file indicator (unused)
//			data - pointer to data (unused)
// returns: size of data written
int pkm_read_proc(char *page, char **start, off_t off,
                        int count, int *eof, void *data)
{
	char *ptr = page;
	struct pkm_task_struct *pkm_task;

	mutex_lock(&pkm_tasks_mutex);
	list_for_each_entry(pkm_task, &pkm_tasks, list)	{
		// Output: PID
		ptr += sprintf( ptr, "%d\n", pkm_task->task->pid);
	}
	mutex_unlock(&pkm_tasks_mutex);

	return ptr - page;
}

// module init - sets up structes, registeres proc file and creates the
// 				 workqueue
// returns: 0 on success, otherwise retrns an error code
static int _pkm_init(void)
{
	struct proc_dir_entry *proc_dir;
	int err, i;
	dev_t dev;

#ifdef DEBUG
	printk(KERN_INFO "pkm: module starting.\n");
#endif

	// create proc directory and file and set functions
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
	proc_file->read_proc = pkm_read_proc;
	proc_file->write_proc = _pkm_write_proc;

	// allocate buffer and initialize pages and positions
	pkm_buffer_first = vmalloc(BUF_SIZE);
	if (!pkm_buffer_first) {
		err = -ENOMEM;
		goto error_rm;
	}
	for(i=0; i<BUF_SIZE; i+=PAGE_SIZE)
		SetPageReserved(vmalloc_to_page((void *)pkm_buffer_first+i));
	pkm_buffer_pos = pkm_buffer_first;
	pkm_buffer_last = (void *)pkm_buffer_first + (BUF_SIZE - sizeof(unsigned long));

	// initialize and add device driver
	if ((err = alloc_chrdev_region(&dev, 0, 1, PKM_NAME)))
		goto error_rm;
	cdev_init(&pkmdev_cdev, &pkmdev_fops);
	if ((err = cdev_add(&pkmdev_cdev, dev, 1)))
		goto error_udv;

#ifdef DEBUG
	printk(KERN_INFO "pkm: module started.\n");
#endif
	return 0;

error_udv:
        unregister_chrdev_region(dev, 1);
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
	int i;
	dev_t dev;

#ifdef DEBUG
	printk(KERN_INFO "pkm: module stopping.\n");
#endif

	// remove proc directory and file
	if (proc_file) {
		remove_proc_entry(FILE_NAME, proc_file->parent);
		remove_proc_entry(DIR_NAME, NULL);
	}

	// if necessary, free remaining tasks and stop worker
	mutex_lock(&pkm_work_mutex);
        mutex_lock(&pkm_tasks_mutex);
        if (!list_empty(&pkm_tasks)) {
                list_for_each_safe(ptr, tmp, &pkm_tasks) {
                        task = list_entry(ptr, struct pkm_task_struct, list);
                        list_del(ptr);
                        kfree(task);
                }
                mutex_unlock(&pkm_work_mutex);
                cancel_delayed_work_sync(pkm_work);
                kfree(pkm_work);
        } else {
                mutex_unlock(&pkm_work_mutex);
        }
        mutex_unlock(&pkm_tasks_mutex);

	// free profile buffer
	if (pkm_buffer_first) {
		for(i=0; i<BUF_SIZE; i+=PAGE_SIZE)
			ClearPageReserved(vmalloc_to_page((void *)pkm_buffer_first + i));
		vfree(pkm_buffer_first);
	}

	// unregister device driver
	dev = pkmdev_cdev.dev;
	cdev_del(&pkmdev_cdev);
	unregister_chrdev_region(dev, 1);

#ifdef DEBUG
	printk(KERN_INFO "pkm: module stopped.\n");
#endif
}

module_init(_pkm_init);
module_exit(pkm_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Group 06");
MODULE_DESCRIPTION("CS423 Fa2011 MP3");
