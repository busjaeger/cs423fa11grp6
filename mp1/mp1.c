/*
 * task stats module (tsm)
 */

#include <linux/init.h>
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/proc_fs.h>
#include <linux/kthread.h>
#include <asm/cputime.h>
#include <asm/uaccess.h>
#include "mp1_given.h"

#define DIR_NAME "mp1"
#define FILE_NAME "status"
#define THREAD_NAME "task-stats-collector"
#define MAX_PID_DIGITS 11 //TODO: derive from pid_t?
#define SYNC_INTERVAL 5000

struct task_stats {
	pid_t pid;
	cputime_t utime;
	struct list_head list;
};

static struct task_stats *create_task_stats(pid_t pid);
static struct task_stats *_find_task_stats(pid_t pid);
static pid_t ustr_to_pid(const char __user *buffer, unsigned long count);
int tsm_write_proc(struct file *file, const char __user *buffer,
			   unsigned long count, void *data);
int tsm_read_proc(char *page, char **start, off_t off,
			int count, int *eof, void *data);
static int stats_collector(void *data);

// proc directory stats file
static struct proc_dir_entry *stats_file;
// stats list
static LIST_HEAD(stats_head);
// stats mutex
static DEFINE_MUTEX(stats_mutex);
// kernel thread struct
static struct task_struct *stats_thread;

/**
 * initializes resources used by this module:
 *  - proc files
 *  - stats thread
 */
static int tsm_init(void)
{
	struct proc_dir_entry *dir;
	// create proc directory and file
	dir = proc_mkdir_mode(DIR_NAME, S_IRUGO | S_IXUGO, NULL);
	if (!dir) {
		printk(KERN_ERR "tsm: unable to create proc directory %s.\n", DIR_NAME);
		return -ENOMEM;
	}
	stats_file = create_proc_entry(FILE_NAME, S_IRUGO | S_IWUGO, dir);
	if (!stats_file) {
		printk(KERN_ERR "tsm: unable to create proc file %s.\n", FILE_NAME);
		remove_proc_entry(DIR_NAME, NULL);
		return -ENOMEM;
	}
	// set proc functions
	stats_file->read_proc = tsm_read_proc;
	stats_file->write_proc = tsm_write_proc;
	// start kernel thread. Note: we also had a version that used
	// setup_timer, mod_timer, wake_up_process, and schedule, but
	// found schedule_timeout to be more convenient/compact
	stats_thread = kthread_run(stats_collector, NULL, THREAD_NAME);
	if (IS_ERR(stats_thread)) {
		printk(KERN_ERR "tsm: failed to create kernel thread %s.\n", THREAD_NAME);
		remove_proc_entry(FILE_NAME, dir);
		remove_proc_entry(DIR_NAME, NULL);
		return PTR_ERR(stats_thread);
	}
	return 0;
}

/**
 * releases module resources and frees dynamically allocated data
 */
static void tsm_exit(void)
{
	struct list_head *ptr, *tmp;
	struct task_stats *stats;
	// remove proc directory and file
	if (stats_file) {
		remove_proc_entry(FILE_NAME, stats_file->parent);
		remove_proc_entry(DIR_NAME, NULL);
	}
	// stop kernel thread
	kthread_stop(stats_thread);
	// free stats list
	mutex_lock(&stats_mutex);
	list_for_each_safe(ptr, tmp, &stats_head) {
		stats = list_entry(ptr, struct task_stats, list);
		list_del(ptr);
		kfree(stats);
	}
	mutex_unlock(&stats_mutex);
}

/**
 * implements proc_fs.h#write_proc_t: user data is converted to pid.
 * If no task stats exist for this pid, a new one is created and added
 * to the stats list.
 */
int tsm_write_proc(struct file *file, const char __user *buffer,
			unsigned long count, void *data)
{
	pid_t pid;
	struct task_stats *stats;
	// parse PID
	pid = ustr_to_pid(buffer, count);
	if (pid < -1) {
		printk(KERN_ERR "tsm: registration failed due to invalid PID");
		return -1;
	}
	// register task stats if not already present
	mutex_lock(&stats_mutex);
	if (!_find_task_stats(pid)) {
		stats = create_task_stats(pid);
		list_add_tail(&stats->list, &stats_head);
		mutex_unlock(&stats_mutex);
	} else {
		mutex_unlock(&stats_mutex);
	}
	return count;
}

/**
 * proc_fs.h#read_proc implementation: each line
 * TODO: understand the contract of this function
 */
int tsm_read_proc(char *page, char **start, off_t off,
			int count, int *eof, void *data)
{
	char *ptr = page;
	struct task_stats *stats;
	mutex_lock(&stats_mutex);
	list_for_each_entry(stats, &stats_head, list)
		ptr += sprintf(ptr, "%d: %ld\n", stats->pid, stats->utime);
	mutex_unlock(&stats_mutex);
	return ptr - page;
}

/**
 * converts character data passed from user space into a pid.
 * TODO: define behavior (max digits, ignores trailing characters etc.)
 */
static pid_t ustr_to_pid(const char __user *buffer, unsigned long count)
{
	pid_t pid = -1;
	char str[MAX_PID_DIGITS];
	if (count < MAX_PID_DIGITS) {
		copy_from_user(str, buffer, count);
		str[count] = '\0';
		pid = simple_strtol(str, NULL, 10);
	}
	return pid;
}

/**
 * creates a new task_stats struct for the given pid and returns the pointer
 * to it, or NULL no memory could be allocated.
 * @pid:   the pid for the task_stats
 */
static struct task_stats *create_task_stats(pid_t pid)
{
	struct task_stats *stats = kmalloc(sizeof(struct task_stats), GFP_KERNEL);
	if (stats) {
		stats->pid = pid;
		stats->utime = 0;
		INIT_LIST_HEAD(&stats->list);
	}
	return stats;
}

/**
 * returns a pointer to the task stats for the given id in the stats list
 * or NULL if not present
 * @pid:   the pid of the task_stats to return
 */
static struct task_stats *_find_task_stats(pid_t pid)
{
	struct task_stats *stats;
	list_for_each_entry(stats, &stats_head, list) {
		if (stats->pid == pid)
			return stats;
	}
	return NULL;
}

/**
 * periodically syncs the stats with the task info
 */
static int stats_collector(void *data)
{
	struct task_stats *stats;
	struct task_struct *task;
	unsigned long timeout;
	timeout = msecs_to_jiffies(SYNC_INTERVAL);
	while (1) {
		__set_current_state(TASK_UNINTERRUPTIBLE);
		schedule_timeout(timeout);
		if (kthread_should_stop())
			break;
		mutex_lock(&stats_mutex);
	        list_for_each_entry(stats, &stats_head, list) {
			if (get_cpu_use(stats->pid, &stats->utime))
				printk(KERN_WARNING "tsm: no process for PID %d found.\n", stats->pid);
		}
		mutex_unlock(&stats_mutex);
	}
	return 0;
}

module_init(tsm_init);
module_exit(tsm_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Group 06");
MODULE_DESCRIPTION("CS423 Fa2011 MP1");
