/*
 * task stats module (tsm)
 */

#include <linux/init.h>
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/proc_fs.h>
#include <asm/cputime.h>
#include <asm/uaccess.h>

#define DIR_NAME "mp1"
#define FILE_NAME "status"
#define MAX_PID_DIGITS 11 //TODO: derive from pid_t?

struct task_stats {
	pid_t pid;
	cputime_t utime;
	struct list_head list;
};

static struct task_stats *create_task_stats(pid_t pid);
static struct task_stats *find_task_stats(pid_t pid);
static pid_t ustr_to_pid(const char __user *buffer, unsigned long count);
int tsm_write_proc(struct file *file, const char __user *buffer,
			   unsigned long count, void *data);
int tsm_read_proc(char *page, char **start, off_t off,
			int count, int *eof, void *data);

// proc directory stats file
static struct proc_dir_entry *stats_file;
// stats list
static LIST_HEAD(stats_head);

/**
 * initializes resources used by this module:
 *  - proc file structure
 */
static int tsm_init(void)
{
	struct proc_dir_entry *dir;
	// create proc directory and file
	dir = proc_mkdir_mode(DIR_NAME, S_IRUGO | S_IXUGO, NULL);
	if (!dir) {
		printk(KERN_ERR "unable to create proc directory %s\n", DIR_NAME);
		return -ENOMEM;
	}
	stats_file = create_proc_entry(FILE_NAME, S_IRUGO | S_IWUGO, dir);
	if (!stats_file) {
		printk(KERN_ERR "unable to create proc file %s\n", FILE_NAME);
		remove_proc_entry(DIR_NAME, NULL);
		return -ENOMEM;
	}
	// set proc functions
	stats_file->read_proc = tsm_read_proc;
	stats_file->write_proc = tsm_write_proc;
	return 0;
}

/**
 * releases module resources and frees dynamically allocated data
 */
static void tsm_exit(void)
{
	struct list_head *ptr, *tmp;
	struct task_stats *stats;
	struct proc_dir_entry *dir;
	// remove proc directory and file
	dir = stats_file->parent;
	if (stats_file)
		remove_proc_entry(FILE_NAME, dir);
	if (dir)
		remove_proc_entry(DIR_NAME, NULL);
	// free stats list
	list_for_each_safe(ptr, tmp, &stats_head) {
		stats = list_entry(ptr, struct task_stats, list);
		list_del(ptr);
		kfree(stats);
	}
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
		printk(KERN_ERR "registration failed: invalid PID");
		return -1;
	}
	// register task stats if not already present
	if (find_task_stats(pid))
		return 0;
	stats = create_task_stats(pid);
	list_add_tail(&stats->list, &stats_head);
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
	list_for_each_entry(stats, &stats_head, list)
		ptr += sprintf(ptr, "%d: %ld\n", stats->pid, stats->utime);
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
static struct task_stats *find_task_stats(pid_t pid)
{
	struct task_stats *stats;
	list_for_each_entry(stats, &stats_head, list) {
		if (stats->pid == pid)
			return stats;
	}
	return NULL;
}

module_init(tsm_init);
module_exit(tsm_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Group 06");
MODULE_DESCRIPTION("CS423 Fa2011 MP1");
