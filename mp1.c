
#include <linux/module.h>     	/* Needed by all modules */
#include <linux/kernel.h>     	/* KERN_INFO, kernel work */
#include <linux/proc_fs.h>    	/* Using proc fs */
#include <linux/list.h>       	/* Kernel Linked List */
#include <asm/uaccess.h>      	/* for copy_from_user */
#include <linux/timer.h>	/* jiffies, kernel timer */
#include <asm/spinlock.h>	/* spinlock */
#include <linux/spinlock.h>	/* spinlock */
#include <linux/kthread.h>
#include <linux/wait.h>
#include "mp1_given.h"

#define PROCFS_DIRNAME "mp1"
#define PROCFS_FILENAME "status"
#define PROCFS_MAX_SIZE 1024
#define PROCFS_BUFFERNAME "buffer1k"
#define NOT_PENDING 0
#define TIMEOUT 5000

static struct proc_dir_entry *mp1_proc_directory;
static struct proc_dir_entry *status_file;
struct node {
  struct list_head list;
  int pid;
  unsigned long cpu_time;
};
static LIST_HEAD(process_list);
static char procfs_buffer[PROCFS_MAX_SIZE];
static unsigned long procfs_buffer_size = 0;
static struct timer_list my_timer;
static spinlock_t lock;
static struct task_struct *sleeping_task;


int thread(void *data)
{
	unsigned long cpu_value = 0;
	unsigned long flags;
	struct list_head *position = NULL;
	struct node *dataptr = NULL;
	

	/* while the thread isn't told to stop */
	while(1)
	{
		printk(KERN_INFO "Kernel thread running\n");
		if (kthread_should_stop())
		{
			break;
		}

		/* for each pid, update the CPU time */
		cpu_value = 0;
		spin_lock_irqsave(&lock, flags);
		list_for_each(position, &process_list){
			dataptr = list_entry(position, struct node, list);
			if (get_cpu_use( dataptr->pid, &cpu_value) == 0)
			{
				dataptr->cpu_time = cpu_value;
				printk(KERN_INFO "Updating pid:%d cpu time\n", dataptr->pid);
			} // else invalid pid, remove from list or ignore?
			else
			{
				printk(KERN_INFO "Invalid pid, did not update\n");
			}
		}
		spin_unlock_irqrestore(&lock, flags);
		
		// reset timer
		if ( timer_pending(&my_timer) == NOT_PENDING ){
			mod_timer(&my_timer, jiffies+msecs_to_jiffies(TIMEOUT));
		}
		// sleep
		sleeping_task = current; //redundant, not needed?
		set_current_state(TASK_INTERRUPTIBLE);
		schedule();
	}
	return 0;
}

/* timer callback function */
void my_timer_callback(unsigned long data)
{
	int ret = 0;

	//wake up kernel thread
	ret = wake_up_process(sleeping_task); //returns 1 if wakes process, 0 if process still running
	if (ret == 1){
		printk(KERN_INFO "Woke up kernel thread\n");
	} else {
		printk(KERN_INFO "Kernel thread still running\n");
	}
	printk(KERN_INFO "Timer callback function (%ld)\n", jiffies);
}

/* Called when proc file is read.
 * Print the list of all registered PIDs and corresponding User Space CPU times
 */
int procfile_read(char *buffer, char **buffer_location, off_t offset, int buffer_length, int *eof, void *data)
{
	char *ptr = buffer;
	struct node *temp;
	unsigned long flags;

	spin_lock_irqsave(&lock, flags);
	list_for_each_entry(temp, &process_list, list){
		ptr += sprintf( ptr, "%d: %ld\n", temp->pid, temp->cpu_time);
	}
	spin_unlock_irqrestore(&lock, flags);

  	return ptr - buffer;
}

/* Called when proc file written to.
 * Take PID and store in Linked List along with CPU time.
 */
int procfile_write( struct file *file, const char *buffer, unsigned long count, void *data)
{
	struct node *obj;
	unsigned long flags;

  	procfs_buffer_size = count;
  	if (procfs_buffer_size > PROCFS_MAX_SIZE){
    		procfs_buffer_size = PROCFS_MAX_SIZE;
  	}

  	/* write data to buffer */
  	if (copy_from_user(procfs_buffer, buffer, procfs_buffer_size) ){
    		return -EFAULT;
  	}

  	/* put contents of procfs_buffer (PID) into the linked list along with CPU time*/
  	printk(KERN_INFO "Storing pid in linked list.\n");
    	
	obj = (struct node *)kmalloc( sizeof(struct node), GFP_KERNEL );
  	if (obj) {
		obj->pid = simple_strtol(procfs_buffer, NULL, 10);
  		obj->cpu_time = 0;
		INIT_LIST_HEAD(&obj->list);
		spin_lock_irqsave(&lock, flags);
		list_add_tail(&obj->list, &process_list);
		spin_unlock_irqrestore(&lock, flags);
	}
  	

  	return procfs_buffer_size;
}



int init_module(void)
{
	int rval = 0;

	printk(KERN_INFO "Hello world 1.\n");
	
	/* Create /proc/mp1 directory */
	mp1_proc_directory = proc_mkdir(PROCFS_DIRNAME, NULL);
	if (mp1_proc_directory == NULL){
	  printk(KERN_ALERT "Error: Could not initialize /proc/%s\n", PROCFS_DIRNAME);
	  return -ENOMEM;
	}

	/* Create /proc/mp1/status file */
	status_file = create_proc_entry(PROCFS_FILENAME, 0666, mp1_proc_directory);
	if (status_file == NULL){
	  	printk(KERN_ALERT "Error: Could not initialize /proc/%s\n", PROCFS_FILENAME);
	  	return -ENOMEM;
	}

	status_file->read_proc = procfile_read;
	status_file->write_proc = procfile_write;
	status_file->mode = S_IFREG | S_IRUGO;
	status_file->uid = 0;
	status_file->gid = 0;
	status_file->size = 100;

	printk(KERN_INFO "/proc/%s/%s created\n", PROCFS_DIRNAME, PROCFS_FILENAME);

	spin_lock_init(&lock);

	setup_timer( &my_timer, my_timer_callback, 0);
	rval = mod_timer(&my_timer, jiffies+msecs_to_jiffies(TIMEOUT));
	if(rval){
		printk("Error in mod_timer\n");
	}
		
	sleeping_task = kthread_run(thread, NULL, "kthread");

	/* A nonzero return means init_module failed; module can't be loaded */
	return 0;
}


void cleanup_module(void)
{
	int rval = 0;
	unsigned long flags;
	struct list_head *pos, *q;
	struct node *tmp;

	/* Free Linked List */
	spin_lock_irqsave(&lock, flags);
        list_for_each_safe(pos, q, &process_list){
                tmp = list_entry(pos, struct node, list);
                list_del(pos);
                kfree(tmp);
        }
	spin_unlock_irqrestore(&lock, flags);

	/* Stop timer */
	rval = del_timer(&my_timer);
	if (rval){
		printk("The timer is still in use... \n");
	}

	/* Stop kernel thread */
	kthread_stop(sleeping_task);

	/* Remove proc dir and file */
        remove_proc_entry(PROCFS_FILENAME, mp1_proc_directory);
        remove_proc_entry(PROCFS_DIRNAME, NULL);
	printk(KERN_INFO "/proc/%s/%s removed\n", PROCFS_DIRNAME, PROCFS_FILENAME);
        printk(KERN_INFO "/proc/%s removed\n", PROCFS_DIRNAME);
	printk(KERN_INFO "Goodbye world 1.\n");
}

MODULE_AUTHOR("Group 6");
MODULE_DESCRIPTION("CS423 Fall 2011 MP1");
MODULE_LICENSE("GPL");
