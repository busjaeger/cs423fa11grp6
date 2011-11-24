package edu.illinois.cs.dlb;

import java.io.File;

import edu.illinois.cs.dlb.TaskStatus.Status;

class Worker implements Runnable {

    private final JobManager jobManager;

    Worker(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void run() {
        while (true) {
            Task task;
            try {
                task = jobManager.getTaskQueue().take();
            } catch (InterruptedException e) {
                return;
            }

            System.out.println("Starting Task "+task.getId());
            // simulate for now
            try {
                File input = jobManager.getFile(task.getInputSplit());
                File output = jobManager.getFile(task.getOutputSplit());
                System.out.println("writing to: "+output.getAbsolutePath());
                FileUtil.copy(output, input);
                task.getTaskStatus().setStatus(Status.SUCCEEDED);
            } catch (Throwable t) {
                task.getTaskStatus().setStatus(Status.FAILED);
                if (t instanceof Error)
                    throw (Error)t;
                t.printStackTrace();
                task.getTaskStatus().setMessage(t.getMessage());
            }
        }
    }

}
