package edu.illinois.cs.dlb;

import java.io.File;

import edu.illinois.cs.dlb.TaskStatus.Status;
import edu.illinois.cs.dlb.util.FileUtil;

class Worker implements Runnable {

    private final WorkManager jobManager;

    Worker(WorkManager jobManager) {
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
                File input = jobManager.getFile(task.getInputFile());
                File output = jobManager.getFile(task.getOutputFile());
                System.out.println("writing to: "+output.getAbsolutePath());
                FileUtil.copy(output, input);
                task.getStatus().setStatus(Status.SUCCEEDED);
            } catch (Throwable t) {
                task.getStatus().setStatus(Status.FAILED);
                if (t instanceof Error)
                    throw (Error)t;
                t.printStackTrace();
                task.getStatus().setMessage(t.getMessage());
            }
        }
    }

}
