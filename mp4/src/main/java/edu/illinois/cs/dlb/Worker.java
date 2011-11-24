package edu.illinois.cs.dlb;

import java.io.File;

import edu.illinois.cs.dlb.TaskStatus.Status;
import edu.illinois.cs.dlb.util.FileUtil;

class Worker implements Runnable {

    private final WorkManager workManager;

    Worker(WorkManager workManager) {
        this.workManager = workManager;
    }

    @Override
    public void run() {
        while (true) {
            Task task;
            try {
                task = workManager.getTaskQueue().take();
            } catch (InterruptedException e) {
                return;
            }

            // simulate for now
            try {
                File input = workManager.getFile(task.getInputFile());
                File output = workManager.getFile(task.getOutputFile());
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