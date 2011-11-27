package edu.illinois.cs.mapreduce;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.sun.corba.se.impl.orbutil.threadpool.TimeoutException;

class TaskExecutor implements TaskExecutorService {

    private static class TaskAttemptExecution {
        private final Future<?> future;
        private final TaskAttempt task;
        private final Semaphore completion;

        TaskAttemptExecution(Future<?> future, TaskAttempt task, Semaphore completion) {
            this.future = future;
            this.task = task;
            this.completion = completion;
        }

        public Future<?> getFuture() {
            return future;
        }

        public TaskAttempt getTaskAttempt() {
            return task;
        }

        public Semaphore getCompletion() {
            return completion;
        }

    }

    private final FileSystem fileSystem;
    private final ExecutorService executorService;
    private final Map<TaskAttemptID, TaskAttemptExecution> executions;

    TaskExecutor(FileSystem fileSystem, int numThreads) {
        this.fileSystem = fileSystem;
        this.executorService = Executors.newFixedThreadPool(numThreads);
        this.executions = new HashMap<TaskAttemptID, TaskAttemptExecution>();
    }

    @Override
    public void execute(TaskAttempt attempt) throws RemoteException {
        Semaphore completion = new Semaphore(0);
        TaskRunner runner = new TaskRunner(attempt, fileSystem, completion);
        Future<?> future = executorService.submit(runner);
        synchronized (executions) {
            executions.put(attempt.getId(), new TaskAttemptExecution(future, attempt, completion));
        }
    }

    @Override
    public boolean cancel(TaskAttemptID id, long timeout, TimeUnit unit) throws IOException, TimeoutException {
        TaskAttemptExecution execution;
        synchronized (executions) {
            execution = executions.get(id);
        }
        if (execution != null) {
            TaskAttempt task = execution.getTaskAttempt();
            TaskAttemptStatus status = task.getStatus();
            if (status.isDone())
                return true;
            Future<?> future = execution.getFuture();
            if (!future.isDone())
                future.cancel(true);
            Semaphore completion = execution.getCompletion();
            try {
                if (completion.tryAcquire(timeout, unit))
                    return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (status.isDone())
                return true;
            return false;
        }
        return true;
    }

    @Override
    public boolean delete(TaskAttemptID id) throws IOException {
        TaskAttemptExecution execution;
        synchronized (executions) {
            execution = executions.get(id);
        }
        if (execution != null) {
            TaskAttempt task = execution.getTaskAttempt();
            TaskAttemptStatus status = task.getStatus();
            if (!status.isDone())
                return false;
            Path outputPath = task.getOutputPath();
            synchronized (outputPath) {
                if (fileSystem.exists(outputPath) && !fileSystem.delete(outputPath))
                    return false;
            }
            synchronized (executions) {
                executions.remove(id);
            }
        }
        return true;
    }

}
