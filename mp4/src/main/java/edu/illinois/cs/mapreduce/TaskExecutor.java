package edu.illinois.cs.mapreduce;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
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

    private Cluster cluster;
    private final ExecutorService executorService;
    private final Map<TaskAttemptID, TaskAttemptExecution> executions;

    TaskExecutor(int numThreads) {
        this.executorService = Executors.newFixedThreadPool(numThreads);
        this.executions = new TreeMap<TaskAttemptID, TaskAttemptExecution>();
    }

    public void start(Cluster cluster) {
        this.cluster = cluster;
        new Thread(new StatusUpdater()).start();
    }

    @Override
    public void execute(TaskAttempt attempt) throws RemoteException {
        Semaphore completion = new Semaphore(0);
        FileSystemService fileSystem = cluster.getFileSystemService(attempt.getNodeID());
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
            FileSystemService fileSystem = cluster.getFileSystemService(task.getNodeID());
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

    private class StatusUpdater implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                Map<NodeID, List<TaskAttemptStatus>> map = new TreeMap<NodeID, List<TaskAttemptStatus>>();
                synchronized (executions) {
                    for (TaskAttemptExecution execution : executions.values()) {
                        TaskAttemptStatus status = new TaskAttemptStatus(execution.getTaskAttempt().getStatus());
                        NodeID nodeId = status.getId().getParentID().getParentID().getParentID();
                        List<TaskAttemptStatus> nodeStatus = map.get(nodeId);
                        if (nodeStatus == null)
                            map.put(nodeId, nodeStatus = new ArrayList<TaskAttemptStatus>());
                        nodeStatus.add(status);
                    }
                }
                for (Entry<NodeID, List<TaskAttemptStatus>> entry : map.entrySet()) {
                    JobManagerService jobManager = cluster.getJobManagerService(entry.getKey());
                    try {
                        jobManager.updateJobStatuses(entry.getValue().toArray(new TaskAttemptStatus[0]));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }
}
