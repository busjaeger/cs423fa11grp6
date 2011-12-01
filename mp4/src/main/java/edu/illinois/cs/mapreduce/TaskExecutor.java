package edu.illinois.cs.mapreduce;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class TaskExecutor implements TaskExecutorService {

    private static class TaskAttemptExecution {
        private final Future<?> future;
        private final TaskExecutorTask task;
        private final Semaphore completion;

        TaskAttemptExecution(Future<?> future, TaskExecutorTask task, Semaphore completion) {
            this.future = future;
            this.task = task;
            this.completion = completion;
        }

        public Future<?> getFuture() {
            return future;
        }

        public TaskExecutorTask getTask() {
            return task;
        }

        public Semaphore getCompletion() {
            return completion;
        }
    }

    private final NodeConfiguration config;
    private final ThreadPoolExecutor executorService;
    private final int cpuProfilingInterval;
    private final int statusUpdateInterval;
    private final Timer timer;
    private Node node; // quasi immutable

    // mutable state
    private final Map<TaskAttemptID, TaskAttemptExecution> executions;
    private Future<?> statusUpdateFuture;
    private double throttle;
    private double cpuUtilization;

    TaskExecutor(NodeConfiguration config) {
        this.config = config;
        this.executorService = (ThreadPoolExecutor)Executors.newFixedThreadPool(config.teNumThreads);
        this.executions = new TreeMap<TaskAttemptID, TaskAttemptExecution>();
        this.throttle = config.teThrottle;
        this.cpuProfilingInterval = config.teCpuProfilingInterval;
        this.statusUpdateInterval = config.teStatusUpdateInterval;
        this.timer = new Timer();
    }

    @Override
    public void start(Node node) {
        this.node = node;
        HardwareMonitorTask hwMonitor = new HardwareMonitorTask(this);
        this.timer.schedule(hwMonitor, 0, this.cpuProfilingInterval);
        statusUpdateFuture = node.getExecutorService().submit(new StatusUpdater());
    }

    @Override
    public void stop() {
        this.timer.cancel();
        this.statusUpdateFuture.cancel(true);
    }

    @Override
    public synchronized void setThrottle(double value) throws IOException {
        this.throttle = value;
    }

    public synchronized void setCpuUtilization(double cpuUtilization) {
        this.cpuUtilization = cpuUtilization;
    }

    @Override
    public void execute(TaskExecutorTask task) throws RemoteException {
        Semaphore completion = new Semaphore(0);
        TaskRunner runner = new TaskRunner(task, completion, node);
        Future<?> future = executorService.submit(runner);
        synchronized (executions) {
            executions.put(task.getId(), new TaskAttemptExecution(future, task, completion));
        }
    }

    @Override
    public boolean cancel(TaskAttemptID id, long timeout, TimeUnit unit) throws IOException, TimeoutException {
        TaskAttemptExecution execution;
        synchronized (executions) {
            execution = executions.get(id);
        }
        if (execution != null) {
            TaskExecutorTask task = execution.getTask();
            if (task.isDone())
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
            if (task.isDone())
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
            TaskExecutorTask task = execution.getTask();
            if (!task.isDone())
                return false;
            FileSystemService fileSystem = node.getFileSystemService(task.getTargetNodeID());
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
                    Thread.sleep(statusUpdateInterval);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    return;
                }
                Map<NodeID, List<TaskAttemptStatus>> map = new TreeMap<NodeID, List<TaskAttemptStatus>>();
                synchronized (executions) {
                    for (TaskAttemptExecution execution : executions.values()) {
                        TaskAttemptStatus status = execution.getTask().toImmutableStatus();
                        NodeID nodeId = status.getNodeID();
                        List<TaskAttemptStatus> nodeStatus = map.get(nodeId);
                        if (nodeStatus == null)
                            map.put(nodeId, nodeStatus = new ArrayList<TaskAttemptStatus>());
                        nodeStatus.add(status);
                    }
                }
                int queueLength = executorService.getQueue().size();
                TaskExecutorStatus status;
                synchronized (this) {
                    status = new TaskExecutorStatus(config.nodeId, cpuUtilization, queueLength, throttle);
                }
                for (Entry<NodeID, List<TaskAttemptStatus>> entry : map.entrySet()) {
                    JobManagerService jobManager = node.getJobManagerService(entry.getKey());
                    TaskAttemptStatus[] statuses = entry.getValue().toArray(new TaskAttemptStatus[0]);
                    try {
                        jobManager.updateStatus(status, statuses);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
