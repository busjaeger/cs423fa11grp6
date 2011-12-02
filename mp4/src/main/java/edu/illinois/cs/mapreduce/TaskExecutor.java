package edu.illinois.cs.mapreduce;

import java.io.IOException;
import java.net.ConnectException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.illinois.cs.mapreduce.Status.State;

class TaskExecutor implements TaskExecutorService {

    private static class TaskExecution {
        private final Future<?> future;
        private final TaskExecutorTask task;
        private final Semaphore completion;

        TaskExecution(Future<?> future, TaskExecutorTask task, Semaphore completion) {
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
    private final Map<TaskAttemptID, TaskExecution> executions;
    private double throttle;
    private double cpuUtilization;
    private double throttleInterval;

    TaskExecutor(NodeConfiguration config) {
        this.config = config;
        this.executorService = (ThreadPoolExecutor)Executors.newFixedThreadPool(config.teNumThreads);
        this.executions = new TreeMap<TaskAttemptID, TaskExecution>();
        this.throttle = config.teThrottle;
        this.cpuProfilingInterval = config.teCpuProfilingInterval;
        this.statusUpdateInterval = config.teStatusUpdateInterval;
        this.throttleInterval = config.teThrottleInterval;
        this.timer = new Timer();
    }

    @Override
    public void start(Node node) {
        this.node = node;
        this.timer.schedule(new HardwareMonitorTask(this), 0, this.cpuProfilingInterval);
        this.timer.schedule(new StatusUpdateTask(), 0, this.statusUpdateInterval);
    }

    @Override
    public void stop() {
        this.timer.cancel();
        this.executorService.shutdown();
    }

    @Override
    public synchronized void setThrottle(double value) throws IOException {
        this.throttle = value;
    }

    public synchronized void setCpuUtilization(double cpuUtilization) {
        this.cpuUtilization = cpuUtilization;
    }

    private int computeSleepForInterval() {
        double interval = ((100.0 - this.throttle) / 100.0) * this.throttleInterval;
        return (int)interval;
    }

    @Override
    public void execute(TaskExecutorTask task) throws RemoteException {
        int sleepFor = this.computeSleepForInterval();
        Semaphore completion = new Semaphore(0);
        TaskRunner runner = new TaskRunner(sleepFor, task, completion, node);
        synchronized (executions) {
            task.setState(State.WAITING);
            Future<?> future = executorService.submit(runner);
            executions.put(task.getId(), new TaskExecution(future, task, completion));
        }
    }

    @Override
    public boolean cancel(TaskAttemptID id, long timeout, TimeUnit unit) throws IOException, TimeoutException {
        TaskExecution execution;
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
        TaskExecution execution;
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

    private class StatusUpdateTask extends TimerTask {
        @Override
        public void run() {
            try {
                Map<NodeID, List<TaskAttemptStatus>> map = new TreeMap<NodeID, List<TaskAttemptStatus>>();
                synchronized (executions) {
                    for (NodeID nodeId : node.getNodeIds()) {
                        List<TaskAttemptStatus> status = new ArrayList<TaskAttemptStatus>();
                        map.put(nodeId, status);
                        // could index by node to avoid repeated iteration
                        for (TaskExecution execution : executions.values()) {
                            TaskExecutorTask task = execution.getTask();
                            if (task.getNodeID().equals(nodeId))
                                status.add(task.toImmutableStatus());
                        }
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
                    } catch (ConnectException e) {
                        System.out.println("cannot reach node "+entry.getKey()+" for status update");
                    }
                }
            } catch (Throwable t) {
                System.out.println("node " + config.nodeId + " failed to update status");
                t.printStackTrace();
            }
        }
    }
}
