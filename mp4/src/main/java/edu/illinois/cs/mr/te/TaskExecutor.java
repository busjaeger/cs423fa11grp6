package edu.illinois.cs.mr.te;

import java.io.IOException;
import java.net.ConnectException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.illinois.cs.mr.Node;
import edu.illinois.cs.mr.NodeConfiguration;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.NodeListener;
import edu.illinois.cs.mr.fs.FileSystemService;
import edu.illinois.cs.mr.fs.Path;
import edu.illinois.cs.mr.jm.AttemptID;
import edu.illinois.cs.mr.jm.AttemptStatus;
import edu.illinois.cs.mr.jm.JobManagerService;
import edu.illinois.cs.mr.util.Status.State;

public class TaskExecutor implements TaskExecutorService, NodeListener {

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
    private Node node; // quasi immutable
    private final ThreadPoolExecutor executorService;

    // mutable state
    private final Map<AttemptID, TaskExecution> executions;
    private double throttle;
    private long[] taskRuntimes;
    private int index;
    private int currentCapacity;

    public TaskExecutor(NodeConfiguration config) {
        this.config = config;
        this.throttle = config.teThrottle;
        this.executorService = (ThreadPoolExecutor)Executors.newFixedThreadPool(config.teNumThreads);
        this.executions = new TreeMap<AttemptID, TaskExecution>();
        this.index = 0;
        this.taskRuntimes = new long[10];
        this.currentCapacity = 0;
    }

    @Override
    public void start(Node node) {
        this.node = node;
        node.getScheduledExecutorService().scheduleAtFixedRate(new StatusUpdater(),
                                                               0,
                                                               config.teStatusUpdateInterval,
                                                               TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        this.executorService.shutdown();
    }

    @Override
    public synchronized void setThrottle(double value) {
        this.throttle = value;
    }

    public synchronized double getThrottle() {
        return this.throttle;
    }

    public int getNumThreads() {
        return config.teNumThreads;
    }

    public int getNumActiveThreads() {
        return executorService.getActiveCount();
    }

    public int getQueueLength() {
        return executorService.getQueue().size();
    }

    private long updateAverage() {
        long average = 0;
        for (long taskRuntime : this.taskRuntimes) {
            average += taskRuntime;
        }
        if (currentCapacity < this.taskRuntimes.length) {
            return (long)((double)average / (double)this.currentCapacity);
        }
        return (long)((double)average / (double)this.taskRuntimes.length);
    }

    public synchronized long done(TaskExecutorTask task) {
        this.currentCapacity++;
        long runtime = task.getDoneTime() - task.getBeginRunningTime();
        taskRuntimes[index] = runtime;
        this.index = (index++) % this.taskRuntimes.length;
        if (this.throttle == 0) {
            return 0;
        }
        Long average = new Long(this.updateAverage());
        return (long)((average.doubleValue() / (this.throttle / 100.0)) - average.doubleValue());
    }

    @Override
    public void execute(TaskExecutorTask task) throws RemoteException {
        Semaphore completion = new Semaphore(0);
        TaskRunner runner = new TaskRunner(this, task, completion, node);
        synchronized (executions) {
            task.setState(State.WAITING);
            Future<?> future = executorService.submit(runner);
            executions.put(task.getId(), new TaskExecution(future, task, completion));
        }
    }

    @Override
    public boolean cancel(AttemptID id, long timeout, TimeUnit unit) throws IOException, TimeoutException {
        TaskExecution execution;
        synchronized (executions) {
            execution = executions.get(id);
        }
        if (execution != null) {
            Future<?> future = execution.getFuture();
            if (!future.isDone())
                future.cancel(true);
            TaskExecutorTask task = execution.getTask();
            synchronized (task) {
                switch (task.getState()) {
                    case CREATED:
                    case WAITING:
                        task.setState(State.CANCELED);
                        return true;
                    case SUCCEEDED:
                    case FAILED:
                    case CANCELED:
                        return true;
                    case RUNNING:
                        break;
                }
            }
            // cancel running task
            Semaphore completion = execution.getCompletion();
            try {
                if (completion.tryAcquire(timeout, unit) && task.isDone())
                    return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return false;
    }

    @Override
    public boolean delete(AttemptID id) throws IOException {
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

    private class StatusUpdater implements Runnable {
        @Override
        public void run() {
            try {
                Map<NodeID, List<AttemptStatus>> map = new TreeMap<NodeID, List<AttemptStatus>>();
                synchronized (executions) {
                    for (TaskExecution execution : executions.values()) {
                        AttemptStatus status = execution.getTask().toImmutableStatus();
                        NodeID nodeId = status.getNodeID();
                        List<AttemptStatus> nodeStatus = map.get(nodeId);
                        if (nodeStatus == null)
                            map.put(nodeId, nodeStatus = new ArrayList<AttemptStatus>());
                        nodeStatus.add(status);
                    }
                }
                for (Entry<NodeID, List<AttemptStatus>> entry : map.entrySet()) {
                    JobManagerService jobManager = node.getJobManagerService(entry.getKey());
                    AttemptStatus[] statuses = entry.getValue().toArray(new AttemptStatus[0]);
                    try {
                        jobManager.updateStatus(statuses);
                    } catch (ConnectException e) {
                        System.out.println("TaskExecutor.StatusUpdater: Node " + entry.getKey() + " unreachable");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
