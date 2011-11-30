package edu.illinois.cs.mapreduce;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
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

    private final NodeID nodeId;
    private Cluster cluster;
    private final ExecutorService executorService;
    private final Map<TaskAttemptID, TaskAttemptExecution> executions;

    // Hardware Monitor
    private double throttle;
    private int intervalCheckCpuUtil;
    private Timer timer;
    private HardwareMonitorTask hwMonitor;

    TaskExecutor(NodeID nodeId, int numThreads) {
        this.nodeId = nodeId;
        this.executorService = Executors.newFixedThreadPool(numThreads);
        this.executions = new TreeMap<TaskAttemptID, TaskAttemptExecution>();
        this.throttle = 0.5; // TODO: set via config or this acceptable default?
        this.intervalCheckCpuUtil = 5000; // TODO set via config?
        this.timer = new Timer();
    }

    public void start(Cluster cluster) {
        this.cluster = cluster;
        this.hwMonitor = new HardwareMonitorTask();
        this.timer.schedule(hwMonitor, 0, this.intervalCheckCpuUtil);
        new Thread(new StatusUpdater()).start();
    }

    @Override
    public void execute(TaskExecutorTask task) throws RemoteException {
        Semaphore completion = new Semaphore(0);
        TaskRunner runner = new TaskRunner(nodeId, task, completion, cluster);
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
            FileSystemService fileSystem = cluster.getFileSystemService(task.getTargetNodeID());
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

    @Override
    public void setThrottle(double value) throws IOException {
        this.throttle = value;

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
                        TaskAttemptStatus status = execution.getTask().toImmutableStatus();
                        NodeID nodeId = status.getNodeID();
                        List<TaskAttemptStatus> nodeStatus = map.get(nodeId);
                        if (nodeStatus == null)
                            map.put(nodeId, nodeStatus = new ArrayList<TaskAttemptStatus>());
                        nodeStatus.add(status);
                    }
                }
                for (Entry<NodeID, List<TaskAttemptStatus>> entry : map.entrySet()) {
                    JobManagerService jobManager = cluster.getJobManagerService(entry.getKey());
                    TaskAttemptStatus[] statuses = entry.getValue().toArray(new TaskAttemptStatus[0]);
                    TaskExecutorStatus status =
                        new TaskExecutorStatus(nodeId, hwMonitor.getCpuUtil(), executions.size(), throttle);
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
