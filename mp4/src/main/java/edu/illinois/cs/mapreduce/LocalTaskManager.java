package edu.illinois.cs.mapreduce;

import java.rmi.RemoteException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.illinois.cs.dfs.FileSystem;

public class LocalTaskManager implements TaskManager {

    private final BlockingQueue<Task> taskInQueue;
    private final BlockingQueue<Task> taskOutQueue;
    private final Thread workerThread;

    public LocalTaskManager(FileSystem fileSystem) {
        this.taskInQueue = new LinkedBlockingQueue<Task>();
        this.taskOutQueue = new LinkedBlockingQueue<Task>();
        this.workerThread = new Thread(new TaskRunner(taskInQueue, taskOutQueue, fileSystem));
        this.workerThread.start();
    }

    @Override
    public void submitTask(Task task) throws RemoteException {
        task.getStatus().setStatus(Status.WAITING);
        taskInQueue.add(task);
    }

}
