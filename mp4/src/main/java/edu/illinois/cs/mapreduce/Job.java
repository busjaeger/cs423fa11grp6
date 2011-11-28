package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import edu.illinois.cs.mapreduce.Status.State;

/**
 * A Job represents a unit of work submitted by the user. The framework splits
 * the work into a set of tasks that can be executed independently on different
 * nodes in the cluster.
 * 
 * @author benjamin
 */
public class Job implements Serializable {

    private static final long serialVersionUID = 5073561871061802007L;

    private final JobID id;
    private final Path path;
    private final JobStatus status;
    private final Path jar;
    private final Map<TaskID, Task> mapTasks;
    private final Map<TaskID, Task> reduceTasks;

    public Job(JobID id, String jarName) {
        this.id = id;
        this.path = new Path(id.toQualifiedString());
        this.jar = path.append(jarName);
        this.status = new JobStatus(id);
        this.mapTasks = new TreeMap<TaskID, Task>(ID.<TaskID> getValueComparator());
        this.reduceTasks = new TreeMap<TaskID, Task>(ID.<TaskID> getValueComparator());
    }

    public JobID getId() {
        return id;
    }

    public Path getPath() {
        return path;
    }

    public Path getJarPath() {
        return jar;
    }

    public synchronized void addMapTask(Task task) {
        assert task.getId().getParentID().equals(id);
        mapTasks.put(task.getId(), task);
        status.addTaskStatus(task.getStatus());
    }

    public synchronized Task getMapTasks(TaskID taskID) {
        assert taskID.getParentID().equals(id);
        return mapTasks.get(taskID);
    }

    public synchronized JobStatus getStatus() {
        return status;
    }

    public synchronized boolean updateStatus() {
        State oldState = status.getState();
        State newState = oldState;
        switch (status.getPhase()) {
            case MAP:
                newState = computeState(mapTasks.values());
                break;
            case REDUCE:
                newState = computeState(reduceTasks.values());
                break;
        }
        if (oldState != newState) {
            status.setState(newState);
            return true;
        }
        return false;
    }

    /**
     * job stats: <br/>
     * <code>
     * CREATED   := all tasks in state {CREATED}
     * WAITING   := all tasks in state {CREATED, WAITING, CANCELED}
     * RUNNING   := all tasks in state {CREATED, WAITING, RUNNING, CANCELED, SUCCEEDED}
     * FAILED    := one task in state {FAILED}
     * CANCELED  := all tasks in state {CANCELED}
     * SUCCEEDED := all tasks in state {SUCCEEDED}
     * </code>
     * 
     * @param tasks
     * @return
     */
    private State computeState(Collection<Task> tasks) {
        if (tasks.isEmpty())
            return State.CREATED;
        boolean nonCanceled = false, nonSucceeded = false, runningOrSucceeded = false, waiting = false;
        for (Task task : tasks) {
            State state = task.getStatus().getState();
            switch (state) {
                case FAILED:
                    return State.FAILED;
                case CANCELED:
                    nonSucceeded = true;
                    break;
                case SUCCEEDED:
                    runningOrSucceeded = true;
                    nonCanceled = true;
                    break;
                case RUNNING:
                    runningOrSucceeded = true;
                    nonSucceeded = true;
                    nonCanceled = true;
                    break;
                case WAITING:
                    waiting = true;
                    nonSucceeded = true;
                    nonCanceled = true;
                    break;
                case CREATED:
                    nonSucceeded = true;
                    nonCanceled = true;
                    break;
            }
        }
        if (!nonSucceeded)
            return State.SUCCEEDED;
        if (!nonCanceled)
            return State.CANCELED;
        if (runningOrSucceeded)
            return State.RUNNING;
        if (waiting)
            return State.WAITING;
        return State.CREATED;
    }
}
