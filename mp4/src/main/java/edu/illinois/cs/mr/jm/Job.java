package edu.illinois.cs.mr.jm;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import edu.illinois.cs.mr.fs.Path;
import edu.illinois.cs.mr.util.ID;
import edu.illinois.cs.mr.util.PhasedStatus;

/**
 * A Job represents a unit of work submitted by the user. The framework splits
 * the work into a set of tasks that can be executed independently on different
 * nodes in the cluster.
 * 
 * @author benjamin
 */
public class Job extends PhasedStatus<JobID, JobStatus, Phase> {

    private static final long serialVersionUID = 5073561871061802007L;

    private final Path path;
    private final Path jarPath;
    private final JobDescriptor descriptor;
    private final Map<TaskID, MapTask> mapTasks;
    private final Map<TaskID, ReduceTask> reduceTasks;

    public Job(JobID id, String jarName, JobDescriptor descriptor) {
        super(id, Phase.class);
        this.path = new Path(id.toQualifiedString());
        this.jarPath = path.append(jarName);
        this.descriptor = descriptor;
        this.mapTasks = new TreeMap<TaskID, MapTask>(ID.<TaskID> getValueComparator());
        this.reduceTasks = new TreeMap<TaskID, ReduceTask>(ID.<TaskID> getValueComparator());
    }

    public Path getPath() {
        return path;
    }

    public Path getJarPath() {
        return jarPath;
    }

    public JobDescriptor getDescriptor() {
        return descriptor;
    }

    public synchronized MapTask getMapTask(TaskID taskID) {
        return mapTasks.get(taskID);
    }

    public synchronized Task getTask(TaskID taskID) {
        assert taskID.getParentID().equals(id);
        return taskID.isMap() ? mapTasks.get(taskID) : reduceTasks.get(taskID);
    }

    public synchronized void addTask(Task task) {
        TaskID taskID = task.getId();
        if (taskID.isMap())
            mapTasks.put(taskID, (MapTask)task);
        else
            reduceTasks.put(taskID, (ReduceTask)task);
    }

    /**
     * Note: this method is not thread safe. A lock on the task must be held
     * while calling this method and using the iterable!
     */
    public Iterable<MapTask> getMapTasks() {
        return mapTasks.values();
    }

    /**
     * Note: this method is not thread safe. A lock on the task must be held
     * while calling this method and using the iterable!
     */
    public Iterable<ReduceTask> getReduceTasks() {
        return reduceTasks.values();
    }

    @Override
    public synchronized JobStatus toImmutableStatus() {
        return new JobStatus(this);
    }

    public synchronized boolean updateStatus(AttemptStatus[] attemptStatuses, int offset, int length) {
        // update task statuses
        boolean stateChange = false;
        int off = offset, len = 1;
        TaskID taskId = attemptStatuses[offset].getTaskID();
        for (int i = offset + 1; i < offset + length; i++) {
            TaskID current = attemptStatuses[i].getTaskID();
            if (!current.equals(taskId)) {
                Task task = getTask(taskId);
                stateChange |= task.updateStatus(attemptStatuses, off, len);
                off = i;
                len = 1;
                taskId = current;
            } else {
                len++;
            }
        }
        Task task = getTask(taskId);
        stateChange |= task.updateStatus(attemptStatuses, off, len);
        // if any tasks have changed, recompute the job status
        return stateChange ? updateState() : stateChange;
    }

    private synchronized boolean updateState() {
        State newState = null;
        switch (getPhase()) {
            case MAP:
                newState = computeState(mapTasks.values());
                break;
            case REDUCE:
                newState = computeState(reduceTasks.values());
                break;
        }
        if (getState() != newState) {
            setState(newState);
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
    private static State computeState(Collection<? extends Task> tasks) {
        if (tasks.isEmpty())
            return State.CREATED;
        boolean nonCanceled = false, nonSucceeded = false, runningOrSucceeded = false, waiting = false;
        for (Task task : tasks) {
            switch (task.getState()) {
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
