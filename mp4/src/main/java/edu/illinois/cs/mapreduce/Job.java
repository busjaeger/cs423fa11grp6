package edu.illinois.cs.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A Job represents a unit of work submitted by the user. The framework splits
 * the work into a set of tasks that can be executed independently on different
 * nodes in the cluster.
 * 
 * @author benjamin
 */
public class Job extends Status<JobID, JobStatus> {

    private static final long serialVersionUID = 5073561871061802007L;

    static enum Phase {
        MAP, REDUCE
    }

    private Phase phase;
    private final Path path;
    private final Path jarPath;
    private final JobDescriptor descriptor;
    private final Map<TaskID, MapTask> mapTasks;
    private final Map<TaskID, ReduceTask> reduceTasks;

    public Job(JobID id, String jarName, JobDescriptor descriptor) {
        super(id);
        this.phase = Phase.MAP;
        this.path = new Path(id.toQualifiedString());
        this.jarPath = path.append(jarName);
        this.descriptor = descriptor;
        this.mapTasks = new TreeMap<TaskID, MapTask>(ID.<TaskID> getValueComparator());
        this.reduceTasks = new TreeMap<TaskID, ReduceTask>(ID.<TaskID> getValueComparator());
    }

    public synchronized Phase getPhase() {
        return phase;
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

    public synchronized List<MapTask> getMapTasks() {
        return new ArrayList<MapTask>(mapTasks.values());
    }

    @Override
    public synchronized JobStatus toImmutableStatus() {
        return new JobStatus(id, state, phase, toTaskStatuses(mapTasks), toTaskStatuses(reduceTasks));
    }

    private static Iterable<TaskStatus> toTaskStatuses(Map<TaskID, ? extends Task> taskMap) {
        Collection<? extends Task> tasks = taskMap.values();
        TaskStatus[] taskStatuses = new TaskStatus[tasks.size()];
        int i = 0;
        for (Task task : tasks)
            taskStatuses[i++] = task.toImmutableStatus();
        return Arrays.asList(taskStatuses);
    }

    public synchronized boolean updateStatus(TaskAttemptStatus[] attemptStatuses, int offset, int length) {
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
        switch (phase) {
            case MAP:
                newState = computeState(mapTasks.values());
                break;
            case REDUCE:
                newState = computeState(reduceTasks.values());
                break;
        }
        if (state != newState) {
            state = newState;
            if (state == State.SUCCEEDED && phase == Phase.MAP) {
                state = State.CREATED;
                phase = Phase.REDUCE;
            }
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
