package edu.illinois.cs.mapreduce;

import edu.illinois.cs.mapreduce.Status.State;

/**
 * thread safe
 * 
 * @author benjamin
 */
public class TaskStatus extends ImmutableStatus<TaskID> {
    private static final long serialVersionUID = -389780613584853202L;

    private final Iterable<TaskAttemptStatus> attemptStatuses;

    public TaskStatus(TaskID id, State state, Iterable<TaskAttemptStatus> attemptStatuses) {
        super(id, state);
        this.attemptStatuses = attemptStatuses;
    }

    public Iterable<TaskAttemptStatus> getAttemptStatuses() {
        return attemptStatuses;
    }

}
