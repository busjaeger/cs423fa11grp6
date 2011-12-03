package edu.illinois.cs.mr.jm;

import edu.illinois.cs.mr.util.ImmutableStatus;



/**
 * thread safe
 * 
 * @author benjamin
 */
public class TaskStatus extends ImmutableStatus<TaskID> {
    private static final long serialVersionUID = -389780613584853202L;

    private final Iterable<AttemptStatus> attemptStatuses;

    public TaskStatus(Task task) {
        super(task);
        this.attemptStatuses = toImmutableStatuses(task.getAttempts());
    }

    public Iterable<AttemptStatus> getAttemptStatuses() {
        return attemptStatuses;
    }

}
