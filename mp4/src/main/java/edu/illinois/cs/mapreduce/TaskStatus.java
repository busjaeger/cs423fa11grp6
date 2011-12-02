package edu.illinois.cs.mapreduce;



/**
 * thread safe
 * 
 * @author benjamin
 */
public class TaskStatus extends ImmutableStatus<TaskID> {
    private static final long serialVersionUID = -389780613584853202L;

    private final Iterable<TaskAttemptStatus> attemptStatuses;

    public TaskStatus(Task task) {
        super(task);
        this.attemptStatuses = toImmutableStatuses(task.getAttempts());
    }

    public Iterable<TaskAttemptStatus> getAttemptStatuses() {
        return attemptStatuses;
    }

}
