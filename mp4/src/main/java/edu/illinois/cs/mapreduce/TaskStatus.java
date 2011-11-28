package edu.illinois.cs.mapreduce;

import java.util.ArrayList;
import java.util.List;

/**
 * thread safe
 * 
 * @author benjamin
 */
public class TaskStatus extends Status<TaskID> {
    private static final long serialVersionUID = -389780613584853202L;

    private final List<TaskAttemptStatus> attemptStatuses;

    public TaskStatus(TaskID taskID) {
        super(taskID);
        this.attemptStatuses = new ArrayList<TaskAttemptStatus>();
    }

    // must be called with lock held
    public TaskStatus(TaskStatus taskStatus) {
        super(taskStatus);
        this.attemptStatuses = taskStatus.getAttemptStatuses();
        for (int i = 0; i < attemptStatuses.size(); i++)
            attemptStatuses.set(i, new TaskAttemptStatus(attemptStatuses.get(i)));
    }

    synchronized void addAttemptStatus(TaskAttemptStatus status) {
        attemptStatuses.add(status);
    }

    public synchronized List<TaskAttemptStatus> getAttemptStatuses() {
        return new ArrayList<TaskAttemptStatus>(attemptStatuses);
    }

}
