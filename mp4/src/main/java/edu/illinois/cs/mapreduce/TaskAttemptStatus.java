package edu.illinois.cs.mapreduce;

import java.io.Serializable;

/**
 * thread safe
 * 
 * @author benjamin
 */
public class TaskAttemptStatus extends ImmutableStatus<TaskAttemptID> implements Serializable {

    private static final long serialVersionUID = -52586956350693369L;

    private final NodeID targetNodeID;
    private final String message;

    public TaskAttemptStatus(TaskAttempt attempt) {
        super(attempt);
        this.targetNodeID = attempt.getTargetNodeID();
        this.message = attempt.getMessage();
    }

    public NodeID getTargetNodeID() {
        return targetNodeID;
    }

    public NodeID getNodeID() {
        return getJobID().getParentID();
    }

    public JobID getJobID() {
        return getTaskID().getParentID();
    }

    public TaskID getTaskID() {
        return id.getParentID();
    }

    public String getMessage() {
        return message;
    }

}
