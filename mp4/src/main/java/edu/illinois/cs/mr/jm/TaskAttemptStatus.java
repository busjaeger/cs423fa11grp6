package edu.illinois.cs.mr.jm;

import java.io.Serializable;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.util.ImmutableStatus;

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
