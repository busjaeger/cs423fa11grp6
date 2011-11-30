package edu.illinois.cs.mapreduce;

import java.io.Serializable;

import edu.illinois.cs.mapreduce.Status.State;

/**
 * thread safe
 * 
 * @author benjamin
 */
public class TaskAttemptStatus extends ImmutableStatus<TaskAttemptID> implements Serializable {

    private static final long serialVersionUID = -52586956350693369L;

    private final NodeID targetNodeID;
    private final String message;

    public TaskAttemptStatus(TaskAttemptID id, State state, NodeID targetNodeID, String message) {
        super(id, state);
        this.targetNodeID = targetNodeID;
        this.message = message;
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
