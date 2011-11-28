package edu.illinois.cs.mapreduce;

import java.io.Serializable;

/**
 * thread safe
 * 
 * @author benjamin
 */
public class TaskAttemptStatus extends Status<TaskAttemptID> implements Serializable {

    private static final long serialVersionUID = -52586956350693369L;

    private final NodeID onNodeID;
    private String message;

    public TaskAttemptStatus(TaskAttemptID id, NodeID nodeID) {
        super(id);
        this.onNodeID = nodeID;
    }

    public TaskAttemptStatus(TaskAttemptStatus status) {
        super(status);
        this.onNodeID = status.getOnNodeID();
        this.message = status.getMessage();
    }

    public NodeID getOnNodeID() {
        return onNodeID;
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

    public synchronized void setMessage(String message) {
        this.message = message;
    }

    public synchronized String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "TaskAttemptStatus [id=" + id.toQualifiedString() + ", message=" + message + ", state=" + getState() + "]";
    }

}
