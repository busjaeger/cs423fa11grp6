package edu.illinois.cs.mapreduce;

import java.io.Serializable;

/**
 * A TaskAttempt represents a scheduled task. It is needed to distinguish
 * different executions of the same task. This class is thread safe.
 * 
 * @author benjamin
 */
public class TaskAttempt extends Status<TaskAttemptID, TaskAttemptStatus> implements Serializable {

    private static final long serialVersionUID = -1954349780451419520L;

    // state owned by task attempt
    protected final Path outputPath;
    protected String message;
    // the ID of the node on which the task is attempted
    protected final NodeID targetNodeID;
    // state owned by job
    protected final Path jarPath;
    protected final JobDescriptor descriptor;

    public TaskAttempt(TaskAttemptID id, NodeID targetNodeID, Path jarPath, JobDescriptor descriptor, Path outputPath) {
        super(id);
        this.targetNodeID = targetNodeID;
        this.jarPath = jarPath;
        this.descriptor = descriptor;
        this.outputPath = outputPath;
    }

    public boolean isMap() {
        return id.getParentID().isMap();
    }

    public NodeID getTargetNodeID() {
        return targetNodeID;
    }

    public Path getJarPath() {
        return jarPath;
    }

    public JobDescriptor getDescriptor() {
        return descriptor;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public synchronized void setFailed(String message) {
        this.state = State.FAILED;
        this.message = message;
    }

    public synchronized boolean updateStatus(TaskAttemptStatus newStatus) {
        if (state == newStatus.getState())
            return false;
        state = newStatus.getState();
        message = newStatus.getMessage();
        return true;
    }

    @Override
    public synchronized TaskAttemptStatus toImmutableStatus() {
        return new TaskAttemptStatus(id, state, targetNodeID, message);
    }

}
