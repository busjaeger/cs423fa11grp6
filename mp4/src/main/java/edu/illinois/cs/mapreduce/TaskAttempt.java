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

    protected final Path outputPath;
    protected String message;
    protected final NodeID targetNodeID;

    public TaskAttempt(TaskAttemptID id, NodeID targetNodeID, Path outputPath) {
        super(id);
        this.targetNodeID = targetNodeID;
        this.outputPath = outputPath;
    }

    public NodeID getTargetNodeID() {
        return targetNodeID;
    }

    public Path getOutputPath() {
        return outputPath;
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
