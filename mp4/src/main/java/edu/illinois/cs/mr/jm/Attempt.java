package edu.illinois.cs.mr.jm;

import java.io.Serializable;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.fs.Path;
import edu.illinois.cs.mr.util.Status;

/**
 * A TaskAttempt represents a scheduled task. It is needed to distinguish
 * different executions of the same task. This class is thread safe.
 * 
 * @author benjamin
 */
public class Attempt extends Status<AttemptID, AttemptStatus> implements Serializable {

    private static final long serialVersionUID = -1954349780451419520L;

    protected final Path outputPath;
    protected String message;
    protected final NodeID targetNodeID;

    public Attempt(AttemptID id, NodeID targetNodeID, Path outputPath) {
        super(id);
        this.targetNodeID = targetNodeID;
        this.outputPath = outputPath;
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

    public Path getOutputPath() {
        return outputPath;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public synchronized boolean update(AttemptStatus newStatus) {
        if (super.update(newStatus)) {
            message = newStatus.getMessage();
            return true;
        }
        return false;
    }

    @Override
    public synchronized AttemptStatus toImmutableStatus() {
        return new AttemptStatus(this);
    }

}
