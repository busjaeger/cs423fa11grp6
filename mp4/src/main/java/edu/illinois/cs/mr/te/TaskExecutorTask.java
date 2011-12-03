package edu.illinois.cs.mr.te;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.fs.Path;
import edu.illinois.cs.mr.jm.JobDescriptor;
import edu.illinois.cs.mr.jm.Attempt;
import edu.illinois.cs.mr.jm.AttemptID;
import edu.illinois.cs.mr.jm.AttemptStatus;

public abstract class TaskExecutorTask extends Attempt {

    private static final long serialVersionUID = 7637704493473769567L;

    protected final Path jarPath;
    protected final JobDescriptor descriptor;
    protected String message;

    public TaskExecutorTask(AttemptID id,
                            Path jarPath,
                            JobDescriptor descriptor,
                            Path outputPath,
                            NodeID targetNodeID) {
        super(id, targetNodeID, outputPath);
        this.jarPath = jarPath;
        this.descriptor = descriptor;
    }

    public Path getJarPath() {
        return jarPath;
    }

    public JobDescriptor getDescriptor() {
        return descriptor;
    }

    abstract boolean isMap();

    public synchronized boolean isDone() {
        return State.isEndState(getState());
    }

    public synchronized String getMessage() {
        return message;
    }

    public synchronized void setMessage(String message) {
        this.message = message;
    }

    public synchronized void setFailed(String message) {
        setState(State.FAILED);
        this.message = message;
    }

    @Override
    public synchronized AttemptStatus toImmutableStatus() {
        return new AttemptStatus(this);
    }

}
