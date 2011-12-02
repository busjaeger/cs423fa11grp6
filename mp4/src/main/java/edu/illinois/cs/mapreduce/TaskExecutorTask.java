package edu.illinois.cs.mapreduce;

public abstract class TaskExecutorTask extends TaskAttempt {

    private static final long serialVersionUID = 7637704493473769567L;

    protected final Path jarPath;
    protected final JobDescriptor descriptor;
    protected String message;

    public TaskExecutorTask(TaskAttemptID id,
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
    public synchronized TaskAttemptStatus toImmutableStatus() {
        return new TaskAttemptStatus(this);
    }

}
