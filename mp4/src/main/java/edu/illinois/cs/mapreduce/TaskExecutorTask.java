package edu.illinois.cs.mapreduce;

public abstract class TaskExecutorTask extends Status<TaskAttemptID, TaskAttemptStatus> {

    private static final long serialVersionUID = 7637704493473769567L;

    protected final Path jarPath;
    protected final JobDescriptor descriptor;
    protected final Path outputPath;
    protected String message;
    protected final NodeID targetNodeID;

    public TaskExecutorTask(TaskAttemptID id,
                            Path jarPath,
                            JobDescriptor descriptor,
                            Path outputPath,
                            NodeID targetNodeID) {
        super(id);
        this.jarPath = jarPath;
        this.descriptor = descriptor;
        this.outputPath = outputPath;
        this.targetNodeID = targetNodeID;
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

    public NodeID getTargetNodeID() {
        return targetNodeID;
    }

    abstract boolean isMap();

    public synchronized String getMessage() {
        return message;
    }

    public synchronized void setMessage(String message) {
        this.message = message;
    }

    public synchronized void setFailed(String message) {
        this.state = State.FAILED;
        this.message = message;
    }

    @Override
    public synchronized TaskAttemptStatus toImmutableStatus() {
        return new TaskAttemptStatus(id, state, targetNodeID, message);
    }

}
