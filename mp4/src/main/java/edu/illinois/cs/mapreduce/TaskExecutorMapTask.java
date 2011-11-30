package edu.illinois.cs.mapreduce;

import edu.illinois.cs.mapreduce.api.Partition;

public class TaskExecutorMapTask extends TaskExecutorTask {

    private static final long serialVersionUID = -2891035001799074650L;

    private final Partition partition;
    private final Path inputPath;

    public TaskExecutorMapTask(TaskAttemptID id,
                               Path jarPath,
                               JobDescriptor descriptor,
                               Path outputPath,
                               NodeID targetNodeID,
                               Partition partition,
                               Path inputPath) {
        super(id, jarPath, descriptor, outputPath, targetNodeID);
        this.partition = partition;
        this.inputPath = inputPath;
    }

    @Override
    boolean isMap() {
        return true;
    }

    public Partition getPartition() {
        return partition;
    }

    public Path getInputPath() {
        return inputPath;
    }

}
