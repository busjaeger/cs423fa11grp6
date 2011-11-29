package edu.illinois.cs.mapreduce;

import edu.illinois.cs.mapreduce.api.Partition;

public class MapTaskAttempt extends TaskAttempt {

    private static final long serialVersionUID = 5058256779951559618L;

    // state owned by the task
    private final Partition partition;
    private final Path inputPath;

    public MapTaskAttempt(TaskAttemptID id,
                          NodeID nodeID,
                          Path jarPath,
                          JobDescriptor descriptor,
                          Partition partition,
                          Path inputPath,
                          Path outputPath) {
        super(id, nodeID, jarPath, descriptor, outputPath);
        this.partition = partition;
        this.inputPath = inputPath;
    }

    public MapTaskAttempt(MapTaskAttempt attempt) {
        super(attempt);
        this.partition = attempt.partition;
        this.inputPath = attempt.inputPath;
    }

    public Partition getPartition() {
        return partition;
    }

    public Path getInputPath() {
        return inputPath;
    }

}
