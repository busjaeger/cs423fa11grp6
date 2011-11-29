package edu.illinois.cs.mapreduce;

import java.util.List;

public class ReduceTaskAttempt extends TaskAttempt {

    private static final long serialVersionUID = 2711392377038986636L;

    private final List<QualifiedPath> inputPaths;

    public ReduceTaskAttempt(TaskAttemptID id,
                             NodeID nodeID,
                             Path jarPath,
                             JobDescriptor descriptor,
                             Path outputPath,
                             List<QualifiedPath> inputPaths) {
        super(id, nodeID, jarPath, descriptor, outputPath);
        this.inputPaths = inputPaths;
    }

    public ReduceTaskAttempt(ReduceTaskAttempt attempt) {
        super(attempt);
        this.inputPaths = attempt.inputPaths;
    }

    public List<QualifiedPath> getInputPaths() {
        return inputPaths;
    }

}
