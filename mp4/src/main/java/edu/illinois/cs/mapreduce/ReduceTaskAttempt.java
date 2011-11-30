package edu.illinois.cs.mapreduce;

import java.util.List;

public class ReduceTaskAttempt extends TaskAttempt {

    private static final long serialVersionUID = 2711392377038986636L;

    private final List<QualifiedPath> inputPaths;

    public ReduceTaskAttempt(TaskAttemptID id,
                             NodeID targetNodeID,
                             Path jarPath,
                             JobDescriptor descriptor,
                             Path outputPath,
                             List<QualifiedPath> inputPaths) {
        super(id, targetNodeID, jarPath, descriptor, outputPath);
        this.inputPaths = inputPaths;
    }

    public List<QualifiedPath> getInputPaths() {
        return inputPaths;
    }

    synchronized ReduceTaskAttempt copy() {
        ReduceTaskAttempt attempt = new ReduceTaskAttempt(id, targetNodeID, jarPath, descriptor, outputPath, inputPaths);
        attempt.state = state;
        attempt.message = message;
        return attempt;
    }
}
