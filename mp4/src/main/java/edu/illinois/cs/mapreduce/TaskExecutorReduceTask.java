package edu.illinois.cs.mapreduce;

import java.util.List;

public class TaskExecutorReduceTask extends TaskExecutorTask {

    private static final long serialVersionUID = -5842321755763808777L;

    private final List<QualifiedPath> inputPaths;

    public TaskExecutorReduceTask(TaskAttemptID id,
                                  Path jarPath,
                                  JobDescriptor descriptor,
                                  Path outputPath,
                                  NodeID targetNodeID,
                                  List<QualifiedPath> inputPaths) {
        super(id, jarPath, descriptor, outputPath, targetNodeID);
        this.inputPaths = inputPaths;
    }

    @Override
    boolean isMap() {
        return false;
    }

    public List<QualifiedPath> getInputPaths() {
        return inputPaths;
    }

}
