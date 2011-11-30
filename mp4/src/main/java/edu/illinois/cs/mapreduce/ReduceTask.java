package edu.illinois.cs.mapreduce;

import java.util.List;

public class ReduceTask extends Task {

    private static final long serialVersionUID = 2711392377038986636L;

    private final List<QualifiedPath> inputPaths;

    public ReduceTask(TaskID id, List<QualifiedPath> inputPaths) {
        super(id);
        this.inputPaths = inputPaths;
    }

    public List<QualifiedPath> getInputPaths() {
        return inputPaths;
    }

}
