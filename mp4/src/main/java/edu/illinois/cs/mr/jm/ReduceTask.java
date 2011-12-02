package edu.illinois.cs.mr.jm;

import java.util.List;

import edu.illinois.cs.mr.fs.QualifiedPath;

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
