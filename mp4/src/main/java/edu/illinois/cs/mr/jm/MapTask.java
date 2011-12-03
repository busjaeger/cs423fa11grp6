package edu.illinois.cs.mr.jm;

import edu.illinois.cs.mapreduce.api.Split;
import edu.illinois.cs.mr.fs.Path;

public class MapTask extends Task {

    private static final long serialVersionUID = 5058256779951559618L;

    // state owned by the task
    private final Split split;
    private final Path inputPath;

    public MapTask(TaskID id, Split split, Path inputPath) {
        super(id);
        this.split = split;
        this.inputPath = inputPath;
    }

    public Split getSplit() {
        return split;
    }

    public Path getInputPath() {
        return inputPath;
    }

}