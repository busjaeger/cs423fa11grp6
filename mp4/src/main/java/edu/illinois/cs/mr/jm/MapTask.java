package edu.illinois.cs.mr.jm;

import edu.illinois.cs.mapreduce.api.Split;
import edu.illinois.cs.mr.fs.Path;

public class MapTask extends Task {

    private static final long serialVersionUID = 5058256779951559618L;

    // state owned by the task
    private final Path inputPath;
    private Split split;

    public MapTask(TaskID id, Path inputPath) {
        super(id);
        this.inputPath = inputPath;
    }

    public Split getSplit() {
        return split;
    }

    public synchronized void setSplit(Split split) {
        this.split = split;
    }

    public Path getInputPath() {
        return inputPath;
    }

}