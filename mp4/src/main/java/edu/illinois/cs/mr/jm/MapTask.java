package edu.illinois.cs.mr.jm;

import edu.illinois.cs.mapreduce.api.Partition;
import edu.illinois.cs.mr.fs.Path;

public class MapTask extends Task {

    private static final long serialVersionUID = 5058256779951559618L;

    // state owned by the task
    private final Partition partition;
    private final Path inputPath;

    public MapTask(TaskID id, Partition partition, Path inputPath) {
        super(id);
        this.partition = partition;
        this.inputPath = inputPath;
    }

    public Partition getPartition() {
        return partition;
    }

    public Path getInputPath() {
        return inputPath;
    }

}