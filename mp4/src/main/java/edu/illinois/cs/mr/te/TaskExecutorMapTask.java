package edu.illinois.cs.mr.te;

import edu.illinois.cs.mapreduce.api.Split;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.fs.Path;
import edu.illinois.cs.mr.jm.JobDescriptor;
import edu.illinois.cs.mr.jm.AttemptID;

public class TaskExecutorMapTask extends TaskExecutorTask {

    private static final long serialVersionUID = -2891035001799074650L;

    private final Split split;
    private final Path inputPath;

    public TaskExecutorMapTask(AttemptID id,
                               Path jarPath,
                               JobDescriptor descriptor,
                               Path outputPath,
                               NodeID targetNodeID,
                               Split split,
                               Path inputPath) {
        super(id, jarPath, descriptor, outputPath, targetNodeID);
        this.split = split;
        this.inputPath = inputPath;
    }

    @Override
    boolean isMap() {
        return true;
    }

    public Split getSplit() {
        return split;
    }

    public Path getInputPath() {
        return inputPath;
    }

}
