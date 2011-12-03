package edu.illinois.cs.mr.te;

import java.util.List;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.fs.Path;
import edu.illinois.cs.mr.fs.QualifiedPath;
import edu.illinois.cs.mr.jm.JobDescriptor;
import edu.illinois.cs.mr.jm.AttemptID;

public class TaskExecutorReduceTask extends TaskExecutorTask {

    private static final long serialVersionUID = -5842321755763808777L;

    private final List<QualifiedPath> inputPaths;

    public TaskExecutorReduceTask(AttemptID id,
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
