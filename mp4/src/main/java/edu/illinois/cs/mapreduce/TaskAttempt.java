package edu.illinois.cs.mapreduce;

import java.io.Serializable;

import edu.illinois.cs.mapreduce.api.Partition;

/**
 * A TaskAttempt represents a scheduled task. It is needed to distinguish
 * different executions of the same task.
 * 
 * @author benjamin
 */
public class TaskAttempt implements Serializable {

    private static final long serialVersionUID = -1954349780451419520L;

    // the ID of the node on which the task is attempted
    private final NodeID nodeID;
    // state owned by job
    private final Path jarPath;
    private final JobDescriptor descriptor;
    // state owned by the task
    private final Partition partition;
    private final Path inputPath;

    // state owned by task attempt
    private final TaskAttemptID id;
    private final TaskAttemptStatus status;
    private final Path outputPath;

    public TaskAttempt(TaskAttemptID id,
                       NodeID nodeID,
                       Path jarPath,
                       JobDescriptor descriptor,
                       Partition partition,
                       Path inputPath,
                       Path outputPath) {
        this.status = new TaskAttemptStatus(id);
        this.id = id;
        this.jarPath = jarPath;
        this.descriptor = descriptor;
        this.partition = partition;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.nodeID = nodeID;
    }

    public NodeID getNodeID() {
        return nodeID;
    }

    public Path getJarPath() {
        return jarPath;
    }

    public JobDescriptor getDescriptor() {
        return descriptor;
    }

    public Partition getPartition() {
        return partition;
    }

    public Path getInputPath() {
        return inputPath;
    }

    public TaskAttemptID getId() {
        return id;
    }

    public TaskAttemptStatus getStatus() {
        return status;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    @Override
    public String toString() {
        return "TaskAttempt [nodeID=" + nodeID
            + ", jarPath="
            + jarPath
            + ", descriptor="
            + descriptor
            + ", partition="
            + partition
            + ", inputPath="
            + inputPath
            + ", id="
            + id
            + ", status="
            + status
            + ", outputPath="
            + outputPath
            + "]";
    }

}
