package edu.illinois.cs.mapreduce;

import java.io.Serializable;

import edu.illinois.cs.mapreduce.api.Partition;

/**
 * A TaskAttempt represents a scheduled task. It is needed to distinguish
 * different executions of the same task. This class is thread safe.
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
    private final Path outputPath;
    private final TaskAttemptStatus status;

    public TaskAttempt(TaskAttemptID id,
                       NodeID nodeID,
                       Path jarPath,
                       JobDescriptor descriptor,
                       Partition partition,
                       Path inputPath,
                       Path outputPath) {
        this.nodeID = nodeID;
        this.jarPath = jarPath;
        this.descriptor = descriptor;
        this.partition = partition;
        this.inputPath = inputPath;
        this.id = id;
        this.outputPath = outputPath;
        this.status = new TaskAttemptStatus(id, nodeID);
    }

    public TaskAttempt(TaskAttempt attempt) {
        this.nodeID = attempt.nodeID;
        this.jarPath = attempt.jarPath;
        this.descriptor = attempt.descriptor;
        this.partition = attempt.partition;
        this.inputPath = attempt.inputPath;
        this.id = attempt.id;
        this.outputPath = attempt.outputPath;
        this.status = new TaskAttemptStatus(attempt.status);
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
