package edu.illinois.cs.mapreduce;

import java.io.Serializable;

import edu.illinois.cs.mapreduce.Job.JobID;
import edu.illinois.cs.mapreduce.api.Partition;

public class Task implements Serializable {

    private static final long serialVersionUID = -6364601903551472322L;

    public static class TaskID extends ChildID<JobID> {
        private static final long serialVersionUID = -8143814273176351822L;

        public TaskID(JobID jobID, int value) {
            super(jobID, value);
        }
    }

    private final TaskID id;
    private final TaskStatus status;
    private final Partition partition;
    private final Path inputPath;
    private final Path outputPath;
    private final Path jarPath;
    private final JobDescriptor descriptor;

    public Task(TaskID id, Partition partition, Path inputPath, Path outputPath, Path jarPath, JobDescriptor descriptor) {
        this.id = id;
        this.status = new TaskStatus(id);
        this.partition = partition;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.jarPath = jarPath;
        this.descriptor = descriptor;
    }

    public TaskID getId() {
        return id;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public Partition getPartition() {
        return partition;
    }

    public Path getInputPath() {
        return inputPath;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public Path getJarPath() {
        return jarPath;
    }

    public JobDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public String toString() {
        return "Task [id=" + id
            + ", status="
            + status
            + ", partition="
            + partition
            + ", inputPath="
            + inputPath
            + ", outputPath="
            + outputPath
            + ", jarPath="
            + jarPath
            + ", descriptor="
            + descriptor
            + "]";
    }

}
