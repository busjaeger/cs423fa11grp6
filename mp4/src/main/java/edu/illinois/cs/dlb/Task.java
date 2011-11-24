package edu.illinois.cs.dlb;

import java.io.Serializable;

import edu.illinois.cs.dlb.Job.JobID;
import edu.illinois.cs.dlb.util.ChildID;
import edu.illinois.cs.dlb.util.Path;

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
    private final boolean remote;
    private final Path inputFile;
    private final Path outputFile;
    private final Path jar;
    private final JobDescriptor descriptor;

    public Task(TaskID id, boolean remote, Path inputFile, Path outputFile, Path jar, JobDescriptor descriptor) {
        this.id = id;
        this.status = new TaskStatus(id);
        this.remote = remote;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.jar = jar;
        this.descriptor = descriptor;
    }

    public TaskID getId() {
        return id;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public boolean isRemote() {
        return remote;
    }

    public Path getInputFile() {
        return inputFile;
    }

    public Path getOutputFile() {
        return outputFile;
    }

    public Path getJar() {
        return jar;
    }

    public JobDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public String toString() {
        return "Task [id=" + id + "]";
    }

}
