package edu.illinois.cs.dlb;

import java.io.Serializable;

import edu.illinois.cs.dlb.Job.JobID;

public class Task implements Serializable {

    private static final long serialVersionUID = -6364601903551472322L;

    public static class TaskID extends ChildID<JobID> {
        private static final long serialVersionUID = -8143814273176351822L;

        public TaskID(JobID jobID, int value) {
            super(jobID, value);
        }
    }

    private final TaskID id;
    private final TaskStatus taskStatus;
    private final boolean remote;
    private final Path inputSplit;
    private final Path outputSplit;
    private final Path jobFile;
    private final JobDescriptor jobDescriptor;

    public Task(TaskID id, boolean remote, Path inputSplit, Path outputSplit, Path jobFile, JobDescriptor jobDescriptor) {
        this.id = id;
        this.taskStatus = new TaskStatus(id);
        this.remote = remote;
        this.inputSplit = inputSplit;
        this.outputSplit = outputSplit;
        this.jobFile = jobFile;
        this.jobDescriptor = jobDescriptor;
    }

    public TaskID getId() {
        return id;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public boolean isRemote() {
        return remote;
    }

    public Path getInputSplit() {
        return inputSplit;
    }

    public Path getOutputSplit() {
        return outputSplit;
    }

    public Path getJobFile() {
        return jobFile;
    }

    public JobDescriptor getJobDescriptor() {
        return jobDescriptor;
    }

    @Override
    public String toString() {
        return "Task [id=" + id + "]";
    }

}
