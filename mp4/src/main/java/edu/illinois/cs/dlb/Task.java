package edu.illinois.cs.dlb;

import edu.illinois.cs.dlb.Job.JobID;

public class Task {

    public static class TaskID extends ChildID<JobID> {
        private static final long serialVersionUID = -8143814273176351822L;

        public TaskID(JobID jobID, int value) {
            super(jobID, value);
        }
    }

    private final TaskID id;
    private final Path inputSplit;
    private final Path outputSplit;
    private final Path jobFile;
    private final JobDescriptor jobDescriptor;

    public Task(TaskID id, Path inputSplit, Path outputSplit, Path jobFile, JobDescriptor jobDescriptor) {
        this.id = id;
        this.inputSplit = inputSplit;
        this.outputSplit = outputSplit;
        this.jobFile = jobFile;
        this.jobDescriptor = jobDescriptor;
    }

    public TaskID getId() {
        return id;
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

}
