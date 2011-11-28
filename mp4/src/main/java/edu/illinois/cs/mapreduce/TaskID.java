package edu.illinois.cs.mapreduce;

public class TaskID extends ChildID<JobID, TaskID> {
    private static final long serialVersionUID = -8143814273176351822L;

    public TaskID(JobID jobID, int value) {
        super(jobID, value);
    }

    @Override
    public String toString() {
        return "task" + super.toString();
    }
}