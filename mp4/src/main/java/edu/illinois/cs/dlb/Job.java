package edu.illinois.cs.dlb;

import java.util.ArrayList;
import java.util.List;

import edu.illinois.cs.dlb.api.JobID;

public class Job {

    private final JobID id;
    private final List<Task> tasks;

    public Job(JobID id) {
        this.id = id;
        this.tasks = new ArrayList<Task>();
    }

    public JobID getId() {
        return id;
    }

    public List<Task> getTasks() {
        return tasks;
    }
}
