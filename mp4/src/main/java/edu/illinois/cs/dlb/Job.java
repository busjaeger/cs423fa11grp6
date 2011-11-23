package edu.illinois.cs.dlb;

import java.util.ArrayList;
import java.util.List;

public class Job {

    public static class JobID extends ChildID<ID> {
        private static final long serialVersionUID = -648883430189232695L;

        public JobID(ID parentID, int value) {
            super(parentID, value);
        }
    }

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
