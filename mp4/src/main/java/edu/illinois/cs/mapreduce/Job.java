package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Job implements Serializable {

    private static final long serialVersionUID = 5073561871061802007L;

    public static class JobID extends ChildID<ID> {
        private static final long serialVersionUID = -648883430189232695L;

        public JobID(ID jobMgrId, int value) {
            super(jobMgrId, value);
        }
    }

    private final JobID id;
    private final JobStatus status;
    private final Path jar;
    private final List<Task> mapTasks;

    public Job(JobID id, Path jar) {
        this.id = id;
        this.jar = jar;
        this.status = new JobStatus();
        this.mapTasks = new ArrayList<Task>();
    }

    public JobID getId() {
        return id;
    }

    public Path getPath() {
        return new Path(id);
    }

    public JobStatus getStatus() {
        return status;
    }

    public Path getJar() {
        return jar;
    }

    public List<Task> getMapTasks() {
        return mapTasks;
    }

}
