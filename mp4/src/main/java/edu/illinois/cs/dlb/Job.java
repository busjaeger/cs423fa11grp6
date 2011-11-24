package edu.illinois.cs.dlb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.illinois.cs.dlb.util.ChildID;
import edu.illinois.cs.dlb.util.ID;
import edu.illinois.cs.dlb.util.Path;

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
    private final JarDescriptor descriptor;
    private final List<Task> tasks;

    public Job(JobID id, Path jar, JarDescriptor descriptor) {
        this.id = id;
        this.jar = jar;
        this.descriptor = descriptor;
        this.status = new JobStatus();
        this.tasks = new ArrayList<Task>();
    }

    public JobID getId() {
        return id;
    }

    public JobStatus getStatus() {
        return status;
    }

    public Path getJar() {
        return jar;
    }

    public JarDescriptor getDescriptor() {
        return descriptor;
    }

    public List<Task> getTasks() {
        return tasks;
    }

}
