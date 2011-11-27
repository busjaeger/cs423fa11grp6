package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A Job represents a unit of work submitted by the user. The framework splits
 * the work into a set of tasks that can be executed independently on different
 * nodes in the cluster.
 * 
 * @author benjamin
 */
public class Job implements Serializable {

    private static final long serialVersionUID = 5073561871061802007L;

    private final JobID id;
    private final Path path;
    private final JobStatus status;
    private final Path jar;
    private final List<Task> mapTasks;

    public Job(JobID id, String jarName) {
        this.id = id;
        this.path = new Path(id.toQualifiedString());
        this.jar = path.append(jarName);
        this.status = new JobStatus();
        this.mapTasks = new ArrayList<Task>();
    }

    public JobID getId() {
        return id;
    }

    public Path getPath() {
        return path;
    }

    public JobStatus getStatus() {
        return status;
    }

    public Path getJarPath() {
        return jar;
    }

    public List<Task> getMapTasks() {
        return mapTasks;
    }

}
