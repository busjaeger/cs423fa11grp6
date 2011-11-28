package edu.illinois.cs.mapreduce;

import java.util.ArrayList;
import java.util.List;

/**
 * thread safe
 * 
 * @author benjamin
 */
public class JobStatus extends Status<JobID> {

    private static final long serialVersionUID = 387393472618054102L;

    public static enum Phase {
        MAP, REDUCE
    }

    private Phase phase;
    private final List<TaskStatus> tasks;

    public JobStatus(JobID id) {
        super(id);
        this.phase = Phase.MAP;
        this.tasks = new ArrayList<TaskStatus>();
    }

    // must be called with lock held
    public JobStatus(JobStatus jobStatus) {
        super(jobStatus);
        this.phase = jobStatus.getPhase();
        this.tasks = jobStatus.getTaskStatuses();
        for (int i = 0; i < tasks.size(); i++)
            tasks.set(i, new TaskStatus(tasks.get(i)));
    }

    public synchronized Phase getPhase() {
        return phase;
    }

    synchronized void setPhase(Phase phase) {
        this.phase = phase;
    }

    public synchronized List<TaskStatus> getTaskStatuses() {
        return new ArrayList<TaskStatus>(tasks);
    }

    synchronized void addTaskStatus(TaskStatus taskStatus) {
        tasks.add(taskStatus);
    }

    @Override
    public String toString() {
        return "JobStatus [phase=" + phase + ", tasks=" + tasks + "]";
    }

}
