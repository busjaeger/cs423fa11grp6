package edu.illinois.cs.mapreduce;

import edu.illinois.cs.mapreduce.Job.Phase;

/**
 * thread safe
 * 
 * @author benjamin
 */
public class JobStatus extends ImmutableStatus<JobID> {

    private static final long serialVersionUID = 387393472618054102L;

    private final Phase phase;
    private final ImmutableStatus<JobID> mapStatus;
    private final Iterable<TaskStatus> mapTaskStatuses;
    private final Iterable<TaskStatus> reduceTaskStatuses;

    public JobStatus(Job job) {
        super(job);
        this.phase = job.getPhase();
        this.mapStatus = job.getMapStatus() == null ? null : job.getMapStatus().toImmutableStatus();
        this.mapTaskStatuses = toImmutableStatuses(job.getMapTasks());
        this.reduceTaskStatuses = toImmutableStatuses(job.getReduceTasks());
    }

    public Phase getPhase() {
        return phase;
    }

    public ImmutableStatus<JobID> getMapStatus() {
        return mapStatus;
    }

    public Iterable<TaskStatus> getMapTaskStatuses() {
        return mapTaskStatuses;
    }

    public Iterable<TaskStatus> getReduceTaskStatuses() {
        return reduceTaskStatuses;
    }
}
