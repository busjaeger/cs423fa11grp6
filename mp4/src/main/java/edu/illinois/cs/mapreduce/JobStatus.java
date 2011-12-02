package edu.illinois.cs.mapreduce;

/**
 * thread safe
 * 
 * @author benjamin
 */
public class JobStatus extends ImmutablePhasedStatus<JobID, Phase> {

    private static final long serialVersionUID = 387393472618054102L;

    private final Iterable<TaskStatus> mapTaskStatuses;
    private final Iterable<TaskStatus> reduceTaskStatuses;

    public JobStatus(Job job) {
        super(job);
        this.mapTaskStatuses = toImmutableStatuses(job.getMapTasks());
        this.reduceTaskStatuses = toImmutableStatuses(job.getReduceTasks());
    }

    public Iterable<TaskStatus> getTaskStatuses(Phase phase) {
        switch (phase) {
            case MAP:
                return mapTaskStatuses;
            case REDUCE:
                return reduceTaskStatuses;
            default:
                throw new RuntimeException("unhandeled case");
        }
    }

}
