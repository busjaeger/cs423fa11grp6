package edu.illinois.cs.mapreduce;

import edu.illinois.cs.mapreduce.Job.Phase;
import edu.illinois.cs.mapreduce.Status.State;

/**
 * thread safe
 * 
 * @author benjamin
 */
public class JobStatus extends ImmutableStatus<JobID> {

    private static final long serialVersionUID = 387393472618054102L;

    private final Phase phase;
    private final Iterable<TaskStatus> mapTaskStatuses;
    private final Iterable<TaskStatus> reduceTaskStatuses;

    public JobStatus(JobID id,
                     State state,
                     Phase phase,
                     Iterable<TaskStatus> mapTaskStatuses,
                     Iterable<TaskStatus> reduceTaskStatuses) {
        super(id, state);
        this.phase = phase;
        this.mapTaskStatuses = mapTaskStatuses;
        this.reduceTaskStatuses = reduceTaskStatuses;
    }

    public Phase getPhase() {
        return phase;
    }

    public Iterable<TaskStatus> getMapTaskStatuses() {
        return mapTaskStatuses;
    }

    public Iterable<TaskStatus> getReduceTaskStatuses() {
        return reduceTaskStatuses;
    }
}
