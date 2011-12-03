package edu.illinois.cs.mapreduce.spi;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.jm.AttemptStatus;
import edu.illinois.cs.mr.jm.JobStatus;

public interface SelectionPolicy {

    public abstract AttemptStatus selectAttempt(NodeID source, NodeID target, Iterable<JobStatus> jobs);

}
