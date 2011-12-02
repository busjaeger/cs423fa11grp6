package edu.illinois.cs.mapreduce.spi;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.jm.Job;
import edu.illinois.cs.mr.jm.TaskAttempt;

public abstract class SelectionPolicy {

    public abstract TaskAttempt selectAttempt(NodeID nodeID, Iterable<Job> jobs);

}
