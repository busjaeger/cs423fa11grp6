package edu.illinois.cs.mapreduce.spi;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.NodeStatus;

public abstract class LocationPolicy {

    public abstract NodeID source(Iterable<NodeStatus> nodeStatuses);

    public abstract NodeID target(Iterable<NodeStatus> nodeStatuses);

}
