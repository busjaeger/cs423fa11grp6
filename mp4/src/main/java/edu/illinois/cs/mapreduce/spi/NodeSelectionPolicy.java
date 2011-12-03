package edu.illinois.cs.mapreduce.spi;

import java.util.Collection;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.lb.NodeStatus;

public interface NodeSelectionPolicy {

    NodeID selectNode(Collection<NodeStatus> nodeStatuses);

}
