package edu.illinois.cs.mapreduce.spi;

import java.util.List;
import java.util.Map;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.NodeStatus;

public abstract class BootstrapPolicy {

    public abstract NodeID selectNode(int taskNumber, List<NodeID> nodeIds, Map<NodeID, NodeStatus> nodeStatuses);

}
