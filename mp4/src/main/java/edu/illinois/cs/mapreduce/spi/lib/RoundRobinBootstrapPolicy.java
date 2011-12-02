package edu.illinois.cs.mapreduce.spi.lib;

import java.util.List;
import java.util.Map;

import edu.illinois.cs.mapreduce.spi.BootstrapPolicy;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.NodeStatus;

public class RoundRobinBootstrapPolicy extends BootstrapPolicy {
    @Override
    public NodeID selectNode(int taskNumber, List<NodeID> nodeIds, Map<NodeID, NodeStatus> nodeStatuses) {
        return nodeIds.get(taskNumber % nodeIds.size());
    }
}