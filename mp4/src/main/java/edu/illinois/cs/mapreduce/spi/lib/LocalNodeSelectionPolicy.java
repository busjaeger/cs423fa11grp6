package edu.illinois.cs.mapreduce.spi.lib;

import java.util.Collection;

import edu.illinois.cs.mapreduce.spi.NodeSelectionPolicy;
import edu.illinois.cs.mr.NodeConfiguration;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.lb.NodeStatus;

public class LocalNodeSelectionPolicy implements NodeSelectionPolicy {
    private final NodeID localID;

    public LocalNodeSelectionPolicy(NodeConfiguration nodeConfig) {
        this.localID = nodeConfig.nodeId;
    }

    @Override
    public NodeID selectNode(Collection<NodeStatus> nodeStatuses) {
        return localID;
    }
}
