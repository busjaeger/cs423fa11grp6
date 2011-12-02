package edu.illinois.cs.mapreduce.spi;

import java.util.List;
import java.util.Map;

import edu.illinois.cs.mapreduce.NodeConfiguration;
import edu.illinois.cs.mapreduce.NodeID;
import edu.illinois.cs.mapreduce.NodeStatus;

public interface BootstrapPolicy {

    NodeID selectNode(int taskNumber, List<NodeID> nodeIds, Map<NodeID, NodeStatus> nodeStatuses);

    public static class RoundRobinBootstrapPolicy implements BootstrapPolicy {
        @Override
        public NodeID selectNode(int taskNumber, List<NodeID> nodeIds, Map<NodeID, NodeStatus> nodeStatuses) {
            return nodeIds.get(taskNumber % nodeIds.size());
        }
    }

    public static class LocalBootstrapPolicy implements BootstrapPolicy {
        private final NodeID localID;

        public LocalBootstrapPolicy(NodeConfiguration nodeConfig) {
            this.localID = nodeConfig.nodeId;
        }

        @Override
        public NodeID selectNode(int taskNumber, List<NodeID> nodeIds, Map<NodeID, NodeStatus> nodeStatuses) {
            return localID;
        }
    }
}
