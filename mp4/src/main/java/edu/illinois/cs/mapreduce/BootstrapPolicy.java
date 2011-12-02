package edu.illinois.cs.mapreduce;

import java.util.List;
import java.util.Map;

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
