package edu.illinois.cs.mapreduce.spi;

import edu.illinois.cs.mapreduce.NodeID;
import edu.illinois.cs.mapreduce.NodeStatus;


public interface LocationPolicy {

    NodeID source(Iterable<NodeStatus> nodeStatuses);

    NodeID target(Iterable<NodeStatus> nodeStatuses);

    public static class ScoreBasedLocationPolicy implements LocationPolicy {

        /**
         * Searches through list of busy nodes to find the busiest node
         * 
         * @param list of busy nodes' statuses
         * @return NodeID of busiest node
         */
        @Override
        public NodeID source(Iterable<NodeStatus> nodeStatuses) {
            NodeID busiest = null;
            double busiestScore = 0.0;
            for (NodeStatus nodeStatus : nodeStatuses) {
                if (busiest == null) {
                    // This will always be the
                    // case on 2 node clusters
                    busiest = nodeStatus.getNodeID();
                    busiestScore = computeNodeScore(nodeStatus);
                } else {
                    double myScore = computeNodeScore(nodeStatus);
                    if (myScore > busiestScore) {
                        busiest = nodeStatus.getNodeID();
                        busiestScore = myScore;
                    }
                }
            }
            return busiest;
        }

        /**
         * Searches through list of idle nodes to find the least busy node
         * 
         * @param list of idle nodes' statuses
         * @return NodeID of least busy node
         */
        @Override
        public NodeID target(Iterable<NodeStatus> nodeStatuses) {
            NodeID idlest = null;
            double idlestScore = 1000.0;
            for (NodeStatus nodeStatus : nodeStatuses) {
                if (idlest == null) {
                    idlest = nodeStatus.getNodeID();
                    idlestScore = computeNodeScore(nodeStatus);
                } else {
                    double myScore = computeNodeScore(nodeStatus);
                    if (myScore < idlestScore) {
                        idlest = nodeStatus.getNodeID();
                        idlestScore = myScore;
                    }
                }
            }
            return idlest;
        }

        /**
         * Gets the node's business score (higher is more busy)
         * 
         * @param ns
         * @return score
         */
        private double computeNodeScore(NodeStatus ns) {
            double averageScore = (ns.getAvgCpuUtilization() + ns.getThrottle() + 1) * (ns.getAvgQueueLength() + 1);
            double lastScore = (ns.getCpuUtilization() + ns.getThrottle() + 1) * (ns.getQueueLength() + 1);
            double score = (lastScore + averageScore) / 2;
            return score;
        }
    }

}
