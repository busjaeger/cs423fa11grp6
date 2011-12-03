package edu.illinois.cs.mapreduce.spi.lib;

import java.util.Collection;

import edu.illinois.cs.mapreduce.spi.NodeSelectionPolicy;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.lb.NodeStatus;

public class BusiestNodeSelectPolicy implements NodeSelectionPolicy {

    @Override
    public NodeID selectNode(Collection<NodeStatus> nodeStatuses) {
        NodeID busiest = null;
        double busiestScore = 0.0;
        for (NodeStatus nodeStatus : nodeStatuses) {
            if (busiest == null) {
                // This will always be the
                // case on 2 node clusters
                busiest = nodeStatus.getId();
                busiestScore = ScoreBasedLocationPolicy.computeNodeScore(nodeStatus);
            } else {
                double myScore = ScoreBasedLocationPolicy.computeNodeScore(nodeStatus);
                if (myScore > busiestScore) {
                    busiest = nodeStatus.getId();
                    busiestScore = myScore;
                }
            }
        }
        return busiest;
    }

}
