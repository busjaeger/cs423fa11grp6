package edu.illinois.cs.mapreduce.spi.lib;

import java.util.Collection;

import edu.illinois.cs.mapreduce.spi.NodeSelectionPolicy;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.lb.NodeStatus;

public class IdlestNodeSelectionPolicy implements NodeSelectionPolicy {

    @Override
    public NodeID selectNode(Collection<NodeStatus> nodeStatuses) {
        NodeID idlest = null;
        double idlestScore = 1000.0;
        for (NodeStatus nodeStatus : nodeStatuses) {
            if (idlest == null) {
                idlest = nodeStatus.getId();
                idlestScore = ScoreBasedLocationPolicy.computeNodeScore(nodeStatus);
            } else {
                double myScore = ScoreBasedLocationPolicy.computeNodeScore(nodeStatus);
                if (myScore < idlestScore) {
                    idlest = nodeStatus.getId();
                    idlestScore = myScore;
                }
            }
        }
        return idlest;
    }

}
