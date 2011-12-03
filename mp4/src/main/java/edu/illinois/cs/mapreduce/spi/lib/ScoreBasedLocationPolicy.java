package edu.illinois.cs.mapreduce.spi.lib;

import edu.illinois.cs.mapreduce.spi.LocationPolicy;
import edu.illinois.cs.mapreduce.spi.NodeSelectionPolicy;
import edu.illinois.cs.mr.lb.NodeStatus;

public class ScoreBasedLocationPolicy implements LocationPolicy {

    private final NodeSelectionPolicy sourcePolicy;
    private final NodeSelectionPolicy targetPolicy;

    public ScoreBasedLocationPolicy() {
        this.sourcePolicy = new BusiestNodeSelectPolicy();
        this.targetPolicy = new IdlestNodeSelectionPolicy();
    }

    @Override
    public NodeSelectionPolicy getSourcePolicy() {
        return sourcePolicy;
    }

    @Override
    public NodeSelectionPolicy getTargetPolicy() {
        return targetPolicy;
    }

    /**
     * Gets the node's business score (higher is more busy)
     * 
     * @param ns
     * @return score
     */
    public static double computeNodeScore(NodeStatus ns) {
        double averageScore = (ns.getAvgCpuUtilization() + ns.getThrottle() + 1) * (ns.getAvgQueueLength() + 1);
        double lastScore = (ns.getCpuUtilization() + ns.getThrottle() + 1) * (ns.getQueueLength() + 1);
        return (lastScore + averageScore) / 2;
    }

}
