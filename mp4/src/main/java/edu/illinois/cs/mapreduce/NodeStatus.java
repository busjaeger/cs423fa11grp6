package edu.illinois.cs.mapreduce;

public class NodeStatus {
    private final NodeID nodeID;
    private final double lastCpuUtilization;
    private final int lastQueueLength;
    private final double avgCpuUtilization;
    private final double avgQueueLength;
    private final double throttle;

    public NodeStatus(NodeID nodeID, double cpuUtil, int queueLength, 
                      double avgCpu, double avgQueue, double throttle) {
        this.nodeID = nodeID;
        this.lastCpuUtilization = cpuUtil;
        this.lastQueueLength = queueLength;
        this.avgCpuUtilization = avgCpu;
        this.avgQueueLength = avgQueue;
        this.throttle = throttle;
    }

    public NodeID getNodeID() {
        return nodeID;
    }

    public double getLastCpuUtilization() {
        return lastCpuUtilization;
    }

    public int getLastQueueLength() {
        return lastQueueLength;
    }

    public double getAvgCpuUtilization() {
        return avgCpuUtilization;
    }

    public double getAvgQueueLength() {
        return avgQueueLength;
    }
    
    public double getThrottle() {
        return throttle;
    }
    
    public boolean isIdle() {
        if(lastQueueLength == 0)
            return true;
        else
            return false;
    }
}
