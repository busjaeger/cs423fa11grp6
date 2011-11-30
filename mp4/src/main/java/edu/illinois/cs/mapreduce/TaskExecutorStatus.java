package edu.illinois.cs.mapreduce;

import java.io.Serializable;

public class TaskExecutorStatus implements Serializable {

    private static final long serialVersionUID = -3981153742009125690L;

    private final NodeID nodeID;
    private final double cpuUtilization;
    private final int queueLength;
    private final double throttle;

    public TaskExecutorStatus(NodeID nodeID, double cpuUtil, int queueLength, double throttle) {
        this.nodeID = nodeID;
        this.cpuUtilization = cpuUtil;
        this.queueLength = queueLength;
        this.throttle = throttle;
    }

    public NodeID getNodeID() {
        return nodeID;
    }

    public double getCpuUtilization() {
        return cpuUtilization;
    }

    public int getQueueLength() {
        return queueLength;
    }

    public double getThrottle() {
        return throttle;
    }

}
