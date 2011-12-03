package edu.illinois.cs.mr.lb;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.util.ImmutableStatus;

public class NodeStatusSnapshot extends ImmutableStatus<NodeID> {

    private static final long serialVersionUID = -4565075909978463681L;

    private double cpuUtilization;
    private int queueLength;
    private double throttle;
    private int threadCount;
    private int activateThreadCount;

    public NodeStatusSnapshot(NodeStatus nodeStatus) {
        super(nodeStatus);
        this.cpuUtilization = nodeStatus.getCpuUtilization();
        this.queueLength = nodeStatus.getQueueLength();
        this.throttle = nodeStatus.getThrottle();
        this.threadCount = nodeStatus.getThreadCount();
        this.activateThreadCount = nodeStatus.getActivateThreadCount();
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

    public int getThreadCount() {
        return threadCount;
    }

    public int getActivateThreadCount() {
        return activateThreadCount;
    }

}
