package edu.illinois.cs.mr;

import java.util.LinkedList;

import edu.illinois.cs.mr.te.TaskExecutorStatus;

/**
 * not thread safe
 * 
 * @author benjamin
 */
public class NodeStatus {

    private static final int NODE_HEALTH_HISTORY = 5;

    private final NodeID nodeID;
    private double lastCpuUtilization;
    private int lastQueueLength;
    private double avgCpuUtilization;
    private double avgQueueLength;
    private double throttle;
    private final LinkedList<TaskExecutorStatus> statuses;

    public NodeStatus(TaskExecutorStatus status) {
        this.nodeID = status.getNodeID();
        this.lastCpuUtilization = status.getCpuUtilization();
        this.lastQueueLength = status.getQueueLength();
        this.throttle = status.getThrottle();
        this.avgCpuUtilization = this.lastCpuUtilization;
        this.avgQueueLength = this.lastQueueLength;
        this.statuses = new LinkedList<TaskExecutorStatus>();
        this.statuses.add(status);
    }

    public NodeID getNodeID() {
        return nodeID;
    }

    public double getCpuUtilization() {
        return lastCpuUtilization;
    }

    public int getQueueLength() {
        return lastQueueLength;
    }

    public double getThrottle() {
        return throttle;
    }

    public double getAvgCpuUtilization() {
        return avgCpuUtilization;
    }

    public double getAvgQueueLength() {
        return avgQueueLength;
    }

    public void update(TaskExecutorStatus status) {
        this.lastCpuUtilization = status.getCpuUtilization();
        this.lastQueueLength = status.getQueueLength();
        this.throttle = status.getThrottle();
        if (statuses.size() == NODE_HEALTH_HISTORY)
            statuses.remove();
        statuses.add(status);
        updateAverages();
    }

    private void updateAverages() {
        double totalCpu = 0.0;
        double totalQueue = 0.0;
        for (TaskExecutorStatus status : statuses) {
            totalCpu += status.getCpuUtilization();
            totalQueue += status.getQueueLength();
        }
        this.avgCpuUtilization = totalCpu / statuses.size();
        this.avgQueueLength = totalQueue / statuses.size();
    }
}
