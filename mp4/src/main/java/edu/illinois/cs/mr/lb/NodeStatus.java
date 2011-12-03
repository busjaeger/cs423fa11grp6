package edu.illinois.cs.mr.lb;

import java.util.LinkedList;

import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.util.Status;

/**
 * not thread safe
 * 
 * @author benjamin
 */
public class NodeStatus extends Status<NodeID, NodeStatusSnapshot> {

    private static final long serialVersionUID = 7745542951399055113L;

    private static final int NODE_HEALTH_HISTORY = 5;

    private double cpuUtilization;
    private int queueLength;
    private double throttle;
    private int threadCount;
    private int activateThreadCount;

    private double avgCpuUtilization;
    private double avgQueueLength;
    private final LinkedList<NodeStatusSnapshot> snapshots;

    public NodeStatus(NodeID nodeID) {
        super(nodeID);
        this.snapshots = new LinkedList<NodeStatusSnapshot>();
    }

    public double getCpuUtilization() {
        return cpuUtilization;
    }

    public void setCpuUtilization(double cpuUtilization) {
        this.cpuUtilization = cpuUtilization;
    }

    public int getQueueLength() {
        return queueLength;
    }

    public void setQueueLength(int queueLength) {
        this.queueLength = queueLength;
    }

    public double getThrottle() {
        return throttle;
    }

    public void setThrottle(double throttle) {
        this.throttle = throttle;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public int getActivateThreadCount() {
        return activateThreadCount;
    }

    public void setActivateThreadCount(int activateThreadCount) {
        this.activateThreadCount = activateThreadCount;
    }

    public double getAvgCpuUtilization() {
        return avgCpuUtilization;
    }

    public double getAvgQueueLength() {
        return avgQueueLength;
    }

    @Override
    public synchronized NodeStatusSnapshot toImmutableStatus() {
        return new NodeStatusSnapshot(this);
    }

    @Override
    public boolean update(NodeStatusSnapshot snapshot) {
        super.update(snapshot);
        this.cpuUtilization = snapshot.getCpuUtilization();
        this.queueLength = snapshot.getQueueLength();
        this.throttle = snapshot.getThrottle();
        this.activateThreadCount = snapshot.getActivateThreadCount();
        this.threadCount = snapshot.getThreadCount();
        updateAverages(snapshot);
        return true;
    }

    private void updateAverages(NodeStatusSnapshot snapshot) {
        if (snapshots.size() == NODE_HEALTH_HISTORY)
            snapshots.remove();
        snapshots.add(snapshot);
        double totalCpu = 0.0;
        double totalQueue = 0.0;
        for (NodeStatusSnapshot status : snapshots) {
            totalCpu += status.getCpuUtilization();
            totalQueue += status.getQueueLength();
        }
        this.avgCpuUtilization = totalCpu / snapshots.size();
        this.avgQueueLength = totalQueue / snapshots.size();
    }
}
