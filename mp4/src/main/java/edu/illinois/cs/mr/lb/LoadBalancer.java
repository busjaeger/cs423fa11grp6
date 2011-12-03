package edu.illinois.cs.mr.lb;

import static edu.illinois.cs.mr.util.ReflectionUtil.newInstance;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import edu.illinois.cs.mapreduce.spi.LocationPolicy;
import edu.illinois.cs.mapreduce.spi.NodeSelectionPolicy;
import edu.illinois.cs.mapreduce.spi.SelectionPolicy;
import edu.illinois.cs.mapreduce.spi.TransferPolicy;
import edu.illinois.cs.mr.Node;
import edu.illinois.cs.mr.NodeConfiguration;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.NodeListener;
import edu.illinois.cs.mr.jm.AttemptStatus;
import edu.illinois.cs.mr.jm.JobManager;
import edu.illinois.cs.mr.jm.JobStatus;
import edu.illinois.cs.mr.te.TaskExecutor;
import edu.illinois.cs.mr.util.CpuProfiler;

public class LoadBalancer implements LoadBalancerService, NodeListener {

    private final NodeConfiguration config;

    private final TransferPolicy transferPolicy;
    private final NodeSelectionPolicy bootstrapPolicy;
    private final LocationPolicy locationPolicy;
    private final SelectionPolicy selectionPolicy;
    private final Timer timer;
    private final CpuProfiler cpuProfiler;
    private final Map<NodeID, NodeStatus> nodeStatuses;
    private final NodeStatus nodeStatus;
    private Node node;

    // mutable state
    private volatile boolean transferring;

    public LoadBalancer(NodeConfiguration config) throws IOException {
        this.config = config;
        this.nodeStatus = new NodeStatus(config.nodeId);
        this.timer = new Timer();
        this.cpuProfiler = new CpuProfiler();
        this.transferPolicy = newInstance(config.lbTransferPolicyClass, NodeConfiguration.class, config);
        this.bootstrapPolicy = newInstance(config.lbBootstrapPolicyClass, NodeConfiguration.class, config);
        this.locationPolicy = newInstance(config.lbLocationPolicyClass, NodeConfiguration.class, config);
        this.selectionPolicy = newInstance(config.lbSelectionPolicyClass, NodeConfiguration.class, config);
        this.nodeStatuses = new TreeMap<NodeID, NodeStatus>();
    }

    @Override
    public void start(Node node) {
        this.node = node;
        this.timer.schedule(new StatusUpdateTask(), 0, config.lbStatusUpdateInterval);
        System.out.println("start waiting for cluster nodes to register");
        synchronized (nodeStatuses) {
            while (nodeStatuses.size() < node.getNodeIds().size()) {
                try {
                    nodeStatuses.wait();
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    return;
                }
            }
        }
    }

    @Override
    public void stop() {
        this.timer.cancel();
    }

    public NodeID selectNode() throws InterruptedException {
        synchronized (nodeStatuses) {
            while (nodeStatuses.size() < node.getNodeIds().size()) {
                System.out.println("selectNode waiting for cluster nodes to register");
                try {
                    nodeStatuses.wait();
                } catch (InterruptedException e) {
                    throw e;
                }
            }
            return bootstrapPolicy.selectNode(nodeStatuses.values());
        }
    }

    @Override
    public boolean updateStatus(NodeStatusSnapshot snapshot) throws IOException {
        synchronized (nodeStatuses) {
            NodeID nodeId = snapshot.getId();
            NodeStatus ns = nodeStatuses.get(nodeId);
            if (ns == null) {
                nodeStatuses.put(nodeId, ns = new NodeStatus(nodeId));
                if (nodeStatuses.size() == node.getNodeIds().size())
                    nodeStatuses.notifyAll();
            }
            ns.update(snapshot);
            final NodeStatus nodeStatus = ns;
            if (!transferring && nodeStatuses.size() > 1
                && transferPolicy.isTransferNeeded(nodeStatus, nodeStatuses.values())) {
                transferring = true;
                node.getExecutorService().submit(new Runnable() {
                    @Override
                    public void run() {
                        rebalance(nodeStatus);
                    }
                });
            }
        }
        return false;
    }

    private void rebalance(NodeStatus nodeStatus) {
        try {
            NodeID source;
            NodeID target;
            synchronized (nodeStatuses) {
                source = locationPolicy.getSourcePolicy().selectNode(nodeStatuses.values());
                target = locationPolicy.getTargetPolicy().selectNode(nodeStatuses.values());
            }
            if (!source.equals(config.nodeId))
                return;
            if (source == target) {
                System.err.println("Same source and target selected");
                return;
            }

            JobManager jobManager = node.getJobManager();
            Iterable<JobStatus> jobs = jobManager.getJobStatuses();
            AttemptStatus attempt = selectionPolicy.selectAttempt(source, target, jobs);
            if (attempt == null) {
                System.out.println("No suitable task found to transfer from " + source + " to " + target);
                return;
            }
            jobManager.migrateTask(target, attempt);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            transferring = false;
        }
    }

    private class StatusUpdateTask extends TimerTask {
        @Override
        public void run() {
            try {
                NodeStatusSnapshot snapshot;
                TaskExecutor taskExecutor = node.getTaskExecutor();
                synchronized (nodeStatus) {
                    nodeStatus.setCpuUtilization(cpuProfiler.readUtilization());
                    nodeStatus.setQueueLength(taskExecutor.getQueueLength());
                    nodeStatus.setThrottle(taskExecutor.getThrottle());
                    nodeStatus.setThreadCount(taskExecutor.getNumThreads());
                    nodeStatus.setActivateThreadCount(taskExecutor.getNumActiveThreads());
                    snapshot = nodeStatus.toImmutableStatus();
                }

                for (NodeID nodeID : node.getNodeIds()) {
                    LoadBalancerService loadBalancer = node.getLoadBalancerService(nodeID);
                    try {
                        loadBalancer.updateStatus(snapshot);
                    } catch (ConnectException e) {
                        System.out.println("Node " + nodeID + " unreachable");
                    }
                }
            } catch (Throwable t) {
                System.out.println("node " + config.nodeId + " failed to update status");
                t.printStackTrace();
            }
        }
    }

}
