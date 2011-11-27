package edu.illinois.cs.mapreduce;

import java.util.List;
import java.util.Map;

public class Cluster {

    private final List<NodeID> nodeIds;
    private final Map<NodeID, FileSystemService> fileSystems;
    private final Map<NodeID, TaskExecutorService> taskExecutors;
    private final Map<NodeID, JobManagerService> jobManagers;

    public Cluster(List<NodeID> nodeIds,
                   Map<NodeID, FileSystemService> fileSystems,
                   Map<NodeID, TaskExecutorService> taskExecutors,
                   Map<NodeID, JobManagerService> jobManagers) {
        this.nodeIds = nodeIds;
        this.fileSystems = fileSystems;
        this.taskExecutors = taskExecutors;
        this.jobManagers = jobManagers;
    }

    public List<NodeID> getNodeIds() {
        return nodeIds;
    }

    public JobManagerService getJobManagerService(NodeID nodeId) {
        return jobManagers.get(nodeId);
    }

    public TaskExecutorService getTaskExecutorService(NodeID nodeId) {
        return taskExecutors.get(nodeId);
    }

    public FileSystemService getFileSystemService(NodeID nodeId) {
        return fileSystems.get(nodeId);
    }

}
