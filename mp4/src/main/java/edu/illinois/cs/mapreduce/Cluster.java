package edu.illinois.cs.mapreduce;

import java.util.Collection;
import java.util.Map;

import edu.illinois.cs.mapreduce.Node.NodeServices;

public class Cluster {

    private final NodeConfiguration config;
    private final JobManager jobManager;
    private final TaskExecutor taskExecutor;
    private final FileSystem fileSystem;
    private final Map<NodeID, NodeServices> nodeMap;

    public Cluster(NodeConfiguration config,
                   JobManager jobManager,
                   TaskExecutor taskExecutor,
                   FileSystem fileSystem,
                   Map<NodeID, NodeServices> nodeMap) {
        this.config = config;
        this.jobManager = jobManager;
        this.taskExecutor = taskExecutor;
        this.fileSystem = fileSystem;
        this.nodeMap = nodeMap;
    }

    public Collection<NodeID> getNodeIds() {
        return nodeMap.keySet();
    }

    public JobManagerService getJobManagerService(NodeID nodeId) {
        if (this.config.nodeId.equals(nodeId))
            return jobManager;
        return nodeMap.get(nodeId);
    }

    public TaskExecutorService getTaskExecutorService(NodeID nodeId) {
        if (this.config.nodeId.equals(nodeId))
            return taskExecutor;
        return nodeMap.get(nodeId);
    }

    public FileSystemService getFileSystemService(NodeID nodeId) {
        if (this.config.nodeId.equals(nodeId))
            return fileSystem;
        return nodeMap.get(nodeId);
    }

}
