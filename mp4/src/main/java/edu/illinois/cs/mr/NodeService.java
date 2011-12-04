package edu.illinois.cs.mr;

import edu.illinois.cs.mr.fs.FileSystemService;
import edu.illinois.cs.mr.jm.JobManagerService;
import edu.illinois.cs.mr.lb.LoadBalancerService;
import edu.illinois.cs.mr.te.TaskExecutorService;

/**
 * Aggregates all services offered by a node into a single interface.
 * 
 * @author benjamin
 */
public interface NodeService extends JobManagerService, FileSystemService, TaskExecutorService, LoadBalancerService {

    /**
     * Returns the cluster-wide unique identifier of this node
     * 
     * @return
     */
    public NodeID getId();

    /**
     * Stops this node
     */
    public void stop();

}