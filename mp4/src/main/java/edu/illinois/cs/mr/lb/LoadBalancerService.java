package edu.illinois.cs.mr.lb;

import java.io.IOException;

/**
 * Remote interface to the LoadBalancer. Accepts status updates from other nodes
 * to schedule and migrate task attempts.
 * 
 * @author benjamin
 */
public interface LoadBalancerService {

    /**
     * Updates the load balancer with the status of a node
     * 
     * @param nodeStatus
     * @return
     * @throws IOException
     */
    boolean updateStatus(NodeStatusSnapshot statusSnapshot) throws IOException;

}
