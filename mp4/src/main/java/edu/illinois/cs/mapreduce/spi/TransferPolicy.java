package edu.illinois.cs.mapreduce.spi;

import edu.illinois.cs.mr.lb.NodeStatus;

public interface TransferPolicy {

    boolean isTransferNeeded(NodeStatus newStatus, Iterable<NodeStatus> statuses);

}
