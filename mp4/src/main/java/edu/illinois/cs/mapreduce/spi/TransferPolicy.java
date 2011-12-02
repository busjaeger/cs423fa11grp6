package edu.illinois.cs.mapreduce.spi;

import edu.illinois.cs.mr.NodeStatus;

public abstract class TransferPolicy {

    public abstract boolean isTransferNeeded(NodeStatus newStatus, Iterable<NodeStatus> statuses);

}
