package edu.illinois.cs.mapreduce.spi.lib;

import edu.illinois.cs.mapreduce.spi.TransferPolicy;
import edu.illinois.cs.mr.lb.NodeStatus;

public class NeverTransferPolicy implements TransferPolicy {
    @Override
    public boolean isTransferNeeded(NodeStatus newStatus, Iterable<NodeStatus> statuses) {
        return false;
    }
}