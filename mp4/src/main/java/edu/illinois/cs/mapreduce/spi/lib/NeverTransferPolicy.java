package edu.illinois.cs.mapreduce.spi.lib;

import edu.illinois.cs.mapreduce.spi.TransferPolicy;
import edu.illinois.cs.mr.NodeStatus;

public class NeverTransferPolicy extends TransferPolicy {
    @Override
    public boolean isTransferNeeded(NodeStatus newStatus, Iterable<NodeStatus> statuses) {
        return false;
    }
}