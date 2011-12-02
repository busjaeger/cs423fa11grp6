package edu.illinois.cs.mapreduce.spi.lib;

import edu.illinois.cs.mapreduce.spi.TransferPolicy;
import edu.illinois.cs.mr.NodeStatus;

public class IdleTransferPolicy extends TransferPolicy {
    @Override
    public boolean isTransferNeeded(NodeStatus newStatus, Iterable<NodeStatus> statuses) {
        if (newStatus.getQueueLength() == 0) {
            for (NodeStatus status : statuses)
                if (status.getQueueLength() > 0)
                    return true;
        }
        return false;
    }
}