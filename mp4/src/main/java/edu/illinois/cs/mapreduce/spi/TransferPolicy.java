package edu.illinois.cs.mapreduce.spi;

import edu.illinois.cs.mapreduce.NodeStatus;

public interface TransferPolicy {

    public static class IdleTransferPolicy implements TransferPolicy {
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

    public static class NeverTransferPolicy implements TransferPolicy {
        @Override
        public boolean isTransferNeeded(NodeStatus newStatus, Iterable<NodeStatus> statuses) {
            return false;
        }
    }

    boolean isTransferNeeded(NodeStatus newStatus, Iterable<NodeStatus> statuses);

}
