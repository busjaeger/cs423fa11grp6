package edu.illinois.cs.mapreduce;

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

    boolean isTransferNeeded(NodeStatus newStatus, Iterable<NodeStatus> statuses);

}
