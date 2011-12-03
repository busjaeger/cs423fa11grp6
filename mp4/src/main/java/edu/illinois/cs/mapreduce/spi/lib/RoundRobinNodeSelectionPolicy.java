package edu.illinois.cs.mapreduce.spi.lib;

import java.util.Collection;
import java.util.Iterator;

import edu.illinois.cs.mapreduce.spi.NodeSelectionPolicy;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.lb.NodeStatus;

public class RoundRobinNodeSelectionPolicy implements NodeSelectionPolicy {

    private int index = 0;

    @Override
    public NodeID selectNode(Collection<NodeStatus> nodeStatuses) {
        int count = index++ % nodeStatuses.size();
        Iterator<NodeStatus> it = nodeStatuses.iterator();
        NodeStatus node = null;
        while (count-- >= 0)
            node = it.next();
        return node.getId();
    }

}
