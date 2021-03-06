package edu.illinois.cs.mr;

import edu.illinois.cs.mr.util.ID;

/**
 * Unique identifier of a node.
 * 
 * @author benjamin
 */
public class NodeID extends ID<NodeID> {

    private static final long serialVersionUID = -7662812834697568342L;

    public NodeID(int value) {
        super(value);
    }

}
