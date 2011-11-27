package edu.illinois.cs.mapreduce;

public class JobID extends ChildID<NodeID> {

    private static final long serialVersionUID = -648883430189232695L;

    public JobID(NodeID nodeId, int value) {
        super(nodeId, value);
    }

    @Override
    public String toString() {
        return "job" + super.toString();
    }

}
