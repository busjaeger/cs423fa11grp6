package edu.illinois.cs.mapreduce;

public class JobID extends ChildID<NodeID, JobID> {

    private static final long serialVersionUID = -648883430189232695L;

    public JobID(NodeID nodeId, int value) {
        super(nodeId, value);
    }

    public static JobID fromQualifiedString(String s) {
        int idx = s.indexOf(SEP);
        NodeID nodeId = new NodeID(ID.valueFromString(s.substring(0, idx)));
        int value = ID.valueFromString(s.substring(idx + 1));
        return new JobID(nodeId, value);
    }
}
