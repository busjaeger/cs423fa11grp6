package edu.illinois.cs.mapreduce;


public class NodeID extends ID {

    private static final long serialVersionUID = -7662812834697568342L;

    public NodeID(int value) {
        super(value);
    }

    @Override
    public String toString() {
        return "node" + super.toString();
    }
}