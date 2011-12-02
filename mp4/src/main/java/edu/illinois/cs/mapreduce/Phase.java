package edu.illinois.cs.mapreduce;

enum Phase {
    MAP("Map"), REDUCE("Reduce");
    private final String s;

    private Phase(String s) {
        this.s = s;
    }

    @Override
    public String toString() {
        return s;
    }
}