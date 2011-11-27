package edu.illinois.cs.mapreduce.api.lib;

import edu.illinois.cs.mapreduce.api.Partition;

public class TextPartition implements Partition {

    private static final long serialVersionUID = 4410819338009937385L;

    private final long firstLineNumber;

    public TextPartition(long firstLineNumber) {
        this.firstLineNumber = firstLineNumber;
    }

    public long getFirstLineNumber() {
        return firstLineNumber;
    }

}
