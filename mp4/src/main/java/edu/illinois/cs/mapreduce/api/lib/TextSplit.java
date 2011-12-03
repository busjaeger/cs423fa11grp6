package edu.illinois.cs.mapreduce.api.lib;

import edu.illinois.cs.mapreduce.api.Split;

public class TextSplit implements Split {

    private static final long serialVersionUID = 4410819338009937385L;

    private final long firstLineNumber;

    public TextSplit(long firstLineNumber) {
        this.firstLineNumber = firstLineNumber;
    }

    public long getFirstLineNumber() {
        return firstLineNumber;
    }

}
