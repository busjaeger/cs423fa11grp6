package edu.illinois.cs.mapreduce.api;

import java.io.IOException;
import java.io.OutputStream;

public abstract class Partitioner {

    protected boolean eof = false;

    public boolean isEOF() {
        return eof;
    }

    public abstract Partition writePartition(OutputStream os) throws IOException;

}