package edu.illinois.cs.mapreduce.api;

import java.io.IOException;
import java.io.OutputStream;

public abstract class Splitter<S extends Split> {

    protected boolean eof = false;

    public boolean isEOF() {
        return eof;
    }

    public abstract S writeSplit(OutputStream os) throws IOException;

}