package edu.illinois.cs.mapreduce.api;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Splits a given file into a set of Splits.
 * 
 * @author benjamin
 * @param <S> Type of Split created by this Splitter. Will be passed to the
 *            RecordReader when the split is read.
 */
public abstract class Splitter<S extends Split> {

    /**
     * must be set by implementations to indicate that the end of the file has
     * been reached.
     */
    protected boolean eof = false;

    /**
     * Checks whether the end of the current split has been reached
     * 
     * @return true if the end of the current split has been reached
     */
    public boolean isEOF() {
        return eof;
    }

    /**
     * Writes a split to the given OutputStream and returns a Split object
     * describing the split.
     * 
     * @param os OutputStream to write to
     * @return Split describing the split
     * @throws IOException If an error occurred writing the split.
     */
    public abstract S writeSplit(OutputStream os) throws IOException;

}
