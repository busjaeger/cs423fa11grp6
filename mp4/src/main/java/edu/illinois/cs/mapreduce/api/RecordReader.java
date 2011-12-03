package edu.illinois.cs.mapreduce.api;

import java.io.Closeable;
import java.io.IOException;

/**
 * Used to traverse some underlying to data source as a set of key/value pairs.
 * 
 * @author benjamin
 * @param <K> type of keys produced by the reader
 * @param <V> type of values produced by the reader
 */
public abstract class RecordReader<K, V> implements Closeable {

    /**
     * advances to the next key/value position
     * 
     * @return true if reader could advance
     * @throws IOException if an IO error occurred reading the next record
     */
    public abstract boolean next() throws IOException;

    /**
     * returns the key for the current record
     * 
     * @return key
     */
    public abstract K getKey();

    /**
     * returns the value for the current record
     * 
     * @return value
     */
    public abstract V getValue();

}
