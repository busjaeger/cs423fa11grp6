package edu.illinois.cs.mapreduce.api;

import java.io.Closeable;
import java.io.IOException;

/**
 * Writes a set of key value pairs to an underlying data sink.
 * 
 * @author benjamin
 * @param <K> type of keys to write
 * @param <V> type of values to write
 */
public abstract class RecordWriter<K, V> implements Closeable {

    /**
     * writes the given key/value pair to the underlying sink
     * 
     * @param key key to write
     * @param value value to write
     * @throws IOException if an error occurred writing the key/value pair
     */
    public abstract void write(K key, V value) throws IOException;

    /**
     * closes this record writer
     */
    public void close() throws IOException {
        // default implementation
    }

}
