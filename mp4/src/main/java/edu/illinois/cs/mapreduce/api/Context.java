package edu.illinois.cs.mapreduce.api;

import java.io.IOException;

/**
 * Context passed to map and reduce functions by the framework. The context is
 * used to store results and report status.
 * 
 * @author benjamin
 * @param <K> type of keys stored in this context
 * @param <V> type of values stored in this context
 */
public interface Context<K, V> {

    /**
     * Stores a given key/value pair in the context.
     * 
     * @param key key to store
     * @param value value to store
     * @throws IOException if any IOException occurred storing the key/value
     *             pair
     */
    void write(K key, V value) throws IOException;

}
