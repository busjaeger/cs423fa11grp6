package edu.illinois.cs.mapreduce.api;

import java.io.IOException;

/**
 * @param <K>
 * @param <V>
 */
public interface Context<K, V> {

    void write(K key, V value) throws IOException;

}