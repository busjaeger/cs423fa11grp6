package edu.illinois.cs.dlb.api;

import java.io.IOException;

/**
 * @param <K>
 * @param <V>
 */
public abstract class Mapper<K, V> {

    /**
     * @param <K>
     * @param <V>
     */
    public interface Context<K, V> {
        void write(K key, V value) throws IOException;
    }

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     */
    public abstract void map(K key, V value, Context<K, V> context) throws IOException;

}
