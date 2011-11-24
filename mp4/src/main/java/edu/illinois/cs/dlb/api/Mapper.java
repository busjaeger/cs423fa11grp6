package edu.illinois.cs.dlb.api;

import java.io.IOException;

/**
 * @param <K>
 * @param <V>
 */
public abstract class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    /**
     * @param <K>
     * @param <V>
     */
    public static interface Context<K, V> {
        void write(K key, V value) throws IOException;
    }

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     */
    public abstract void map(KEYIN key, VALUEIN value, Context<KEYOUT, VALUEOUT> context) throws IOException;

}