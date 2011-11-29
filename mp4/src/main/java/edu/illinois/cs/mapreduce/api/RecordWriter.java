package edu.illinois.cs.mapreduce.api;

import java.io.Closeable;
import java.io.IOException;

public abstract class RecordWriter<K, V> implements Closeable {

    public abstract void write(K key, V value) throws IOException;

    public void close() throws IOException {
        // default implementation
    }

}
