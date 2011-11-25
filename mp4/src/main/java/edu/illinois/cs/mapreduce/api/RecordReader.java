package edu.illinois.cs.mapreduce.api;

import java.io.Closeable;
import java.io.IOException;

public abstract class RecordReader<K, V> implements Closeable {

    public abstract boolean next() throws IOException;

    public abstract K getKey();

    public abstract V getValue();

}