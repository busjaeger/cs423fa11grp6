package edu.illinois.cs.mapreduce.api;

import java.io.InputStream;
import java.util.Properties;

public abstract class InputFormat<K, V, S extends Split> {

    public abstract Splitter<S> createSplitter(InputStream is, Properties properties);

    public abstract RecordReader<K, V> createRecordReader(S split, InputStream is);

}