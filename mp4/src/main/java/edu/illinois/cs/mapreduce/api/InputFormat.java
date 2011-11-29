package edu.illinois.cs.mapreduce.api;

import java.io.InputStream;
import java.util.Properties;

public abstract class InputFormat<K, V, P extends Partition> {

    public abstract Partitioner<P> createPartitioner(InputStream is, Properties properties);

    public abstract RecordReader<K, V> createRecordReader(P partition, InputStream is);

}