package edu.illinois.cs.mapreduce.api;

import java.io.InputStream;
import java.util.Properties;

public interface InputFormat<K, V, P extends Partition> {

    Partitioner createPartitioner(InputStream is, Properties properties);

    RecordReader<K, V> createRecordReader(P partition, InputStream is);

}