package edu.illinois.cs.mapreduce.api;

import java.io.OutputStream;
import java.util.Properties;

public abstract class OutputFormat<K, V> {

    public abstract RecordWriter<K, V> createRecordWriter(OutputStream os, Properties properties);

}
