package edu.illinois.cs.mapreduce.api;

import java.io.OutputStream;
import java.util.Properties;

/**
 * Implements a specific output format by means of a RecordWriter
 * 
 * @author benjamin
 * @param <K> type of keys the record writer expects
 * @param <V> type of values the record writer expects
 */
public abstract class OutputFormat<K, V> {

    /**
     * Creates a new RecordWriter that will write values to the given
     * OutputStream.
     * 
     * @param os OutputStream to write to
     * @param properties configuration values
     * @return RecordWriter for the given OutputStream and Properties
     */
    public abstract RecordWriter<K, V> createRecordWriter(OutputStream os, Properties properties);

}
