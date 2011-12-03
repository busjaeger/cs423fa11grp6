package edu.illinois.cs.mapreduce.api;

import java.io.InputStream;
import java.util.Properties;

/**
 * Implements specifics of an input format by means of a Splitter and
 * RecordReader.
 * 
 * @author benjamin
 * @param <K> type of key produced by the RecordReader
 * @param <V> type of value produced by the RecordReader
 * @param <S>
 */
public abstract class InputFormat<K, V, S extends Split> {

    /**
     * Returns a Splitter for the given InputStream and Job Properties.
     * 
     * @param is InputStream to split
     * @param properties Properties containing job configuration
     * @return Splitter for the given InputStream and Properties
     */
    public abstract Splitter<S> createSplitter(InputStream is, Properties properties);

    /**
     * Returns a RecordReader for the given InputStream described by the Split.
     * 
     * @param split Split describing the current split being read
     * @param is InputStream holding the data of the split
     * @return RecordReader for the split
     */
    public abstract RecordReader<K, V> createRecordReader(S split, InputStream is);

}
