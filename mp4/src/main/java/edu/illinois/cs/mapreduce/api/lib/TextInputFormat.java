package edu.illinois.cs.mapreduce.api.lib;

import java.io.InputStream;
import java.util.Properties;

import edu.illinois.cs.mapreduce.api.InputFormat;
import edu.illinois.cs.mapreduce.api.RecordReader;

public class TextInputFormat extends InputFormat<Long, String, TextPartition> {

    private static final String LPP = "text.input.format.lines.per.partition";
    private static final String LPP_DEFAULT = "10000";

    @Override
    public TextPartitioner createPartitioner(InputStream is, Properties properties) {
        long linesPerPartition = Long.parseLong(properties.getProperty(LPP, LPP_DEFAULT));
        return new TextPartitioner(is, linesPerPartition);
    }

    @Override
    public RecordReader<Long, String> createRecordReader(TextPartition partition, InputStream is) {
        long firstLineNumber = ((TextPartition)partition).getFirstLineNumber();
        return new LineRecordReader(is, firstLineNumber);
    }

}
