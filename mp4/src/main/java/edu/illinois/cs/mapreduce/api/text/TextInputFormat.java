package edu.illinois.cs.mapreduce.api.text;

import java.io.InputStream;
import java.util.Properties;

import edu.illinois.cs.mapreduce.api.InputFormat;
import edu.illinois.cs.mapreduce.api.RecordReader;

public class TextInputFormat implements InputFormat<Long, String, TextPartition> {

    private static final String LPP = "lines.per.partition";
    private static final String LPP_DEFAULT = "100"; // 10K

    @Override
    public TextPartitioner createPartitioner(InputStream is, Properties properties) {
        long linesPerPartition = Long.parseLong(properties.getProperty(LPP, LPP_DEFAULT));
        return new TextPartitioner(is, linesPerPartition);
    }

    @Override
    public RecordReader<Long, String> createRecordReader(TextPartition partition, InputStream is) {
        long firstLineNumber = partition.getFirstLineNumber();
        return new LineRecordReader(is, firstLineNumber);
    }

}
