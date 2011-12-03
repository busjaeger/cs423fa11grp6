package edu.illinois.cs.mapreduce.api.lib;

import java.io.InputStream;
import java.util.Properties;

import edu.illinois.cs.mapreduce.api.InputFormat;
import edu.illinois.cs.mapreduce.api.RecordReader;

/**
 * Implementation of a line-based text input file. Text files are split by line
 * number.
 * 
 * @author benjamin
 */
public class TextInputFormat extends InputFormat<Long, String, TextSplit> {

    private static final String LPP = "text.input.format.lines.per.split";
    private static final String LPP_DEFAULT = "10000";

    @Override
    public TextSplitter createSplitter(InputStream is, Properties properties) {
        long linesPerSplit = Long.parseLong(properties.getProperty(LPP, LPP_DEFAULT));
        return new TextSplitter(is, linesPerSplit);
    }

    @Override
    public RecordReader<Long, String> createRecordReader(TextSplit split, InputStream is) {
        long firstLineNumber = ((TextSplit)split).getFirstLineNumber();
        return new LineRecordReader(is, firstLineNumber);
    }

}
