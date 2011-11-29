package edu.illinois.cs.mapreduce.api.lib;

import java.io.OutputStream;
import java.util.Properties;

import edu.illinois.cs.mapreduce.api.OutputFormat;
import edu.illinois.cs.mapreduce.api.RecordWriter;

public class TextOutputFormat<K, V> extends OutputFormat<K, V> {

    private static final String SEP = "text.output.format.separator";
    private static final String SEP_DEFAULT = "\t";

    @Override
    public RecordWriter<K, V> createRecordWriter(OutputStream os, Properties properties) {
        String separator = properties.getProperty(SEP, SEP_DEFAULT);
        return new TextRecordWriter<K, V>(os, separator);
    }

}
