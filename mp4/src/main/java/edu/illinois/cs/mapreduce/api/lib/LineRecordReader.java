package edu.illinois.cs.mapreduce.api.lib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import edu.illinois.cs.mapreduce.api.RecordReader;

/**
 * Reads line number + line records
 * 
 * @author benjamin
 */
public class LineRecordReader extends RecordReader<Long, String> {

    private final BufferedReader reader;
    private long lineNumber;
    private String line;

    public LineRecordReader(InputStream is, long firstLineNumber) {
        this(new BufferedReader(new InputStreamReader(is)), firstLineNumber);
    }

    public LineRecordReader(BufferedReader reader, long firstLineNumber) {
        this.reader = reader;
        this.lineNumber = firstLineNumber;
    }

    @Override
    public boolean next() throws IOException {
        line = reader.readLine();
        lineNumber++;
        return line != null;
    }

    @Override
    public Long getKey() {
        return lineNumber;
    }

    @Override
    public String getValue() {
        return line;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

}
