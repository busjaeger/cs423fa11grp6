package edu.illinois.cs.dlb.api.lib;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import edu.illinois.cs.dlb.api.RecordReader;

public class LineRecordReader extends RecordReader<Long, String> {

    private final BufferedReader reader;
    private long lineNumber;
    private String line;

    public LineRecordReader(File file, long firstLineNumber) throws FileNotFoundException {
        this(new BufferedReader(new FileReader(file)), firstLineNumber);
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
