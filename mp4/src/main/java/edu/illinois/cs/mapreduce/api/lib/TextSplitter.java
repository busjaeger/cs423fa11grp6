package edu.illinois.cs.mapreduce.api.lib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import edu.illinois.cs.mapreduce.api.Splitter;

/**
 * Splits a text file into a number of separate text files based on the
 * configurable number of lines.
 * 
 * @author benjamin
 */
public class TextSplitter extends Splitter<TextSplit> {

    private final BufferedReader reader;
    private final long numLines;
    private long lineNumber;

    public TextSplitter(InputStream is, long numLines) {
        this(new BufferedReader(new InputStreamReader(is)), numLines);
    }

    public TextSplitter(BufferedReader reader, long numLines) {
        this.reader = reader;
        this.numLines = numLines;
    }

    @Override
    public TextSplit writeSplit(OutputStream os) throws IOException {
        Writer writer = new OutputStreamWriter(os);
        TextSplit split = new TextSplit(lineNumber);
        String line;
        for (long i = numLines; i > 0; i--) {
            if ((line = reader.readLine()) == null) {
                eof = true;
                break;
            }
            writer.write(line);
            writer.write('\n');
            lineNumber++;
        }
        writer.flush();
        return split;
    }

}
