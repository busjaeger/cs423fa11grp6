package edu.illinois.cs.mapreduce.api.text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import edu.illinois.cs.mapreduce.api.Partition;
import edu.illinois.cs.mapreduce.api.Partitioner;

// TODO could be enhanced to read lines for given
// numbers of bits as opposed to number of lines
public class TextPartitioner extends Partitioner {

    private final BufferedReader reader;
    private final long numLines;
    private long lineNumber;

    public TextPartitioner(InputStream is, long numLines) {
        this(new BufferedReader(new InputStreamReader(is)), numLines);
    }

    public TextPartitioner(BufferedReader reader, long numLines) {
        this.reader = reader;
        this.numLines = numLines;
    }

    @Override
    public Partition writePartition(OutputStream os) throws IOException {
        Writer writer = new OutputStreamWriter(os);
        TextPartition partition = new TextPartition(lineNumber);
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
        return partition;
    }

}
