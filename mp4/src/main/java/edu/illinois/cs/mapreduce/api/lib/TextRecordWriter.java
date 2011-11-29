package edu.illinois.cs.mapreduce.api.lib;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import edu.illinois.cs.mapreduce.api.RecordWriter;

public class TextRecordWriter<K, V> extends RecordWriter<K, V> {

    private final Writer writer;
    private final String separator;

    public TextRecordWriter(OutputStream os, String separator) {
        this.writer = new OutputStreamWriter(os);
        this.separator = separator;
    }

    @Override
    public void write(K key, V value) throws IOException {
        writer.write(key.toString());
        writer.write(separator);
        writer.write(value.toString());
        writer.write("\n");
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
