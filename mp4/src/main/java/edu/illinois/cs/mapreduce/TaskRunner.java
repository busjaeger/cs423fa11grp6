package edu.illinois.cs.mapreduce;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import edu.illinois.cs.dfs.FileSystem;
import edu.illinois.cs.dfs.Path;
import edu.illinois.cs.mapreduce.api.Mapper;
import edu.illinois.cs.mapreduce.api.RecordReader;
import edu.illinois.cs.mapreduce.api.Mapper.Context;
import edu.illinois.cs.mapreduce.api.text.LineRecordReader;

class TaskRunner implements Runnable {

    private final BlockingQueue<Task> inQueue;
    private final BlockingQueue<Task> outQueue;
    // currently needs local file system to load jar
    private final FileSystem fileSystem;

    public TaskRunner(BlockingQueue<Task> inQueue, BlockingQueue<Task> outQueue, FileSystem fileSystem) {
        this.inQueue = inQueue;
        this.outQueue = outQueue;
        this.fileSystem = fileSystem;
    }

    @Override
    public void run() {
        while (true) {
            Task task;
            try {
                task = inQueue.take();
            } catch (InterruptedException e) {
                break;
            }

            try {
                runMap(task);
                task.getStatus().setStatus(Status.SUCCEEDED);
            } catch (Throwable t) {
                task.getStatus().setStatus(Status.FAILED);
                if (t instanceof Error)
                    throw (Error)t;
                t.printStackTrace();
                task.getStatus().setMessage(t.getMessage());
            }

            try {
                outQueue.put(task);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private <K1, V1, K2, V2> void runMap(Task task) throws Exception {
        ClassLoader cl = newClassLoader(task);
        Mapper<K1, V1, K2, V2> mapper = newMapper(task, cl);
        RecordReader<K1, V1> reader = newRecordReader(task, cl);
        try {
            FileContext<K2, V2> context = newFileContext(task);
            try {
                while (reader.next()) {
                    K1 key = reader.getKey();
                    V1 value = reader.getValue();
                    mapper.map(key, value, context);
                }
            } finally {
                context.close();
            }
        } finally {
            reader.close();
        }
    }

    private ClassLoader newClassLoader(Task task) throws IOException {
        Path jarPath = task.getJarPath();
        URL[] urls = new URL[] {fileSystem.toURL(jarPath)};
        return new URLClassLoader(urls);
    }

    private <K1, V1, K2, V2> Mapper<K1, V1, K2, V2> newMapper(Task task, ClassLoader cl) throws Exception {
        String mapperClass = task.getDescriptor().getMapperClass();
        @SuppressWarnings("unchecked")
        Class<Mapper<K1, V1, K2, V2>> clazz = (Class<Mapper<K1, V1, K2, V2>>)cl.loadClass(mapperClass);
        return clazz.newInstance();
    }

    // TODO RecordReader hard-coded right now
    private <K, V> RecordReader<K, V> newRecordReader(Task task, ClassLoader cl) throws IOException {
        Path input = task.getInputPath();
        InputStream is = fileSystem.read(input);
        @SuppressWarnings("unchecked")
        RecordReader<K, V> reader = (RecordReader<K, V>)new LineRecordReader(is, 0);
        return reader;
    }

    private <K, V> FileContext<K, V> newFileContext(Task task) throws IOException {
        Path output = task.getOutputPath();
        OutputStream os = fileSystem.write(output);
        return new FileContext<K, V>(os);
    }

    // TODO avoid keeping whole set in memory: limit buffer and spill to
    // separate files once full. Merge spills at the end
    static class FileContext<K, V> implements Context<K, V>, Closeable {

        private final Map<K, V> map = new TreeMap<K, V>();
        private final Writer writer;

        public FileContext(OutputStream os) throws IOException {
            this.writer = new OutputStreamWriter(os);
        }

        @Override
        public void write(K key, V value) throws IOException {
            map.put(key, value);

        }

        @Override
        public void close() throws IOException {
            for (Entry<K, V> entry : map.entrySet())
                writer.write(entry.getKey() + "\t" + entry.getValue() + "\n");
            writer.close();
        }
    }
}
