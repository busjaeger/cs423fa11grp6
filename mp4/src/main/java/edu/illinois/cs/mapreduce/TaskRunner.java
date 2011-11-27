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
import java.util.concurrent.Semaphore;
import java.util.TreeMap;

import edu.illinois.cs.mapreduce.api.Mapper;
import edu.illinois.cs.mapreduce.api.Mapper.Context;
import edu.illinois.cs.mapreduce.api.RecordReader;
import edu.illinois.cs.mapreduce.api.lib.LineRecordReader;

class TaskRunner implements Runnable {

    private final TaskAttempt task;
    private final FileSystemService fileSystem;
    private final Semaphore completion;

    public TaskRunner(TaskAttempt task, FileSystemService fileSystem, Semaphore completion) {
        this.task = task;
        this.fileSystem = fileSystem;
        this.completion = completion;
    }

    @Override
    public void run() {
        try {
            task.getStatus().setStatus(Status.RUNNING);
            runMap();
            task.getStatus().setStatus(Status.SUCCEEDED);
        } catch (InterruptedException e) {
            task.getStatus().setStatus(Status.CANCELED);
        } catch (Throwable t) {
            synchronized (task) {
                task.getStatus().setStatus(Status.FAILED);
                task.getStatus().setMessage(t.getMessage());
            }
            if (t instanceof Error)
                throw (Error)t;
            t.printStackTrace();
        } finally {
            completion.release();
        }
    }

    private <K1, V1, K2, V2> void runMap() throws Exception {
        ClassLoader cl = newClassLoader();
        Mapper<K1, V1, K2, V2> mapper = newMapper(cl);
        RecordReader<K1, V1> reader = newRecordReader(cl);
        try {
            FileContext<K2, V2> context = newFileContext();
            try {
                while (reader.next()) {
                    if (Thread.interrupted())
                        throw new InterruptedException();
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

    private ClassLoader newClassLoader() throws IOException {
        Path jarPath = task.getJarPath();
        URL[] urls = new URL[] {fileSystem.toURL(jarPath)};
        return new URLClassLoader(urls);
    }

    private <K1, V1, K2, V2> Mapper<K1, V1, K2, V2> newMapper(ClassLoader cl) throws Exception {
        String mapperClass = task.getDescriptor().getMapperClass();
        @SuppressWarnings("unchecked")
        Class<Mapper<K1, V1, K2, V2>> clazz = (Class<Mapper<K1, V1, K2, V2>>)cl.loadClass(mapperClass);
        return clazz.newInstance();
    }

    private <K, V> RecordReader<K, V> newRecordReader(ClassLoader cl) throws IOException {
        Path input = task.getInputPath();
        InputStream is = fileSystem.read(input);
        @SuppressWarnings("unchecked")
        RecordReader<K, V> reader = (RecordReader<K, V>)new LineRecordReader(is, 0);
        return reader;
    }

    private <K, V> FileContext<K, V> newFileContext() throws IOException {
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
