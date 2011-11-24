package edu.illinois.cs.dlb;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;

import edu.illinois.cs.dlb.TaskStatus.Status;
import edu.illinois.cs.dlb.api.Mapper;
import edu.illinois.cs.dlb.api.Mapper.Context;
import edu.illinois.cs.dlb.api.RecordReader;
import edu.illinois.cs.dlb.api.lib.LineRecordReader;

class Worker implements Runnable {

    private final WorkManager workManager;

    Worker(WorkManager workManager) {
        this.workManager = workManager;
    }

    @Override
    public void run() {
        while (true) {
            Task task;
            try {
                task = workManager.getTaskQueue().take();
            } catch (InterruptedException e) {
                return;
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
        File jarFile = workManager.getFile(task.getJar());
        URL[] urls = new URL[] {jarFile.toURI().toURL()};
        return new URLClassLoader(urls);
    }

    private <K1, V1, K2, V2> Mapper<K1, V1, K2, V2> newMapper(Task task, ClassLoader cl) throws Exception {
        String mapperClass = task.getDescriptor().getMapperClass();
        @SuppressWarnings("unchecked")
        Class<Mapper<K1, V1, K2, V2>> clazz = (Class<Mapper<K1, V1, K2, V2>>)cl.loadClass(mapperClass);
        return clazz.newInstance();
    }

    private <K, V> RecordReader<K, V> newRecordReader(Task task, ClassLoader cl) throws IOException {
        File input = workManager.getFile(task.getInputFile());
        // TODO hard-coded right now
        @SuppressWarnings("unchecked")
        RecordReader<K, V> reader = (RecordReader<K, V>)new LineRecordReader(input, 0);
        return reader;
    }

    private <K, V> FileContext<K, V> newFileContext(Task task) throws IOException {
        File output = workManager.getFile(task.getOutputFile());
        return new FileContext<K, V>(output);
    }

    static class FileContext<K, V> implements Context<K, V>, Closeable {

        private final Writer writer;

        public FileContext(File file) throws IOException {
            this.writer = new FileWriter(file);
        }

        @Override
        public void write(K key, V value) throws IOException {
            String keyS = key.toString();
            String valueS = value.toString();
            writer.write(keyS + "\t" + valueS + "\n");
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}
