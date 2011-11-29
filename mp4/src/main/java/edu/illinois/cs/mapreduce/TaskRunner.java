package edu.illinois.cs.mapreduce;

import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

import edu.illinois.cs.mapreduce.Status.State;
import edu.illinois.cs.mapreduce.api.Context;
import edu.illinois.cs.mapreduce.api.InputFormat;
import edu.illinois.cs.mapreduce.api.Mapper;
import edu.illinois.cs.mapreduce.api.Partition;
import edu.illinois.cs.mapreduce.api.RecordReader;
import edu.illinois.cs.mapreduce.api.Reducer;

class TaskRunner implements Runnable {

    private final TaskAttempt task;
    private final FileSystemService fileSystem;
    private final Semaphore completion;

    private ClassLoader classLoader;
    private JobDescriptor descriptor;

    public TaskRunner(TaskAttempt task, FileSystemService fileSystem, Semaphore completion) {
        this.task = task;
        this.fileSystem = fileSystem;
        this.completion = completion;
    }

    @Override
    public void run() {
        TaskAttemptStatus status = task.getStatus();
        try {
            status.setState(State.RUNNING);
            init();
            runMap();
            status.setState(State.SUCCEEDED);
        } catch (InterruptedException e) {
            status.setState(State.CANCELED);
        } catch (Throwable t) {
            synchronized (status) {
                status.setState(State.FAILED);
                status.setMessage(t.getMessage());
            }
            if (t instanceof Error)
                throw (Error)t;
            t.printStackTrace();
        } finally {
            completion.release();
        }
    }

    private void init() throws IOException {
        Path jarPath = task.getJarPath();
        URL jarURL = fileSystem.toURL(jarPath);
        this.classLoader = ReflectionUtil.createClassLoader(jarURL);
        this.descriptor = task.getDescriptor();
    }

    private <K1, V1, K2, V2> void runMap() throws Exception {
        Mapper<K1, V1, K2, V2> mapper = newInstance(descriptor.getMapperClass());
        InputFormat<K1, V1, ? super Partition> inputFormat = newInstance(descriptor.getInputFormatClass());
        Partition partition = task.getPartition();
        Path path = task.getInputPath();
        InputStream is = fileSystem.read(path);
        try {
            RecordReader<K1, V1> reader = inputFormat.createRecordReader(partition, is);
            try {
                Path output = task.getOutputPath();
                Reducer<K2, V2, K2, V2> combiner = null;
                String combinerClass = descriptor.getCombinerClass();
                if (combinerClass != null)
                    combiner = newInstance(combinerClass);
                OutputStream os = fileSystem.write(output);
                try {
                    MapOutputContext<K2, V2> context = new MapOutputContext<K2, V2>(combiner, os);
                    while (reader.next()) {
                        if (Thread.interrupted())
                            throw new InterruptedException();
                        K1 key = reader.getKey();
                        V1 value = reader.getValue();
                        mapper.map(key, value, context);
                    }
                    context.flush();
                } finally {
                    os.close();
                }
            } finally {
                reader.close();
            }
        } finally {
            is.close();
        }
    }

    private <T> T newInstance(String className) throws ClassNotFoundException, InstantiationException,
        IllegalAccessException {
        return ReflectionUtil.newInstance(className, classLoader);
    }

    // TODO avoid keeping whole set in memory: limit buffer and spill to
    // separate files once full. Merge spills at the end.
    static class MapOutputContext<K, V> implements Context<K, V>, Flushable {

        private final Reducer<K, V, K, V> combiner;
        private final OutputStream os;
        private final Map<K, List<V>> map;

        public MapOutputContext(Reducer<K, V, K, V> combiner, OutputStream os) {
            this.combiner = combiner;
            this.os = os;
            this.map = new TreeMap<K, List<V>>();
        }

        @Override
        public void write(K key, V value) throws IOException {
            List<V> values = map.get(key);
            if (values == null)
                map.put(key, values = new LinkedList<V>());
            values.add(value);
        }

        @Override
        public void flush() throws IOException {
            if (map.isEmpty())
                return;
            if (combiner != null) {
                MapOutputContext<K, V> subContext = new MapOutputContext<K, V>(null, os);
                try {
                    for (Entry<K, List<V>> entry : map.entrySet())
                        combiner.reduce(entry.getKey(), entry.getValue(), subContext);
                } finally {
                    subContext.flush();
                }
            } else {
                ObjectOutputStream oos = new ObjectOutputStream(os);
                try {
                    oos.writeObject(map);
                } finally {
                    oos.flush();
                }
            }
        }
    }

}
