package edu.illinois.cs.mapreduce;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
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
import edu.illinois.cs.mapreduce.api.OutputFormat;
import edu.illinois.cs.mapreduce.api.Partition;
import edu.illinois.cs.mapreduce.api.RecordReader;
import edu.illinois.cs.mapreduce.api.RecordWriter;
import edu.illinois.cs.mapreduce.api.Reducer;

class TaskRunner implements Runnable {

    private final TaskAttempt task;
    private final Cluster cluster;
    private final Semaphore completion;

    private FileSystemService fileSystem;
    private ClassLoader classLoader;
    private JobDescriptor descriptor;

    public TaskRunner(TaskAttempt task, Semaphore completion, Cluster cluster) {
        this.task = task;
        this.completion = completion;
        this.cluster = cluster;
    }

    private void init() throws IOException {
        NodeID nodeID = task.getNodeID();
        this.fileSystem = cluster.getFileSystemService(nodeID);
        Path jarPath = task.getJarPath();
        URL jarURL = fileSystem.toURL(jarPath);
        this.classLoader = ReflectionUtil.createClassLoader(jarURL);
        this.descriptor = task.getDescriptor();
    }

    @Override
    public void run() {
        TaskAttemptStatus status = task.getStatus();
        try {
            status.setState(State.RUNNING);
            init();
            if (task.isMap())
                runMap((MapTaskAttempt)task);
            else
                runReduce((ReduceTaskAttempt)task);
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

    private <K1, V1, K2, V2> void runMap(MapTaskAttempt mapTask) throws Exception {
        // create object instances
        Mapper<K1, V1, K2, V2> mapper = newInstance(descriptor.getMapperClass());
        InputFormat<K1, V1, ? super Partition> inputFormat = newInstance(descriptor.getInputFormatClass());
        Partition partition = mapTask.getPartition();
        Reducer<K2, V2, K2, V2> combiner = null;
        String combinerClass = descriptor.getCombinerClass();
        if (combinerClass != null)
            combiner = newInstance(combinerClass);
        // run map
        InputStream is = fileSystem.read(mapTask.getInputPath());
        try {
            RecordReader<K1, V1> reader = inputFormat.createRecordReader(partition, is);
            try {
                OutputStream os = fileSystem.write(mapTask.getOutputPath());
                try {
                    MapOutputContext<K2, V2> context = new MapOutputContext<K2, V2>(combiner, os);
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
    static class MapOutputContext<K, V> implements Context<K, V>, Closeable {

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
        public void close() throws IOException {
            if (map.isEmpty())
                return;
            if (combiner != null) {
                MapOutputContext<K, V> subContext = new MapOutputContext<K, V>(null, os);
                try {
                    for (Entry<K, List<V>> entry : map.entrySet())
                        combiner.reduce(entry.getKey(), entry.getValue(), subContext);
                } finally {
                    subContext.close();
                }
            } else {
                ObjectOutputStream oos = new ObjectOutputStream(os);
                try {
                    for (Entry<K, List<V>> entry : map.entrySet()) {
                        oos.writeObject(entry.getKey());
                        oos.writeObject(entry.getValue());
                    }
                    oos.writeObject(null);// sentinel
                } finally {
                    oos.close();
                }
            }
        }
    }

    private <K1, V1, K2, V2> void runReduce(ReduceTaskAttempt reduceTask) throws Exception {
        // 1. merge sorted files into one big sorted file
        Path outputPath = reduceTask.getOutputPath();
        Path mergedPath = outputPath.beforeLast().append(outputPath.last() + "-merged");
        List<QualifiedPath> inputPaths = reduceTask.getInputPaths();
        shuffle(inputPaths, mergedPath);

        // 3. create object instances
        Reducer<K1, V1, K2, V2> reducer = newInstance(descriptor.getReducerClass());
        OutputFormat<K2, V2> outputFormat = newInstance(descriptor.getOutputFormatClass());

        // 2. run actual reducer
        try {
            InputStream is = fileSystem.read(mergedPath);
            try {
                ObjectInputStream ois = new ObjectInputStream(is);
                try {
                    OutputStream os = fileSystem.write(outputPath);
                    try {
                        RecordWriter<K2, V2> writer = outputFormat.createRecordWriter(os, descriptor.getProperties());
                        try {
                            ReducerOutputContext<K2, V2> context = new ReducerOutputContext<K2, V2>(writer);
                            K1 key = null;
                            List<V1> values = new ArrayList<V1>();
                            while (true) {
                                @SuppressWarnings("unchecked")
                                K1 curKey = (K1)ois.readObject();
                                if (curKey == null)
                                    break;
                                @SuppressWarnings("unchecked")
                                List<V1> curValues = (List<V1>)ois.readObject();
                                if (key == null) {
                                    key = curKey;
                                    values.addAll(curValues);
                                } else if (curKey.equals(key)) {
                                    values.addAll(curValues);
                                } else {
                                    reducer.reduce(key, values, context);
                                    key = curKey;
                                    values = new ArrayList<V1>(curValues);
                                }
                            }
                            reducer.reduce(key, values, context);
                        } finally {
                            writer.close();
                        }
                    } finally {
                        os.close();
                    }
                } finally {
                    ois.close();
                }
            } finally {
                is.close();
            }
        } finally {
            fileSystem.delete(mergedPath);
        }
    }

    private Path shuffle(Iterable<QualifiedPath> inputPaths, Path mergedPath) throws IOException,
        ClassNotFoundException {
        List<InputStream> iss = new ArrayList<InputStream>();
        try {
            for (QualifiedPath qPath : inputPaths) {
                FileSystemService fs = cluster.getFileSystemService(qPath.getNodeId());
                InputStream is = fs.read(qPath.getPath());
                iss.add(is);
            }
            OutputStream os = fileSystem.write(mergedPath);
            try {
                merge(iss, os);
            } finally {
                os.close();
            }
        } finally {
            for (InputStream is : iss)
                is.close();
        }
        return mergedPath;
    }

    private static void merge(List<InputStream> inputStreams, OutputStream outputStream) throws IOException,
        ClassNotFoundException {
        List<ObjectInputStream> iss = new ArrayList<ObjectInputStream>(inputStreams.size());
        for (InputStream is : inputStreams)
            iss.add(new ObjectInputStream(is));
        ObjectOutputStream os = new ObjectOutputStream(outputStream);
        try {
            List<Object[]> pairs = new ArrayList<Object[]>(iss.size());
            for (int i = 0; i < iss.size(); i++)
                pairs.add(null);
            Object[] lowest;
            int idx;
            while (!iss.isEmpty()) {
                lowest = null;
                idx = -1;
                for (int i = 0; i < iss.size(); i++) {
                    Object[] pair = pairs.get(i);
                    if (pair == null) {
                        ObjectInputStream ois = iss.get(i);
                        pair = new Object[2];
                        pair[0] = ois.readObject();
                        if (pair[0] == null) {
                            iss.remove(i);
                            pairs.remove(i);
                            continue;
                        }
                        pair[1] = ois.readObject();
                        pairs.set(i, pair);
                    }
                    @SuppressWarnings("unchecked")
                    Comparable<Object> key = (Comparable<Object>)pair[0];
                    if (lowest == null || key.compareTo(lowest[0]) <= 0) {
                        lowest = pair;
                        idx = i;
                    }
                }
                if (lowest != null) {
                    os.writeObject(lowest[0]);
                    os.writeObject(lowest[1]);
                    pairs.set(idx, null);
                }
            }
            os.writeObject(null);
        } finally {
            os.flush();
        }
    }

    static class ReducerOutputContext<K, V> implements Context<K, V> {

        private final RecordWriter<K, V> writer;

        public ReducerOutputContext(RecordWriter<K, V> writer) {
            this.writer = writer;
        }

        @Override
        public void write(K key, V value) throws IOException {
            writer.write(key, value);
        }

    }
}
