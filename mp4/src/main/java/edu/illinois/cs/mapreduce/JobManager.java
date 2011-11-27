package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.illinois.cs.mapreduce.api.InputFormat;
import edu.illinois.cs.mapreduce.api.Partition;
import edu.illinois.cs.mapreduce.api.Partitioner;

/**
 * TODO recover from common failures TODO synchronize properly
 */
public class JobManager implements JobManagerService {

    private final NodeID nodeId;
    private final NodeID[] nodeIds;
    private final Map<NodeID, TaskExecutorService> taskExecutors;
    private final Map<NodeID, FileSystemService> fileSystems;
    private final AtomicInteger counter;
    private final Map<JobID, Job> jobs;

    JobManager(NodeID id, Map<NodeID, TaskExecutorService> taskExecutors, Map<NodeID, FileSystemService> fileSystems) {
        this.nodeId = id;
        this.nodeIds = taskExecutors.keySet().toArray(new NodeID[0]);
        this.taskExecutors = taskExecutors;
        this.fileSystems = fileSystems;
        this.counter = new AtomicInteger();
        this.jobs = new ConcurrentHashMap<JobID, Job>();
    }

    @Override
    public JobID submitJob(File jarFile, File inputFile) throws IOException {
        // 1. create job
        JobID jobId = new JobID(nodeId, counter.incrementAndGet());
        Job job = new Job(jobId, jarFile.getName());
        jobs.put(jobId, job);
        // 2. submit tasks
        submitMapTasks(job, jarFile, inputFile);
        return jobId;
    }

    private void submitMapTasks(Job job, File jarFile, File inputFile) throws IOException {
        JobDescriptor descriptor = JobDescriptor.read(jarFile);
        ClassLoader cl = new URLClassLoader(new URL[] {jarFile.toURI().toURL()});
        InputFormat<?, ?, ?> inputFormat;
        try {
            inputFormat = (InputFormat<?, ?, ?>)cl.loadClass(descriptor.getInputFormatClass()).newInstance();
        } catch (Exception e) {
            throw new IOException(e);
        }

        InputStream is = new FileInputStream(inputFile);
        try {
            Partitioner partitioner = inputFormat.createPartitioner(is, descriptor.getProperties());
            Set<ID> nodesWithJar = new HashSet<ID>();
            int num = 0;
            while (!partitioner.isEOF()) {
                // 1. create sub task for the partition
                TaskID taskId = new TaskID(job.getId(), num);
                Path inputPath = job.getPath().append(taskId + "_input");
                Task task = new Task(taskId, inputPath);
                job.getMapTasks().add(task);

                // 2. chose node to run task on
                // current selection policy: round-robin
                // TODO: capacity-based selection policy
                NodeID nodeId = nodeIds[num % nodeIds.length];

                // 3. write partition to node's file system
                FileSystemService fs = fileSystems.get(nodeId);
                OutputStream os = fs.write(inputPath);
                Partition partition;
                try {
                    partition = partitioner.writePartition(os);
                } finally {
                    os.close();
                }

                // 4. write job file if not already written
                if (!nodesWithJar.contains(nodeId)) {
                    fs.copy(job.getJarPath(), jarFile);
                    nodesWithJar.add(nodeId);
                }

                // 5. create and submit task attempt
                TaskAttemptID attemptID = new TaskAttemptID(taskId, task.nextAttemptID());
                Path outputPath = job.getPath().append(attemptID.toQualifiedString(1)+"_output");
                TaskAttempt attempt =
                    new TaskAttempt(attemptID, nodeId, job.getJarPath(), descriptor, partition, inputPath, outputPath);
                task.getAttempts().add(attempt);

                // 6. submit task
                TaskExecutorService taskExecutor = taskExecutors.get(nodeId);
                taskExecutor.execute(attempt);

                num++;
            }
        } finally {
            is.close();
        }
    }

}
