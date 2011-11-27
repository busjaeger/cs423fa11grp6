package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.illinois.cs.mapreduce.Job.JobID;
import edu.illinois.cs.mapreduce.Task.TaskID;
import edu.illinois.cs.mapreduce.api.InputFormat;
import edu.illinois.cs.mapreduce.api.Partition;
import edu.illinois.cs.mapreduce.api.Partitioner;

/**
 * TODO recover from common failures TODO synchronize properly
 */
public class JobManager implements JobManagerService {

    private static final NumberFormat NF = NumberFormat.getInstance();
    static {
        NF.setMinimumIntegerDigits(5);
        NF.setGroupingUsed(false);
    }

    private final ID nodeId;
    private final ID[] nodeIds;
    private final Map<ID, TaskManagerService> taskManagers;
    private final Map<ID, FileSystemService> fileSystems;
    private final AtomicInteger counter; // TODO persist and resume
    private final Map<JobID, Job> jobs;

    JobManager(ID id, Map<ID, TaskManagerService> taskManagers, Map<ID, FileSystemService> fileSystems) {
        this.nodeId = id;
        this.nodeIds = taskManagers.keySet().toArray(new ID[0]);
        this.taskManagers = taskManagers;
        this.fileSystems = fileSystems;
        this.counter = new AtomicInteger(0);
        this.jobs = new ConcurrentHashMap<JobID, Job>();
    }

    @Override
    public JobID submitJob(File jarFile, File inputFile) throws IOException {
        // 1. create job
        JobID jobId = new JobID(nodeId, counter.incrementAndGet());
        Path jar = new Path(jobId).append(jarFile.getName());
        Job job = new Job(jobId, jar);
        jobs.put(jobId, job);
        // 2. submit tasks
        submitMapTasks(job, jarFile, inputFile);
        return jobId;
    }

    private void submitMapTasks(Job job, File jarFile, File inputFile) throws IOException {
        Path inputDir = job.getPath().append("input");
        Path outputDir = job.getPath().append("output");
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
                // 1. chose task manager
                // current selection policy: robin.
                // TODO: try capacity-based policy here
                ID nodeId = nodeIds[num % nodeIds.length];

                // 2. initialize partition paths
                String partitionPath = "partition-" + NF.format(num);
                Path inputPath = inputDir.append(partitionPath);
                Path outputPath = outputDir.append(partitionPath);

                // 3. write partition to task manager's file system
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
                    fs.copy(job.getJar(), jarFile);
                    nodesWithJar.add(nodeId);
                }

                // 5. create task
                TaskID taskId = new TaskID(job.getId(), num);
                Task task = new Task(taskId, partition, inputPath, outputPath, job.getJar(), descriptor);
                job.getMapTasks().add(task);

                // 6. submit task
                TaskManagerService taskManager = taskManagers.get(nodeId);
                taskManager.submitTask(task);

                num++;
            }
        } finally {
            is.close();
        }
    }

}
