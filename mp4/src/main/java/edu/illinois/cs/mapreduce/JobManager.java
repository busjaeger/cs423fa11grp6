package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import edu.illinois.cs.mapreduce.Status.State;
import edu.illinois.cs.mapreduce.api.InputFormat;
import edu.illinois.cs.mapreduce.api.Partition;
import edu.illinois.cs.mapreduce.api.Partitioner;
import edu.illinois.cs.mapreduce.spi.BootstrapPolicy;
import edu.illinois.cs.mapreduce.spi.LocationPolicy;
import edu.illinois.cs.mapreduce.spi.SelectionPolicy;
import edu.illinois.cs.mapreduce.spi.TransferPolicy;

/**
 * This job manager splits jobs into individual tasks and distributes them to
 * the TaskExecutors in the cluster. It derives job status from periodic status
 * updates from the TaskExecutors.
 */
public class JobManager implements JobManagerService {

    private final NodeConfiguration config;
    private Node node;
    private final TransferPolicy transferPolicy;
    private final BootstrapPolicy bootstrapPolicy;
    private final LocationPolicy locationPolicy;
    private final SelectionPolicy selectionPolicy;
    private final AtomicInteger counter;
    private volatile boolean transferring;
    private final Map<JobID, Job> jobs;
    private final Map<NodeID, NodeStatus> nodeStatuses;

    JobManager(NodeConfiguration config) throws Exception {
        this(config, ReflectionUtil.<TransferPolicy> newInstance(config.jmTransferPolicyClass, config), ReflectionUtil
            .<BootstrapPolicy> newInstance(config.jmBootstrapPolicyClass, config), ReflectionUtil
            .<LocationPolicy> newInstance(config.jmLocationPolicyClass, config), ReflectionUtil
            .<SelectionPolicy> newInstance(config.jmSelectionPolicyClass, config));
    }

    JobManager(NodeConfiguration config,
               TransferPolicy transferPolicy,
               BootstrapPolicy bootstrapPolicy,
               LocationPolicy locationPolicy,
               SelectionPolicy selectionPolicy) {
        this.config = config;
        this.transferPolicy = transferPolicy;
        this.bootstrapPolicy = bootstrapPolicy;
        this.locationPolicy = locationPolicy;
        this.selectionPolicy = selectionPolicy;
        this.counter = new AtomicInteger();
        this.jobs = new TreeMap<JobID, Job>();
        this.nodeStatuses = new TreeMap<NodeID, NodeStatus>();
    }

    @Override
    public void start(Node node) {
        this.node = node;
    }

    @Override
    public void stop() {
        // nothing to do
    }

    /**
     * Returns an array of job IDs
     * 
     * @return
     */
    public JobID[] getJobIDs() {
        synchronized (jobs) {
            return (JobID[])jobs.keySet().toArray();
        }
    }

    /**
     * @see edu.illinois.cs.mapreduce.JobManagerService#submitJob(java.io.File,
     *      java.io.File)
     */
    @Override
    public JobID submitJob(File jarFile, File inputFile) throws IOException {
        // 1. create job
        JobDescriptor descriptor = JobDescriptor.read(jarFile);
        JobID jobId = new JobID(config.nodeId, counter.incrementAndGet());
        Job job = new Job(jobId, jarFile.getName(), descriptor);
        synchronized (jobs) {
            jobs.put(jobId, job);
        }
        // 2. submit tasks
        try {
            submitMapTasks(job, jarFile, inputFile);
        } catch (Throwable t) {
            job.setState(State.FAILED);
            if (t instanceof IOException)
                throw (IOException)t;
            if (t instanceof RuntimeException)
                throw (RuntimeException)t;
            if (t instanceof Error)
                throw (Error)t;
            throw new RuntimeException(t);
        }
        return jobId;
    }

    /**
     * <p>
     * Tasks are created on the same thread, because we need data from the
     * inputFile, but do not want to assume that it will still exist after
     * submitJob returns. I.e. a user is free to delete or edit the input file
     * after submitting the job. We also do not want to create a copy of the
     * file, because we want to support very large input files, so partitioning
     * it directly is faster.
     * </p>
     * <p>
     * Task attempts can be submitted as soon as the necessary data is copied
     * and the task attempt is registered with the job. However, we only submit
     * attempts after the next attempt has been created and registered.
     * Otherwise, if all currently registered attempts complete quickly before
     * scheduling the next one, the JobManager may think the map phase has
     * completed and schedule the reducer.
     * </p>
     * 
     * @param job
     * @param jarFile
     * @param inputFile
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private void submitMapTasks(Job job, File jarFile, File inputFile) throws ClassNotFoundException,
        InstantiationException, IllegalAccessException, IOException {
        JobDescriptor descriptor = job.getDescriptor();
        InputFormat<?, ?, ?> inputFormat = ReflectionUtil.newInstance(descriptor.getInputFormatClass(), jarFile);
        InputStream is = new FileInputStream(inputFile);
        try {
            final Partitioner<?> partitioner = inputFormat.createPartitioner(is, descriptor.getProperties());
            Set<NodeID> nodesWithJar = new HashSet<NodeID>();
            int num = 0;
            MapTask previous = null;
            TaskAttempt previousAttempt = null;
            while (!partitioner.isEOF()) {
                // 1. chose node to run task on
                // current selection policy: round-robin
                // TODO: capacity-based selection policy
                NodeID targetNodeId;
                synchronized (nodeStatuses) {
                    targetNodeId = bootstrapPolicy.selectNode(num, node.getNodeIds(), nodeStatuses);
                }

                // 1. create sub task for the partition
                TaskID taskId = new TaskID(job.getId(), num, true);
                Path inputPath = job.getPath().append(taskId + "-input");

                // 3. write partition to node's file system
                final FileSystemService fs = node.getFileSystemService(targetNodeId);

                Partition partition = writePartition(partitioner, inputPath, fs);

                // 4. write job file if not already written
                if (!nodesWithJar.contains(targetNodeId)) {
                    InputStream fis = new FileInputStream(jarFile);
                    try {
                        fs.write(job.getJarPath(), fis);
                    } finally {
                        fis.close();
                    }
                    nodesWithJar.add(targetNodeId);
                }

                MapTask task = new MapTask(taskId, partition, inputPath);
                job.addTask(task);
                // 5. create and register task attempt
                TaskAttemptID attemptID = task.nextAttemptID();
                Path outputPath = job.getPath().append(attemptID.toQualifiedString(1) + "-output");
                TaskAttempt attempt = new TaskAttempt(attemptID, targetNodeId, outputPath);
                task.addAttempt(attempt);

                // 6. submit previous task
                if (previous != null)
                    submitMapTaskAttempt(job, previous, previousAttempt);
                previous = task;
                previousAttempt = attempt;
                num++;
            }
            // submit last task attempt
            if (previous != null)
                submitMapTaskAttempt(job, previous, previousAttempt);
        } finally {
            is.close();
        }
    }

    /**
     * the only purpose of this complex chunk of code is to turn the
     * OutputStream the partitioner needs into an InputStream for the file
     * system
     */
    private Partition writePartition(final Partitioner<?> partitioner, Path inputPath, final FileSystemService fs)
        throws IOException {
        Partition partition;
        final PipedOutputStream pos = new PipedOutputStream();
        try {
            final PipedInputStream pis = new PipedInputStream(pos);
            Future<Partition> future = node.getExecutorService().submit(new Callable<Partition>() {
                @Override
                public Partition call() throws IOException {
                    try {
                        return partitioner.writePartition(pos);
                    } finally {
                        pos.close();
                    }
                }
            });
            fs.write(inputPath, pis);
            try {
                partition = future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                Throwable t = e.getCause();
                if (t instanceof IOException)
                    throw (IOException)t;
                if (t instanceof Error)
                    throw (Error)t;
                if (t instanceof RuntimeException)
                    throw (RuntimeException)t;
                throw new RuntimeException(e);
            }
        } finally {
            pos.close();
        }
        return partition;
    }

    private void submitMapTaskAttempt(Job job, MapTask task, TaskAttempt attempt) throws IOException {
        TaskExecutorService taskExecutor = node.getTaskExecutorService(attempt.getTargetNodeID());
        taskExecutor.execute(new TaskExecutorMapTask(attempt.getId(), job.getJarPath(), job.getDescriptor(), attempt
            .getOutputPath(), attempt.getTargetNodeID(), task.getPartition(), task.getInputPath()));
    }

    /**
     * Currently we schedule one reduce task once all map tasks have completed.
     * The framework could be extended to support multiple reducers. In that
     * case each job would output multiple output files that the user would have
     * to aggregate. The map output files are not copied here, but by the reduce
     * task itself when run.
     * 
     * @param job
     * @throws IOException
     */
    private void submitReduceTasks(Job job) throws IOException {
        // 1. create reduce task
        TaskID taskID = new TaskID(job.getId(), 1, false);
        // collect all map output paths
        List<QualifiedPath> inputPaths = new ArrayList<QualifiedPath>();
        synchronized (job) {
            for (MapTask mapTask : job.getMapTasks()) {
                TaskAttempt attempt = mapTask.getSuccessfulAttempt();
                if (attempt == null)
                    throw new IllegalStateException("Map task " + mapTask.getId()
                        + " does not have a succeeded attempt");
                QualifiedPath qPath = new QualifiedPath(attempt.getTargetNodeID(), attempt.getOutputPath());
                inputPaths.add(qPath);
            }
        }
        ReduceTask task = new ReduceTask(taskID, inputPaths);

        // 2. create attempt
        TaskAttemptID attemptId = new TaskAttemptID(taskID, 1);
        Path outputPath = job.getPath().append("output");
        TaskAttempt attempt = new TaskAttempt(attemptId, config.nodeId, outputPath);
        task.addAttempt(attempt);

        // 3. register and submit task
        job.addTask(task);
        submitReduceTaskAttemp(job, task, attempt);
    }

    private void submitReduceTaskAttemp(Job job, ReduceTask task, TaskAttempt attempt) throws IOException {
        TaskExecutorService taskExecutor = node.getTaskExecutorService(attempt.getTargetNodeID());
        taskExecutor.execute(new TaskExecutorReduceTask(attempt.getId(), job.getJarPath(), job.getDescriptor(), attempt
            .getOutputPath(), attempt.getTargetNodeID(), task.getInputPaths()));
    }

    /**
     * @see {@link edu.illinois.cs.mapreduce.JobMangerService#}
     */
    @Override
    public JobStatus getJobStatus(JobID jobID) throws IOException {
        Job job;
        synchronized (jobs) {
            job = jobs.get(jobID);
        }
        return job.toImmutableStatus();
    }

    /**
     * Updates all local jobs with the attempt status sent by a remote node. The
     * contract requires that attempt status objects be sorted by id.
     */
    @Override
    public boolean updateStatus(TaskExecutorStatus status, TaskAttemptStatus[] taskStatuses) throws IOException {
        boolean stateChange = false;
        if (taskStatuses.length > 0) {
            int offset = 0, len = 1;
            JobID jobId = taskStatuses[0].getJobID();
            for (int i = 1; i < taskStatuses.length; i++) {
                JobID current = taskStatuses[i].getJobID();
                if (!current.equals(jobId)) {
                    stateChange |= updateJobStatus(jobId, taskStatuses, offset, len);
                    offset = i;
                    len = 1;
                    jobId = current;
                } else {
                    len++;
                }
            }
            stateChange |= updateJobStatus(jobId, taskStatuses, offset, len);
        }
        synchronized (nodeStatuses) {
            NodeID nodeId = status.getNodeID();
            NodeStatus ns = nodeStatuses.get(nodeId);
            if (ns == null)
                nodeStatuses.put(nodeId, ns = new NodeStatus(status));
            ns.update(status);
            final NodeStatus nodeStatus = ns;
            if (!transferring && nodeStatuses.size() > 1
                && transferPolicy.isTransferNeeded(nodeStatus, nodeStatuses.values())) {
                transferring = true;
                node.getExecutorService().submit(new Runnable() {
                    @Override
                    public void run() {
                        rebalance(nodeStatus);
                    }
                });
            }
        }
        return stateChange;
    }

    /**
     * Updates the status of the job for the given ID and schedules a task to
     * start the job's reduce phase if the job was transitioned into the reduce
     * phase.
     * 
     * @param jobId
     * @param statuses
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    private boolean updateJobStatus(JobID jobId, TaskAttemptStatus[] statuses, int offset, int length)
        throws IOException {
        boolean stateChanged;
        final Job job;
        synchronized (jobs) {
            job = jobs.get(jobId);
        }
        synchronized (job) {
            Phase phase = job.getPhase();
            stateChanged = job.updateStatus(statuses, offset, length);
            if (stateChanged && phase != job.getPhase()) {
                node.getExecutorService().submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            submitReduceTasks(job);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
        return stateChanged;
    }

    private void rebalance(NodeStatus nodeStatus) {
        try {
            NodeID source;
            NodeID target;
            synchronized (nodeStatuses) {
                source = locationPolicy.source(nodeStatuses.values());
                target = locationPolicy.target(nodeStatuses.values());
            }
            if (!source.equals(config.nodeId))
                return;
            if (source == target) {
                System.err.println("Same source and target selected");
                return;
            }
            TaskAttempt attempt;
            synchronized (jobs) {
                attempt = selectionPolicy.selectAttempt(source, jobs.values());
            }
            if (attempt == null) {
                System.out.println("No suitable task found to transfer from " + source + " to " + target);
                return;
            }
            transferTask(target, attempt);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            transferring = false;
        }
    }

    /**
     * transfer a task attempt from current node to a different one
     * 
     * @param busy
     * @param free
     * @throws IOException
     */
    private void transferTask(NodeID target, TaskAttempt attempt) throws IOException {
        Job job;
        synchronized (jobs) {
            job = jobs.get(attempt.getJobID());
        }
        TaskID taskId = attempt.getTaskID();
        if (!taskId.isMap())
            throw new IllegalArgumentException("Can currently only transfer map tasks");
        MapTask task = (MapTask)job.getMapTask(taskId);

        NodeID source = attempt.getTargetNodeID();
        FileSystemService sourceFs = node.getFileSystemService(source);
        FileSystemService targetFs = node.getFileSystemService(target);

        // make sure jar is present
        Path jarPath = job.getJarPath();
        if (!targetFs.exists(jarPath))
            targetFs.write(jarPath, sourceFs.read(jarPath));

        // make sure input file is present
        Path inputPath = task.getInputPath();
        if (!targetFs.exists(inputPath))
            targetFs.write(inputPath, sourceFs.read(inputPath));

        // create a new attempt
        TaskAttemptID attemptID = task.nextAttemptID();
        Path outputPath = job.getPath().append(attemptID.toQualifiedString(1) + "-output");
        TaskAttempt newAttempt = new TaskAttempt(attemptID, target, outputPath);
        task.addAttempt(newAttempt);

        submitMapTaskAttempt(job, task, newAttempt);
        try {
            node.getTaskExecutorService(source).cancel(attempt.getId(), 10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // ignore
            e.printStackTrace();
        }
        return;
    }
}
