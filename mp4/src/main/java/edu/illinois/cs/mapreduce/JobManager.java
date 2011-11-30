package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import edu.illinois.cs.mapreduce.Job.Phase;
import edu.illinois.cs.mapreduce.Status.State;
import edu.illinois.cs.mapreduce.api.InputFormat;
import edu.illinois.cs.mapreduce.api.Partition;
import edu.illinois.cs.mapreduce.api.Partitioner;

/**
 * This job manager splits jobs into individual tasks and distributes them to
 * the TaskExecutors in the cluster. It derives job status from periodic status
 * updates from the TaskExecutors.
 */
public class JobManager implements JobManagerService {

    private final NodeID nodeId;
    private Cluster cluster;
    private final ExecutorService executorService;
    private final AtomicInteger counter;
    private final Map<JobID, Job> jobs;

    JobManager(NodeID id) {
        this.nodeId = id;
        this.counter = new AtomicInteger();
        this.jobs = new ConcurrentHashMap<JobID, Job>();
        this.executorService = Executors.newCachedThreadPool();
    }

    public void start(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * @see edu.illinois.cs.mapreduce.JobManagerService#submitJob(java.io.File,
     *      java.io.File)
     */
    @Override
    public JobID submitJob(File jarFile, File inputFile) throws IOException {
        // 1. create job
        JobDescriptor descriptor = JobDescriptor.read(jarFile);
        JobID jobId = new JobID(nodeId, counter.incrementAndGet());
        Job job = new Job(jobId, jarFile.getName(), descriptor);
        jobs.put(jobId, job);
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
            Partitioner<?> partitioner = inputFormat.createPartitioner(is, descriptor.getProperties());
            Set<NodeID> nodesWithJar = new HashSet<NodeID>();
            int num = 0;
            List<NodeID> nodeIds = cluster.getNodeIds();
            MapTask previous = null;
            TaskAttempt previousAttempt = null;
            while (!partitioner.isEOF()) {
                // 1. chose node to run task on
                // current selection policy: round-robin
                // TODO: capacity-based selection policy
                NodeID targetNodeId = nodeIds.get(num % nodeIds.size());

                // 1. create sub task for the partition
                TaskID taskId = new TaskID(job.getId(), num, true);
                Path inputPath = job.getPath().append(taskId + "-input");

                // 3. write partition to node's file system
                FileSystemService fs = cluster.getFileSystemService(targetNodeId);
                OutputStream os = fs.write(inputPath);
                Partition partition;
                try {
                    partition = partitioner.writePartition(os);
                } finally {
                    os.close();
                }

                // 4. write job file if not already written
                if (!nodesWithJar.contains(targetNodeId)) {
                    fs.copy(job.getJarPath(), jarFile);
                    nodesWithJar.add(targetNodeId);
                }

                MapTask task = new MapTask(taskId, partition, inputPath);
                job.addTask(task);
                // 5. create and register task attempt
                TaskAttemptID attemptID = new TaskAttemptID(taskId, task.nextAttemptID());
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

    private void submitMapTaskAttempt(Job job, MapTask task, TaskAttempt attempt) throws IOException {
        TaskExecutorService taskExecutor = cluster.getTaskExecutorService(attempt.getTargetNodeID());
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
        List<MapTask> mapTasks = job.getMapTasks();
        List<QualifiedPath> inputPaths = new ArrayList<QualifiedPath>(mapTasks.size());
        for (MapTask mapTask : mapTasks) {
            TaskAttempt attempt = mapTask.getSuccessfulAttempt();
            if (attempt == null)
                throw new IllegalStateException("Map task " + mapTask.getId() + " does not have a succeeded attempt");
            QualifiedPath qPath = new QualifiedPath(attempt.getTargetNodeID(), attempt.getOutputPath());
            inputPaths.add(qPath);
        }
        ReduceTask task = new ReduceTask(taskID, inputPaths);

        // 2. create attempt
        TaskAttemptID attemptId = new TaskAttemptID(taskID, 1);
        Path outputPath = job.getPath().append("output");
        TaskAttempt attempt = new TaskAttempt(attemptId, nodeId, outputPath);
        task.addAttempt(attempt);

        // 3. register and submit task
        job.addTask(task);
        submitReduceTaskAttemp(job, task, attempt);
    }

    private void submitReduceTaskAttemp(Job job, ReduceTask task, TaskAttempt attempt) throws IOException {
        TaskExecutorService taskExecutor = cluster.getTaskExecutorService(attempt.getTargetNodeID());
        taskExecutor.execute(new TaskExecutorReduceTask(attempt.getId(), job.getJarPath(), job.getDescriptor(), attempt
            .getOutputPath(), attempt.getTargetNodeID(), task.getInputPaths()));
    }

    /**
     * @see {@link edu.illinois.cs.mapreduce.JobMangerService#}
     */
    @Override
    public JobStatus getJobStatus(JobID jobID) throws IOException {
        Job job = jobs.get(jobID);
        return job.toImmutableStatus();
    }

    /**
     * Updates all local jobs with the attempt status sent by a remote node. The
     * contract requires that attempt status objects be sorted by id.
     */
    @Override
    public boolean updateStatus(NodeID srcNodeId, TaskAttemptStatus[] statuses) throws IOException {
        boolean stateChange = false;
        if (statuses.length > 0) {
            int offset = 0, len = 1;
            JobID jobId = statuses[0].getJobID();
            for (int i = 1; i < statuses.length; i++) {
                JobID current = statuses[i].getJobID();
                if (!current.equals(jobId)) {
                    stateChange |= updateJobStatus(jobId, statuses, offset, len);
                    offset = i;
                    len = 1;
                    jobId = current;
                } else {
                    len++;
                }
            }
            stateChange |= updateJobStatus(jobId, statuses, offset, len);
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
     */
    private boolean updateJobStatus(JobID jobId, TaskAttemptStatus[] statuses, int offset, int length) {
        boolean stateChanged;
        final Job job = jobs.get(jobId);
        synchronized (job) {
            Phase phase = job.getPhase();
            stateChanged = job.updateStatus(statuses, offset, length);
            if (stateChanged && phase != job.getPhase()) {
                executorService.submit(new Runnable() {
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

}
