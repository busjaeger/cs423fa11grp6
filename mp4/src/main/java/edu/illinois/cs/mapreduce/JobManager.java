package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

    private final NodeConfiguration config;
    private Node node;
    private final AtomicInteger counter;
    private final Map<JobID, Job> jobs;
    private Hashtable<NodeID, ArrayList<TaskExecutorStatus>> NodeHealth; 
    public static final int NODE_HEALTH_HISTORY = 5;

    JobManager(NodeConfiguration config) {
        this.config = config;
        this.counter = new AtomicInteger();
        this.jobs = new ConcurrentHashMap<JobID, Job>();
        this.NodeHealth = new Hashtable<NodeID, ArrayList<TaskExecutorStatus>>();
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
     * @return
     */
    public JobID[] getJobIDs()  {
        return (JobID[]) jobs.keySet().toArray();
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
            final Partitioner<?> partitioner = inputFormat.createPartitioner(is, descriptor.getProperties());
            Set<NodeID> nodesWithJar = new HashSet<NodeID>();
            int num = 0;
            List<NodeID> nodeIds = node.getNodeIds();
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
                final FileSystemService fs = node.getFileSystemService(targetNodeId);

                // this complex chunk of code is just to present an output
                // stream interface to partitioner
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
                    throw new IllegalStateException("Map task " + mapTask.getId() + " does not have a succeeded attempt");
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
        Job job = jobs.get(jobID);
        return job.toImmutableStatus();
    }

    /**
     * Updates all local jobs with the attempt status sent by a remote node. The
     * contract requires that attempt status objects be sorted by id.
     */
    @Override
    public boolean updateStatus(TaskExecutorStatus status, TaskAttemptStatus[] taskStatuses) throws IOException {
        boolean stateChange = false;
        
        //TODO: OK here?
        UpdateNodeHealth(status);
        
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
        
        //TODO: Is this a good place to check for rebalancing?
        JobStatus js = job.toImmutableStatus();
        CheckLoadDistribution(js);
        
        return stateChanged;
    }
    
    /**
     * Reviews the stored node status information and determines if a rebalance 
     * will occur.
     */
    private void CheckLoadDistribution(JobStatus js) {
        NodeID free = null, busy = null;
        
        ArrayList<NodeStatus> idleNodeStats = new ArrayList<NodeStatus>();
        ArrayList<NodeStatus> busyNodeStats = new ArrayList<NodeStatus>();
        
        synchronized(NodeHealth) {
            if (NodeHealth.size()<2)
                return; // Don't have data on other nodes yet, so no decision can be made
            
            // For each node in the list, calculate their average and last queue/cpu stats and store in a list
            for (Enumeration<NodeID> e = NodeHealth.keys(); e.hasMoreElements(); ) {
                ArrayList<TaskExecutorStatus> TaskExecStats = NodeHealth.get(e.nextElement());
                int size = TaskExecStats.size();
                double sumCpu=0;
                double sumQueue=0;
                if(size>0) {
                    for(int i=0; i<TaskExecStats.size(); i++) {
                        sumCpu += TaskExecStats.get(i).getCpuUtilization();
                        sumQueue += TaskExecStats.get(i).getQueueLength();
                    }
                    
                    NodeStatus ns = new NodeStatus(
                           TaskExecStats.get(0).getNodeID(),                    // NodeID
                           TaskExecStats.get(size-1).getCpuUtilization(),       // Last CPU Percentage
                           TaskExecStats.get(size-1).getQueueLength(),          // Last Queue Length 
                           (sumCpu/size),                                       // Average CPU over recorded span
                           (sumQueue/size),                                     // Average Queue Length over recorded span
                           TaskExecStats.get(size-1).getThrottle());            // Last throttle value 
                    if(ns.isIdle())
                        idleNodeStats.add(ns);
                    else
                        busyNodeStats.add(ns);
                }
            }
        }
        
        // Data gathering complete, so the logic can be run outside of the sync block
        // If either all of the nodes are busy or no nodes have queued tasks, no need to rebalance
        if(idleNodeStats.size()>0 && busyNodeStats.size()>0)  
        {
            busy = FindBusiestNode(busyNodeStats);
            free = FindIdlestNode(idleNodeStats);
            assert(busy != free);
            RebalanceTasks(js, busy, free);
        }
    }
    
    /**
     * Searches through list of busy nodes to find the busiest node
     * 
     * @param list of busy nodes' statuses
     * @return NodeID of busiest node
     */
    private NodeID FindBusiestNode(ArrayList<NodeStatus> list) {
        NodeID busiest = null;
        double busiestScore = 0.0;
        for(int i=0; i<list.size(); i++) {
            if(busiest==null) {
                busiest = list.get(i).getNodeID();      // This will always be the case on 2 node clusters
                busiestScore = FindNodeScore(list.get(i));
            }
            else {
                double myScore = FindNodeScore(list.get(i));
                if(myScore > busiestScore) {
                    busiest = list.get(i).getNodeID();
                    busiestScore = myScore;
                }
            }
        }
        return busiest;
    }
    
    /**
     * Searches through list of idle nodes to find the least busy node
     * 
     * @param list of idle nodes' statuses
     * @return NodeID of least busy node
     */
    private NodeID FindIdlestNode(ArrayList<NodeStatus> list) {
        NodeID idlest = null;
        double idlestScore = 1000.0;
        for(int i=0; i<list.size(); i++) {
            if(idlest==null) {
                idlest = list.get(i).getNodeID();
                idlestScore = FindNodeScore(list.get(i));
            }
            else {
                double myScore = FindNodeScore(list.get(i));
                if(myScore < idlestScore) {
                    idlest = list.get(i).getNodeID();
                    idlestScore = myScore;
                }
            }
        }
        return idlest;
    }
    
    /**
     * Gets the node's business score (higher is more busy)
     * 
     * @param ns
     * @return score
     */
    private double FindNodeScore(NodeStatus ns) {
        double averageScore = (ns.getAvgCpuUtilization() + ns.getThrottle() + 1) *
            (ns.getAvgQueueLength() + 1);
        
        double lastScore = (ns.getLastCpuUtilization() + ns.getThrottle() + 1) *
            (ns.getLastQueueLength() + 1);
        
        double score = (lastScore + averageScore) / 2;
        
        return score;
    }
    
    /**
     * Updates the local cache of node health data to be used in determining if
     * a rebalance of tasks is needed.
     * 
     * @param status
     */
    private void UpdateNodeHealth(TaskExecutorStatus status) {
        synchronized(NodeHealth) {
            if (NodeHealth.containsKey(status.getNodeID())) {
                ArrayList<TaskExecutorStatus> statuses = NodeHealth.get(status.getNodeID());
                statuses.add(status);
                if(statuses.size()>NODE_HEALTH_HISTORY) {
                    statuses.remove(0);
                }
                NodeHealth.put(status.getNodeID(), statuses);
            }
            else {
                ArrayList<TaskExecutorStatus> statuses = new ArrayList<TaskExecutorStatus>();
                statuses.add(status);
                NodeHealth.put(status.getNodeID(), statuses);
            }
        }
    }
    
    /**
     * Removes a waiting task from the specified busy node and assigns it to 
     * the free node
     * 
     * @param busy
     * @param free
     */
    private void RebalanceTasks(JobStatus js, NodeID busy, NodeID free) {
        for (TaskStatus task : js.getMapTaskStatuses()) {
            if(task.getState() == State.WAITING)
                for (TaskAttemptStatus attempt : task.getAttemptStatuses()) {
                    if (attempt.getState() == State.WAITING && attempt.getTargetNodeID() == busy) {
                        if(node.getTaskExecutorService(busy).cancel(attempt.getId(), 10, TimeUnit.SECONDS)) {
                            node.getTaskExecutorService(free).execute(task);      //TODO: execute task on free node
                            node.getTaskExecutorService(busy).delete(attempt.getId()); 
                        }
                    }
                }
        }
    }
}
