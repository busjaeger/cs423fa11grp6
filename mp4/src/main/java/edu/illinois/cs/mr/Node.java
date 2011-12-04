package edu.illinois.cs.mr;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.illinois.cs.mr.NodeConfiguration.Endpoint;
import edu.illinois.cs.mr.fs.FileSystem;
import edu.illinois.cs.mr.fs.FileSystemService;
import edu.illinois.cs.mr.fs.Path;
import edu.illinois.cs.mr.jm.AttemptID;
import edu.illinois.cs.mr.jm.AttemptStatus;
import edu.illinois.cs.mr.jm.JobID;
import edu.illinois.cs.mr.jm.JobManager;
import edu.illinois.cs.mr.jm.JobManagerService;
import edu.illinois.cs.mr.jm.JobStatus;
import edu.illinois.cs.mr.lb.LoadBalancer;
import edu.illinois.cs.mr.lb.LoadBalancerService;
import edu.illinois.cs.mr.lb.NodeStatusSnapshot;
import edu.illinois.cs.mr.te.TaskExecutor;
import edu.illinois.cs.mr.te.TaskExecutorService;
import edu.illinois.cs.mr.te.TaskExecutorTask;
import edu.illinois.cs.mr.util.RPC;
import edu.illinois.cs.mr.util.RPC.RPCServer;

/**
 * A node represents a configured runtime instance. It is uniquely identified by
 * a numerical ID and listens on its own network port.
 * 
 * @author benjamin
 */
public class Node implements NodeService {

    /**
     * Main method to start a new node. If no configuration file is specified,
     * the default is used.
     * 
     * @param args empty or one argument specifying the configuration file
     * @throws IOException if an error occurred reading the configuration or
     *             starting the Node
     */
    public static void main(String[] args) throws IOException {
        String configPath = null;
        if (args.length == 1) {
            configPath = args[0];
        } else if (args.length > 1) {
            System.out.println("no more than one argument allowed");
            System.exit(1);
        }
        Node node = getInstance(configPath);
        node.start();
    }

    /** holds nodes by configuration within this JVM */
    private static final Map<String, Node> instances = new HashMap<String, Node>();

    public static synchronized Node getInstance(String cfgPath) throws IOException {
        Node instance = instances.get(cfgPath);
        if (instance == null)
            instances.put(cfgPath, instance = createNode(cfgPath));
        return instance;
    }

    /**
     * Creates a new (stopped) node for the given configuration. The services
     * offered by this node are hard-coded here, i.e. there is no dynamic
     * registration facility. This method also creates the RPC client stubs for
     * all remote nodes listed in the node configuration.
     * 
     * @param cfg path to configuration file
     * @return created node
     * @throws IOException if an IOException occurs loading the configuration
     */
    private static Node createNode(String cfg) throws IOException {
        NodeConfiguration config = (cfg == null) ? NodeConfiguration.load() : NodeConfiguration.load(new File(cfg));
        FileSystem fileSystem = new FileSystem(config);
        TaskExecutor taskExecutor = new TaskExecutor(config);
        JobManager jobManager = new JobManager(config);
        LoadBalancer loadBalancer = new LoadBalancer(config);

        Map<NodeID, NodeService> nodeMap = new HashMap<NodeID, NodeService>();
        for (Entry<NodeID, Endpoint> node : config.nodeMap.entrySet()) {
            NodeID nodeId = node.getKey();
            Endpoint endpoint = node.getValue();
            nodeMap.put(nodeId, RPC.newClient(endpoint.host, endpoint.port, NodeService.class));
        }

        return new Node(config, loadBalancer, jobManager, taskExecutor, fileSystem, nodeMap);
    }

    private final NodeConfiguration config;
    private final LoadBalancer loadBalancer;
    private final JobManager jobManager;
    private final TaskExecutor taskExecutor;
    private final FileSystem fileSystem;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final List<NodeID> nodeIds;
    private final Map<NodeID, NodeService> nodeMap;
    private final RPCServer server;

    private boolean started;

    public Node(NodeConfiguration config,
                LoadBalancer loadBalancer,
                JobManager jobManager,
                TaskExecutor taskExecutor,
                FileSystem fileSystem,
                Map<NodeID, NodeService> nodeMap) {
        this.config = config;
        this.loadBalancer = loadBalancer;
        this.jobManager = jobManager;
        this.taskExecutor = taskExecutor;
        this.fileSystem = fileSystem;
        this.nodeMap = nodeMap;
        List<NodeID> ids = new ArrayList<NodeID>(nodeMap.keySet());
        ids.add(config.nodeId);
        this.nodeIds = Collections.unmodifiableList(ids);
        this.executorService = Executors.newCachedThreadPool();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(2);
        this.server = RPC.newServer(executorService, config.port, NodeService.class, this);
    }

    /**
     * Starts this node
     *
     * @throws IOException if an error occurred during startup
     */
    public synchronized void start() throws IOException {
        if (started)
            return;

        // start server
        server.start();

        // start services
        taskExecutor.start(this);
        jobManager.start(this);
        loadBalancer.start(this);

        System.out.println("node " + config.nodeId + " started");
        started = true;
        // block thread
        try {
            wait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("node " + config.nodeId + " stopped");
    }

    /**
     * Stops this node
     */
    @Override
    public synchronized void stop() {
        if (started) {
            loadBalancer.stop();
            jobManager.stop();
            taskExecutor.stop();
            scheduledExecutorService.shutdown();
            server.stop();
            executorService.shutdown();
            started = false;
            notifyAll();
        }
    }

    public NodeID getId() {
        return config.nodeId;
    }

    public List<NodeID> getNodeIds() {
        return nodeIds;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    public JobManager getJobManager() {
        return jobManager;
    }

    public TaskExecutor getTaskExecutor() {
        return taskExecutor;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public LoadBalancerService getLoadBalancerService(NodeID nodeId) {
        if (this.config.nodeId.equals(nodeId))
            return loadBalancer;
        return nodeMap.get(nodeId);
    }

    public JobManagerService getJobManagerService(NodeID nodeId) {
        if (this.config.nodeId.equals(nodeId))
            return jobManager;
        return nodeMap.get(nodeId);
    }

    public TaskExecutorService getTaskExecutorService(NodeID nodeId) {
        if (this.config.nodeId.equals(nodeId))
            return taskExecutor;
        return nodeMap.get(nodeId);
    }

    public FileSystemService getFileSystemService(NodeID nodeId) {
        if (this.config.nodeId.equals(nodeId))
            return fileSystem;
        return nodeMap.get(nodeId);
    }

    // delegation methods

    @Override
    public JobID submitJob(File jarFile, File inputFile) throws IOException {
        return jobManager.submitJob(jarFile, inputFile);
    }

    @Override
    public JobStatus getJobStatus(JobID jobID) throws IOException {
        return jobManager.getJobStatus(jobID);
    }

    @Override
    public JobID[] getJobIDs() {
        return jobManager.getJobIDs();
    }

    @Override
    public boolean updateStatus(AttemptStatus[] statuses) throws IOException {
        return jobManager.updateStatus(statuses);
    }

    @Override
    public boolean writeOutput(JobID jobID, File file) throws IOException {
        return jobManager.writeOutput(jobID, file);
    }

    @Override
    public InputStream read(Path path) throws IOException {
        return fileSystem.read(path);
    }

    @Override
    public void write(Path dest, InputStream is) throws IOException {
        fileSystem.write(dest, is);
    }

    @Override
    public boolean mkdir(Path path) throws IOException {
        return fileSystem.mkdir(path);
    }

    @Override
    public boolean delete(Path path) throws IOException {
        return fileSystem.delete(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return fileSystem.exists(path);
    }

    @Override
    public void execute(TaskExecutorTask task) throws IOException {
        taskExecutor.execute(task);
    }

    @Override
    public boolean cancel(AttemptID id, long timeout, TimeUnit unit) throws IOException, TimeoutException {
        return taskExecutor.cancel(id, timeout, unit);
    }

    @Override
    public boolean delete(AttemptID id) throws IOException {
        return taskExecutor.delete(id);
    }

    @Override
    public void setThrottle(double value) throws IOException {
        taskExecutor.setThrottle(value);
    }

    @Override
    public boolean updateStatus(NodeStatusSnapshot nodeStatus) throws IOException {
        return loadBalancer.updateStatus(nodeStatus);
    }

}
