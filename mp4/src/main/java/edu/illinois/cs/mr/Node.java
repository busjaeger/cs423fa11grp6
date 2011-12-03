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
 * The node encapsulates the runtime bootstrap and the remote method invocation
 * implementation.
 * 
 * @author benjamin
 */
public class Node {

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

    private static final Map<String, Node> instances = new HashMap<String, Node>();

    public static synchronized Node getInstance(String cfgPath) throws IOException {
        Node instance = instances.get(cfgPath);
        if (instance == null)
            instances.put(cfgPath, instance = createNode(cfgPath));
        return instance;
    }

    private static Node createNode(String cfg) throws IOException {
        NodeConfiguration config = (cfg == null) ? NodeConfiguration.load() : NodeConfiguration.load(new File(cfg));
        FileSystem fileSystem = new FileSystem(config);
        TaskExecutor taskExecutor = new TaskExecutor(config);
        JobManager jobManager = new JobManager(config);
        LoadBalancer loadBalancer = new LoadBalancer(config);

        Map<NodeID, NodeServices> nodeMap = new HashMap<NodeID, NodeServices>();
        for (Entry<NodeID, Endpoint> node : config.nodeMap.entrySet()) {
            NodeID nodeId = node.getKey();
            Endpoint endpoint = node.getValue();
            nodeMap.put(nodeId, RPC.newClient(endpoint.host, endpoint.port, NodeServices.class));
        }
        return new Node(config, loadBalancer, jobManager, taskExecutor, fileSystem, nodeMap);
    }

    public interface NodeService {
        void start(Node node);

        void stop();
    }

    public interface NodeServices extends JobManagerService, FileSystemService, TaskExecutorService,
        LoadBalancerService {
        public void stopNode();
        public NodeID getNodeID();
    }

    private final NodeConfiguration config;
    private final LoadBalancer loadBalancer;
    private final JobManager jobManager;
    private final TaskExecutor taskExecutor;
    private final FileSystem fileSystem;
    private final ExecutorService executorService;
    private final List<NodeID> nodeIds;
    private final Map<NodeID, NodeServices> nodeMap;
    private final NodeServices services;

    private boolean started;
    private RPCServer server;

    public Node(NodeConfiguration config,
                LoadBalancer loadBalancer,
                JobManager jobManager,
                TaskExecutor taskExecutor,
                FileSystem fileSystem,
                Map<NodeID, NodeServices> nodeMap) {
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
        this.services = new NodeServicesImpl();
    }

    public synchronized void start() throws IOException {
        if (started)
            return;

        // start server
        server = RPC.newServer(executorService, config.port, NodeServices.class, services);
        server.start();
        // start services
        services.start(this);

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

    public synchronized void stop() {
        if (started) {
            services.stop();
            server.stop();
            executorService.shutdown();
            started = false;
            notifyAll();
        }
    }

    public NodeID getID() {
        return config.nodeId;
    }

    public List<NodeID> getNodeIds() {
        return nodeIds;
    }

    public ExecutorService getExecutorService() {
        return executorService;
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

    class NodeServicesImpl implements NodeServices {

        @Override
        public void stopNode() {
            Node.this.stop();
        }
        
        @Override
        public NodeID getNodeID() {
            return Node.this.getID();
        }

        @Override
        public void start(Node node) {
            fileSystem.start(node);
            taskExecutor.start(node);
            jobManager.start(node);
            loadBalancer.start(node);
        }

        @Override
        public void stop() {
            loadBalancer.stop();
            jobManager.stop();
            taskExecutor.stop();
            fileSystem.stop();
        }

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
}
