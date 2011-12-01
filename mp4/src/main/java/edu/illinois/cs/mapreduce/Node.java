package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
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

import edu.illinois.cs.mapreduce.NodeConfiguration.Endpoint;

/**
 * The node encapsulates the runtime bootstrap and the remote method invocation
 * implementation.
 * 
 * @author benjamin
 */
public class Node {

    public static void main(String[] args) throws IOException {
        String path = args.length > 1 && args[0].equals("-config") ? args[1] : null;
        Node node = getInstance(path);
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

        Map<NodeID, NodeServices> nodeMap = new HashMap<NodeID, NodeServices>();
        for (Entry<NodeID, Endpoint> node : config.nodeMap.entrySet()) {
            NodeID nodeId = node.getKey();
            Endpoint endpoint = node.getValue();
            nodeMap.put(nodeId, RPCClient.<NodeServices> newProxy(endpoint.host, endpoint.port, NodeServices.class));
        }
        return new Node(config, jobManager, taskExecutor, fileSystem, nodeMap);
    }

    public interface NodeService {
        void start(Node node);

        void stop();
    }

    public interface NodeServices extends JobManagerService, FileSystemService, TaskExecutorService {
        //
    }

    private final NodeConfiguration config;
    private final JobManager jobManager;
    private final TaskExecutor taskExecutor;
    private final FileSystem fileSystem;
    private final ExecutorService executorService;
    private final List<NodeID> nodeIds;
    private final Map<NodeID, NodeServices> nodeMap;
    private boolean started;
    private RPCServer server;

    public Node(NodeConfiguration config,
                JobManager jobManager,
                TaskExecutor taskExecutor,
                FileSystem fileSystem,
                Map<NodeID, NodeServices> nodeMap) {
        this.config = config;
        this.jobManager = jobManager;
        this.taskExecutor = taskExecutor;
        this.fileSystem = fileSystem;
        this.nodeMap = nodeMap;
        this.nodeIds = Collections.unmodifiableList(new ArrayList<NodeID>(nodeMap.keySet()));
        this.executorService = Executors.newCachedThreadPool();
    }

    public synchronized void start() throws IOException {
        if (started)
            return;
        started = true;

        // start services
        NodeServices services = new NodeServicesImpl();
        services.start(this);

        // start server
        ServerSocket serverSocket = new ServerSocket(config.port);
        RPCServer server = new RPCServer(executorService, serverSocket, NodeServices.class, services);
        executorService.submit(server);

        // block thread
        try {
            wait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public synchronized void stop() {
        if (started) {
            server.stop();
            notifyAll();
        }
    }

    public NodeID getID() {
        return config.nodeId;
    }

    public List<NodeID> getNodeIds() {
        return nodeIds;
    }

    public FileSystem getLocalFileSystem() {
        return fileSystem;
    }

    public ExecutorService getExecutorService() {
        return executorService;
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
        public void start(Node node) {
            fileSystem.start(node);
            taskExecutor.start(node);
            jobManager.start(node);
        }

        @Override
        public void stop() {
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
        public boolean updateStatus(TaskExecutorStatus status, TaskAttemptStatus[] statuses) throws IOException {
            return jobManager.updateStatus(status, statuses);
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
        public boolean cancel(TaskAttemptID id, long timeout, TimeUnit unit) throws IOException, TimeoutException {
            return taskExecutor.cancel(id, timeout, unit);
        }

        @Override
        public boolean delete(TaskAttemptID id) throws IOException {
            return taskExecutor.delete(id);
        }

        @Override
        public void setThrottle(double value) throws IOException {
            taskExecutor.setThrottle(value);
        }

    }
}
