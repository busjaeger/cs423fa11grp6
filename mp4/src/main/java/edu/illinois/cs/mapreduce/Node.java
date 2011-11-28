package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamClient;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;
import com.sun.corba.se.impl.orbutil.threadpool.TimeoutException;

/**
 * The node encapsulates the runtime bootstrap and the remote method invocation
 * implementation.
 * 
 * @author benjamin
 */
public class Node {

    public static NodeConfiguration config;
    private static Registry registry;

    public static void main(String[] args) throws IOException {
        String path = args.length > 1 && args[0].equals("-config") ? args[1] : null;
        init(path);

        // file systems
        FileSystem fileSystem = new FileSystem(config.fsRootDir);
        Map<NodeID, FileSystemService> fileSystems = newMap(config.nodeId, (FileSystemService)fileSystem);
        for (NodeID remoteNodeId : config.remoteNodeIds)
            fileSystems.put(remoteNodeId, new RemoteFileSystemAdapter(new LazyRemoteFileSystem(remoteNodeId)));

        // task executors
        TaskExecutor taskExecutor = new TaskExecutor(config.teNumThreads);
        Map<NodeID, TaskExecutorService> taskExecutors = newMap(config.nodeId, (TaskExecutorService)taskExecutor);
        for (NodeID remoteNodeId : config.remoteNodeIds)
            taskExecutors.put(remoteNodeId, new LazyTaskExecutorService(remoteNodeId));

        // job managers
        JobManager jobManager = new JobManager(config.nodeId);
        Map<NodeID, JobManagerService> jobManagers = newMap(config.nodeId, (JobManagerService)jobManager);
        for (NodeID remoteNodeId : config.remoteNodeIds)
            jobManagers.put(remoteNodeId, new LazyJobManagerService(remoteNodeId));

        ArrayList<NodeID> nodeIDs = new ArrayList<NodeID>(config.remoteNodeIds);
        nodeIDs.add(config.nodeId);
        Cluster cluster = new Cluster(nodeIDs, fileSystems, taskExecutors, jobManagers);
        taskExecutor.start(cluster);
        jobManager.start(cluster);

        rebind(RemoteFileSystem.class, new FileSystemServiceAdapter(fileSystem), config.fsPort);
        rebind(TaskExecutorService.class, taskExecutor, config.tePort);
        rebind(JobManagerService.class, jobManager, config.jmPort);

        System.out.println("node " + config.nodeId + " started");
    }

    public static void init(String cfgPath) throws IOException {
        config = cfgPath == null ? NodeConfiguration.load() : NodeConfiguration.load(new File(cfgPath));
        registry = LocateRegistry.getRegistry(config.registryHost, config.registryPort);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Remote> T lookup(NodeID nodeId, Class<T> type) throws IOException {
        String name = name(nodeId, type);
        try {
            return (T)registry.lookup(name);
        } catch (NotBoundException e) {
            throw new ServiceUnavailableException(e);
        }
    }

    private static <T extends Remote> void rebind(Class<T> type, T obj, int port) throws IOException {
        Remote remote = UnicastRemoteObject.exportObject(obj, port);
        String name = name(config.nodeId, type);
        System.out.println("activating service: "+name);
        registry.rebind(name, remote);
    }

    private static String name(NodeID nodeId, Class<?> type) {
        return "/" + nodeId + "/" + type.getName();
    }

    private static <K, V> Map<K, V> newMap(K key, V value) {
        Map<K, V> map = new HashMap<K, V>();
        map.put(key, value);
        return map;
    }

    /*
     * below are a bunch of adapters to export our services via RMI
     */

    /**
     * The file system interface cannot be exported directly via RMI, because
     * Input/Output streams are not serializable. We use RMIIO library to wrap
     * streams, so we define a wrapper interface and adapters here.
     * 
     * @author benjamin
     */
    public static interface RemoteFileSystem extends Remote {

        RemoteInputStream read(Path path) throws RemoteException, IOException;

        RemoteOutputStream write(Path path) throws RemoteException, IOException;

        void copy(Path dest, RemoteInputStream src) throws RemoteException, IOException;

        boolean mkdir(Path path) throws IOException;

        boolean delete(Path path) throws IOException;

        boolean exists(Path path) throws IOException;
    }

    /**
     * When a local node is started, the remote services may not have been
     * started yet. With RMI we would get an exception if we try to lookup the
     * services before they are bound. We use a lazy proxy to shield users of
     * the remote file system from this implementation detail. Note that each
     * method throws a ServiceUnavailableException, so that our implementation
     * could handle the case that a service is (temporarily) not available at
     * the time of use.
     * 
     * @author benjamin
     */
    private static class LazyProxy<T extends Remote> {
        private final NodeID nodeId;
        private final Class<T> type;
        private T delegate;

        protected LazyProxy(NodeID nodeId, Class<T> type) {
            this.nodeId = nodeId;
            this.type = type;
        }

        protected T getDelegate() throws IOException {
            if (delegate == null)
                delegate = lookup(nodeId, type);
            return delegate;
        }
    }

    static class LazyRemoteFileSystem extends LazyProxy<RemoteFileSystem> implements RemoteFileSystem {
        protected LazyRemoteFileSystem(NodeID nodeId) {
            super(nodeId, RemoteFileSystem.class);
        }

        @Override
        public RemoteInputStream read(Path path) throws RemoteException, IOException {
            return getDelegate().read(path);
        }

        @Override
        public RemoteOutputStream write(Path path) throws RemoteException, IOException {
            return getDelegate().write(path);
        }

        @Override
        public void copy(Path dest, RemoteInputStream src) throws RemoteException, IOException {
            getDelegate().copy(dest, src);
        }

        @Override
        public boolean mkdir(Path path) throws IOException {
            return getDelegate().mkdir(path);
        }

        @Override
        public boolean delete(Path path) throws IOException {
            return getDelegate().delete(path);
        }

        @Override
        public boolean exists(Path path) throws IOException {
            return getDelegate().exists(path);
        }
    }

    static class LazyTaskExecutorService extends LazyProxy<TaskExecutorService> implements TaskExecutorService {
        protected LazyTaskExecutorService(NodeID nodeId) {
            super(nodeId, TaskExecutorService.class);
        }

        @Override
        public void execute(TaskAttempt attempt) throws IOException {
            getDelegate().execute(attempt);
        }

        @Override
        public boolean cancel(TaskAttemptID id, long timeout, TimeUnit unit) throws IOException, TimeoutException {
            return getDelegate().cancel(id, timeout, unit);
        }

        @Override
        public boolean delete(TaskAttemptID id) throws IOException {
            return getDelegate().delete(id);
        }
    }

    static class LazyJobManagerService extends LazyProxy<JobManagerService> implements JobManagerService {
        protected LazyJobManagerService(NodeID nodeId) {
            super(nodeId, JobManagerService.class);
        }

        @Override
        public JobID submitJob(File jarFile, File inputFile) throws IOException {
            return getDelegate().submitJob(jarFile, inputFile);
        }

        @Override
        public boolean updateJobStatuses(TaskAttemptStatus[] statuses) throws IOException {
            return getDelegate().updateJobStatuses(statuses);
        }

        @Override
        public JobStatus getJobStatus(JobID jobID) throws IOException {
            return getDelegate().getJobStatus(jobID);
        }
    }

    /**
     * Adapt FileSystemService so that it can be published via RMI as a remote
     * file system.
     * 
     * @author benjamin
     */
    public static class FileSystemServiceAdapter implements RemoteFileSystem {

        private final FileSystemService fileSystemService;

        public FileSystemServiceAdapter(FileSystemService fileSystemService) {
            this.fileSystemService = fileSystemService;
        }

        @Override
        public RemoteInputStream read(Path path) throws RemoteException, IOException {
            InputStream is = fileSystemService.read(path);
            return new SimpleRemoteInputStream(is).export();
        }

        @Override
        public RemoteOutputStream write(Path path) throws RemoteException, IOException {
            OutputStream os = fileSystemService.write(path);
            return new SimpleRemoteOutputStream(os).export();
        }

        @Override
        public void copy(Path dest, RemoteInputStream src) throws IOException {
            InputStream is = RemoteInputStreamClient.wrap(src);
            fileSystemService.copy(dest, is);
        }

        @Override
        public boolean mkdir(Path path) throws IOException {
            return fileSystemService.mkdir(path);
        }

        @Override
        public boolean delete(Path path) throws IOException {
            return fileSystemService.delete(path);
        }

        @Override
        public boolean exists(Path path) throws IOException {
            return fileSystemService.exists(path);
        }
    }

    /**
     * Adapt a RemoteFileSystem so that it can be used locally via the
     * FileSystemService interface
     * 
     * @author benjamin
     */
    public static class RemoteFileSystemAdapter implements FileSystemService {

        private final RemoteFileSystem remoteFileSystem;

        public RemoteFileSystemAdapter(RemoteFileSystem remoteFileSystem) {
            this.remoteFileSystem = remoteFileSystem;
        }

        @Override
        public URL toURL(Path jarPath) throws IOException {
            throw new UnsupportedOperationException("toURL currently not supported");
        }

        @Override
        public InputStream read(Path path) throws IOException {
            RemoteInputStream ris = remoteFileSystem.read(path);
            return RemoteInputStreamClient.wrap(ris);
        }

        @Override
        public OutputStream write(Path path) throws IOException {
            RemoteOutputStream ros = remoteFileSystem.write(path);
            return RemoteOutputStreamClient.wrap(ros);
        }

        @Override
        public void copy(Path dest, File src) throws IOException {
            FileInputStream is = new FileInputStream(src);
            try {
                copy(dest, is);
            } finally {
                is.close();
            }
        }

        @Override
        public void copy(Path dest, InputStream is) throws IOException {
            RemoteInputStream ris = new SimpleRemoteInputStream(is);
            remoteFileSystem.copy(dest, ris);
        }

        @Override
        public boolean mkdir(Path path) throws IOException {
            return remoteFileSystem.mkdir(path);
        }

        @Override
        public boolean delete(Path path) throws IOException {
            return remoteFileSystem.delete(path);
        }

        @Override
        public boolean exists(Path path) throws IOException {
            return remoteFileSystem.exists(path);
        }

    }
}
