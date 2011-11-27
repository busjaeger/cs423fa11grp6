package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamClient;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;

/**
 * The node encapsulates the runtime bootstrap and the remote method invocation
 * implementation.
 * 
 * @author benjamin
 */
public class Node {

    public static Configuration config;
    private static Registry registry;

    public static void main(String[] args) throws IOException {
        String path = args.length > 1 && args[0].equals("-config") ? args[1] : null;
        init(path);

        // file systems
        FileSystem fileSystem = new FileSystem(config.fsRootDir);
        Map<ID, FileSystemService> fileSystems = newMap(config.nodeId, (FileSystemService)fileSystem);
        for (ID remoteNodeId : config.remoteNodeIds)
            fileSystems.put(remoteNodeId, new RemoteFileSystemAdapter(new LazyRemoteFileSystem(remoteNodeId)));

        // task managers
        TaskManagerService taskManager = new TaskManager(fileSystem);
        Map<ID, TaskManagerService> taskManagers = newMap(config.nodeId, taskManager);
        for (ID remoteNodeId : config.remoteNodeIds)
            taskManagers.put(remoteNodeId, new LazyTaskManagerService(remoteNodeId));

        // job managers
        JobManagerService jobManager = new JobManager(config.nodeId, taskManagers, fileSystems);

        rebind(RemoteFileSystem.class, new FileSystemServiceAdapter(fileSystem), config.fsPort);
        rebind(TaskManagerService.class, taskManager, config.tmPort);
        rebind(JobManagerService.class, jobManager, config.jmPort);

        System.out.println("node " + config.nodeId + " started");
    }

    public static void init(String cfgPath) throws IOException {
        config = cfgPath == null ? Configuration.load() : Configuration.load(new File(cfgPath));
        registry = LocateRegistry.getRegistry(config.registryHost, config.registryPort);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Remote> T lookup(ID nodeId, Class<T> type) throws IOException {
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
        registry.rebind(name, remote);
    }

    private static String name(ID nodeId, Class<?> type) {
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
        private final ID nodeId;
        private final Class<T> type;
        private T delegate;

        protected LazyProxy(ID nodeId, Class<T> type) {
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
        protected LazyRemoteFileSystem(ID nodeId) {
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
    }

    static class LazyTaskManagerService extends LazyProxy<TaskManagerService> implements TaskManagerService {
        protected LazyTaskManagerService(ID nodeId) {
            super(nodeId, TaskManagerService.class);
        }

        @Override
        public void submitTask(Task task) throws IOException {
            getDelegate().submitTask(task);
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
    }
}
