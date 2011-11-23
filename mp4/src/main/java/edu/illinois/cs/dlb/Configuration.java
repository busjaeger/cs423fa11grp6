package edu.illinois.cs.dlb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {

    private static final String ID = "id";
    private static final String SPLIT_SIZE = "split.size";
    private static final String LOCAL_DIR = "local.dir";
    private static final String RMI_PORT = "rmi.port";
    private static final String RMI_REGISTRY_HOST = "rmi.registry.host";
    private static final String RMI_REGISTRY_PORT = "registry.port";
    private static final String PEER_ID = "peer.id";

    public static Configuration load() throws IOException {
        Properties config = new Properties();
        InputStream is = JobManager.class.getClassLoader().getResourceAsStream("job-manager.properties");
        try {
            config.load(is);
        } finally {
            is.close();
        }
        int id = getPositiveInteger(config, ID);
        long splitSize = getPositiveLong(config, SPLIT_SIZE);
        File localDir = new File(config.getProperty(LOCAL_DIR));
        int rmiPort = getPort(config, RMI_PORT);
        String rmiRegistryHost = getString(config, RMI_REGISTRY_HOST);
        int rmiRegistryPort = getPort(config, RMI_REGISTRY_PORT);
        int peerId = getPositiveInteger(config, PEER_ID);
        return new Configuration(id, splitSize, localDir, rmiPort, rmiRegistryHost, rmiRegistryPort, peerId);
    }

    private static int getPort(Properties props, String name) {
        int port = Integer.parseInt(props.getProperty(name));
        if (port <= 0 || port > 65535)
            throw new IllegalArgumentException("invalid " + name + ": out of range");
        return port;
    }

    private static String getString(Properties props, String name) {
        String s = props.getProperty(name);
        if (s == null || s.length() == 0)
            throw new IllegalArgumentException("invalid " + name + ": empty");
        return s;
    }

    private static int getPositiveInteger(Properties props, String name) {
        int i = Integer.parseInt(props.getProperty(name));
        if (i < 0)
            throw new IllegalArgumentException("invalid " + name + ": negative value");
        return i;
    }

    private static long getPositiveLong(Properties props, String name) {
        long i = Long.parseLong(props.getProperty(name));
        if (i < 0)
            throw new IllegalArgumentException("invalid " + name + ": negative value");
        return i;
    }
    
    private final int id;
    private final long splitSize;
    private final File localDir;
    private final int rmiPort;
    private final String rmiRegistryHost;
    private final int rmiRegistryPort;
    private final int peerId;

    public Configuration(int id,
                         long splitSize,
                         File localDir,
                         int rmiPort,
                         String rmiRegistryHost,
                         int rmiRegistryPort,
                         int peerId) {
        this.id = id;
        this.splitSize = splitSize;
        this.localDir = localDir;
        this.rmiPort = rmiPort;
        this.rmiRegistryHost = rmiRegistryHost;
        this.rmiRegistryPort = rmiRegistryPort;
        this.peerId = peerId;
    }

    public int getId() {
        return id;
    }

    public long getSplitSize() {
        return splitSize;
    }

    public File getLocalDir() {
        return localDir;
    }

    public int getRmiPort() {
        return rmiPort;
    }

    public String getRmiRegistryHost() {
        return rmiRegistryHost;
    }

    public int getRmiRegistryPort() {
        return rmiRegistryPort;
    }

    public int getPeerId() {
        return peerId;
    }

}
