package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

public class Configuration {

    public static Configuration load() throws IOException {
        URL url = JobManager.class.getClassLoader().getResource("node-default.properties");
        return load(url);
    }

    public static Configuration load(File file) throws IOException {
        return load(file.toURI().toURL());
    }

    public static Configuration load(URL url) throws IOException {
        Properties config = new Properties();
        InputStream is = url.openStream();
        try {
            config.load(is);
        } finally {
            is.close();
        }
        return load(config);
    }

    public static Configuration load(Properties props) {
        ID nodeId = toID(props.getProperty("node.id", "1"));
        Iterable<ID> remoteNodeIds = getIDs(props, "node.remote.ids");
        String rmiRegistryHost = props.getProperty("node.rmi.registry.host", "localhost");
        int rmiRegistryPort = toPort(props.getProperty("node.registry.port", "1099"));

        File fsRootDir = new File(props.getProperty("fs.root.dir", "/tmp/node1"));
        int fsPort = toPort(props.getProperty("fs.rmi.port", "60001"));

        int jmPort = toPort(props.getProperty("jm.rmi.port", "60002"));

        int tmPort = toPort(props.getProperty("tm.rmi.port", "60003"));

        return new Configuration(nodeId, remoteNodeIds, rmiRegistryHost, rmiRegistryPort, jmPort, tmPort, fsPort,
                                 fsRootDir);
    }

    private static int toPort(String value) {
        int port = Integer.parseInt(value);
        if (port <= 0 || port > 65535)
            throw new IllegalArgumentException("invalid " + value + ": out of port range");
        return port;
    }

    private static Iterable<ID> getIDs(Properties props, String key) {
        StringTokenizer tokenizer = new StringTokenizer(props.getProperty(key));
        List<ID> list = new ArrayList<ID>();
        while (tokenizer.hasMoreTokens())
            list.add(toID(tokenizer.nextToken()));
        return Collections.unmodifiableList(list);
    }

    private static ID toID(String s) {
        int i = Integer.parseInt(s);
        if (i < 0)
            throw new IllegalArgumentException("invalid " + s + ": negative value");
        return new ID(i);
    }

    // node configuration
    public final ID nodeId;
    public final Iterable<ID> remoteNodeIds;
    public final String registryHost;
    public final int registryPort;

    // job manager configuration
    public final int jmPort;

    // task manager configuration
    public final int tmPort;

    // file system configuration
    public final int fsPort;
    public final File fsRootDir;

    public Configuration(ID nodeId,
                         Iterable<ID> remoteNodeIds,
                         String registryHost,
                         int registryPort,
                         int jmPort,
                         int tmPort,
                         int fsPort,
                         File fsRootDir) {
        this.nodeId = nodeId;
        this.remoteNodeIds = remoteNodeIds;
        this.registryHost = registryHost;
        this.registryPort = registryPort;
        this.jmPort = jmPort;
        this.tmPort = tmPort;
        this.fsPort = fsPort;
        this.fsRootDir = fsRootDir;
    }

}
