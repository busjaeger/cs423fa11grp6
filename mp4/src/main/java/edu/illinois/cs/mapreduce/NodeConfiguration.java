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

public class NodeConfiguration {

    public static NodeConfiguration load() throws IOException {
        URL url = JobManager.class.getClassLoader().getResource("node-default.properties");
        return load(url);
    }

    public static NodeConfiguration load(File file) throws IOException {
        return load(file.toURI().toURL());
    }

    public static NodeConfiguration load(URL url) throws IOException {
        Properties config = new Properties();
        InputStream is = url.openStream();
        try {
            config.load(is);
        } finally {
            is.close();
        }
        return load(config);
    }

    public static NodeConfiguration load(Properties props) {
        NodeID nodeId = toNodeID(props.getProperty("node.id", "1"));
        Iterable<NodeID> remoteNodeIds = getNodeIDs(props, "node.remote.ids");
        String registryHost = props.getProperty("node.rmi.registry.host", "localhost");
        int registryPort = toPort(props.getProperty("node.registry.port", "1099"));

        File fsRootDir = new File(props.getProperty("fs.root.dir", "/tmp/node1"));
        int fsPort = toPort(props.getProperty("fs.rmi.port", "60001"));

        int jmPort = toPort(props.getProperty("jm.rmi.port", "60002"));

        int tePort = toPort(props.getProperty("te.rmi.port", "60003"));
        int teNumThreads = toPositiveInt(props.getProperty("te.num.threads", "1"));

        return new NodeConfiguration(nodeId, remoteNodeIds, registryHost, registryPort, jmPort, tePort, teNumThreads,
                                     fsPort, fsRootDir);
    }

    private static int toPort(String value) {
        int port = Integer.parseInt(value);
        if (port <= 0 || port > 65535)
            throw new IllegalArgumentException("invalid " + value + ": out of port range");
        return port;
    }

    private static Iterable<NodeID> getNodeIDs(Properties props, String key) {
        StringTokenizer tokenizer = new StringTokenizer(props.getProperty(key));
        List<NodeID> list = new ArrayList<NodeID>();
        while (tokenizer.hasMoreTokens())
            list.add(toNodeID(tokenizer.nextToken()));
        return Collections.unmodifiableList(list);
    }

    private static int toPositiveInt(String s) {
        int i = Integer.parseInt(s);
        if (i < 0)
            throw new IllegalArgumentException("invalid " + s + ": negative value");
        return i;
    }

    private static NodeID toNodeID(String s) {
        return new NodeID(toPositiveInt(s));
    }

    // node configuration
    public final NodeID nodeId;
    public final Iterable<NodeID> remoteNodeIds;
    public final String registryHost;
    public final int registryPort;

    // job manager configuration
    public final int jmPort;

    // task executor configuration
    public final int tePort;
    public final int teNumThreads;

    // file system configuration
    public final int fsPort;
    public final File fsRootDir;

    NodeConfiguration(NodeID nodeId,
                      Iterable<NodeID> remoteNodeIds,
                      String registryHost,
                      int registryPort,
                      int jmPort,
                      int tePort,
                      int teNumThreads,
                      int fsPort,
                      File fsRootDir) {
        this.nodeId = nodeId;
        this.remoteNodeIds = remoteNodeIds;
        this.registryHost = registryHost;
        this.registryPort = registryPort;
        this.jmPort = jmPort;
        this.tePort = tePort;
        this.teNumThreads = teNumThreads;
        this.fsPort = fsPort;
        this.fsRootDir = fsRootDir;
    }

}
