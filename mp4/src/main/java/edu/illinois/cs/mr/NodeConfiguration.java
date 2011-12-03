package edu.illinois.cs.mr;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import edu.illinois.cs.mapreduce.spi.lib.WaitingTaskSelectionPolicy;
import edu.illinois.cs.mapreduce.spi.lib.IdleTransferPolicy;
import edu.illinois.cs.mapreduce.spi.lib.RoundRobinNodeSelectionPolicy;
import edu.illinois.cs.mapreduce.spi.lib.ScoreBasedLocationPolicy;
import edu.illinois.cs.mr.jm.JobManager;

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
        int port = toPort(props.getProperty("node.port", "60001"));
        Map<NodeID, Endpoint> nodeMap = getNodeMap(props, "node.peers");

        File fsRootDir = new File(props.getProperty("fs.root.dir", "/tmp/node1"));

        int teNumThreads = toPositiveInt(props.getProperty("te.num.threads", "1"));
        double teThrottle = Double.parseDouble(props.getProperty("te.throttle", "0.0"));
        int teStatusUpdateInterval = toPositiveInt(props.getProperty("te.status.update.interval", "5000"));

        String lbBootstrapPolicyClass =
            props.getProperty("lb.boostrap.policy", RoundRobinNodeSelectionPolicy.class.getName());
        String lbTransferPolicyClass = props.getProperty("lb.transfer.policy", IdleTransferPolicy.class.getName());
        String lbLocationPolicyClass =
            props.getProperty("lb.location.policy", ScoreBasedLocationPolicy.class.getName());
        String lbSelectionPolicyClass =
            props.getProperty("lb.selection.policy", WaitingTaskSelectionPolicy.class.getName());
        int lbStatusUpdateInterval = toPositiveInt(props.getProperty("lb.status.update.interval", "5000"));

        return new NodeConfiguration(nodeId, port, nodeMap, lbBootstrapPolicyClass, lbTransferPolicyClass,
                                     lbLocationPolicyClass, lbSelectionPolicyClass, lbStatusUpdateInterval,
                                     teNumThreads, teThrottle, teStatusUpdateInterval, fsRootDir);
    }

    private static int toPort(String value) {
        int port = Integer.parseInt(value);
        if (port <= 0 || port > 65535)
            throw new IllegalArgumentException("invalid " + value + ": out of port range");
        return port;
    }

    private static Map<NodeID, Endpoint> getNodeMap(Properties props, String key) {
        StringTokenizer tokenizer = new StringTokenizer(props.getProperty(key));
        Map<NodeID, Endpoint> map = new HashMap<NodeID, Endpoint>();
        while (tokenizer.hasMoreTokens()) {
            String node = tokenizer.nextToken();
            String[] parts = node.split(":");
            NodeID nodeId = toNodeID(parts[0]);
            String host = parts[1];
            int port = toPort(parts[2]);
            map.put(nodeId, new Endpoint(host, port));
        }
        return Collections.unmodifiableMap(map);
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

    static class Endpoint {
        public final String host;
        public final int port;

        public Endpoint(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    // node configuration
    public final NodeID nodeId;
    public final int port;
    public final Map<NodeID, Endpoint> nodeMap;

    public final String lbBootstrapPolicyClass;
    public final String lbTransferPolicyClass;
    public final String lbLocationPolicyClass;
    public final String lbSelectionPolicyClass;
    public final int lbStatusUpdateInterval;

    // task executor configuration
    public final int teNumThreads;
    public final double teThrottle;
    public final int teStatusUpdateInterval;

    // file system configuration
    public final File fsRootDir;

    public NodeConfiguration(NodeID nodeId,
                             int port,
                             Map<NodeID, Endpoint> nodeMap,
                             String lbBootstrapPolicyClass,
                             String lbTransferPolicyClass,
                             String lbLocationPolicyClass,
                             String lbSelectionPolicyClass,
                             int lbStatusUpdateInterval,
                             int teNumThreads,
                             double teThrottle,
                             int teStatusUpdateInterval,
                             File fsRootDir) {
        this.nodeId = nodeId;
        this.port = port;
        this.nodeMap = nodeMap;
        this.lbBootstrapPolicyClass = lbBootstrapPolicyClass;
        this.lbTransferPolicyClass = lbTransferPolicyClass;
        this.lbLocationPolicyClass = lbLocationPolicyClass;
        this.lbSelectionPolicyClass = lbSelectionPolicyClass;
        this.lbStatusUpdateInterval = lbStatusUpdateInterval;
        this.teNumThreads = teNumThreads;
        this.teThrottle = teThrottle;
        this.teStatusUpdateInterval = teStatusUpdateInterval;
        this.fsRootDir = fsRootDir;
    }

}
