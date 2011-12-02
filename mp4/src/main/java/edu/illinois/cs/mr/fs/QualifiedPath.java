package edu.illinois.cs.mr.fs;

import edu.illinois.cs.mr.NodeID;

/**
 * immutable (and therefore thread-safe)
 * 
 * @author benjamin
 */
public class QualifiedPath {

    private final NodeID nodeId;
    private final Path path;

    public QualifiedPath(NodeID nodeId, Path path) {
        this.nodeId = nodeId;
        this.path = path;
    }

    public NodeID getNodeId() {
        return nodeId;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "QualifiedPath [nodeId=" + nodeId + ", path=" + path + "]";
    }

}
