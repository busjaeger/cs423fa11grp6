package edu.illinois.cs.mr;

import java.io.IOException;

/**
 * Services can implement this interface to receive notifications when the node
 * is started and stopped to obtain a handle to the node and create/destroy any
 * internal state.
 * 
 * @author benjamin
 */
public interface NodeListener {

    /**
     * Starts the node service
     * 
     * @param node
     */
    void start(Node node) throws IOException;

    /**
     * stops the node service
     */
    void stop();

}
