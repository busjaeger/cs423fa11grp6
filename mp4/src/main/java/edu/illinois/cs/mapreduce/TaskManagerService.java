package edu.illinois.cs.mapreduce;

import java.io.IOException;
import java.rmi.Remote;

/**
 * Remote interface to TakManager
 * 
 * @author benjamin
 */
public interface TaskManagerService extends Remote {

    void submitTask(Task task) throws IOException;

}