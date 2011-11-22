package edu.illinois.cs.dlb;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import edu.illinois.cs.dlb.api.ID;
import edu.illinois.cs.dlb.api.Job;

public interface JobManager extends Remote {

    // client APIs
    
    ID submitJob(Job job) throws IOException, RemoteException;

    
    // inter job manager APIs

}