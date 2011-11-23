package edu.illinois.cs.dlb;

import java.io.File;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import edu.illinois.cs.dlb.api.JobID;

public interface JobClient extends Remote {

    JobID submitJob(File jarFile, File inputFile, File outputFile) throws IOException, RemoteException;

    void deleteJob(JobID jobId) throws IOException, RemoteException;

}