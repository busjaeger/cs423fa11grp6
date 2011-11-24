package edu.illinois.cs.dlb;

import java.io.File;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import edu.illinois.cs.dlb.Job.JobID;

public interface JobManager extends Remote {

    JobID submitJob(File jarFile, File inputFile) throws IOException, RemoteException;

}