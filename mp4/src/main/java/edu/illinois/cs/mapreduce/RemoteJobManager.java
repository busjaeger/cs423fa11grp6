package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import edu.illinois.cs.mapreduce.Job.JobID;

public interface RemoteJobManager extends Remote {

    JobID submitJob(File jarFile, File inputFile) throws IOException, RemoteException;

}