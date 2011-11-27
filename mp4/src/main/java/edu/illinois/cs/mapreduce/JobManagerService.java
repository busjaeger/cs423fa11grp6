package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.IOException;
import java.rmi.Remote;


/**
 * remote interface to job manager
 * 
 * @author benjamin
 *
 */
public interface JobManagerService extends Remote {

    JobID submitJob(File jarFile, File inputFile) throws IOException;

}