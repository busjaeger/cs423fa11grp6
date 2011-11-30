package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.IOException;
import java.rmi.Remote;

/**
 * A JobManagerService manages the execution of jobs.
 * 
 * @author benjamin
 */
public interface JobManagerService extends Remote {

    /**
     * Submits a job for the given job jar file and input file.
     *
     * @param jarFile
     * @param inputFile
     * @return
     * @throws IOException
     */
    JobID submitJob(File jarFile, File inputFile) throws IOException;

    JobStatus getJobStatus(JobID jobID) throws IOException;

    /**
     * 
     * @param status
     * @param statuses  must be sorted by {@link TaskAttemptID}
     * @return
     * @throws IOException
     */
    boolean updateStatus(TaskExecutorStatus status, TaskAttemptStatus[] statuses) throws IOException;

}
