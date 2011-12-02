package edu.illinois.cs.mr.jm;

import java.io.File;
import java.io.IOException;

import edu.illinois.cs.mr.Node.NodeService;
import edu.illinois.cs.mr.te.TaskExecutorStatus;

/**
 * A JobManagerService manages the execution of jobs.
 * 
 * @author benjamin
 */
public interface JobManagerService extends NodeService {

    /**
     * Submits a job for the given job jar file and input file.
     *
     * @param jarFile
     * @param inputFile
     * @return
     * @throws IOException
     */
    JobID submitJob(File jarFile, File inputFile) throws IOException;

    JobID[] getJobIDs();
    
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
