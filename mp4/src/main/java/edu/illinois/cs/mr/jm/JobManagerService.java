package edu.illinois.cs.mr.jm;

import java.io.File;
import java.io.IOException;

/**
 * A JobManagerService manages the execution of jobs.
 * 
 * @author benjamin
 */
public interface JobManagerService {

    /**
     * Creates and schedules a job for the given job executable and input file.
     * <p>
     * The jar file must describe the job using the following manifest
     * attributes:
     * <ul>
     * <li>MapperClass (mandatory): fully qualified name of the class
     * implementing the map function. The class must extend
     * {@link edu.illinois.cs.mapreduce.api.Mapper}</li>
     * <li>CombinerClass (optional): fully qualified name of the class
     * implementing the combined function. Depending on the job, this may be the
     * same as the reduce function. The class must extend
     * {@link edu.illinois.cs.mapreduce.api.Reducer}</li>
     * <li>ReducerClass (mandatory): fully qualified name of the class
     * implementing the reduce function. The class must extend
     * {@link edu.illinois.cs.mapreduce.api.Reducer}</li>
     * <li>InputFormatClass (mandatory): fully qualified name of the class
     * implementing the input format. The class must extend
     * {@link edu.illinois.cs.mapreduce.api.InputFormat}</li>
     * <li>OutputFormatClass (mandatory): fully qualified name of the class
     * implementing the output format. The class must extend
     * {@link edu.illinois.cs.mapreduce.api.OutputFormat}</li>
     * </ul>
     * Jobs without
     * </p>
     * <p>
     * Depending on the node configuration, the job may be split into several
     * tasks which may be distributed across nodes in the cluster.
     * </p>
     * 
     * @param jarFile job jar file containing the executable functions
     * @param inputFile the input file to which the functions are to be applied
     * @return
     * @throws IOException
     */
    JobID submitJob(File jarFile, File inputFile) throws IOException;

    JobID[] getJobIDs() throws IOException;

    JobStatus getJobStatus(JobID jobID) throws IOException;

    boolean writeOutput(JobID jobID, File file) throws IOException;

    /**
     * @param status
     * @param statuses must be sorted by {@link AttemptID}
     * @return
     * @throws IOException
     */
    boolean updateStatus(AttemptStatus[] statuses) throws IOException;

}
