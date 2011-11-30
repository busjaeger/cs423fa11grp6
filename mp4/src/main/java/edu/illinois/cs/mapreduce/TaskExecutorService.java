package edu.illinois.cs.mapreduce;

import java.io.IOException;
import java.rmi.Remote;
import java.util.concurrent.TimeUnit;

import com.sun.corba.se.impl.orbutil.threadpool.TimeoutException;

/**
 * Remote interface to TakManager
 * 
 * @author benjamin
 */
public interface TaskExecutorService extends Remote {

    /**
     * Execute the given task attempt through this executor service
     * 
     * @param attempt
     * @throws IOException
     */
    void execute(TaskExecutorTask task) throws IOException;

    /**
     * Try to cancel the given id.
     * 
     * @param id
     * @param timeout
     * @param unit
     * @return false if task is present, but could not be canceled, i.e. the
     *         task attempt could not be moved into a done state.
     * @throws IOException
     * @throws TimeoutException
     */
    boolean cancel(TaskAttemptID id, long timeout, TimeUnit unit) throws IOException, TimeoutException;

    /**
     * Must be called only after successful call to
     * {@link #cancel(TaskAttemptID, long, TimeUnit)}
     * 
     * @param id
     * @return
     * @throws IOException
     */
    boolean delete(TaskAttemptID id) throws IOException;
    
    void setThrottle(double value) throws IOException;

}
