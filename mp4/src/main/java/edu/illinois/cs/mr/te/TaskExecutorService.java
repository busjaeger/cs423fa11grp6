package edu.illinois.cs.mr.te;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.illinois.cs.mr.jm.AttemptID;

/**
 * Remote interface to the TaskExecutor
 * 
 * @author benjamin
 */
public interface TaskExecutorService {

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
    boolean cancel(AttemptID id, long timeout, TimeUnit unit) throws IOException, TimeoutException;

    /**
     * Must be called only after successful call to
     * {@link #cancel(AttemptID, long, TimeUnit)}
     * 
     * @param id
     * @return
     * @throws IOException
     */
    boolean delete(AttemptID id) throws IOException;

    /**
     * Throttle the threads executing tasks. The throttle value represents the
     * percentage at which the threads are to operate.
     * 
     * @param value
     * @throws IOException
     */
    void setThrottle(double value) throws IOException;

}
