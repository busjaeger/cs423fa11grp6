package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Task is an independent partition of a Job that can be scheduled on nodes on
 * the Cluster.
 * 
 * @author benjamin
 */
public class Task implements Serializable {

    private static final long serialVersionUID = -6364601903551472322L;

    // immutable state
    private final TaskID id;
    private final Path inputPath;
    // mutable state
    private final AtomicInteger counter;
    private final Collection<TaskAttempt> attempts;
    private Status status;

    public Task(TaskID id, Path inputPath) {
        this.id = id;
        this.status = Status.CREATED;
        this.inputPath = inputPath;
        this.counter = new AtomicInteger();
        this.attempts = new ArrayList<TaskAttempt>();
    }

    public TaskID getId() {
        return id;
    }

    public Path getInputPath() {
        return inputPath;
    }

    public int nextAttemptID() {
        return counter.incrementAndGet();
    }

    public Collection<TaskAttempt> getAttempts() {
        return attempts;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}
