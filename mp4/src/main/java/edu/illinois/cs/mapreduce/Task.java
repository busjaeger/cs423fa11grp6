package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.illinois.cs.mapreduce.Status.State;

/**
 * A Task is an independent partition of a Job that can be scheduled on nodes on
 * the Cluster. thread safe
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
    private final Map<TaskAttemptID, TaskAttempt> attempts;
    private final TaskStatus status;

    public Task(TaskID id, Path inputPath) {
        this.id = id;
        this.inputPath = inputPath;
        this.status = new TaskStatus(id);
        this.counter = new AtomicInteger();
        this.attempts = new TreeMap<TaskAttemptID, TaskAttempt>(ID.<TaskAttemptID> getValueComparator());
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

    public synchronized void addAttempt(TaskAttempt attempt) {
        attempts.put(attempt.getId(), attempt);
        status.addAttemptStatus(attempt.getStatus());
    }

    public synchronized TaskAttempt getAttempt(TaskAttemptID attemptId) {
        return attempts.get(attemptId);
    }

    public synchronized TaskStatus getStatus() {
        return status;
    }

    public synchronized boolean updateStatus() {
        State oldState = status.getState();
        State newState = computeState();
        if (oldState != newState) {
            status.setState(newState);
            return true;
        }
        return false;
    }

    /**
     * Task state is derived from task attempt states: <br/>
     * <code>
     * CREATED   := all attempts in state {CREATED, FAILED, CANCELED}
     * WAITING   := all attempts in state {CREATED, WAITING, FAILED, CANCELED}
     * RUNNING   := all attempts in state {CREATED, WAITING, RUNNING, FAILED, CANCELED}
     * FAILED    := all attempts in state {CANCELED, FAILED} and last attempt {FAILED}
     * CANCELED  := all attempts in state {CANCELED, FAILED} and last attempt {CANCELED}
     * SUCCEEDED := one attempt in state {SUCCEEDED}
     * </code>
     * 
     * @param task
     * @return
     */
    private State computeState() {
        if (attempts.isEmpty())
            return State.CREATED;
        State last = null;
        boolean running = false, waiting = false, created = false;
        for (TaskAttempt attempt : attempts.values()) {
            State state = attempt.getStatus().getState();
            switch (state) {
                case SUCCEEDED:
                    return State.SUCCEEDED;
                case RUNNING:
                    running = true;
                    break;
                case WAITING:
                    waiting = true;
                    break;
                case CREATED:
                    created = true;
                    break;
                default:
            }
            last = state;
        }
        if (running)
            return State.RUNNING;
        if (waiting)
            return State.WAITING;
        if (created)
            return State.CREATED;
        return last;
    }
}
