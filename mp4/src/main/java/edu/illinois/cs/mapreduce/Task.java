package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Task is an independent partition of a Job that can be scheduled on nodes on
 * the Cluster. thread safe
 * 
 * @author benjamin
 */
public class Task extends Status<TaskID, TaskStatus> implements Serializable {

    private static final long serialVersionUID = -6364601903551472322L;

    private final AtomicInteger attemptCounter;
    private final Map<TaskAttemptID, TaskAttempt> attempts;

    public Task(TaskID id) {
        super(id);
        this.attemptCounter = new AtomicInteger();
        this.attempts = new TreeMap<TaskAttemptID, TaskAttempt>(ID.<TaskAttemptID> getValueComparator());
    }

    public TaskAttemptID nextAttemptID() {
        return new TaskAttemptID(id, attemptCounter.incrementAndGet());
    }

    public synchronized void addAttempt(TaskAttempt attempt) {
        attempts.put(attempt.getId(), attempt);
    }

    public synchronized TaskAttempt getAttempt(TaskAttemptID attemptId) {
        return attempts.get(attemptId);
    }

    public synchronized TaskAttempt getSuccessfulAttempt() {
        for (TaskAttempt attempt : attempts.values())
            if (attempt.getState() == State.SUCCEEDED)
                return attempt;
        return null;
    }

    /**
     * Note: this method is not thread safe. A lock on the task must be held
     * while calling this method and using the iterable!
     * 
     * @return
     */
    public Iterable<TaskAttempt> getAttempts() {
        return attempts.values();
    }

    @Override
    public synchronized TaskStatus toImmutableStatus() {
        return new TaskStatus(this);
    }

    public synchronized boolean updateStatus(TaskAttemptStatus[] statuses, int offset, int length) {
        boolean stateChange = false;
        for (int i = offset; i < offset + length; i++) {
            TaskAttemptStatus status = statuses[i];
            TaskAttempt attempt = attempts.get(status.getId());
            stateChange |= attempt.updateStatus(status);
        }
        return stateChange ? updateStatus() : stateChange;
    }

    private synchronized boolean updateStatus() {
        State newState = computeState();
        if (getState() != newState) {
            setState(newState);
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
            State state = attempt.getState();
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
