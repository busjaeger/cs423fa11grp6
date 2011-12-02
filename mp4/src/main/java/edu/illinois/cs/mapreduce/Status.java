package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.EnumSet;

/**
 * Thread safe
 * 
 * @author benjamin
 * @param <T>
 */
public class Status<T extends ID<T>, I extends ImmutableStatus<T>> implements Serializable {

    private static final long serialVersionUID = 682178835833949654L;

    public static enum State {
        CREATED, WAITING, RUNNING, CANCELED, FAILED, SUCCEEDED
    }

    private static EnumSet<State> END_STATES = EnumSet.of(State.FAILED, State.CANCELED, State.SUCCEEDED);

    protected final T id;
    private State state;
    private long createdTime;
    private long beginWaitingTime;
    private long beginRunningTime;
    private long doneTime;

    public Status(T id) {
        this(id, State.CREATED);
    }

    private Status(T id, State state) {
        this.id = id;
        setState(state);
    }

    protected Status(Status<T, ?> status) {
        this.id = status.getId();
        this.state = status.getState();
        this.createdTime = status.getCreatedTime();
        this.beginWaitingTime = status.getBeginWaitingTime();
        this.beginRunningTime = status.getBeginRunningTime();
        this.doneTime = status.getDoneTime();
    }

    public T getId() {
        return id;
    }

    public synchronized State getState() {
        return state;
    }

    public final synchronized void setState(State state) {
        if (this.state != state) {
            this.state = state;
            long currentTime = System.currentTimeMillis();
            switch (state) {
                case CREATED:
                    createdTime = currentTime;
                    break;
                case WAITING:
                    beginWaitingTime = currentTime;
                    break;
                case RUNNING:
                    beginRunningTime = currentTime;
                    break;
                case CANCELED:
                case FAILED:
                case SUCCEEDED:
                    doneTime = currentTime;
                    break;
            }
        }
    }

    public synchronized boolean isDone() {
        return END_STATES.contains(state);
    }

    public synchronized long getCreatedTime() {
        return createdTime;
    }

    public synchronized long getBeginWaitingTime() {
        return beginWaitingTime;
    }

    public synchronized long getBeginRunningTime() {
        return beginRunningTime;
    }

    public synchronized long getDoneTime() {
        return doneTime;
    }

    @SuppressWarnings("unchecked")
    public synchronized I toImmutableStatus() {
        return (I)new ImmutableStatus<T>(this);
    }
}
