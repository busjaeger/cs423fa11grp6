package edu.illinois.cs.mr.util;

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
        CREATED("Created"), WAITING("Waiting"), RUNNING("Running"), CANCELED("Canceled"), FAILED("Failed"), SUCCEEDED(
            "Succeeded");

        private static EnumSet<State> END_STATES = EnumSet.of(State.FAILED, State.CANCELED, State.SUCCEEDED);

        public static boolean isEndState(State state) {
            return END_STATES.contains(state);
        }

        private final String s;

        private State(String s) {
            this.s = s;
        }

        @Override
        public String toString() {
            return s;
        }
    }

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
        beginWaitingTime = beginRunningTime = doneTime = -1;
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

    public synchronized void setState(State state) {
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

    public synchronized boolean update(I status) {
        if (status.getState() != this.state) {
            this.state = status.getState();
            this.createdTime = status.getCreatedTime();
            this.beginWaitingTime = status.getBeginWaitingTime();
            this.beginRunningTime = status.getBeginRunningTime();
            this.doneTime = status.getDoneTime();
            return true;
        }
        return false;
    }

}
