package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.EnumSet;

/**
 * Thread safe
 * 
 * @author benjamin
 * @param <T>
 */
public abstract class Status<T extends ID<T>, I extends ImmutableStatus<T>> implements Serializable {

    private static final long serialVersionUID = 682178835833949654L;

    public static enum State {
        CREATED, WAITING, RUNNING, CANCELED, FAILED, SUCCEEDED
    }

    private static EnumSet<State> END_STATES = EnumSet.of(State.FAILED, State.CANCELED, State.SUCCEEDED);

    protected final T id;
    protected State state;

    public Status(T id) {
        this(id, State.CREATED);
    }

    private Status(T id, State state) {
        this.id = id;
        this.state = state;
    }

    public T getId() {
        return id;
    }

    public synchronized State getState() {
        return state;
    }

    public synchronized void setState(State state) {
        this.state = state;
    }

    public synchronized boolean isDone() {
        return END_STATES.contains(state);
    }

    public abstract I toImmutableStatus();
}
