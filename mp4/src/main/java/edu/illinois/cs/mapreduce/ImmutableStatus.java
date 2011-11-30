package edu.illinois.cs.mapreduce;

import java.io.Serializable;

import edu.illinois.cs.mapreduce.Status.State;

public class ImmutableStatus<T extends ID<T>> implements Serializable {

    private static final long serialVersionUID = -980418512629352542L;

    protected final T id;
    protected final State state;

    public ImmutableStatus(T id, State state) {
        this.id = id;
        this.state = state;
    }

    public T getId() {
        return id;
    }

    public State getState() {
        return state;
    }

}
