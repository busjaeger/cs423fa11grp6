package edu.illinois.cs.mr.util;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.illinois.cs.mr.util.Status.State;

/**
 * @author benjamin
 * @param <T>
 */
public class ImmutableStatus<T extends ID<T>> implements Serializable {

    private static final long serialVersionUID = -980418512629352542L;

    protected final T id;
    protected final State state;
    private final long createdTime;
    private final long beginWaitingTime;
    private final long beginRunningTime;
    private final long doneTime;

    public ImmutableStatus(Status<T, ?> status) {
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

    public State getState() {
        return state;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getBeginWaitingTime() {
        return beginWaitingTime;
    }

    public long getBeginRunningTime() {
        return beginRunningTime;
    }

    public long getDoneTime() {
        return doneTime;
    }

    public boolean isDone() {
        return State.isEndState(state);
    }

    protected static <I extends ID<I>, S extends Status<I, IS>, IS extends ImmutableStatus<I>> List<IS> toImmutableStatuses(Iterable<? extends S> statuses) {
        List<IS> immutableStatuses = new ArrayList<IS>();
        for (S status : statuses)
            immutableStatuses.add(status == null ? null : status.toImmutableStatus());
        return Collections.unmodifiableList(immutableStatuses);
    }

    protected static <IS extends ImmutableStatus<?>> IS[] toImmutableStatuses(Status<?, ? extends IS>[] statuses,
                                                                              Class<? extends IS> clazz) {
        @SuppressWarnings("unchecked")
        IS[] immutableStatuses = (IS[])Array.newInstance(clazz, statuses.length);
        for (int i = 0; i < statuses.length; i++)
            immutableStatuses[i] = statuses[i] == null ? null : statuses[i].toImmutableStatus();
        return immutableStatuses;
    }
}
