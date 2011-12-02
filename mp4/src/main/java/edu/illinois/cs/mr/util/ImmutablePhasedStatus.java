package edu.illinois.cs.mr.util;


public class ImmutablePhasedStatus<T extends ID<T>, E extends Enum<E>> extends ImmutableStatus<T> {

    private static final long serialVersionUID = -6840770338601824409L;

    private final E phase;
    private final ImmutableStatus<T>[] phaseStatuses;

    @SuppressWarnings("unchecked")
    protected ImmutablePhasedStatus(PhasedStatus<T, ? extends ImmutablePhasedStatus<T, E>, E> status) {
        super(status);
        this.phase = status.getPhase();
        this.phaseStatuses = toImmutableStatuses(status.getPhaseStatuses(), ImmutableStatus.class);
    }
    
    public E getPhase() {
        return phase;
    }

    public ImmutableStatus<T> getPhaseStatus(E phase) {
        return phaseStatuses[phase.ordinal()];
    }

}
