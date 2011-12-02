package edu.illinois.cs.mr.util;



public class PhasedStatus<I extends ID<I>, IS extends ImmutableStatus<I>, E extends Enum<E>> extends Status<I, IS> {

    private static final long serialVersionUID = 1426704438341068445L;

    private final E[] phases;
    private final Status<I, IS>[] phaseStatuses;
    private E phase;

    @SuppressWarnings("unchecked")
    protected PhasedStatus(I id, Class<E> phaseEnum) {
        super(id);
        this.phases = phaseEnum.getEnumConstants();
        this.phase = phases[0];
        this.phaseStatuses = new Status[phases.length];
        this.phaseStatuses[0] = new Status<I, IS>(id);
        super.setState(State.CREATED);
    }

    public E getPhase() {
        return phase;
    }

    /**
     * Note: this method is not thread safe. A lock on the task must be held
     * while calling this method and using the iterable!
     */
    public Status<I, IS>[] getPhaseStatuses() {
        return phaseStatuses;
    }

    public synchronized Status<I, IS> getPhaseStatus(E phase) {
        return phaseStatuses[phase.ordinal()];
    }

    @Override
    public synchronized void setState(State state) {
        if (getState() != state) {
            // skip call from super constructor
            if (phase == null)
                return;
            phaseStatuses[phase.ordinal()].setState(state);
            // one phase failed/canceled > all failed/canceled
            if (state == State.FAILED || state == State.CANCELED) {
                super.setState(state);
            }
            // one phase succeeded > move to next phase or mark done
            else if (state == State.SUCCEEDED) {
                if (phase.ordinal() == phases.length - 1) {
                    super.setState(State.SUCCEEDED);
                } else {
                    int next = phase.ordinal() + 1;
                    phase = phases[next];
                    phaseStatuses[next] = new Status<I, IS>(id);
                }
            }
            // if created, waiting or running state, only relevant if first
            else if (phase.ordinal() == 0) {
                super.setState(state);
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public synchronized IS toImmutableStatus() {
        return (IS)new ImmutablePhasedStatus(this);
    }

}
