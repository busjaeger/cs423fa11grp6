package edu.illinois.cs.dlb;

public class ChildID<T extends ID> extends ID {

    private static final long serialVersionUID = -6271684130353007650L;

    private static final char SEP = '-';

    protected final T parentID;

    public ChildID(T parentID, int value) {
        super(value);
        assert parentID != null;
        this.parentID = parentID;
    }

    public T getParentID() {
        return parentID;
    }

    @Override
    public int hashCode() {
        return super.hashCode() * parentID.hashCode() * 17;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj))
            return false;
        if (!(obj instanceof ChildID))
            return false;
        @SuppressWarnings("unchecked")
        ChildID<T> id = (ChildID<T>)obj;
        return id.parentID.equals(parentID);
    }

    @Override
    public String toString() {
        return parentID.toString() + SEP + super.toString();
    }

}
