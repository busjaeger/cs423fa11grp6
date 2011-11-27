package edu.illinois.cs.mapreduce;

public class ChildID<T extends ID> extends ID {

    private static final long serialVersionUID = -6271684130353007650L;
    protected static final char SEP = '-';

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

    public String toQualifiedString() {
        return (parentID instanceof ChildID ? ((ChildID<?>)parentID).toQualifiedString() : parentID.toString()) + SEP
            + toString();
    }

    public String toQualifiedString(int levels) {
        StringBuilder builder = new StringBuilder(toString());
        ID p = parentID;
        for (int i = 0; i < levels; i++) {
            if (p == null)
                throw new IllegalArgumentException("too many levels " + levels);
            builder.insert(0, SEP);
            builder.insert(0, p.toString());
            p = p instanceof ChildID ? ((ChildID<?>)p).parentID : null;
        }
        return builder.toString();
    }
}
