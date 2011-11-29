package edu.illinois.cs.mapreduce;

public class TaskID extends ChildID<JobID, TaskID> {
    private static final long serialVersionUID = -8143814273176351822L;

    private final boolean map;

    public TaskID(JobID jobID, int value, boolean map) {
        super(jobID, value);
        this.map = map;
    }

    public boolean isMap() {
        return map;
    }

    @Override
    public int hashCode() {
        return super.hashCode() * parentID.hashCode() * (map ? 17 : 31);
    }

    @Override
    public int compareTo(TaskID o) {
        int c = parentID.compareTo(o.parentID);
        if (c == 0)
            c = super.compareTo(o);
        if (c == 0)
            c = Boolean.valueOf(map).compareTo(o.map);
        return c;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj))
            return false;
        if (!(obj instanceof TaskID))
            return false;
        return map == ((TaskID)obj).map;
    }

    @Override
    public String toString() {
        return (map ? 'm' : 'r') + "task" + super.toString();
    }
}
