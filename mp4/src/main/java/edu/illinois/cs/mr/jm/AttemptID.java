package edu.illinois.cs.mr.jm;

import edu.illinois.cs.mr.util.ChildID;

public class AttemptID extends ChildID<TaskID, AttemptID> {

    private static final long serialVersionUID = 6020078575717454617L;

    public AttemptID(TaskID taskID, int value) {
        super(taskID, value);
    }

    @Override
    public String toString() {
        return "attempt" + super.toString();
    }
}
