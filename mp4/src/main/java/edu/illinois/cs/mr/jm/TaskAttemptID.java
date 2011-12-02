package edu.illinois.cs.mr.jm;

import edu.illinois.cs.mr.util.ChildID;

public class TaskAttemptID extends ChildID<TaskID, TaskAttemptID> {

    private static final long serialVersionUID = 6020078575717454617L;

    public TaskAttemptID(TaskID taskID, int value) {
        super(taskID, value);
    }

    @Override
    public String toString() {
        return "attempt" + super.toString();
    }
}