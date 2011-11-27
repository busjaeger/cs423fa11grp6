package edu.illinois.cs.mapreduce;

public class TaskAttemptID extends ChildID<TaskID> {

    private static final long serialVersionUID = 6020078575717454617L;

    public TaskAttemptID(TaskID taskID, int value) {
        super(taskID, value);
    }

    @Override
    public String toString() {
        return "attempt" + super.toString();
    }
}