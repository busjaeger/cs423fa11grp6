package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.EnumSet;

public class TaskAttemptStatus implements Serializable {

    private static final long serialVersionUID = -52586956350693369L;

    private final TaskAttemptID id;
    private Status status;
    private String message;

    public TaskAttemptStatus(TaskAttemptID id) {
        this.id = id;
        this.status = Status.CREATED;
    }

    public TaskAttemptID getId() {
        return id;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public synchronized boolean isDone() {
        return EnumSet.of(Status.FAILED, Status.CANCELED, Status.SUCCEEDED).contains(status);
    }
}
