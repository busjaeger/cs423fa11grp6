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

    public TaskAttemptStatus(TaskAttemptStatus status) {
        synchronized (status) {
            this.id = status.id;
            this.status = status.status;
            this.message = status.message;
        }
    }

    public TaskAttemptID getId() {
        return id;
    }

    public synchronized Status getStatus() {
        return status;
    }

    public synchronized void setStatus(Status status) {
        this.status = status;
    }

    public synchronized void setMessage(String message) {
        this.message = message;
    }

    public synchronized String getMessage() {
        return message;
    }

    public synchronized boolean isDone() {
        return EnumSet.of(Status.FAILED, Status.CANCELED, Status.SUCCEEDED).contains(status);
    }

    @Override
    public String toString() {
        return "TaskAttemptStatus [id=" + id.toQualifiedString() + ", status=" + status + ", message=" + message + "]";
    }

}
