package edu.illinois.cs.mapreduce;

import java.io.Serializable;

import edu.illinois.cs.mapreduce.Task.TaskID;

public class TaskStatus implements Serializable {

    private static final long serialVersionUID = -52586956350693369L;

    private final TaskID taskID;
    private Status status;
    private String message;

    public TaskStatus(TaskID taskID) {
        this.taskID = taskID;
        this.status = Status.CREATED;
    }

    public TaskID getTaskID() {
        return taskID;
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

}
