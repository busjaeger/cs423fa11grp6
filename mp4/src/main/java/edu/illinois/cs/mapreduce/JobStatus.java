package edu.illinois.cs.mapreduce;

import java.io.Serializable;

public class JobStatus implements Serializable {

    private static final long serialVersionUID = 387393472618054102L;

    private Status status;

    public JobStatus() {
        this.status = Status.CREATED;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

}