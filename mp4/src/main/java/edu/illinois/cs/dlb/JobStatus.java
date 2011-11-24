package edu.illinois.cs.dlb;

import java.io.Serializable;

public class JobStatus implements Serializable {

    private static final long serialVersionUID = 387393472618054102L;

    public static enum Status {
        RUNNING, FAILED, SUCCEEDED
    }

    private Status status;

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

}
