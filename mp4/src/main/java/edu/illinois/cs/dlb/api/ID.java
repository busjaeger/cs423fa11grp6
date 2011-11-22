package edu.illinois.cs.dlb.api;

import java.io.Serializable;

public class ID implements Serializable {

    private static final long serialVersionUID = -648883430189232695L;

    private final int jobManagerId;
    private final int jobId;

    public ID(int jobManagerId, int jobId) {
        this.jobManagerId = jobManagerId;
        this.jobId = jobId;
    }

    public int getJobManagerId() {
        return jobManagerId;
    }

    public int getJobId() {
        return jobId;
    }

    @Override
    public String toString() {
        return jobManagerId + "_" + jobId;
    }
}
