package edu.illinois.cs.dlb.api;

import java.io.Serializable;

public class JobID implements Serializable {

    private static final long serialVersionUID = -648883430189232695L;

    private final int jobManagerId;
    private final int jobId;

    public JobID(int jobManagerId, int jobId) {
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
    public int hashCode() {
        return 17 * jobManagerId + jobId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj instanceof JobID) {
            JobID other = (JobID)obj;
            return other.jobId == jobId && other.jobManagerId == jobManagerId;
        }
        return false;
    }

    @Override
    public String toString() {
        return jobManagerId + "-" + jobId;
    }
}
