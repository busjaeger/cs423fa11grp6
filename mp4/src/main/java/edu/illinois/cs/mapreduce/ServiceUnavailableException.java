package edu.illinois.cs.mapreduce;

import java.io.IOException;

public class ServiceUnavailableException extends IOException {
    private static final long serialVersionUID = -4858630475547003121L;

    public ServiceUnavailableException() {
        super();
    }

    public ServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceUnavailableException(String message) {
        super(message);
    }

    public ServiceUnavailableException(Throwable cause) {
        super(cause);
    }

}
