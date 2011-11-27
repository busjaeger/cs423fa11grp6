package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

public class Path implements Serializable {

    private static final long serialVersionUID = 994529050146607958L;

    private final String[] segments;

    public Path(String... segments) {
        this.segments = segments;
    }

    public Path(Object... segments) {
        this(toString(segments));
    }

    private static String[] toString(Object... os) {
        String[] s = new String[os.length];
        for (int i = 0; i < os.length; i++)
            s[i] = os[i].toString();
        return s;
    }

    public Iterable<String> segments() {
        return Collections.unmodifiableList(Arrays.asList(segments));
    }

    public String last() {
        return segments[segments.length - 1];
    }

    public Path beforeLast() {
        String[] newSegments = new String[segments.length - 1];
        System.arraycopy(segments, 0, newSegments, 0, segments.length - 1);
        return new Path(newSegments);
    }

    public Path append(String segmenet) {
        String[] newSegments = new String[segments.length + 1];
        System.arraycopy(segments, 0, newSegments, 0, segments.length);
        newSegments[segments.length] = segmenet;
        return new Path(newSegments);
    }

    public int length() {
        return segments.length;
    }
}
