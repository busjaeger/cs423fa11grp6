package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.util.Comparator;

public class ID<T extends ID<T>> implements Serializable, Comparable<T> {

    private static final long serialVersionUID = -5906669020259138592L;

    protected final int value;

    protected ID(int value) {
        assert value >= 0;
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public int compareTo(T o) {
        return Integer.valueOf(value).compareTo(o.value);
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ID && ((ID<?>)obj).value == value;
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }

    public static int valueFromString(String s) {
        return Integer.parseInt(s);
    }

    public static <T extends ID<T>> Comparator<T> getValueComparator() {
        return new Comparator<T>() {
            @Override
            public int compare(T id1, T id2) {
                return Integer.valueOf(id1.getValue()).compareTo(id2.getValue());
            }
        };
    }
}
