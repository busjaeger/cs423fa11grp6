package edu.illinois.cs.dlb;

import java.io.Serializable;

public class ID implements Serializable {

    private static final long serialVersionUID = -5906669020259138592L;

    protected final int value;

    public ID(int value) {
        assert value >= 0;
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ID && ((ID)obj).value == value;
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }
}