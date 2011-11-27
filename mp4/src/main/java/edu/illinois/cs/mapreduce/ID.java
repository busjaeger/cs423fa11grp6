package edu.illinois.cs.mapreduce;

import java.io.Serializable;
import java.text.NumberFormat;

public class ID implements Serializable {

    private static final long serialVersionUID = -5906669020259138592L;
    private static final NumberFormat NF = NumberFormat.getInstance();
    static {
        NF.setMinimumIntegerDigits(5);
        NF.setGroupingUsed(false);
    }

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
        return NF.format(value);
    }

}
