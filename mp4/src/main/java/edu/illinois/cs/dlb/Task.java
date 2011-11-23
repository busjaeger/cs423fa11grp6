package edu.illinois.cs.dlb;

import java.io.File;

public class Task {

    private final int id;
    private final File inputSplit;
    private final File outputSplit;

    public Task(int id, File inputSplit, File outputSplit) {
        this.id = id;
        this.inputSplit = inputSplit;
        this.outputSplit = outputSplit;
    }

    public int getId() {
        return id;
    }

    public File getInputSplit() {
        return inputSplit;
    }

    public File getOutputSplit() {
        return outputSplit;
    }

}