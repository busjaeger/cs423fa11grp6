package edu.illinois.cs.dlb.api;

import java.io.Serializable;

public class Job implements Serializable {

    private static final long serialVersionUID = 2403104226863768716L;

    private final String inputFile;
    private final String jarFile;
    private final String mapperClass;
    private final String inputKeyClass;
    private final String inputValueClass;
    private final String outputKeyClass;
    private final String outputValueClass;

    public Job(String inputFile,
               String jarFile,
               String mapperClass,
               String inputKeyClass,
               String inputValueClass,
               String outputKeyClass,
               String outputValueClass) {
        super();
        this.inputFile = inputFile;
        this.jarFile = jarFile;
        this.mapperClass = mapperClass;
        this.inputKeyClass = inputKeyClass;
        this.inputValueClass = inputValueClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public String getInputFile() {
        return inputFile;
    }

    public String getJarFile() {
        return jarFile;
    }

    public String getMapperClass() {
        return mapperClass;
    }

    public String getInputKeyClass() {
        return inputKeyClass;
    }

    public String getInputValueClass() {
        return inputValueClass;
    }

    public String getOutputKeyClass() {
        return outputKeyClass;
    }

    public String getOutputValueClass() {
        return outputValueClass;
    }

    @Override
    public String toString() {
        return "Job [inputFile=" + inputFile
            + ", jarFile="
            + jarFile
            + ", mapperClass="
            + mapperClass
            + ", inputKeyClass="
            + inputKeyClass
            + ", inputValueClass="
            + inputValueClass
            + ", outputKeyClass="
            + outputKeyClass
            + ", outputValueClass="
            + outputValueClass
            + "]";
    }

}
