package edu.illinois.cs.dlb;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class JobDescriptor implements Serializable {

    private static final long serialVersionUID = 2403104226863768716L;

    public static JobDescriptor read(File jobFile) throws IOException {
        JarFile jar = new JarFile(jobFile);
        try {
            Manifest manifest = jar.getManifest();
            return JobDescriptor.read(manifest);
        } finally {
            jar.close();
        }
    }

    // TODO Validate
    public static JobDescriptor read(Manifest manifest) {
        Attributes attrs = manifest.getMainAttributes();
        String mapperClass = attrs.getValue("MapperClass");
        String inputKeyClass = attrs.getValue("InputKeyClass");
        String inputValueClass = attrs.getValue("InputValueClass");
        String outputKeyClass = attrs.getValue("OutputKeyClass");
        String outputValueClass = attrs.getValue("OutputValueClass");
        return new JobDescriptor(mapperClass, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass);
    }

    private final String mapperClass;
    private final String inputKeyClass;
    private final String inputValueClass;
    private final String outputKeyClass;
    private final String outputValueClass;

    public JobDescriptor(String mapperClass,
                         String inputKeyClass,
                         String inputValueClass,
                         String outputKeyClass,
                         String outputValueClass) {
        super();
        this.mapperClass = mapperClass;
        this.inputKeyClass = inputKeyClass;
        this.inputValueClass = inputValueClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
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
        return "JobDescriptor [mapperClass=" + mapperClass
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
