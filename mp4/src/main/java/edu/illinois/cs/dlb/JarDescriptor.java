package edu.illinois.cs.dlb;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class JarDescriptor implements Serializable {

    private static final long serialVersionUID = 2403104226863768716L;

    public static JarDescriptor read(File jobFile) throws IOException {
        JarFile jar = new JarFile(jobFile);
        try {
            Manifest manifest = jar.getManifest();
            return JarDescriptor.read(manifest);
        } finally {
            jar.close();
        }
    }

    // TODO Validate
    public static JarDescriptor read(Manifest manifest) {
        Attributes attrs = manifest.getMainAttributes();
        String mapperClass = attrs.getValue("MapperClass");
        String outputKeyClass = attrs.getValue("OutputKeyClass");
        String outputValueClass = attrs.getValue("OutputValueClass");
        return new JarDescriptor(mapperClass, outputKeyClass, outputValueClass);
    }

    private final String mapperClass;
    private final String outputKeyClass;
    private final String outputValueClass;

    public JarDescriptor(String mapperClass, String outputKeyClass, String outputValueClass) {
        super();
        this.mapperClass = mapperClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public String getMapperClass() {
        return mapperClass;
    }

    public String getOutputKeyClass() {
        return outputKeyClass;
    }

    public String getOutputValueClass() {
        return outputValueClass;
    }

    @Override
    public String toString() {
        return "JarDescriptor [mapperClass=" + mapperClass
            + ", outputKeyClass="
            + outputKeyClass
            + ", outputValueClass="
            + outputValueClass
            + "]";
    }

}
