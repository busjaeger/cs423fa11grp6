package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

public class JobDescriptor implements Serializable {

    private static final long serialVersionUID = 2403104226863768716L;

    public static JobDescriptor read(File jobFile) throws IOException {
        JarFile jar = new JarFile(jobFile);
        try {
            // load job descriptor
            Manifest manifest = jar.getManifest();
            Attributes attrs = manifest.getMainAttributes();
            String mapperClass = getRequiredAttribute(attrs, "MapperClass");
            String combinerClass = attrs.getValue("CombinerClass");
            String reducerClass = getRequiredAttribute(attrs, "ReducerClass");
            String inputFormatClass = getRequiredAttribute(attrs, "InputFormatClass");
            String outputKeyClass = null;//getRequiredAttribute(attrs, "OutputKeyClass");
            String outputValueClass = null;//getRequiredAttribute(attrs, "OutputValueClass");

            // load job properties
            Properties properties = new Properties();
            ZipEntry entry = jar.getEntry("job.properties");
            if (entry != null) {
                InputStream is = jar.getInputStream(entry);
                try {
                    properties.load(is);
                } finally {
                    is.close();
                }
            }
            return new JobDescriptor(mapperClass, combinerClass, reducerClass, inputFormatClass, outputKeyClass,
                                     outputValueClass, properties);
        } finally {
            jar.close();
        }
    }

    private static String getRequiredAttribute(Attributes attrs, String name) {
        String value = attrs.getValue(name);
        if (value == null || value.length() == 0)
            throw new IllegalArgumentException("'" + name + "' is a required attribute");
        return value;
    }

    private final String mapperClass;
    private final String combinerClass;
    private final String reducerClass;
    private final String inputFormatClass;
    private final String outputKeyClass;
    private final String outputValueClass;
    private final Properties properties;

    public JobDescriptor(String mapperClass,
                         String combinerClass,
                         String reducerClass,
                         String inputFormatClass,
                         String outputKeyClass,
                         String outputValueClass,
                         Properties properties) {
        this.mapperClass = mapperClass;
        this.combinerClass = combinerClass;
        this.reducerClass = reducerClass;
        this.inputFormatClass = inputFormatClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
        this.properties = properties;
    }

    public String getMapperClass() {
        return mapperClass;
    }

    public String getCombinerClass() {
        return combinerClass;
    }

    public String getReducerClass() {
        return reducerClass;
    }

    public String getInputFormatClass() {
        return inputFormatClass;
    }

    public String getOutputKeyClass() {
        return outputKeyClass;
    }

    public String getOutputValueClass() {
        return outputValueClass;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "JobDescriptor [mapperClass=" + mapperClass
            + ", combinerClass="
            + combinerClass
            + ", reducerClass="
            + reducerClass
            + ", inputFormatClass="
            + inputFormatClass
            + ", outputKeyClass="
            + outputKeyClass
            + ", outputValueClass="
            + outputValueClass
            + ", properties="
            + properties
            + "]";
    }

}
