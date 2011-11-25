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

    // TODO validate
    public static JobDescriptor read(File jobFile) throws IOException {
        JarFile jar = new JarFile(jobFile);
        try {
            // load job descriptor
            Manifest manifest = jar.getManifest();
            Attributes attrs = manifest.getMainAttributes();
            String mapperClass = attrs.getValue("MapperClass");
            String inputFormatClass = attrs.getValue("InputFormatClass");
            String outputKeyClass = attrs.getValue("OutputKeyClass");
            String outputValueClass = attrs.getValue("OutputValueClass");

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
            return new JobDescriptor(mapperClass, inputFormatClass, outputKeyClass, outputValueClass, properties);
        } finally {
            jar.close();
        }
    }

    private final String mapperClass;
    private final String inputFormatClass;
    private final String outputKeyClass;
    private final String outputValueClass;
    private final Properties properties;

    public JobDescriptor(String mapperClass,
                         String inputFormatClass,
                         String outputKeyClass,
                         String outputValueClass,
                         Properties properties) {
        this.mapperClass = mapperClass;
        this.inputFormatClass = inputFormatClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
        this.properties = properties;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public String getMapperClass() {
        return mapperClass;
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
            + ", inputFormatClass="
            + inputFormatClass
            + ", outputKeyClass="
            + outputKeyClass
            + ", outputValueClass="
            + outputValueClass
            + "]";
    }

}
