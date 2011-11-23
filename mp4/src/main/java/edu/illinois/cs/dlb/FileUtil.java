package edu.illinois.cs.dlb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class FileUtil {

    private FileUtil() {
        super();
    }

    /**
     * Copies the source file to the target location. Overwrites destination if
     * present. Creates parent directories if necessary.
     * 
     * @param dest
     * @param src
     * @throws IOException
     */
    public static void copy(File dest, File src) throws IOException {
        FileInputStream is = new FileInputStream(src);
        try {
            write(dest, is);
        } finally {
            is.close();
        }
    }

    /**
     * Ensures parent directories exist and opens an output stream.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static OutputStream open(File file) throws IOException {
        ensureDirExists(file.getParentFile());
        return new FileOutputStream(file);
    }

    /**
     * Writes the given input stream to the file.
     *
     * @param file
     * @param is
     * @throws IOException
     */
    public static void write(File file, InputStream is) throws IOException {
        ensureDirExists(file.getParentFile());
        FileOutputStream os = new FileOutputStream(file);
        try {
            transfer(is, os);
        } finally {
            os.close();
        }
    }

    public static void ensureDirExists(File dir) throws IOException {
        if (dir.isDirectory())
            return;
        if (dir.exists())
            throw new IOException("Cannot create directory '" + dir.getAbsolutePath()
                + "'. File with same name already exists ");
        if (!dir.mkdirs())
            throw new IOException("Failed to create directory " + dir.getAbsolutePath());
    }

    public static void transfer(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[4096];
        int read;
        while ((read = is.read(buf)) != -1)
            os.write(buf, 0, read);
    }
}
