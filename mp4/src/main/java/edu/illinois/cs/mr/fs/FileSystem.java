package edu.illinois.cs.mr.fs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import edu.illinois.cs.mr.Node;
import edu.illinois.cs.mr.NodeConfiguration;
import edu.illinois.cs.mr.NodeListener;
import edu.illinois.cs.mr.util.FileUtil;

/**
 * Local file system implementation. Resolves paths relative to a local
 * directory. This implementation provides several additional methods used by
 * clients using this implementation locally.
 * 
 * @author benjamin
 */
public class FileSystem implements FileSystemService, NodeListener {

    private final File dir;

    public FileSystem(NodeConfiguration config) throws IOException {
        this.dir = config.fsRootDir;
    }

    @Override
    public void start(Node node) throws IOException {
        FileUtil.ensureDirExists(dir);
    }

    @Override
    public void stop() {
        // no work to do
    }

    /**
     * @see edu.illinois.cs.mr.fs.FileSystemService#read(edu.illinois.cs.mr.fs.Path)
     */
    @Override
    public InputStream read(Path path) throws IOException {
        File file = resolve(path);
        return new BufferedInputStream(new FileInputStream(file));
    }

    /**
     * @see edu.illinois.cs.mr.fs.FileSystemService#write(edu.illinois.cs.mr.fs.Path,
     *      java.io.InputStream)
     */
    @Override
    public void write(Path dest, InputStream src) throws IOException {
        File file = resolve(dest);
        FileUtil.copy(file, src);
    }

    /**
     * @see edu.illinois.cs.mr.fs.FileSystemService#delete(edu.illinois.cs.mr.fs.Path)
     */
    @Override
    public boolean delete(Path path) throws IOException {
        File file = resolve(path);
        return file.delete();
    }

    /**
     * @see edu.illinois.cs.mr.fs.FileSystemService#exists(edu.illinois.cs.mr.fs.Path)
     */
    @Override
    public boolean exists(Path path) throws IOException {
        File file = resolve(path);
        return file.exists();
    }

    /**
     * Returns a URL for the given path
     * 
     * @param path
     * @return
     * @throws IOException
     */
    public URL toURL(Path path) throws IOException {
        return resolve(path).toURI().toURL();
    }

    /**
     * Returns an output stream to write to the file at the given path
     * 
     * @param dest must be closed by client when finished
     * @return
     * @throws IOException
     */
    public OutputStream write(Path dest) throws IOException {
        File file = resolve(dest);
        return FileUtil.write(file);
    }

    /**
     * Copies the given file to the file pointed to by the path
     * 
     * @param dest
     * @param src
     * @throws IOException
     */
    public void copy(Path dest, File src) throws IOException {
        File file = resolve(dest);
        FileUtil.copy(file, src);
    }

    /**
     * Copies the file at the given path to the specified file
     * 
     * @param dest
     * @param src
     * @throws IOException
     */
    public void copy(File dest, Path src) throws IOException {
        File srcFile = resolve(src);
        FileUtil.copy(dest, srcFile);
    }

    private File resolve(Path path) {
        File file = dir;
        for (String segment : path.segments())
            file = new File(file, segment);
        return file;
    }

}
