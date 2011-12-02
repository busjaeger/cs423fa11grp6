package edu.illinois.cs.mr.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import edu.illinois.cs.mr.Node;
import edu.illinois.cs.mr.NodeConfiguration;
import edu.illinois.cs.mr.util.FileUtil;

/**
 * local file system implementation. Resolves paths relative to a local
 * directory.
 * 
 * @author benjamin
 */
public class FileSystem implements FileSystemService {

    private final File dir;

    public FileSystem(NodeConfiguration config) throws IOException {
        this.dir = config.fsRootDir;
        FileUtil.ensureDirExists(dir);
    }

    @Override
    public void start(Node node) {
        // nothing to do
    }

    @Override
    public void stop() {
        // nothing to do
    }

    @Override
    public InputStream read(Path path) throws IOException {
        File file = resolve(path);
        return new FileInputStream(file);
    }

    @Override
    public void write(Path dest, InputStream src) throws IOException {
        File file = resolve(dest);
        FileUtil.copy(file, src);
    }

    @Override
    public boolean mkdir(Path path) throws IOException {
        File file = resolve(path);
        return file.mkdir();
    }

    @Override
    public boolean delete(Path path) throws IOException {
        File file = resolve(path);
        return file.delete();
    }

    @Override
    public boolean exists(Path path) throws IOException {
        File file = resolve(path);
        return file.exists();
    }

    public URL toURL(Path path) throws IOException {
        return resolve(path).toURI().toURL();
    }

    public OutputStream write(Path dest) throws IOException {
        File file = resolve(dest);
        return FileUtil.write(file);
    }

    public void copy(Path dest, File src) throws IOException {
        File file = resolve(dest);
        FileUtil.copy(file, src);
    }

    private File resolve(Path path) {
        File file = dir;
        for (String segment : path.segments())
            file = new File(file, segment);
        return file;
    }

}
