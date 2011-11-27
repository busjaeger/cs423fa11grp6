package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

/**
 * local file system implementation. Resolves paths relative to a local
 * directory.
 * 
 * @author benjamin
 */
public class FileSystem implements FileSystemService {

    private final File dir;

    public FileSystem(File dir) throws IOException {
        FileUtil.ensureDirExists(dir);
        this.dir = dir;
    }

    public URL toURL(Path path) throws IOException {
        return resolve(path).toURI().toURL();
    }

    @Override
    public InputStream read(Path path) throws IOException {
        File file = resolve(path);
        return new FileInputStream(file);
    }

    @Override
    public OutputStream write(Path dest) throws IOException {
        File file = resolve(dest);
        return FileUtil.write(file);
    }

    @Override
    public void copy(Path dest, File src) throws IOException {
        File file = resolve(dest);
        FileUtil.copy(file, src);
    }

    @Override
    public void copy(Path dest, InputStream src) throws IOException {
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

    private File resolve(Path path) {
        File file = dir;
        for (String segment : path.segments())
            file = new File(file, segment);
        return file;
    }

}