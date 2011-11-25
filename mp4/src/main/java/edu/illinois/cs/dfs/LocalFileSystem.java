package edu.illinois.cs.dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

public class LocalFileSystem implements FileSystem {

    private final File dir;

    public LocalFileSystem(File dir) {
        this.dir = dir;
    }

    @Override
    public URL toURL(Path path) throws IOException {
        return getFile(path).toURI().toURL();
    }

    @Override
    public InputStream read(Path path) throws IOException {
        File file = getFile(path);
        return new FileInputStream(file);
    }

    @Override
    public OutputStream write(Path dest) throws IOException {
        File file = getFile(dest);
        return FileUtil.write(file);
    }

    @Override
    public void copy(Path dest, File src) throws IOException {
        File file = getFile(dest);
        FileUtil.copy(file, src);
    }

    @Override
    public void copy(Path dest, InputStream src) throws IOException {
        File file = getFile(dest);
        FileUtil.copy(file, src);
    }

    private File getFile(Path path) {
        File file = dir;
        for (String segment : path.segments())
            file = new File(file, segment);
        return file;
    }

}